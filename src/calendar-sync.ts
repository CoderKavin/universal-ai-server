/**
 * Server-side Google Calendar sync.
 * Fetches events from all calendars, stores in PostgreSQL.
 */

import { getPool } from './db'
import { getValidAccessToken, type AccountType } from './google-auth'
import crypto from 'crypto'

const CALENDAR_API = 'https://www.googleapis.com/calendar/v3'

interface GCalEvent {
  id: string
  summary?: string
  start?: { dateTime?: string; date?: string }
  end?: { dateTime?: string; date?: string }
  location?: string
  attendees?: { email: string; displayName?: string; responseStatus?: string }[]
  recurrence?: string[]
  recurringEventId?: string
  status?: string
}

export async function runCalendarSync(account: AccountType = 'primary'): Promise<{ events: number }> {
  const pool = getPool()
  const accessToken = await getValidAccessToken(account)
  const syncKey = account === 'school' ? 'calendar_school' : 'calendar'
  const obsSource = account === 'school' ? 'calendar_school' : 'calendar'

  await pool.query(`INSERT INTO sync_state (integration, last_sync_at, status, total_processed) VALUES ($1, NOW(), 'syncing', 0) ON CONFLICT (integration) DO UPDATE SET last_sync_at = NOW(), status = 'syncing'`, [syncKey])

  console.log(`[calendar-sync] Starting ${account} calendar sync...`)

  // List calendars (fall back to primary only on 403)
  let calendars: { id: string; summary: string }[]
  try {
    calendars = await listCalendars(accessToken)
    console.log(`[calendar-sync] Found ${calendars.length} calendars`)
  } catch {
    calendars = [{ id: 'primary', summary: 'Primary' }]
  }

  // Date range: 30 days ago to 14 days ahead
  const timeMin = new Date(Date.now() - 30 * 86400000).toISOString()
  const timeMax = new Date(Date.now() + 14 * 86400000).toISOString()

  // Clear old events before re-importing
  await pool.query(`DELETE FROM calendar_events`)

  let totalEvents = 0

  for (const cal of calendars) {
    const events = await fetchEvents(accessToken, cal.id, timeMin, timeMax)

    for (const event of events) {
      if (!event.id) continue

      const startTime = event.start?.dateTime ?? event.start?.date ?? ''
      const endTime = event.end?.dateTime ?? event.end?.date ?? ''
      if (!startTime) continue

      const allDay = !event.start?.dateTime ? 1 : 0
      const participants = JSON.stringify(
        (event.attendees ?? []).map(a => a.displayName ?? a.email).filter(Boolean)
      )
      const recurring = (event.recurrence?.length || event.recurringEventId) ? 1 : 0

      await pool.query(
        `INSERT INTO calendar_events (id, summary, start_time, end_time, all_day, location, participants, recurring, status, calendar_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (id) DO UPDATE SET summary=$2, start_time=$3, end_time=$4, all_day=$5, location=$6, participants=$7, recurring=$8, status=$9, calendar_name=$10`,
        [event.id, event.summary ?? '(No title)', startTime, endTime, allDay, event.location ?? null, participants, recurring, event.status ?? 'confirmed', cal.summary]
      )

      // Observation for each event
      const participantList = (event.attendees ?? []).map(a => a.displayName ?? a.email).filter(Boolean)
      await pool.query(
        `INSERT INTO observations (id, source, event_type, raw_content) VALUES ($1, $2, 'created', $3)`,
        [crypto.randomUUID(), obsSource, `${event.summary ?? '(No title)'} at ${startTime}${participantList.length > 0 ? ' with ' + participantList.slice(0, 3).join(', ') : ''}`]
      )

      totalEvents++
    }
  }

  await pool.query(`UPDATE sync_state SET status = 'complete', total_processed = $1 WHERE integration = $2`, [totalEvents, syncKey])
  console.log(`[calendar-sync] ${account} complete: ${totalEvents} events`)

  return { events: totalEvents }
}

export async function createCalendarEvent(
  summary: string,
  startTime: string,
  endTime: string,
  description?: string,
  account: AccountType = 'primary'
): Promise<string> {
  const accessToken = await getValidAccessToken(account)

  const res = await fetch(`${CALENDAR_API}/calendars/primary/events`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      summary,
      description: description ?? '',
      start: { dateTime: startTime },
      end: { dateTime: endTime }
    })
  })

  if (!res.ok) {
    const err = await res.text()
    throw new Error(`Failed to create calendar event: ${res.status} ${err}`)
  }

  const data = await res.json() as { id: string }
  return data.id
}

async function listCalendars(accessToken: string): Promise<{ id: string; summary: string }[]> {
  const res = await fetch(`${CALENDAR_API}/users/me/calendarList`, {
    headers: { Authorization: `Bearer ${accessToken}` }
  })
  if (!res.ok) throw new Error(`Calendar API error: ${res.status}`)
  const data = await res.json() as { items?: { id: string; summary?: string }[] }
  return (data.items ?? []).map(c => ({ id: c.id, summary: c.summary ?? c.id }))
}

async function fetchEvents(
  accessToken: string,
  calendarId: string,
  timeMin: string,
  timeMax: string
): Promise<GCalEvent[]> {
  const allEvents: GCalEvent[] = []
  let pageToken: string | undefined

  while (true) {
    const url = new URL(`${CALENDAR_API}/calendars/${encodeURIComponent(calendarId)}/events`)
    url.searchParams.set('timeMin', timeMin)
    url.searchParams.set('timeMax', timeMax)
    url.searchParams.set('singleEvents', 'true')
    url.searchParams.set('maxResults', '250')
    url.searchParams.set('orderBy', 'startTime')
    if (pageToken) url.searchParams.set('pageToken', pageToken)

    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${accessToken}` }
    })

    if (!res.ok) {
      console.warn(`[calendar-sync] Skipping calendar ${calendarId}: ${res.status}`)
      break
    }

    const data = await res.json() as { items?: GCalEvent[]; nextPageToken?: string }
    if (data.items) allEvents.push(...data.items)

    pageToken = data.nextPageToken
    if (!pageToken) break
  }

  return allEvents
}

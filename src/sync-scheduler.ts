/**
 * Sync scheduler — runs Gmail, Calendar, and Drive syncs on intervals.
 * Guards against concurrent syncs per service.
 */

import { runGmailSync } from './gmail-sync'
import { runCalendarSync } from './calendar-sync'
import { runDriveSync } from './drive-sync'
import { isGoogleConnected } from './google-auth'
import { getPool } from './db'

let gmailTimer: ReturnType<typeof setInterval> | null = null
let calendarTimer: ReturnType<typeof setInterval> | null = null
let driveTimer: ReturnType<typeof setInterval> | null = null

let gmailSyncing = false
let calendarSyncing = false
let driveSyncing = false

async function safeGmailSync(): Promise<void> {
  if (gmailSyncing) return
  if (!(await isGoogleConnected())) return
  gmailSyncing = true
  try {
    await runGmailSync()
  } catch (err: any) {
    console.error('[sync-scheduler] Gmail sync failed:', err.message)
  } finally {
    gmailSyncing = false
  }
}

async function safeCalendarSync(): Promise<void> {
  if (calendarSyncing) return
  if (!(await isGoogleConnected())) return
  calendarSyncing = true
  try {
    await runCalendarSync()
  } catch (err: any) {
    console.error('[sync-scheduler] Calendar sync failed:', err.message)
  } finally {
    calendarSyncing = false
  }
}

async function safeDriveSync(): Promise<void> {
  if (driveSyncing) return
  if (!(await isGoogleConnected())) return
  driveSyncing = true
  try {
    await runDriveSync()
  } catch (err: any) {
    console.error('[sync-scheduler] Drive sync failed:', err.message)
  } finally {
    driveSyncing = false
  }
}

export function startSyncScheduler(): void {
  // Stagger initial syncs to avoid thundering herd
  setTimeout(() => safeGmailSync(), 15_000)
  setTimeout(() => safeCalendarSync(), 20_000)
  setTimeout(() => safeDriveSync(), 25_000)

  // Gmail: every 10 minutes
  gmailTimer = setInterval(() => safeGmailSync(), 10 * 60 * 1000)
  // Calendar: every 15 minutes
  calendarTimer = setInterval(() => safeCalendarSync(), 15 * 60 * 1000)
  // Drive: every 15 minutes
  driveTimer = setInterval(() => safeDriveSync(), 15 * 60 * 1000)

  console.log('[sync-scheduler] Started: Gmail 10m, Calendar 15m, Drive 15m')
}

export function stopSyncScheduler(): void {
  if (gmailTimer) { clearInterval(gmailTimer); gmailTimer = null }
  if (calendarTimer) { clearInterval(calendarTimer); calendarTimer = null }
  if (driveTimer) { clearInterval(driveTimer); driveTimer = null }
}

export async function triggerSync(service?: 'gmail' | 'calendar' | 'drive'): Promise<void> {
  if (!service || service === 'gmail') safeGmailSync()
  if (!service || service === 'calendar') safeCalendarSync()
  if (!service || service === 'drive') safeDriveSync()
}

export async function getSyncStatus(): Promise<Record<string, any>> {
  const pool = getPool()
  const r = await pool.query(`SELECT integration, last_sync_at, status, total_processed FROM sync_state WHERE integration IN ('gmail', 'calendar', 'drive')`)
  const status: Record<string, any> = {}
  for (const row of r.rows) {
    status[row.integration] = {
      last_sync: row.last_sync_at,
      status: row.status,
      total: row.total_processed
    }
  }
  return status
}

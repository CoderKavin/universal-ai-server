/**
 * Routine Model + Exception Detector
 *
 * Analyzes 60 days of observations to derive behavioral routines:
 * - When does Kavin usually do X? (time-of-day patterns)
 * - What does he do on which days? (day-of-week patterns)
 * - What's his typical rhythm? (morning/afternoon/evening)
 * - What recurs with what frequency? (weekly/daily habits)
 *
 * Then watches current behavior against these routines and detects breaks.
 * This is the "You haven't touched the EE in 6 days" capability.
 *
 * Runs daily for routine extraction, continuously for exception detection.
 */

import { getPool, getSettings } from './db'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface TimeSlot {
  hour: number
  dayOfWeek: number // 0=Sun, 6=Sat
  count: number
}

interface RoutineCandidate {
  description: string
  patternType: 'daily' | 'weekly' | 'time_of_day' | 'recurring'
  timeWindow: string
  dayPattern: string
  frequency: string
  evidenceCount: number
  confidence: number
  variance: string
  source: string
  lastObserved: string
}

// ── Constants ─────────────────────────────────────────────────────

const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
const TIME_LABELS: Record<string, [number, number]> = {
  'early morning': [5, 8],
  'morning': [8, 12],
  'afternoon': [12, 17],
  'evening': [17, 21],
  'night': [21, 24],
  'late night': [0, 5]
}

// IST offset: UTC+5:30
const IST_OFFSET_HOURS = 5.5

// ── Main: extract routines from observation history ───────────────

export async function extractRoutines(): Promise<number> {
  const pool = getPool()
  const day60ago = new Date(Date.now() - 60 * 86400000).toISOString()

  console.log('[routines] Extracting routines from last 60 days...')

  const routines: RoutineCandidate[] = []

  // ── 1. App usage by time-of-day ─────────────────────────────────

  const appTimeRes = await pool.query(
    `SELECT raw_content, timestamp FROM observations
     WHERE source = 'app_activity' AND timestamp >= $1
     ORDER BY timestamp`,
    [day60ago]
  )

  const appByHour: Record<string, Record<number, number>> = {} // app → hour → count
  for (const row of appTimeRes.rows) {
    const appMatch = (row.raw_content || '').match(/Switched to:\s*(.+?)(?:\s*—|$)/)
    if (!appMatch) continue
    const app = appMatch[1].trim()
    const hour = toISTHour(new Date(row.timestamp))
    if (!appByHour[app]) appByHour[app] = {}
    appByHour[app][hour] = (appByHour[app][hour] || 0) + 1
  }

  for (const [app, hours] of Object.entries(appByHour)) {
    const total = Object.values(hours).reduce((s, c) => s + c, 0)
    if (total < 5) continue // not enough data

    // Find peak hours (top 3 consecutive hours)
    const peakHour = Object.entries(hours).sort((a, b) => b[1] - a[1])[0]
    if (!peakHour) continue

    const peak = parseInt(peakHour[0])
    const peakCount = hours[peak] || 0
    const peakPct = peakCount / total

    if (peakPct > 0.2 && peakCount >= 3) {
      const timeLabel = getTimeLabel(peak)
      routines.push({
        description: `Kavin typically uses ${app} in the ${timeLabel} (peak around ${formatHour(peak)})`,
        patternType: 'time_of_day',
        timeWindow: `${formatHour(peak)}-${formatHour((peak + 2) % 24)}`,
        dayPattern: 'daily',
        frequency: `${total} observations over 60 days`,
        evidenceCount: total,
        confidence: Math.min(0.9, 0.4 + peakPct + Math.min(0.3, total / 50)),
        variance: `Peak: ${(peakPct * 100).toFixed(0)}% of usage at ${formatHour(peak)}`,
        source: 'app_activity',
        lastObserved: appTimeRes.rows[appTimeRes.rows.length - 1]?.timestamp || ''
      })
    }
  }

  // ── 2. Browser topics by day-of-week ────────────────────────────

  const browserDayRes = await pool.query(
    `SELECT raw_content, timestamp FROM observations
     WHERE source = 'browser' AND timestamp >= $1`,
    [day60ago]
  )

  const domainByDay: Record<string, Record<number, number>> = {} // domain → dayOfWeek → count
  for (const row of browserDayRes.rows) {
    const domainMatch = (row.raw_content || '').match(/—\s*([^\s]+)$/)
    if (!domainMatch) continue
    const domain = domainMatch[1].replace(/^www\./, '').split('/')[0]
    const date = new Date(row.timestamp)
    const day = toISTDay(date)
    if (!domainByDay[domain]) domainByDay[domain] = {}
    domainByDay[domain][day] = (domainByDay[domain][day] || 0) + 1
  }

  for (const [domain, days] of Object.entries(domainByDay)) {
    const total = Object.values(days).reduce((s, c) => s + c, 0)
    if (total < 10) continue

    // Find if there's a strong day-of-week bias
    const entries = Object.entries(days).map(([d, c]) => ({ day: parseInt(d), count: c }))
    entries.sort((a, b) => b.count - a.count)
    const topDay = entries[0]
    const topDayPct = topDay.count / total

    if (topDayPct > 0.25 && topDay.count >= 5) {
      routines.push({
        description: `Kavin visits ${domain} most on ${DAYS[topDay.day]}s (${topDay.count} of ${total} visits)`,
        patternType: 'weekly',
        timeWindow: '',
        dayPattern: DAYS[topDay.day],
        frequency: `${total} visits over 60 days`,
        evidenceCount: total,
        confidence: Math.min(0.85, 0.3 + topDayPct + Math.min(0.2, total / 100)),
        variance: `${(topDayPct * 100).toFixed(0)}% on ${DAYS[topDay.day]}`,
        source: 'browser',
        lastObserved: browserDayRes.rows[browserDayRes.rows.length - 1]?.timestamp || ''
      })
    }
  }

  // ── 3. WhatsApp activity patterns ───────────────────────────────

  const waTimeRes = await pool.query(
    `SELECT timestamp FROM observations
     WHERE source = 'whatsapp' AND event_type = 'sent' AND timestamp >= $1`,
    [day60ago]
  )

  if (waTimeRes.rows.length >= 10) {
    const waByHour: Record<number, number> = {}
    for (const row of waTimeRes.rows) {
      const hour = toISTHour(new Date(row.timestamp))
      waByHour[hour] = (waByHour[hour] || 0) + 1
    }

    // Find silent hours (zero messages consistently)
    const totalWa = waTimeRes.rows.length
    const activeHours = Object.keys(waByHour).map(Number)
    const silentHours: number[] = []
    for (let h = 0; h < 24; h++) {
      if (!waByHour[h] || waByHour[h] < totalWa * 0.01) silentHours.push(h)
    }

    // Find consecutive silent blocks during waking hours (7am-11pm)
    const wakingSilent = silentHours.filter(h => h >= 7 && h <= 23)
    if (wakingSilent.length >= 2) {
      // Group consecutive
      const blocks: number[][] = []
      let current = [wakingSilent[0]]
      for (let i = 1; i < wakingSilent.length; i++) {
        if (wakingSilent[i] === current[current.length - 1] + 1) {
          current.push(wakingSilent[i])
        } else {
          if (current.length >= 2) blocks.push(current)
          current = [wakingSilent[i]]
        }
      }
      if (current.length >= 2) blocks.push(current)

      for (const block of blocks) {
        routines.push({
          description: `Kavin goes silent on WhatsApp between ${formatHour(block[0])}-${formatHour(block[block.length - 1] + 1)} (possible tuition/focus time)`,
          patternType: 'daily',
          timeWindow: `${formatHour(block[0])}-${formatHour(block[block.length - 1] + 1)}`,
          dayPattern: 'daily',
          frequency: `Consistent pattern across ${totalWa} messages`,
          evidenceCount: totalWa,
          confidence: 0.6,
          variance: 'Silent during these hours in 60-day window',
          source: 'whatsapp',
          lastObserved: waTimeRes.rows[waTimeRes.rows.length - 1]?.timestamp || ''
        })
      }
    }

    // Find peak WhatsApp hour
    const peakWaHour = Object.entries(waByHour).sort((a, b) => b[1] - a[1])[0]
    if (peakWaHour) {
      const h = parseInt(peakWaHour[0])
      const peakCount = Number(peakWaHour[1])
      routines.push({
        description: `Kavin is most active on WhatsApp around ${formatHour(h)} (${peakCount} of ${totalWa} messages)`,
        patternType: 'time_of_day',
        timeWindow: formatHour(h),
        dayPattern: 'daily',
        frequency: `${totalWa} messages over 60 days`,
        evidenceCount: peakCount,
        confidence: 0.7,
        variance: `${((peakCount / totalWa) * 100).toFixed(0)}% of messages at this hour`,
        source: 'whatsapp',
        lastObserved: waTimeRes.rows[waTimeRes.rows.length - 1]?.timestamp || ''
      })
    }
  }

  // ── 4. Email patterns ───────────────────────────────────────────

  const emailTimeRes = await pool.query(
    `SELECT event_type, timestamp FROM observations
     WHERE source = 'email' AND timestamp >= $1`,
    [day60ago]
  )

  if (emailTimeRes.rows.length >= 10) {
    const sent = emailTimeRes.rows.filter((r: any) => r.event_type === 'sent')
    const received = emailTimeRes.rows.filter((r: any) => r.event_type === 'received')

    // Find when Kavin checks email (first email interaction per day)
    const dailyFirst: Record<string, number> = {} // dateStr → hour
    for (const row of emailTimeRes.rows) {
      const date = new Date(row.timestamp)
      const dateStr = toISTDateStr(date)
      const hour = toISTHour(date)
      if (!dailyFirst[dateStr] || hour < dailyFirst[dateStr]) {
        dailyFirst[dateStr] = hour
      }
    }

    const firstHours = Object.values(dailyFirst)
    if (firstHours.length >= 5) {
      const avgFirst = Math.round(firstHours.reduce((s, h) => s + h, 0) / firstHours.length)
      const variance = Math.round(Math.sqrt(firstHours.reduce((s, h) => s + (h - avgFirst) ** 2, 0) / firstHours.length) * 10) / 10

      routines.push({
        description: `Kavin typically checks email first around ${formatHour(avgFirst)} (±${variance}h variance)`,
        patternType: 'daily',
        timeWindow: formatHour(avgFirst),
        dayPattern: 'daily',
        frequency: `${firstHours.length} days observed`,
        evidenceCount: firstHours.length,
        confidence: variance < 2 ? 0.8 : 0.5,
        variance: `${variance}h standard deviation`,
        source: 'email',
        lastObserved: emailTimeRes.rows[emailTimeRes.rows.length - 1]?.timestamp || ''
      })
    }
  }

  // ── 5. Screen capture / work session patterns ───────────────────

  const screenRes = await pool.query(
    `SELECT raw_content, timestamp FROM observations
     WHERE source = 'screen_capture' AND timestamp >= $1
     AND raw_content LIKE '%App:%'`,
    [day60ago]
  )

  // Track which "work apps" appear at which times
  const workApps = new Set(['Terminal', 'Visual Studio Code', 'Xcode', 'IntelliJ IDEA', 'WebStorm', 'Cursor'])
  const studyApps = new Set(['Google Docs', 'Microsoft Word', 'Notion', 'Obsidian', 'Google Chrome'])

  const workByHour: Record<number, number> = {}
  for (const row of screenRes.rows) {
    const appMatch = (row.raw_content || '').match(/App:\s*(.+?)(?:\n|$)/)
    if (!appMatch) continue
    const app = appMatch[1].trim()
    if (workApps.has(app) || (studyApps.has(app) && (row.raw_content || '').toLowerCase().includes('essay'))) {
      const hour = toISTHour(new Date(row.timestamp))
      workByHour[hour] = (workByHour[hour] || 0) + 1
    }
  }

  const workTotal = Object.values(workByHour).reduce((s, c) => s + c, 0)
  if (workTotal >= 5) {
    const peakWork = Object.entries(workByHour).sort((a, b) => b[1] - a[1])[0]
    if (peakWork) {
      const h = parseInt(peakWork[0])
      routines.push({
        description: `Kavin's peak coding/work time is around ${formatHour(h)}-${formatHour((h + 2) % 24)}`,
        patternType: 'time_of_day',
        timeWindow: `${formatHour(h)}-${formatHour((h + 2) % 24)}`,
        dayPattern: 'daily',
        frequency: `${workTotal} work sessions in 60 days`,
        evidenceCount: workTotal,
        confidence: 0.65,
        variance: `${Number(peakWork[1])} of ${workTotal} sessions at peak hour`,
        source: 'screen_capture',
        lastObserved: screenRes.rows[screenRes.rows.length - 1]?.timestamp || ''
      })
    }
  }

  // ── 6. Centrum task patterns ────────────────────────────────────

  const centrumRes = await pool.query(
    `SELECT raw_content, timestamp FROM observations
     WHERE source = 'centrum' AND timestamp >= $1`,
    [day60ago]
  )

  if (centrumRes.rows.length >= 3) {
    // Check for regular board activity
    const centrumDays = new Set<string>()
    for (const row of centrumRes.rows) {
      centrumDays.add(toISTDateStr(new Date(row.timestamp)))
    }

    if (centrumDays.size >= 3) {
      routines.push({
        description: `Kavin uses Centrum for task management (active on ${centrumDays.size} of last 60 days)`,
        patternType: 'recurring',
        timeWindow: '',
        dayPattern: `${centrumDays.size} days active`,
        frequency: `${centrumRes.rows.length} total interactions`,
        evidenceCount: centrumRes.rows.length,
        confidence: 0.7,
        variance: '',
        source: 'centrum',
        lastObserved: centrumRes.rows[centrumRes.rows.length - 1]?.timestamp || ''
      })
    }
  }

  // ── 7. Overall daily activity volume pattern ────────────────────

  const allByHour: Record<number, number> = {}
  const hourRes = await pool.query(
    `SELECT EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Asia/Kolkata')::int as h, COUNT(*)::int as c
     FROM observations WHERE timestamp >= $1
     GROUP BY h ORDER BY h`,
    [day60ago]
  )

  for (const row of hourRes.rows) {
    allByHour[row.h] = row.c
  }

  const totalAll = Object.values(allByHour).reduce((s, c) => s + c, 0)
  if (totalAll > 100) {
    // Find wake/sleep boundary
    const sortedHours = Object.entries(allByHour).sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
    let wakeHour = 7, sleepHour = 23
    for (const [h, c] of sortedHours) {
      const hour = parseInt(h)
      if (hour >= 5 && hour <= 10 && c > totalAll * 0.02) { wakeHour = hour; break }
    }
    for (let i = sortedHours.length - 1; i >= 0; i--) {
      const hour = parseInt(sortedHours[i][0])
      if (hour >= 20 && sortedHours[i][1] > totalAll * 0.02) { sleepHour = hour + 1; break }
    }

    routines.push({
      description: `Kavin is typically digitally active from ${formatHour(wakeHour)} to ${formatHour(sleepHour)} IST`,
      patternType: 'daily',
      timeWindow: `${formatHour(wakeHour)}-${formatHour(sleepHour)}`,
      dayPattern: 'daily',
      frequency: `${totalAll} observations over 60 days`,
      evidenceCount: totalAll,
      confidence: 0.85,
      variance: `Active window: ${sleepHour - wakeHour} hours`,
      source: 'all',
      lastObserved: new Date().toISOString()
    })
  }

  // ── Store routines ──────────────────────────────────────────────

  // Deactivate old routines
  await pool.query("UPDATE routines SET active = false WHERE active = true")

  let stored = 0
  for (const r of routines) {
    const id = `routine-${crypto.randomUUID().slice(0, 8)}`
    await pool.query(
      `INSERT INTO routines (id, description, pattern_type, time_window, day_pattern, frequency,
        evidence_count, confidence, variance, source, last_observed, active)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true)`,
      [id, r.description, r.patternType, r.timeWindow, r.dayPattern, r.frequency,
        r.evidenceCount, r.confidence, r.variance, r.source, r.lastObserved]
    )
    stored++
  }

  console.log(`[routines] Extracted ${stored} routines from ${totalAll || 0} observations`)
  return stored
}

// ── Exception detection: check for routine breaks ─────────────────

export async function detectRoutineBreaks(): Promise<number> {
  const pool = getPool()
  const now = new Date()
  const day7ago = new Date(now.getTime() - 7 * 86400000).toISOString()
  const day1ago = new Date(now.getTime() - 1 * 86400000).toISOString()

  // Load active routines
  const routinesRes = await pool.query(
    "SELECT * FROM routines WHERE active = true AND confidence >= 0.5"
  )

  if (routinesRes.rows.length === 0) return 0

  let breaks = 0

  for (const routine of routinesRes.rows) {
    const source = routine.source
    const lastObserved = routine.last_observed ? new Date(routine.last_observed) : null

    // Check if this routine's source has recent activity
    let recentCount = 0
    if (source && source !== 'all') {
      const res = await pool.query(
        "SELECT COUNT(*)::int as c FROM observations WHERE source = $1 AND timestamp >= $2",
        [source, day7ago]
      )
      recentCount = res.rows[0]?.c || 0
    }

    // For time-of-day routines: check if the expected hour had activity recently
    if (routine.pattern_type === 'time_of_day' && routine.time_window) {
      const hourMatch = routine.time_window.match(/(\d+)/)
      if (hourMatch) {
        const expectedHour = parseInt(hourMatch[1])
        const hourRes = await pool.query(
          `SELECT COUNT(*)::int as c FROM observations
           WHERE source = $1 AND timestamp >= $2
           AND EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Asia/Kolkata') BETWEEN $3 AND $4`,
          [source === 'all' ? 'browser' : source, day7ago, expectedHour - 1, expectedHour + 1]
        )
        const hourCount = hourRes.rows[0]?.c || 0

        // Expected: at least 1 per day in this time window
        if (hourCount < 2 && routine.evidence_count > 10) {
          // Routine is broken: expected activity at this time, didn't happen
          const daysBroken = lastObserved
            ? Math.round((now.getTime() - lastObserved.getTime()) / 86400000)
            : 7

          if (daysBroken >= 2) {
            await insertRoutineBreak(
              routine.id,
              `Routine break: ${routine.description}`,
              `Expected ${source} activity around ${routine.time_window}`,
              `Only ${hourCount} observations in the last 7 days at this time (expected ~7+)`,
              daysBroken >= 5 ? 'high' : daysBroken >= 3 ? 'medium' : 'low',
              daysBroken
            )
            breaks++
          }
        }
      }
    }

    // For recurring routines: check if the source went silent
    if (routine.pattern_type === 'recurring' || routine.pattern_type === 'daily') {
      if (source && source !== 'all' && recentCount === 0 && routine.evidence_count > 5) {
        const daysBroken = lastObserved
          ? Math.round((now.getTime() - lastObserved.getTime()) / 86400000)
          : 7

        if (daysBroken >= 3) {
          await insertRoutineBreak(
            routine.id,
            `Routine break: ${routine.description}`,
            `Expected regular ${source} activity`,
            `No ${source} observations in the last 7 days (normally active)`,
            daysBroken >= 5 ? 'high' : 'medium',
            daysBroken
          )
          breaks++
        }
      }
    }

    // For weekly patterns: check if expected day had activity
    if (routine.pattern_type === 'weekly' && routine.day_pattern) {
      const dayName = routine.day_pattern
      const dayNum = DAYS.indexOf(dayName)
      if (dayNum >= 0) {
        // Check if last week had activity on this day
        const lastWeekDay = new Date(now)
        lastWeekDay.setDate(lastWeekDay.getDate() - ((now.getDay() - dayNum + 7) % 7 || 7))
        const dayStart = new Date(lastWeekDay.setHours(0, 0, 0, 0)).toISOString()
        const dayEnd = new Date(lastWeekDay.setHours(23, 59, 59, 999)).toISOString()

        const dayRes = await pool.query(
          "SELECT COUNT(*)::int as c FROM observations WHERE source = $1 AND timestamp >= $2 AND timestamp <= $3",
          [source, dayStart, dayEnd]
        )

        if (dayRes.rows[0]?.c === 0 && routine.evidence_count > 5) {
          await insertRoutineBreak(
            routine.id,
            `Weekly routine missed: ${routine.description}`,
            `Expected ${source} activity on ${dayName}`,
            `No activity last ${dayName}`,
            'low',
            1
          )
          breaks++
        }
      }
    }
  }

  if (breaks > 0) {
    console.log(`[routines] Detected ${breaks} routine breaks`)
  }
  return breaks
}

async function insertRoutineBreak(
  routineId: string,
  description: string,
  expected: string,
  actual: string,
  severity: string,
  daysBroken: number
): Promise<void> {
  const pool = getPool()
  const id = crypto.randomUUID()

  // Don't duplicate: check if a similar break was already logged recently
  const existing = await pool.query(
    "SELECT id FROM routine_breaks WHERE routine_id = $1 AND detected_at > NOW() - INTERVAL '24 hours'",
    [routineId]
  )
  if (existing.rows.length > 0) return

  await pool.query(
    `INSERT INTO routine_breaks (id, routine_id, description, expected_behavior, actual_behavior, severity, days_broken)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [id, routineId, description, expected, actual, severity, daysBroken]
  )

  // Emit as observation
  const { insertObservation } = await import('./db')
  await insertObservation(
    'routine_break', severity,
    `${description}\nExpected: ${expected}\nActual: ${actual}\nDays broken: ${daysBroken}`,
    { routine_id: routineId, severity, days_broken: daysBroken },
    severity === 'high' ? 'significant' : 'neutral',
    [], ''
  )
}

// ── Get routines and breaks for context injection ─────────────────

export async function getActiveRoutines(): Promise<any[]> {
  const pool = getPool()
  const r = await pool.query(
    "SELECT description, confidence, pattern_type, time_window FROM routines WHERE active = true AND confidence >= 0.5 ORDER BY confidence DESC LIMIT 15"
  )
  return r.rows
}

export async function getRecentBreaks(limit: number = 5): Promise<any[]> {
  const pool = getPool()
  const r = await pool.query(
    "SELECT description, expected_behavior, actual_behavior, severity, days_broken, detected_at FROM routine_breaks ORDER BY detected_at DESC LIMIT $1",
    [limit]
  )
  return r.rows
}

export function formatRoutinesForContext(routines: any[], breaks: any[]): string {
  const parts: string[] = []

  if (routines.length > 0) {
    const routineLines = routines.slice(0, 8).map((r: any) =>
      `- ${r.description} (${Math.round(r.confidence * 100)}% confidence)`
    )
    parts.push(`KNOWN ROUTINES:\n${routineLines.join('\n')}`)
  }

  if (breaks.length > 0) {
    const breakLines = breaks.map((b: any) => {
      const severity = b.severity === 'high' ? '⚠️' : b.severity === 'medium' ? '⚡' : '•'
      return `${severity} ${b.description} (${b.days_broken}d break, ${b.severity})`
    })
    parts.push(`ROUTINE BREAKS (unusual behavior):\n${breakLines.join('\n')}`)
  }

  return parts.join('\n\n')
}

// ── Helpers ───────────────────────────────────────────────────────

function toISTHour(date: Date): number {
  const utcHour = date.getUTCHours()
  const utcMin = date.getUTCMinutes()
  return Math.floor((utcHour + IST_OFFSET_HOURS + (utcMin >= 30 ? 0 : 0)) % 24)
}

function toISTDay(date: Date): number {
  const ist = new Date(date.getTime() + IST_OFFSET_HOURS * 3600000)
  return ist.getUTCDay()
}

function toISTDateStr(date: Date): string {
  const ist = new Date(date.getTime() + IST_OFFSET_HOURS * 3600000)
  return ist.toISOString().slice(0, 10)
}

function formatHour(h: number): string {
  h = ((h % 24) + 24) % 24
  if (h === 0) return '12am'
  if (h === 12) return '12pm'
  return h < 12 ? `${h}am` : `${h - 12}pm`
}

function getTimeLabel(h: number): string {
  for (const [label, [start, end]] of Object.entries(TIME_LABELS)) {
    if (h >= start && h < end) return label
  }
  return 'night'
}

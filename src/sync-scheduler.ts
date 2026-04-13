/**
 * Sync scheduler — runs Gmail, Calendar, and Drive syncs on intervals.
 * Supports multiple Google accounts (primary + school).
 * Guards against concurrent syncs per service+account.
 */

import { runGmailSync } from './gmail-sync'
import { runCalendarSync } from './calendar-sync'
import { runDriveSync } from './drive-sync'
import { isGoogleConnected, getConnectedAccounts, type AccountType } from './google-auth'
import { getPool } from './db'

let gmailTimer: ReturnType<typeof setInterval> | null = null
let calendarTimer: ReturnType<typeof setInterval> | null = null
let driveTimer: ReturnType<typeof setInterval> | null = null

const syncing = new Set<string>()

async function safeSyncAll(service: 'gmail' | 'calendar' | 'drive'): Promise<void> {
  const accounts = await getConnectedAccounts()
  for (const { account } of accounts) {
    const key = `${service}:${account}`
    if (syncing.has(key)) continue
    syncing.add(key)
    try {
      if (service === 'gmail') await runGmailSync(account)
      else if (service === 'calendar') await runCalendarSync(account)
      else if (service === 'drive') await runDriveSync(account)
    } catch (err: any) {
      console.error(`[sync-scheduler] ${service} ${account} failed:`, err.message)
    } finally {
      syncing.delete(key)
    }
  }
}

async function safeSyncAccount(service: 'gmail' | 'calendar' | 'drive', account: AccountType): Promise<void> {
  const key = `${service}:${account}`
  if (syncing.has(key)) return
  if (!(await isGoogleConnected(account))) return
  syncing.add(key)
  try {
    if (service === 'gmail') await runGmailSync(account)
    else if (service === 'calendar') await runCalendarSync(account)
    else if (service === 'drive') await runDriveSync(account)
  } catch (err: any) {
    console.error(`[sync-scheduler] ${service} ${account} failed:`, err.message)
  } finally {
    syncing.delete(key)
  }
}

export function startSyncScheduler(): void {
  // Stagger initial syncs to avoid thundering herd
  setTimeout(() => safeSyncAll('gmail'), 15_000)
  setTimeout(() => safeSyncAll('calendar'), 20_000)
  setTimeout(() => safeSyncAll('drive'), 25_000)

  // Gmail: every 10 minutes (all accounts)
  gmailTimer = setInterval(() => safeSyncAll('gmail'), 10 * 60 * 1000)
  // Calendar: every 15 minutes (all accounts)
  calendarTimer = setInterval(() => safeSyncAll('calendar'), 15 * 60 * 1000)
  // Drive: every 15 minutes (all accounts)
  driveTimer = setInterval(() => safeSyncAll('drive'), 15 * 60 * 1000)

  console.log('[sync-scheduler] Started: Gmail 10m, Calendar 15m, Drive 15m (all accounts)')
}

export function stopSyncScheduler(): void {
  if (gmailTimer) { clearInterval(gmailTimer); gmailTimer = null }
  if (calendarTimer) { clearInterval(calendarTimer); calendarTimer = null }
  if (driveTimer) { clearInterval(driveTimer); driveTimer = null }
}

export async function triggerSync(service?: 'gmail' | 'calendar' | 'drive', account?: AccountType): Promise<void> {
  if (account) {
    // Sync specific account
    if (!service || service === 'gmail') safeSyncAccount('gmail', account)
    if (!service || service === 'calendar') safeSyncAccount('calendar', account)
    if (!service || service === 'drive') safeSyncAccount('drive', account)
  } else {
    // Sync all accounts
    if (!service || service === 'gmail') safeSyncAll('gmail')
    if (!service || service === 'calendar') safeSyncAll('calendar')
    if (!service || service === 'drive') safeSyncAll('drive')
  }
}

export async function getSyncStatus(): Promise<Record<string, any>> {
  const pool = getPool()
  const r = await pool.query(
    `SELECT integration, last_sync_at, status, total_processed FROM sync_state
     WHERE integration IN ('gmail', 'calendar', 'drive', 'gmail_school', 'calendar_school', 'drive_school')`
  )
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

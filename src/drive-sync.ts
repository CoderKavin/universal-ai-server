/**
 * Server-side Google Drive sync.
 * Indexes files from the user's Drive into PostgreSQL.
 */

import { getPool } from './db'
import { getValidAccessToken, type AccountType } from './google-auth'
import crypto from 'crypto'

const DRIVE_API = 'https://www.googleapis.com/drive/v3'

// Track known files in memory per account (server is long-running)
const knownFiles = new Map<string, string>() // fileId → lastModified
const knownFilesSchool = new Map<string, string>()

interface DriveFile {
  id: string
  name: string
  mimeType: string
  modifiedTime: string
  owners?: { displayName: string; emailAddress?: string }[]
  sharingUser?: { displayName: string; emailAddress?: string }
  shared?: boolean
  webViewLink?: string
  parents?: string[]
}

export async function runDriveSync(account: AccountType = 'primary'): Promise<{ files: number }> {
  const pool = getPool()
  const accessToken = await getValidAccessToken(account)
  const syncKey = account === 'school' ? 'drive_school' : 'drive'
  const obsSource = account === 'school' ? 'google_drive_school' : 'google_drive'
  const fileMap = account === 'school' ? knownFilesSchool : knownFiles

  await pool.query(`INSERT INTO sync_state (integration, last_sync_at, status, total_processed) VALUES ($1, NOW(), 'syncing', 0) ON CONFLICT (integration) DO UPDATE SET last_sync_at = NOW(), status = 'syncing'`, [syncKey])

  console.log(`[drive-sync] Starting ${account} Drive sync...`)

  try {
    const cutoff = new Date(Date.now() - 90 * 86400000).toISOString()
    const params = new URLSearchParams({
      q: `modifiedTime > '${cutoff}' and trashed = false`,
      fields: 'files(id,name,mimeType,modifiedTime,owners,sharingUser,shared,webViewLink,parents)',
      orderBy: 'modifiedTime desc',
      pageSize: '200'
    })

    const res = await fetch(`${DRIVE_API}/files?${params}`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    })

    if (!res.ok) {
      const errText = await res.text()
      if (res.status === 403) {
        console.log('[drive-sync] Drive API access denied (scope may be missing)')
        await pool.query(`UPDATE sync_state SET status = 'no_scope' WHERE integration = $1`, [syncKey])
        return { files: 0 }
      }
      throw new Error(`Drive API ${res.status}: ${errText.slice(0, 200)}`)
    }

    const data = await res.json() as { files: DriveFile[] }
    const files = data.files || []
    let newCount = 0

    for (const file of files) {
      const prevModified = fileMap.get(file.id)
      if (prevModified === file.modifiedTime) continue

      const isNew = !prevModified
      fileMap.set(file.id, file.modifiedTime)

      // Get content preview for Google Docs/Sheets/Slides
      let preview = ''
      const exportable = [
        'application/vnd.google-apps.document',
        'application/vnd.google-apps.spreadsheet',
        'application/vnd.google-apps.presentation'
      ]
      if (exportable.includes(file.mimeType)) {
        preview = await fetchDocPreview(accessToken, file.id)
      }

      const typeLabel = simplifyMimeType(file.mimeType)
      const ownerNames = (file.owners || []).map(o => o.displayName).join(', ')
      const sharedInfo = file.shared ? ' [shared]' : ''

      const summary = preview || `${typeLabel}, modified ${file.modifiedTime.slice(0, 10)}`

      // Upsert document
      await pool.query(
        `INSERT INTO documents (id, path, filename, type, summary, tags, size_bytes, last_modified)
         VALUES ($1, $2, $3, $4, $5, $6, 0, $7)
         ON CONFLICT (id) DO UPDATE SET filename=$3, type=$4, summary=$5, last_modified=$7, indexed_at=NOW()`,
        [`drive-${file.id}`, file.webViewLink || file.id, file.name, file.mimeType, summary.slice(0, 2000), JSON.stringify([typeLabel]), file.modifiedTime]
      )

      // Observation
      const content = [
        `${file.name} (${typeLabel}${sharedInfo})`,
        `Modified: ${file.modifiedTime.slice(0, 10)}`,
        ownerNames ? `Owner: ${ownerNames}` : '',
        preview ? `Preview: ${preview.slice(0, 300)}` : ''
      ].filter(Boolean).join('\n')

      await pool.query(
        `INSERT INTO observations (id, source, event_type, raw_content) VALUES ($1, $4, $2, $3)`,
        [crypto.randomUUID(), isNew ? 'created' : 'modified', content, obsSource]
      )

      newCount++
    }

    await pool.query(`UPDATE sync_state SET status = 'complete', total_processed = $1 WHERE integration = $2`, [files.length, syncKey])

    if (newCount > 0) {
      console.log(`[drive-sync] Synced ${newCount} new/modified files (${files.length} total)`)
    } else {
      console.log(`[drive-sync] No changes (${files.length} files checked)`)
    }

    return { files: files.length }
  } catch (err: any) {
    console.error('[drive-sync] Sync error:', err.message)
    await pool.query(`UPDATE sync_state SET status = 'error' WHERE integration = $1`, [syncKey])
    return { files: 0 }
  }
}

async function fetchDocPreview(accessToken: string, docId: string): Promise<string> {
  try {
    const res = await fetch(
      `${DRIVE_API}/files/${docId}/export?mimeType=text/plain`,
      { headers: { Authorization: `Bearer ${accessToken}` } }
    )
    if (!res.ok) return ''
    const text = await res.text()
    return text.split(/\s+/).slice(0, 500).join(' ').slice(0, 2000)
  } catch {
    return ''
  }
}

function simplifyMimeType(mime: string): string {
  const map: Record<string, string> = {
    'application/vnd.google-apps.document': 'Google Doc',
    'application/vnd.google-apps.spreadsheet': 'Google Sheet',
    'application/vnd.google-apps.presentation': 'Google Slides',
    'application/vnd.google-apps.folder': 'Folder',
    'application/pdf': 'PDF',
    'image/jpeg': 'Image',
    'image/png': 'Image',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'Word Doc',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'Excel',
    'text/plain': 'Text',
  }
  return map[mime] || mime.split('/').pop() || 'File'
}

/**
 * Server-side Gmail sync — fetches emails and processes them into
 * contacts, threads, and commitments directly in PostgreSQL.
 */

import { getPool } from './db'
import { getValidAccessToken, getGoogleEmail, type AccountType } from './google-auth'
import Anthropic from '@anthropic-ai/sdk'
import { claudeWithFallback } from './claude-fallback'
import crypto from 'crypto'

const GMAIL_API = 'https://gmail.googleapis.com/gmail/v1/users/me'

// ── Commitment extraction hardening ──────────────────────────────
// Parses Claude's JSON output defensively: handles truncated arrays
// (if max_tokens was still exceeded) by trimming back to the last
// complete object and closing the array.
function safeParseCommitmentArray(raw: string): any[] {
  const bracketStart = raw.indexOf('[')
  if (bracketStart < 0) return []
  let body = raw.slice(bracketStart)
  try { return JSON.parse(body) } catch {}
  // Truncated: walk backward to last complete }, then close the array.
  const lastClose = body.lastIndexOf('}')
  if (lastClose > 0) {
    const repaired = body.slice(0, lastClose + 1) + ']'
    try { return JSON.parse(repaired) } catch {}
  }
  return []
}

// Quality gate for extracted commitments. Anything that doesn't clearly
// look like a complete, human-readable commitment is dropped at INSERT
// time so garbage never reaches the overlay.
function isValidExtractedCommitment(item: any): boolean {
  if (!item || typeof item !== 'object') return false
  const desc = typeof item.description === 'string' ? item.description.trim() : ''
  if (desc.length < 10) return false
  if (desc.length > 280) return false

  // Truncation artifacts: ends with " X" (single capital) or " Xy" stub
  if (/\s[A-Z]\.?$/.test(desc)) return false
  const trailing = desc.split(/\s+/).slice(-1)[0] || ''
  const allowStubs = new Set(['I', 'a', 'A', 'in', 'on', 'at', 'to', 'of', 'by', 'is', 'it', 'am', 'pm', 'an', 'or', 'be', 'up', 'us'])
  if (/^[A-Za-z]{1,2}$/.test(trailing) && !allowStubs.has(trailing)) return false

  // Unbalanced punctuation
  const open = (desc.match(/[([]/g) || []).length
  const close = (desc.match(/[)\]]/g) || []).length
  if (open !== close) return false

  // Placeholder / null markers
  if (/\[(missing|placeholder|todo|\.\.\.|null|undefined)\]/i.test(desc)) return false
  if (/\b(null|undefined|NaN)\b/.test(desc)) return false

  return true
}

// Accept only real ISO 8601 dates; reject garbage like "next week".
function coerceISODate(due: any): string | null {
  if (due === null || due === undefined || due === '') return null
  const s = String(due).trim()
  if (!/^\d{4}-\d{2}-\d{2}/.test(s)) return null
  const d = new Date(s)
  if (Number.isNaN(d.getTime())) return null
  // Reject unix-zero placeholders
  if (s.startsWith('1970-01-01') || s.startsWith('0000-')) return null
  return s.slice(0, 10)
}

// ── Types ─────────────────────────────────────────────────────────

interface GmailMessageMeta {
  id: string
  threadId: string
  labelIds: string[]
  snippet: string
  internalDate: string
  from: string
  to: string
  cc: string
  subject: string
  date: string
}

interface ParsedAddress {
  name: string
  email: string
}

interface ThreadAccum {
  threadId: string
  subject: string
  messages: {
    id: string
    from: string
    date: string
    snippet: string
    labelIds: string[]
  }[]
  participants: Set<string>
}

// ── Main sync function ────────────────────────────────────────────

export async function runGmailSync(account: AccountType = 'primary'): Promise<{ contacts: number; threads: number; commitments: number }> {
  const pool = getPool()
  const accessToken = await getValidAccessToken(account)
  const userEmail = (await getGoogleEmail(account)) || ''
  const syncKey = account === 'school' ? 'gmail_school' : 'gmail'
  const sourceTag = account === 'school' ? 'email_school' : 'email'

  await pool.query(
    `INSERT INTO sync_state (integration, last_sync_at, status, total_processed)
     VALUES ($1, NOW(), 'syncing', 0)
     ON CONFLICT (integration) DO UPDATE SET last_sync_at = NOW(), status = 'syncing'`,
    [syncKey]
  )

  console.log(`[gmail-sync] Starting ${account} Gmail sync (${userEmail})...`)

  // Fetch emails (90 days, max 500)
  let emails: GmailMessageMeta[]
  try {
    emails = await fetchEmails(accessToken, 90, 500)
  } catch (err: any) {
    await pool.query(`UPDATE sync_state SET status = 'error' WHERE integration = $1`, [syncKey])
    throw err
  }

  console.log(`[gmail-sync] Fetched ${emails.length} emails from ${account}`)

  if (emails.length === 0) {
    await pool.query(`UPDATE sync_state SET status = 'complete', total_processed = 0 WHERE integration = $1`, [syncKey])
    return { contacts: 0, threads: 0, commitments: 0 }
  }

  // Process into contacts, threads, commitments
  const result = await processEmails(pool, emails, userEmail, sourceTag)

  await pool.query(
    `UPDATE sync_state SET status = 'complete', total_processed = $1 WHERE integration = $2`,
    [emails.length, syncKey]
  )

  console.log(
    `[gmail-sync] ${account} complete: ${result.contacts} contacts, ${result.threads} threads, ${result.commitments} commitments`
  )

  return result
}

// ── Gmail API: fetch email list + metadata ────────────────────────

async function fetchEmails(
  accessToken: string,
  days: number,
  maxMessages: number
): Promise<GmailMessageMeta[]> {
  // List message IDs
  console.log('[gmail-sync] Listing emails...')
  const messageIds: string[] = []
  let pageToken: string | undefined

  const query = `newer_than:${days}d`

  while (messageIds.length < maxMessages) {
    const url = new URL(`${GMAIL_API}/messages`)
    url.searchParams.set('q', query)
    url.searchParams.set('maxResults', '100')
    if (pageToken) url.searchParams.set('pageToken', pageToken)

    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${accessToken}` }
    })
    if (!res.ok) throw new Error(`Gmail API error: ${res.status} ${await res.text()}`)

    const data = (await res.json()) as {
      messages?: { id: string; threadId: string }[]
      nextPageToken?: string
      resultSizeEstimate?: number
    }

    if (!data.messages) break
    for (const m of data.messages) {
      if (messageIds.length >= maxMessages) break
      messageIds.push(m.id)
    }

    console.log(
      `[gmail-sync] Listed ${messageIds.length}/${data.resultSizeEstimate ?? '?'} emails`
    )

    pageToken = data.nextPageToken
    if (!pageToken) break
  }

  if (messageIds.length === 0) return []

  // Fetch each message's metadata + snippet (concurrency-limited)
  const total = messageIds.length
  const results: GmailMessageMeta[] = []
  const CONCURRENCY = 15

  for (let i = 0; i < total; i += CONCURRENCY) {
    const batch = messageIds.slice(i, i + CONCURRENCY)
    const batchResults = await Promise.all(
      batch.map((id) => fetchMessageMeta(id, accessToken))
    )
    for (const r of batchResults) {
      if (r) results.push(r)
    }
    console.log(`[gmail-sync] Fetched metadata ${results.length}/${total}`)
  }

  return results
}

async function fetchMessageMeta(
  messageId: string,
  accessToken: string
): Promise<GmailMessageMeta | null> {
  try {
    const url =
      `${GMAIL_API}/messages/${messageId}?format=metadata` +
      `&metadataHeaders=From&metadataHeaders=To&metadataHeaders=Cc` +
      `&metadataHeaders=Subject&metadataHeaders=Date`
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${accessToken}` }
    })
    if (!res.ok) return null

    const data = (await res.json()) as {
      id: string
      threadId: string
      labelIds?: string[]
      snippet?: string
      internalDate?: string
      payload?: { headers?: { name: string; value: string }[] }
    }

    const headers = data.payload?.headers ?? []
    const getHeader = (name: string) =>
      headers.find((h) => h.name.toLowerCase() === name.toLowerCase())?.value ?? ''

    return {
      id: data.id,
      threadId: data.threadId,
      labelIds: data.labelIds ?? [],
      snippet: data.snippet ?? '',
      internalDate: data.internalDate ?? '',
      from: getHeader('From'),
      to: getHeader('To'),
      cc: getHeader('Cc'),
      subject: getHeader('Subject'),
      date: getHeader('Date')
    }
  } catch {
    return null
  }
}

// ── Email processing pipeline ─────────────────────────────────────

async function processEmails(
  pool: import('pg').Pool,
  emails: GmailMessageMeta[],
  userEmail: string,
  sourceTag: string = 'email'
): Promise<{ contacts: number; threads: number; commitments: number }> {
  const userEmailLower = userEmail.toLowerCase()

  // Phase 1: Build contact frequency map and thread map
  console.log('[gmail-sync] Phase 1: Analyzing contacts & threads...')

  const contactMap = new Map<
    string,
    { name: string; sent: number; received: number; lastDate: string }
  >()
  const threadMap = new Map<string, ThreadAccum>()

  for (const email of emails) {
    const from = parseAddress(email.from)
    const toList = parseAddressList(email.to)
    const ccList = parseAddressList(email.cc)
    const date = parseEmailDate(email.date, email.internalDate)
    const isFromMe = from !== null && from.email.toLowerCase() === userEmailLower

    // Lightweight observation for each email
    try {
      await pool.query(
        `INSERT INTO observations (id, source, event_type, raw_content) VALUES ($1, $2, $3, $4)`,
        [
          crypto.randomUUID(),
          sourceTag,
          isFromMe ? 'sent' : 'received',
          `${isFromMe ? 'To' : 'From'}: ${isFromMe ? email.to : email.from}\nSubject: ${email.subject}\n${email.snippet.slice(0, 200)}`
        ]
      )
    } catch {
      // Non-critical — skip observation on conflict/error
    }

    // Accumulate thread data
    let thread = threadMap.get(email.threadId)
    if (!thread) {
      thread = {
        threadId: email.threadId,
        subject: email.subject,
        messages: [],
        participants: new Set()
      }
      threadMap.set(email.threadId, thread)
    }
    thread.messages.push({
      id: email.id,
      from: from?.email ?? '',
      date,
      snippet: email.snippet,
      labelIds: email.labelIds
    })
    if (from) thread.participants.add(from.email.toLowerCase())
    for (const a of [...toList, ...ccList]) thread.participants.add(a.email.toLowerCase())

    // Accumulate contact data
    if (isFromMe) {
      for (const recip of [...toList, ...ccList]) {
        const key = recip.email.toLowerCase()
        if (key === userEmailLower) continue
        const c = contactMap.get(key) ?? { name: recip.name || recip.email, sent: 0, received: 0, lastDate: '' }
        c.sent++
        if (date > c.lastDate) c.lastDate = date
        if (recip.name && recip.name !== recip.email) c.name = recip.name
        contactMap.set(key, c)
      }
    } else if (from) {
      const key = from.email.toLowerCase()
      const c = contactMap.get(key) ?? { name: from.name || from.email, sent: 0, received: 0, lastDate: '' }
      c.received++
      if (date > c.lastDate) c.lastDate = date
      if (from.name && from.name !== from.email) c.name = from.name
      contactMap.set(key, c)
    }
  }

  // Phase 2: Store contacts
  console.log('[gmail-sync] Phase 2: Storing contacts...')
  for (const [email, data] of contactMap) {
    await pool.query(
      `INSERT INTO contacts (id, name, email, emails_sent, emails_received, last_interaction_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (email) DO UPDATE SET
         name = COALESCE(NULLIF($2, ''), contacts.name),
         emails_sent = $4,
         emails_received = $5,
         last_interaction_at = $6`,
      [crypto.randomUUID(), data.name, email, data.sent, data.received, data.lastDate]
    )
  }

  // Phase 3: Calculate relationship scores
  console.log('[gmail-sync] Phase 3: Calculating relationship scores...')
  const now = Date.now()
  for (const [email, data] of contactMap) {
    const totalEmails = data.sent + data.received
    const frequencyScore = Math.min(totalEmails / 50, 1.0)

    const daysSinceLast = data.lastDate
      ? (now - new Date(data.lastDate).getTime()) / (1000 * 60 * 60 * 24)
      : 90
    const recencyScore = Math.exp(-daysSinceLast / 30)

    const reciprocity =
      data.sent > 0 && data.received > 0
        ? Math.min(data.sent, data.received) / Math.max(data.sent, data.received)
        : 0

    const score =
      0.35 * frequencyScore +
      0.35 * recencyScore +
      0.30 * reciprocity

    await pool.query(
      `UPDATE contacts SET relationship_score = $1, updated_at = NOW() WHERE email = $2`,
      [Math.round(score * 100) / 100, email]
    )
  }

  // Build set of addresses the user has sent to (real correspondents)
  const addressesISentTo = new Set<string>()
  for (const [email, data] of contactMap) {
    if (data.sent > 0) addressesISentTo.add(email)
  }

  // Phase 4: Process threads
  console.log('[gmail-sync] Phase 4: Analyzing threads...')
  let threadsSaved = 0

  for (const thread of threadMap.values()) {
    thread.messages.sort((a, b) => a.date.localeCompare(b.date))
    const lastMsg = thread.messages[thread.messages.length - 1]
    if (!lastMsg) continue

    const lastFromLower = lastMsg.from.toLowerCase()
    const lastFromMe = lastFromLower === userEmailLower
    const isInInbox = lastMsg.labelIds.includes('INBOX')

    // Awaiting reply filter (strict):
    //   1. Last message is not from me
    //   2. Thread is in my inbox
    //   3. Sender is not a noreply/automated address
    //   4. I have previously sent email TO this sender
    //   5. Thread has at least one message from me (bidirectional)
    let awaitingReply = false
    if (!lastFromMe && isInInbox) {
      const senderIsReal = !isAutomatedAddress(lastFromLower)
      const iCorrespondWithThem = addressesISentTo.has(lastFromLower)
      const iParticipatedInThread = thread.messages.some(
        (m) => m.from.toLowerCase() === userEmailLower
      )
      awaitingReply = senderIsReal && iCorrespondWithThem && iParticipatedInThread
    }

    // Stale: I sent the last message and it's been > 3 days with no reply
    let staleDays = 0
    if (lastFromMe && lastMsg.date) {
      staleDays = Math.floor(
        (now - new Date(lastMsg.date).getTime()) / (1000 * 60 * 60 * 24)
      )
    }

    const participants = Array.from(thread.participants).filter(
      (p) => p !== userEmailLower
    )

    await pool.query(
      `INSERT INTO email_threads (thread_id, subject, last_message_from, last_message_date, awaiting_reply, stale_days, participants, message_count)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (thread_id) DO UPDATE SET
         subject = $2,
         last_message_from = $3,
         last_message_date = $4,
         awaiting_reply = $5,
         stale_days = $6,
         participants = $7,
         message_count = $8`,
      [
        thread.threadId,
        thread.subject,
        lastMsg.from,
        lastMsg.date,
        awaitingReply ? 1 : 0,
        staleDays,
        JSON.stringify(participants),
        thread.messages.length
      ]
    )
    threadsSaved++
  }

  // Phase 5: Extract commitments using Claude API
  console.log('[gmail-sync] Phase 5: Extracting commitments with AI...')
  let commitmentsFound = 0

  const apiKeyRes = await pool.query(`SELECT value FROM settings WHERE key = 'api_key'`)
  const modelRes = await pool.query(`SELECT value FROM settings WHERE key = 'model'`)
  const apiKey = apiKeyRes.rows[0]?.value
  const model = modelRes.rows[0]?.value || 'claude-sonnet-4-20250514'

  if (apiKey) {
    // Only analyze emails from real correspondents (people user has emailed)
    const candidateEmails = emails.filter((e) => {
      const from = parseAddress(e.from)
      if (!from) return false
      const fromLower = from.email.toLowerCase()
      return fromLower === userEmailLower || addressesISentTo.has(fromLower)
    })

    // Batch emails for Claude: 15 at a time
    const BATCH_SIZE = 15
    for (let i = 0; i < candidateEmails.length; i += BATCH_SIZE) {
      const batch = candidateEmails.slice(i, i + BATCH_SIZE)
      const batchText = batch
        .map(
          (e, idx) =>
            `[${idx + 1}] From: ${e.from}\n    Subject: ${e.subject}\n    Snippet: ${e.snippet}`
        )
        .join('\n\n')

      try {
        const client = new Anthropic({ apiKey })
        const response = await claudeWithFallback(client, {
          model,
          max_tokens: 2000,
          system: `Extract real commitments and deadlines from these emails. ONLY return genuine commitments — things someone promised to do, deadlines, or action items. Ignore marketing, newsletters, verification codes, receipts, and generic CTAs like "click here" or "take our survey".

Each description must be a COMPLETE, self-contained sentence under 200 chars. Never truncate mid-word or mid-phrase. If you cannot describe a commitment fully, omit it entirely — do not output a partial.

Dates must be ISO 8601 (YYYY-MM-DD) or null. Never output relative dates like "Wednesday" or "next week".

Return JSON array:
[{"email_index": 1, "description": "what was committed", "by": "who committed", "to": "for whom", "due": "YYYY-MM-DD or null"}]

If NO real commitments exist, return []. Return ONLY valid JSON.`,
          messages: [{ role: 'user', content: batchText }]
        }, 'gmail-commitments')

        const raw = response.content?.[0]?.type === 'text' ? response.content[0].text : '[]'
        const parsed = safeParseCommitmentArray(raw)
        for (const item of parsed) {
          if (!isValidExtractedCommitment(item)) continue
          const emailIdx = (item.email_index ?? 1) - 1
          if (emailIdx < 0 || emailIdx >= batch.length) continue
          const sourceEmail = batch[emailIdx]
          const dueISO = coerceISODate(item.due)

          await pool.query(
            `INSERT INTO commitments (id, description, source_type, source_ref, committed_to, committed_by, due_at, status, ai_confidence)
             VALUES ($1, $2, 'email', $3, $4, $5, $6, 'active', $7)`,
            [
              crypto.randomUUID(),
              String(item.description).trim().slice(0, 280),
              sourceEmail?.threadId ?? null,
              item.to ? String(item.to).slice(0, 200) : null,
              item.by ? String(item.by).slice(0, 200) : null,
              dueISO,
              0.8
            ]
          )
          commitmentsFound++
        }
      } catch (err: any) {
        console.error('[gmail-sync] Claude commitment extraction error:', err.message)
      }
    }
  }

  return {
    contacts: contactMap.size,
    threads: threadsSaved,
    commitments: commitmentsFound
  }
}

// ── Send / Trash (execution layer) ────────────────────────────────

/**
 * Send an email via Gmail API. Returns the sent message ID.
 */
export async function sendEmail(
  to: string,
  subject: string,
  body: string,
  account: AccountType = 'primary'
): Promise<string> {
  const accessToken = await getValidAccessToken(account)
  const userEmail = (await getGoogleEmail(account)) || 'me'

  const message = [
    `From: ${userEmail}`,
    `To: ${to}`,
    `Subject: ${subject}`,
    'Content-Type: text/plain; charset=utf-8',
    '',
    body
  ].join('\r\n')

  const raw = Buffer.from(message).toString('base64url')

  const res = await fetch(`${GMAIL_API}/messages/send`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ raw })
  })

  if (!res.ok) {
    const err = await res.text()
    throw new Error(`Failed to send email: ${res.status} ${err}`)
  }

  const data = (await res.json()) as { id: string }
  return data.id
}

/**
 * Trash a sent email (for undo). Returns true on success.
 */
export async function trashMessage(messageId: string, account: AccountType = 'primary'): Promise<boolean> {
  const accessToken = await getValidAccessToken(account)
  const res = await fetch(`${GMAIL_API}/messages/${messageId}/trash`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}` }
  })
  return res.ok
}

// ── Email address parsing ─────────────────────────────────────────

function parseAddress(raw: string): ParsedAddress | null {
  if (!raw) return null
  raw = raw.trim()

  // "Name" <email@example.com> or Name <email@example.com>
  const bracketMatch = raw.match(/^"?([^"<]*)"?\s*<([^>]+)>/)
  if (bracketMatch) {
    return { name: bracketMatch[1].trim() || bracketMatch[2], email: bracketMatch[2].trim() }
  }

  // Plain email
  if (raw.includes('@')) {
    return { name: raw, email: raw }
  }

  return null
}

function parseAddressList(raw: string): ParsedAddress[] {
  if (!raw) return []
  return raw
    .split(',')
    .map((s) => parseAddress(s.trim()))
    .filter((a): a is ParsedAddress => a !== null)
}

function parseEmailDate(dateHeader: string, internalDate: string): string {
  // Try parsing the Date header first
  if (dateHeader) {
    const d = new Date(dateHeader)
    if (!isNaN(d.getTime())) return d.toISOString()
  }
  // Fallback to internalDate (milliseconds since epoch)
  if (internalDate) {
    const d = new Date(parseInt(internalDate, 10))
    if (!isNaN(d.getTime())) return d.toISOString()
  }
  return new Date().toISOString()
}

// ── Automated address detection ───────────────────────────────────

const NOREPLY_PATTERNS = [
  /^no[-_.]?reply/i,
  /^do[-_.]?not[-_.]?reply/i,
  /^mailer[-_.]?daemon/i,
  /^postmaster@/i,
  /^bounce/i,
  /^notifications?@/i,
  /^alerts?@/i,
  /^news(letters?)?@/i,
  /^updates?@/i,
  /^info@/i,
  /^support@/i,
  /^hello@/i,
  /^team@/i,
  /^campaigns?@/i,
  /^marketing@/i,
  /^promo(tions)?@/i,
  /^digest@/i,
  /^noreply@/i,
  /^feedback@/i,
  /^receipts?@/i,
  /^billing@/i,
  /^orders?@/i,
  /^no_reply@/i,
  /^automated@/i,
  /@.*\.noreply\./i,
  /@noreply\./i,
  /@mail\./i,
  /@e\./i,
  /@txt\./i,
  /@learn\./i,
  /@discover\./i,
  /@updates\./i,
  /@adm\./i,
  /^admissions?@/i,
  /^admission@/i,
  /^enroll(ment)?@/i,
  /^summer@/i,
  /^events?@/i,
  /^communications?@/i,
  /^customer[-_.]?reviews?/i,
  /^deals@/i,
  /^offers@/i,
  /^shop@/i,
  /^store@/i,
  /^donotreply/i
]

function isAutomatedAddress(email: string): boolean {
  return NOREPLY_PATTERNS.some((p) => p.test(email))
}

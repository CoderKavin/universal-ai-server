/**
 * Whisper validation pipeline.
 *
 * Whispers are native macOS notifications — they appear in Kavin's face.
 * Every bad whisper costs trust, so nothing fires without clearing the
 * same bar the overlay clears: content quality, freshness, dedup, render.
 *
 * Unlike the overlay, whispers are triggered server-side on a 60s poll,
 * so the full pipeline runs here rather than being driven by client signals.
 */

import type { Pool } from 'pg'

// ── Types ─────────────────────────────────────────────────────────

export interface WhisperCandidate {
  id?: string
  type: string                 // 'life_event' | 'chain_cascade' | 'deadline' | 'priority_contact' | 'routine_break' | 'trend'
  severity: 'critical' | 'high' | 'normal' | 'low'
  body: string                 // user-facing body (final form — no internal labels)
  subtitle?: string            // optional single-line footer ("5m ago · Calendar")
  trigger_hash: string         // dedup key
  trigger_data?: Record<string, unknown>
  trigger_source?: {           // optional — enables source-based suppression
    email_from?: string
    source_table?: string
    source_id?: string
  }
  actions?: Array<{ label: string; action: string }>
}

export type WhisperValidation =
  | { ok: true }
  | { ok: false; reason: string }

// ── Source filters ───────────────────────────────────────────────

// Email senders that produce noise, not life signals. Automated mail
// from these sources should never trigger life events, chain risks, or
// whispers.
const AUTOMATED_SENDER_PATTERNS: RegExp[] = [
  /\bnoreply\b/i,
  /\bno-reply\b/i,
  /\bno_reply\b/i,
  /\bdonotreply\b/i,
  /\bdo-not-reply\b/i,
  /\bupdates-noreply\b/i,
  /\bnotifications?@/i,
  /\bmarketing@/i,
  /\bnewsletter@/i,
  /\balerts?@/i,
  /\bhello@/i,       // many newsletter senders
  /\bteam@/i,        // generic
  /\bsupport@/i,
  /\bbilling@/i,
  /\binfo@/i,
]

const AUTOMATED_DOMAINS = new Set([
  'linkedin.com', 'email.linkedin.com', 'e.linkedin.com',
  'medium.com', 'mail.medium.com',
  'substack.com', 'mail.substack.com',
  'mailchimp.com', 'mailchi.mp',
  'mailgun.net', 'mailgun.org',
  'sendgrid.net', 'sendgrid.com',
  'convertkit.com',
  'hubspotemail.net',
  'mktomail.com',
  'constant-contact.com', 'constantcontact.com',
  'intuit.com',           // receipts
  'stripe.com',           // receipts/payment alerts
  'googlegroups.com',     // typically mailing lists
  'youtube.com',          // YT notifications
  'uber.com',             // receipts
  'amazon.com', 'amazonses.com',
])

/**
 * Is this email sender an automated / marketing / no-reply address?
 * Accepts an email or a full "From: Name <addr@domain>" header.
 */
export function isAutomatedSender(fromHeader: string | null | undefined): boolean {
  if (!fromHeader) return false
  const s = String(fromHeader).toLowerCase()

  // Pattern match on full header
  if (AUTOMATED_SENDER_PATTERNS.some(re => re.test(s))) return true

  // Extract email address for domain check
  const addrMatch = s.match(/<([^>]+)>/) || s.match(/\b([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})\b/)
  if (!addrMatch) return false
  const addr = addrMatch[1]
  const domain = addr.split('@')[1] || ''
  if (!domain) return false

  if (AUTOMATED_DOMAINS.has(domain)) return true
  // Any subdomain of a listed automated domain
  for (const d of AUTOMATED_DOMAINS) {
    if (domain === d || domain.endsWith('.' + d)) return true
  }
  return false
}

/**
 * Extracts "From: ..." line from a raw_content blob (emails stored as
 * "From: X\nSubject: Y\n..."). Returns null if none found.
 */
export function extractFromHeader(rawContent: string | null | undefined): string | null {
  if (!rawContent) return null
  const m = String(rawContent).match(/^From:\s*(.+)$/m)
  return m ? m[1].trim() : null
}

// ── Content quality gate ─────────────────────────────────────────

// Phrases that leak internal labels into user-facing copy. Any whisper
// body starting with or containing these literals is rejected — the
// caller must format a clean, natural-language body instead.
const INTERNAL_LABELS: RegExp[] = [
  /\bchain risk:/i,
  /\blife event:/i,
  /\bnew project\/role:/i,
  /\bnew project\/role\b/i,        // even without the colon
  /\bacceptance detected:/i,
  /\brejection detected:/i,
  /\brelationship change \(/i,
  /\bmedical event:/i,
  /\bfinancial event:/i,
  /\brelationship anomaly:/i,
  /\bstale thread:/i,
  /\bsource:\s*[a-z]/i,
  /\bpattern:/i,
  /\btrigger:/i,
  /\bfrom:\s*[a-z0-9]/i,            // raw email From: header leaking in
  /\bto:\s*[a-z0-9]+@/i,
  /\bsubject:\s*re:/i,
]

// Truncation artifacts — broken punctuation, unfinished URLs, email stubs.
const TRUNCATION_ARTIFACTS: RegExp[] = [
  /[<(\[]\s*[a-z]*$/i,             // ends in <updates-norepl, (example
  /@[a-z0-9.-]*$/i,                 // unterminated @domain
  /https?:\/\/\S{0,15}$/i,          // cut-off URL
  /\s[A-Za-z]{1,2}\.?$/,            // single-letter trailing stub ("by W")
  /\.\.\.$/,                         // raw ellipsis (use natural phrasing)
]

/**
 * Validate the final user-facing content of a whisper. Returns ok:true
 * only if the body reads like a finished sentence a human would send.
 */
export function validateWhisperContent(c: Pick<WhisperCandidate, 'body' | 'subtitle' | 'type'>): WhisperValidation {
  const body = (c.body || '').trim()
  const subtitle = (c.subtitle || '').trim()
  const combined = `${body} ${subtitle}`

  if (body.length < 10) return { ok: false, reason: 'body_too_short' }
  if (body.length > 140) return { ok: false, reason: 'body_too_long' }

  const wordCount = body.split(/\s+/).filter(Boolean).length
  if (wordCount < 3) return { ok: false, reason: 'body_too_few_words' }
  if (wordCount > 40) return { ok: false, reason: 'body_too_many_words' }

  for (const re of INTERNAL_LABELS) {
    if (re.test(combined)) return { ok: false, reason: `internal_label_leaked:${re.source}` }
  }
  for (const re of TRUNCATION_ARTIFACTS) {
    if (re.test(body)) return { ok: false, reason: `truncation_artifact:${re.source}` }
  }

  const openP = (combined.match(/\(/g) || []).length
  const closeP = (combined.match(/\)/g) || []).length
  if (openP !== closeP) return { ok: false, reason: 'unbalanced_parens' }

  const openB = (combined.match(/\[/g) || []).length
  const closeB = (combined.match(/]/g) || []).length
  if (openB !== closeB) return { ok: false, reason: 'unbalanced_brackets' }

  const openAng = (combined.match(/</g) || []).length
  const closeAng = (combined.match(/>/g) || []).length
  if (openAng !== closeAng) return { ok: false, reason: 'unbalanced_angle_brackets' }

  // Raw email address in body (not a URL) is a leak of technical detail.
  if (/@[a-z0-9.-]+\.[a-z]{2,}/i.test(body) && !/https?:\/\//.test(body)) {
    return { ok: false, reason: 'raw_email_address_in_body' }
  }

  if (/\b(null|undefined|NaN|TBD|TODO|\[missing]|\[placeholder])\b/i.test(combined)) {
    return { ok: false, reason: 'placeholder_token' }
  }

  return { ok: true }
}

// ── Rate limit + cross-surface coordination ──────────────────────

export interface WhisperRateState {
  fires_today: number
  fires_this_hour: number
  last_fire_ms: number
  entity_last_fire_ms: number     // for the specific trigger_hash
}

export const WHISPER_DAILY_BUDGET = 5
export const WHISPER_HOURLY_BUDGET = 1
export const WHISPER_MIN_GAP_MS = 15 * 60 * 1000   // 15 min
export const WHISPER_PER_ENTITY_COOLDOWN_MS = 24 * 3600 * 1000   // 24h

/**
 * Fetch rate state for a given trigger_hash.
 */
export async function getWhisperRateState(pool: Pool, triggerHash: string): Promise<WhisperRateState> {
  const now = new Date()
  const today = now.toISOString().slice(0, 10)

  const r = await pool.query(
    `SELECT
       SUM(CASE WHEN fired_at::date = NOW()::date AND COALESCE(suppressed, FALSE) = FALSE THEN 1 ELSE 0 END)::int AS today,
       SUM(CASE WHEN fired_at > NOW() - INTERVAL '1 hour'  AND COALESCE(suppressed, FALSE) = FALSE THEN 1 ELSE 0 END)::int AS hour,
       MAX(CASE WHEN COALESCE(suppressed, FALSE) = FALSE THEN fired_at END)                        AS last_fire,
       MAX(CASE WHEN trigger_context = $1 AND COALESCE(suppressed, FALSE) = FALSE THEN fired_at END) AS entity_last
     FROM whisper_log
     WHERE fired_at > NOW() - INTERVAL '48 hours'`,
    [triggerHash],
  )
  const row = r.rows[0] || {}
  return {
    fires_today: Number(row.today || 0),
    fires_this_hour: Number(row.hour || 0),
    last_fire_ms: row.last_fire ? new Date(row.last_fire).getTime() : 0,
    entity_last_fire_ms: row.entity_last ? new Date(row.entity_last).getTime() : 0,
  }
}

export function checkWhisperRateLimit(
  state: WhisperRateState,
  c: Pick<WhisperCandidate, 'severity'>,
  now: number = Date.now(),
): WhisperValidation {
  const critical = c.severity === 'critical'

  if (now - state.entity_last_fire_ms < WHISPER_PER_ENTITY_COOLDOWN_MS) {
    return { ok: false, reason: 'entity_cooldown_24h' }
  }
  if (now - state.last_fire_ms < WHISPER_MIN_GAP_MS) {
    return { ok: false, reason: 'min_gap_15min_violated' }
  }
  if (!critical && state.fires_this_hour >= WHISPER_HOURLY_BUDGET) {
    return { ok: false, reason: 'hourly_budget_exhausted' }
  }
  if (!critical && state.fires_today >= WHISPER_DAILY_BUDGET) {
    return { ok: false, reason: 'daily_budget_exhausted' }
  }
  return { ok: true }
}

/**
 * Surface coordination: if the overlay is currently showing OR the corner
 * feed is visible OR spotlight is open, a whisper competes for attention
 * and should be deferred unless critical.
 */
export async function isCompetingSurfaceVisible(pool: Pool): Promise<{ busy: boolean; which?: string }> {
  try {
    const r = await pool.query(
      `SELECT surface FROM surface_state
       WHERE visible = TRUE
         AND surface IN ('overlay', 'feed', 'spotlight')
         AND last_event_at > NOW() - INTERVAL '30 seconds'
       LIMIT 1`,
    )
    if (r.rows[0]) return { busy: true, which: r.rows[0].surface }
  } catch {}
  return { busy: false }
}

// ── End-to-end validator ─────────────────────────────────────────

/**
 * Full gate that a WhisperCandidate must clear to fire. Applies source
 * filter, content quality, surface coordination, and rate limits in one
 * call. Use this from /api/whispers/check.
 */
export async function validateWhisperForFiring(
  pool: Pool,
  c: WhisperCandidate,
): Promise<WhisperValidation> {
  // 0. Source filter — automated senders are noise.
  if (c.trigger_source?.email_from && isAutomatedSender(c.trigger_source.email_from)) {
    return { ok: false, reason: 'automated_sender' }
  }

  // 1. Content quality.
  const cq = validateWhisperContent(c)
  if (!cq.ok) return cq

  // 2. Surface coordination — don't compete with visible surfaces unless critical.
  if (c.severity !== 'critical') {
    const busy = await isCompetingSurfaceVisible(pool)
    if (busy.busy) return { ok: false, reason: `surface_busy:${busy.which}` }
  }

  // 3. Rate limits.
  const rate = await getWhisperRateState(pool, c.trigger_hash)
  const rl = checkWhisperRateLimit(rate, c)
  if (!rl.ok) return rl

  return { ok: true }
}

// ── Logging ──────────────────────────────────────────────────────

export async function logWhisperSuppression(
  pool: Pool,
  c: WhisperCandidate,
  reason: string,
): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO whisper_log
         (id, type, severity, title, body, subtitle, trigger_context, trigger_data, actions,
          suppressed, suppressed_reason)
       VALUES (gen_random_uuid()::text, $1, $2, 'IRIS', $3, $4, $5, $6, '[]', TRUE, $7)`,
      [
        c.type, c.severity,
        String(c.body || '').slice(0, 200),
        String(c.subtitle || '').slice(0, 200),
        c.trigger_hash,
        JSON.stringify(c.trigger_data || {}),
        reason,
      ],
    )
  } catch (err: any) {
    console.error('[whisper-log] suppression insert failed:', err.message)
  }
}

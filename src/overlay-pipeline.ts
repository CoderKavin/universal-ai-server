/**
 * Overlay decision pipeline (10 stages).
 *
 * The contextual overlay only fires when a candidate passes every stage.
 * Silence is the default; firing is the exception. Target rate: 3-10 fires/day.
 *
 * Stages:
 *   1. Candidate generation (done by caller: matchers in index.ts)
 *   2. Quality filter            — reject garbled / truncated content
 *   3. Freshness filter          — reject stale data (long-overdue, expired, etc.)
 *   4. Duplication filter        — reject what the user already saw elsewhere
 *   5. Relevance gate            — reject anything unrelated to current moment
 *   6. Actionability gate        — reject anything the user can't act on now
 *   7. Rate limit                — enforce per-min/hr/day budgets
 *   8. Scoring                   — pick single highest-scoring candidate
 *   9. Content rendering         — enforce strict length & style rules
 *  10. Active learning           — apply personal dismissal history
 */

import crypto from 'crypto'
import type { Pool } from 'pg'

// ── Types ─────────────────────────────────────────────────────────

export interface Signals {
  ocr: string              // lowercase OCR text
  title: string            // lowercase window title
  url: string              // lowercase URL
  file: string             // lowercase file path
  app: string              // lowercase app name
  mode?: 'proactive' | 'reactive'
  typing_rate?: number     // text changes per second (client-reported, if available)
  app_switched_at?: number // ms timestamp of last app switch (client-reported, if available)
}

export type OverlayLabel =
  | 'UPCOMING' | 'OVERDUE' | 'REPLY' | 'DRAFT READY' | 'DEADLINE'
  | 'COMMITMENT' | 'PATTERN' | 'FOCUS' | 'PERSON' | 'PREP'
  | 'CONFLICT' | 'DECISION' | 'MEETING' | 'EMAIL' | 'CONTACT'
  | 'DOCUMENT' | 'CHAIN PLAN' | 'RELATIONSHIP' | 'PREDICTION'

export interface Candidate {
  matcher_name: string          // "contact" | "calendar" | ... | "predictive" | ...
  type: string                  // coarse type used by client
  label: OverlayLabel | string  // uppercase category shown on card line 1
  entity_id: string             // "email@x.com" | "commitment:<uuid>" | "thread-..." etc.
  raw_title: string             // raw proposed title (pre-render)
  raw_body: string              // raw proposed body (pre-render)
  raw_content?: string          // snippet of underlying DB row for audit
  base_confidence: number       // 0-100 from matcher
  freshness_timestamp?: string  // ISO — when underlying data was last updated
  due_date?: string | null      // ISO date if applicable
  source_table?: string         // "commitments" | "predicted_actions" | ...
  source_id?: string            // PK in source_table (for data-quality quarantine)
  button?: string               // "View" | "Reply" | "Draft" | "Done" | "Skip"
  action_type?: string          // "view" | "reply" | "use_draft" | "view_chain" | ...
  chain_hint?: string
  upgrade?: string              // matcher family ("proactive" | "predictive" | ...)
  pattern_score?: string
  suggested_action?: string     // human-readable: what would the user DO in 30s?
  critical?: boolean            // bypass per-hour/per-day budgets (not per-entity/per-2-min)
  // Pipeline-internal fields
  _score?: number
  _score_breakdown?: Record<string, number>
  _final_title?: string
  _final_body?: string
}

export interface PipelineOutcome {
  fired: Candidate | null
  suppressed: Array<{
    matcher: string; stage: number; reason: string; entity_id: string; type: string; title: string
  }>
}

// ── Constants ─────────────────────────────────────────────────────

const ALLOWED_LABELS = new Set<string>([
  'UPCOMING', 'OVERDUE', 'REPLY', 'DRAFT READY', 'DEADLINE',
  'COMMITMENT', 'PATTERN', 'FOCUS', 'PERSON', 'PREP',
  'CONFLICT', 'DECISION', 'MEETING', 'EMAIL', 'CONTACT',
  'DOCUMENT', 'CHAIN PLAN', 'RELATIONSHIP', 'PREDICTION',
])

// Short words that look like truncation stubs but are real English.
const SHORT_WORD_ALLOWLIST = new Set([
  'a', 'A', 'I', 'an', 'An', 'as', 'As', 'at', 'At', 'be', 'Be',
  'by', 'By', 'do', 'Do', 'go', 'Go', 'he', 'He', 'if', 'If',
  'in', 'In', 'is', 'Is', 'it', 'It', 'me', 'Me', 'my', 'My',
  'no', 'No', 'of', 'Of', 'on', 'On', 'or', 'Or', 'so', 'So',
  'to', 'To', 'up', 'Up', 'us', 'Us', 'we', 'We', 'am', 'Am',
  'pm', 'PM', 'PM.',
])

// User-specific allowed acronyms (safe to display raw).
const USER_ACRONYMS = new Set(['EE', 'IB', 'CAS', 'IRIS', 'HL', 'SL', 'TOK', 'EE.'])

// Apps that indicate the user is deeply focused — overlay must be extra strict.
const DEEP_FOCUS_APPS = [
  'code', 'cursor', 'xcode', 'intellij', 'pycharm', 'webstorm', 'goland',
  'vim', 'nvim', 'emacs', 'neovide', 'terminal', 'iterm', 'warp',
  'figma', 'sketch', 'blender', 'unity', 'logic', 'ableton', 'photoshop', 'illustrator',
]
// Apps where the overlay is most welcome and actionable.
const NEUTRAL_APPS = [
  'chrome', 'safari', 'firefox', 'arc', 'brave', 'edge',
  'mail', 'messages', 'whatsapp', 'slack', 'telegram', 'imessage',
  'notion', 'obsidian', 'docs', 'pages', 'word',
  'calendar', 'fantastical', 'spark',
]
// Apps that indicate active communication — messaging-type matchers especially welcome.
const MESSAGING_APPS = ['messages', 'whatsapp', 'slack', 'telegram', 'imessage', 'mail']

// Video-call apps — overlay only fires if about current participants.
const VIDEO_CALL_APPS = ['zoom', 'google meet', 'meet', 'teams', 'webex', 'facetime']

// Per-matcher daily dismissal threshold before we raise that matcher's score bar.
const LEARNING_DISMISS_THRESHOLD = 3
const LEARNING_ACT_THRESHOLD = 3

// Rate limits (ms).
const MIN_GAP_MS = 2 * 60 * 1000          // 2 minutes
const HOURLY_BUDGET = 3
const DAILY_BUDGET = 10
const PER_ENTITY_COOLDOWN_MS = 6 * 60 * 60 * 1000   // 6 hours
const PER_MATCHER_COOLDOWN_MS = 20 * 60 * 1000      // 20 minutes

// Final score threshold — below this, nothing fires.
const MIN_SCORE = 65

// ── Main entry point ─────────────────────────────────────────────

export async function runPipeline(
  pool: Pool,
  candidates: Candidate[],
  signals: Signals,
): Promise<PipelineOutcome> {
  const suppressed: PipelineOutcome['suppressed'] = []

  const suppress = (c: Candidate, stage: number, reason: string): void => {
    suppressed.push({
      matcher: c.matcher_name,
      stage,
      reason,
      entity_id: c.entity_id,
      type: c.type,
      title: c.raw_title,
    })
  }

  // Stage 2
  candidates = await stageQuality(pool, candidates, suppress)
  if (candidates.length === 0) return { fired: null, suppressed }

  // Stage 3
  candidates = stageFreshness(candidates, suppress)
  if (candidates.length === 0) return { fired: null, suppressed }

  // Stage 4
  candidates = await stageDuplication(pool, candidates, signals, suppress)
  if (candidates.length === 0) return { fired: null, suppressed }

  // Stage 5
  candidates = await stageRelevance(pool, candidates, signals, suppress)
  if (candidates.length === 0) return { fired: null, suppressed }

  // Stage 6
  candidates = stageActionability(candidates, signals, suppress)
  if (candidates.length === 0) return { fired: null, suppressed }

  // Stage 7
  candidates = await stageRateLimit(pool, candidates, suppress)
  if (candidates.length === 0) return { fired: null, suppressed }

  // Stage 8 + Stage 10
  let best = await stageScoreAndLearn(pool, candidates, signals, suppress)
  if (!best) return { fired: null, suppressed }

  // Stage 9
  const rendered = stageRender(best)
  if (!rendered.ok) {
    suppress(best, 9, rendered.reason)
    return { fired: null, suppressed }
  }

  return { fired: rendered.candidate, suppressed }
}

// ── Stage 2: Quality filter ──────────────────────────────────────

const TRUNC_END_SINGLE_LETTER = /\s[A-Za-z]\.?$/            // "commit by W"
const TRUNC_ENDS_UNCLOSED_QUOTE = /["'][^"']*$/             // "said 'something
// Consecutive capital letters that aren't a known acronym are suspicious.
const SUSPICIOUS_CAPS = /\b[A-Z]{4,}\b/g
// Words that look cut off at the end: 5+ letters without a terminal punctuation
// AND followed by whitespace/EOF where a word break would be unnatural.
// Example: "collaborati" or "interv".
const CUT_MIDWORD = /\b([a-z]{6,})$/

async function stageQuality(
  pool: Pool,
  candidates: Candidate[],
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Promise<Candidate[]> {
  const kept: Candidate[] = []
  for (const c of candidates) {
    const reason = assessQuality(c)
    if (reason) {
      suppress(c, 2, reason)
      // Escalate to data-quality quarantine if repeated.
      if (c.source_table && c.source_id) {
        await bumpDataQualityIssue(pool, c.source_table, c.source_id, reason)
      }
      continue
    }
    // Already-quarantined? skip permanently.
    if (c.source_table && c.source_id && await isQuarantined(pool, c.source_table, c.source_id)) {
      suppress(c, 2, 'quarantined_data')
      continue
    }
    kept.push(c)
  }
  return kept
}

function assessQuality(c: Candidate): string | null {
  const title = (c.raw_title || '').trim()
  const body = (c.raw_body || '').trim()
  const combined = `${title} ${body}`

  if (title.length < 5) return 'title_too_short'
  if (title.length > 80) return 'title_too_long'  // a bit slack; rendering will shorten to 40
  if (body.length < 15) return 'body_too_short'
  if (body.length > 200) return 'body_too_long'   // rendering shortens to 140

  if (TRUNC_END_SINGLE_LETTER.test(title) || TRUNC_END_SINGLE_LETTER.test(body)) {
    return 'truncated_single_letter'
  }

  // Mid-word stub at the end of title or body
  for (const text of [title, body]) {
    const trailing = text.split(/\s+/).slice(-1)[0] || ''
    // Strip trailing punctuation.
    const core = trailing.replace(/[.,!?;:]+$/, '')
    if (/^[A-Za-z]{1,2}$/.test(core) && !SHORT_WORD_ALLOWLIST.has(core)) {
      return 'truncated_trailing_stub'
    }
  }

  const openP = (combined.match(/\(/g) || []).length
  const closeP = (combined.match(/\)/g) || []).length
  if (openP !== closeP) return 'unbalanced_parens'
  const openB = (combined.match(/\[/g) || []).length
  const closeB = (combined.match(/]/g) || []).length
  if (openB !== closeB) return 'unbalanced_brackets'
  const quotes = (combined.match(/"/g) || []).length
  if (quotes % 2 !== 0) return 'unbalanced_quotes'

  if (/\[(missing|placeholder|todo|\.\.\.|null|undefined)\]/i.test(combined)) return 'placeholder_token'
  if (/\b(null|undefined|NaN|TBD|TODO)\b/.test(combined)) return 'placeholder_word'
  if (/(1970-01-01|0000-00-00)/.test(combined)) return 'zero_date'

  // Suspicious all-caps runs (length 4+) that aren't known acronyms.
  const caps = combined.match(SUSPICIOUS_CAPS) || []
  for (const cap of caps) {
    if (!USER_ACRONYMS.has(cap)) return 'unknown_acronym'
  }

  // Source-data completeness: commitments must have a due_date OR description.
  if (c.source_table === 'commitments' && !c.due_date && !c.raw_content) {
    return 'commitment_missing_critical_field'
  }

  return null
}

async function bumpDataQualityIssue(pool: Pool, table: string, id: string, reason: string): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO overlay_data_quality_issues (id, source_table, source_id, failure_count, last_reason, last_seen_at)
       VALUES ($1, $2, $3, 1, $4, NOW())
       ON CONFLICT (source_table, source_id) DO UPDATE SET
         failure_count = overlay_data_quality_issues.failure_count + 1,
         last_reason = EXCLUDED.last_reason,
         last_seen_at = NOW()`,
      [crypto.randomUUID(), table, id, reason],
    )
  } catch (err: any) {
    console.error('[pipeline/quality] quarantine bump failed:', err.message)
  }
}

async function isQuarantined(pool: Pool, table: string, id: string): Promise<boolean> {
  try {
    const r = await pool.query(
      `SELECT failure_count FROM overlay_data_quality_issues
       WHERE source_table = $1 AND source_id = $2 AND reviewed = FALSE`,
      [table, id],
    )
    return (r.rows[0]?.failure_count ?? 0) >= 3
  } catch { return false }
}

// ── Stage 3: Freshness filter ────────────────────────────────────

function stageFreshness(
  candidates: Candidate[],
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Candidate[] {
  const now = Date.now()
  const kept: Candidate[] = []

  for (const c of candidates) {
    const freshErr = assessFreshness(c, now)
    if (freshErr) {
      suppress(c, 3, freshErr)
      continue
    }
    kept.push(c)
  }
  return kept
}

function assessFreshness(c: Candidate, now: number): string | null {
  // Commitments: overdue 1-7 days fires once with OVERDUE (caller enforces
  // "once" via commitment auto-surface counter + per-entity cooldown).
  // Overdue by more than 7 days is junk — drop it.
  if (c.source_table === 'commitments' && c.due_date) {
    const dueMs = new Date(c.due_date).getTime()
    if (!Number.isFinite(dueMs)) return 'commitment_unparseable_due_date'
    const ageDays = (now - dueMs) / 86_400_000
    if (ageDays > 7) return `stale_commitment_${Math.round(ageDays)}d_overdue`
  }

  // Calendar events: end_time must be in the future, and start_time must not
  // already be in the past by more than 5 minutes (nobody cares once it started).
  if (c.source_table === 'calendar_events' && c.freshness_timestamp) {
    const startMs = new Date(c.freshness_timestamp).getTime()
    if (Number.isFinite(startMs) && now - startMs > 5 * 60_000) {
      return 'calendar_event_already_started'
    }
  }

  // predicted_actions: predicted_at older than 7 days → stale
  if (c.source_table === 'predicted_actions' && c.freshness_timestamp) {
    const ageMs = now - new Date(c.freshness_timestamp).getTime()
    if (ageMs > 7 * 86_400_000) return 'prediction_older_than_7_days'
  }

  // action_chains: trigger activity in last 14 days
  if (c.source_table === 'action_chains' && c.freshness_timestamp) {
    const ageMs = now - new Date(c.freshness_timestamp).getTime()
    if (ageMs > 14 * 86_400_000) return 'chain_inactive_14_days'
  }

  // email_threads freshness handled by caller (needs awaiting_reply + top-20)

  return null
}

// ── Stage 4: Duplication filter ──────────────────────────────────

async function stageDuplication(
  pool: Pool,
  candidates: Candidate[],
  signals: Signals,
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Promise<Candidate[]> {
  // Batch one query: any overlay_log entries for these entities in the relevant windows.
  const entityIds = Array.from(new Set(candidates.map(c => c.entity_id).filter(Boolean)))
  if (entityIds.length === 0) return candidates

  const recent = await pool.query(
    `SELECT entity_id, matcher_name, content_snapshot, timestamp, outcome
     FROM overlay_log
     WHERE timestamp > NOW() - INTERVAL '4 hours'
       AND entity_id = ANY($1::text[])`,
    [entityIds],
  )
  const recentByEntity = new Map<string, any[]>()
  for (const row of recent.rows) {
    const arr = recentByEntity.get(row.entity_id) || []
    arr.push(row)
    recentByEntity.set(row.entity_id, arr)
  }

  // Also check: entity mentioned in a recent spotlight query or voice query.
  const recentQueries = await pool.query(
    `SELECT query_text FROM query_memory WHERE timestamp > NOW() - INTERVAL '5 minutes' ORDER BY timestamp DESC LIMIT 20`,
  ).catch(() => ({ rows: [] as any[] }))
  const recentVoice = await pool.query(
    `SELECT raw_content FROM observations
     WHERE source = 'glasses_voice' AND timestamp > NOW() - INTERVAL '5 minutes'
     ORDER BY timestamp DESC LIMIT 3`,
  ).catch(() => ({ rows: [] as any[] }))
  const userJustAskedAbout = new Set<string>()
  for (const q of recentQueries.rows) {
    for (const w of String(q.query_text || '').toLowerCase().split(/[^a-z0-9]+/)) {
      if (w.length >= 4) userJustAskedAbout.add(w)
    }
  }
  for (const v of recentVoice.rows) {
    for (const w of String(v.raw_content || '').toLowerCase().split(/[^a-z0-9]+/)) {
      if (w.length >= 4) userJustAskedAbout.add(w)
    }
  }

  // Whisper overlap: entity fired as whisper in last 2 hours?
  const recentWhispers = await pool.query(
    `SELECT trigger_data, body FROM whisper_log
     WHERE fired_at > NOW() - INTERVAL '2 hours' AND COALESCE(suppressed, FALSE) = FALSE`,
  ).catch(() => ({ rows: [] as any[] }))
  const whisperEntityRefs: string[] = recentWhispers.rows.map((w: any) => {
    try {
      const td = typeof w.trigger_data === 'string' ? JSON.parse(w.trigger_data) : w.trigger_data
      return `${td?.entity_id || ''} ${w.body || ''}`.toLowerCase()
    } catch { return String(w.body || '').toLowerCase() }
  })

  const kept: Candidate[] = []
  const now = Date.now()

  for (const c of candidates) {
    const entityEvents = recentByEntity.get(c.entity_id) || []

    // 4a — same entity + matcher fired in last 5 min
    const sameMatcher5Min = entityEvents.find(
      (e: any) => e.matcher_name === c.matcher_name && e.outcome === 'fired' &&
        (now - new Date(e.timestamp).getTime()) < 5 * 60_000,
    )
    if (sameMatcher5Min) { suppress(c, 4, 'same_matcher_entity_last_5min'); continue }

    // 4b — same entity any matcher last 60 min
    const anyMatcher60Min = entityEvents.find(
      (e: any) => e.outcome === 'fired' && (now - new Date(e.timestamp).getTime()) < 60 * 60_000,
    )
    if (anyMatcher60Min) { suppress(c, 4, 'same_entity_last_60min'); continue }

    // 4c — same insight title fired last 4h
    const sameText4h = entityEvents.find(
      (e: any) => e.outcome === 'fired' && String(e.content_snapshot || '').includes(c.raw_title),
    )
    if (sameText4h) { suppress(c, 4, 'same_insight_text_last_4h'); continue }

    // 4d — user just asked about the entity (spotlight)
    const entityTokens = entityTokensOf(c)
    if (entityTokens.some(t => userJustAskedAbout.has(t))) {
      suppress(c, 4, 'user_just_queried_entity'); continue
    }

    // 4e — whisper just covered this
    const fingerprint = entityTokens.join('|')
    if (fingerprint && whisperEntityRefs.some(w => entityTokens.some(t => w.includes(t)))) {
      suppress(c, 4, 'whisper_already_covered_last_2h'); continue
    }

    kept.push(c)
  }
  return kept
}

function entityTokensOf(c: Candidate): string[] {
  const raw = c.entity_id || ''
  const afterColon = raw.split(':').pop() || raw
  const beforeAt = afterColon.split('@')[0]
  const parts = beforeAt.split(/[^a-zA-Z0-9]/).filter(p => p.length >= 4)
  return parts.map(p => p.toLowerCase())
}

// ── Stage 5: Relevance gate ──────────────────────────────────────

async function stageRelevance(
  pool: Pool,
  candidates: Candidate[],
  signals: Signals,
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Promise<Candidate[]> {
  // Hard-suppression categories — apply to ALL candidates regardless of test result.
  const appLower = signals.app || ''

  // Video calls: only fire if about the participants of the current meeting.
  // Detecting participants requires matching against calendar — easier: only
  // proceed if candidate label is MEETING/UPCOMING AND any entity token is in
  // the current window title (participant names usually appear there).
  const inVideoCall = VIDEO_CALL_APPS.some(a => appLower.includes(a))

  // Rapid typing: if client reports >2 text changes/sec, suppress entirely.
  if ((signals.typing_rate ?? 0) > 2) {
    for (const c of candidates) suppress(c, 5, 'rapid_typing_suppress')
    return []
  }

  // Presentation/fullscreen: detected via app-name heuristic + title keyword.
  const looksLikePresentation = /keynote|powerpoint|present|slides/.test(appLower) ||
    /presentation|fullscreen/.test(signals.title || '')
  if (looksLikePresentation) {
    for (const c of candidates) suppress(c, 5, 'presentation_fullscreen')
    return []
  }

  // Other IRIS surfaces currently visible? If so, do not compete.
  const surfaceBusy = await anyOtherSurfaceVisible(pool)
  if (surfaceBusy.busy) {
    for (const c of candidates) suppress(c, 5, `surface_busy:${surfaceBusy.which}`)
    return []
  }

  // Morning session detection: first fire of the day + session-start window.
  const morningOk = await isMorningSession(pool)

  const isDeep = DEEP_FOCUS_APPS.some(a => appLower.includes(a))
  const isNeutral = NEUTRAL_APPS.some(a => appLower.includes(a))
  const isMessaging = MESSAGING_APPS.some(a => appLower.includes(a))
  const isEmail = appLower.includes('mail') || (signals.url || '').includes('mail.google.com')
  const isCalendar = appLower.includes('calendar') || (signals.url || '').includes('calendar.google.com')

  const recentSwitchMs = signals.app_switched_at ? Date.now() - signals.app_switched_at : Number.POSITIVE_INFINITY
  const justSwitchedApps = recentSwitchMs < 10_000

  const allText = `${signals.ocr} ${signals.title} ${signals.url} ${signals.file}`.toLowerCase()

  const kept: Candidate[] = []

  for (const c of candidates) {
    // Video-call hard rule
    if (inVideoCall) {
      const tokens = entityTokensOf(c)
      const participantMatch = tokens.some(t => signals.title.includes(t) || signals.ocr.includes(t))
      if (!participantMatch) { suppress(c, 5, 'video_call_not_about_participants'); continue }
    }

    const entityDirectMatch = directEntityMatch(c, signals)
    const topicMatch = topicMatches(c, { isEmail, isCalendar, isMessaging })

    // Deep-focus: only direct entity match OR a same-project self-reference.
    if (isDeep) {
      const selfRef = c.source_table === 'documents' || c.matcher_name === 'file'
      if (!entityDirectMatch && !selfRef) {
        suppress(c, 5, 'deep_focus_no_direct_match'); continue
      }
    }

    // Test A: direct entity match
    const testA = entityDirectMatch
    // Test B: topical match
    const testB = topicMatch
    // Test C: strong temporal urgency + neutral context
    let testC = false
    if (c.due_date) {
      const msUntil = new Date(c.due_date).getTime() - Date.now()
      if (msUntil > 0 && msUntil < 2 * 3600_000 && (isNeutral || isMessaging || isEmail || isCalendar)) {
        testC = true
      }
    }
    if (c.label === 'UPCOMING' && (isNeutral || isMessaging || isEmail || isCalendar)) {
      testC = true  // approaching calendar event
    }
    // Test D: morning session (first 10 min of day, first fire)
    const testD = morningOk
    // Test E: user just switched apps and candidate is topical to the new app
    const testE = justSwitchedApps && topicMatch

    const anyPass = testA || testB || testC || testD || testE
    if (!anyPass) { suppress(c, 5, 'no_relevance_test_passed'); continue }

    // Mark the tie-breaker evidence for later scoring.
    if (entityDirectMatch) (c as any)._relevance_boost = 20
    else if (testC) (c as any)._relevance_boost = 15
    else if (topicMatch) (c as any)._relevance_boost = 8
    else (c as any)._relevance_boost = 0

    kept.push(c)
  }

  return kept
}

function directEntityMatch(c: Candidate, signals: Signals): boolean {
  const tokens = entityTokensOf(c)
  if (tokens.length === 0) return false
  const hay = `${signals.ocr} ${signals.title} ${signals.url} ${signals.file}`
  return tokens.some(t => hay.includes(t))
}

function topicMatches(
  c: Candidate,
  ctx: { isEmail: boolean; isCalendar: boolean; isMessaging: boolean },
): boolean {
  const t = c.type
  if (ctx.isEmail && (t === 'email' || t === 'contact' || t === 'predictive')) return true
  if (ctx.isCalendar && (t === 'calendar' || t === 'proactive')) return true
  if (ctx.isMessaging && (t === 'contact' || t === 'email' || t === 'sequence')) return true
  return false
}

async function anyOtherSurfaceVisible(pool: Pool): Promise<{ busy: boolean; which: string }> {
  try {
    const r = await pool.query(
      `SELECT surface FROM surface_state
       WHERE visible = TRUE
         AND surface <> 'overlay'
         AND last_event_at > NOW() - INTERVAL '30 seconds'
       LIMIT 1`,
    )
    if (r.rows[0]) return { busy: true, which: r.rows[0].surface }
  } catch {}
  return { busy: false, which: '' }
}

async function isMorningSession(pool: Pool): Promise<boolean> {
  try {
    // First overlay fire of day + within 10 min of earliest observation today.
    const dayFires = await pool.query(
      `SELECT COUNT(*)::int AS c FROM overlay_log
       WHERE outcome = 'fired' AND timestamp::date = NOW()::date`,
    )
    if ((dayFires.rows[0]?.c ?? 0) > 0) return false

    const firstObs = await pool.query(
      `SELECT timestamp FROM observations
       WHERE timestamp::date = NOW()::date ORDER BY timestamp ASC LIMIT 1`,
    )
    if (!firstObs.rows[0]) return false
    const firstMs = new Date(firstObs.rows[0].timestamp).getTime()
    return (Date.now() - firstMs) < 10 * 60_000
  } catch { return false }
}

// ── Stage 6: Actionability gate ──────────────────────────────────

function stageActionability(
  candidates: Candidate[],
  signals: Signals,
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Candidate[] {
  const appLower = signals.app || ''
  const inEmail = appLower.includes('mail') || (signals.url || '').includes('mail.google.com')
  const inMessaging = MESSAGING_APPS.some(a => appLower.includes(a))
  const inCalendar = appLower.includes('calendar')

  const kept: Candidate[] = []
  for (const c of candidates) {
    const action = deriveSuggestedAction(c, { inEmail, inMessaging, inCalendar })
    if (!action) { suppress(c, 6, 'no_concrete_action'); continue }
    c.suggested_action = action
    kept.push(c)
  }
  return kept
}

function deriveSuggestedAction(
  c: Candidate,
  ctx: { inEmail: boolean; inMessaging: boolean; inCalendar: boolean },
): string | null {
  // Explicit: matcher already provided a button
  if (c.button && c.action_type) {
    return `Click ${c.button.toLowerCase()} to ${c.action_type.replace(/_/g, ' ')}`
  }

  // Context-driven action categories
  if (ctx.inMessaging && (c.type === 'contact' || c.type === 'email')) {
    return 'reply to this person now'
  }
  if (ctx.inEmail && (c.type === 'email' || c.type === 'contact' || c.type === 'predictive')) {
    return 'send or draft an email'
  }
  if (ctx.inCalendar && (c.type === 'calendar' || c.type === 'proactive')) {
    return 'open event or add prep'
  }
  if (c.type === 'file' || c.source_table === 'documents') {
    return 'open the file'
  }
  if (c.label === 'UPCOMING' || c.label === 'MEETING') {
    return 'join meeting or open event'
  }
  if (c.label === 'DEADLINE' || c.label === 'OVERDUE' || c.label === 'COMMITMENT') {
    return 'complete or mark done'
  }
  if (c.type === 'centrum' || c.label === 'CHAIN PLAN') {
    return 'open chain plan'
  }

  // No clear action → fail
  return null
}

// ── Stage 7: Rate limit ──────────────────────────────────────────

async function stageRateLimit(
  pool: Pool,
  candidates: Candidate[],
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Promise<Candidate[]> {
  // One global query: all fired events within max window we need (24 hours).
  const recent = await pool.query(
    `SELECT entity_id, matcher_name, timestamp FROM overlay_log
     WHERE outcome = 'fired' AND timestamp > NOW() - INTERVAL '24 hours'`,
  )
  const fires = recent.rows.map((r: any) => ({
    entity_id: r.entity_id as string,
    matcher_name: r.matcher_name as string,
    ts: new Date(r.timestamp).getTime(),
  }))

  const now = Date.now()
  const day = fires.filter(f => now - f.ts < 24 * 3600_000)
  const hour = fires.filter(f => now - f.ts < 60 * 60_000)
  const lastFireTs = fires.length > 0 ? Math.max(...fires.map(f => f.ts)) : 0

  const dailyExhausted = day.length >= DAILY_BUDGET
  const hourlyExhausted = hour.length >= HOURLY_BUDGET
  const minGapViolated = (now - lastFireTs) < MIN_GAP_MS

  const kept: Candidate[] = []
  for (const c of candidates) {
    // Hard 2-minute minimum always applies.
    if (minGapViolated) { suppress(c, 7, 'min_gap_2min_violated'); continue }

    // Per-entity 6-hour cooldown (hard)
    const entityFire = fires.find(f => f.entity_id === c.entity_id && (now - f.ts) < PER_ENTITY_COOLDOWN_MS)
    if (entityFire) { suppress(c, 7, 'entity_cooldown_6h'); continue }

    // Per-matcher 20-minute cooldown (hard)
    const matcherFire = fires.find(f => f.matcher_name === c.matcher_name && (now - f.ts) < PER_MATCHER_COOLDOWN_MS)
    if (matcherFire) { suppress(c, 7, 'matcher_cooldown_20min'); continue }

    // Hourly + daily budgets — critical can bypass these only.
    if (hourlyExhausted && !c.critical) { suppress(c, 7, 'hourly_budget_exhausted'); continue }
    if (dailyExhausted && !c.critical) { suppress(c, 7, 'daily_budget_exhausted'); continue }

    kept.push(c)
  }
  return kept
}

// ── Stage 8 + 10: Score, then apply personal learning ────────────

async function stageScoreAndLearn(
  pool: Pool,
  candidates: Candidate[],
  signals: Signals,
  suppress: (c: Candidate, stage: number, reason: string) => void,
): Promise<Candidate | null> {
  // Fetch learning signals once (90-day window).
  const learning = await pool.query(
    `SELECT trigger_action, user_action, trigger_context
     FROM correction_log
     WHERE timestamp > NOW() - INTERVAL '90 days'`,
  ).catch(() => ({ rows: [] as any[] }))
  const learningRows: any[] = learning.rows

  const now = Date.now()

  const scored: Candidate[] = candidates.map(c => {
    const breakdown: Record<string, number> = {}
    breakdown.base = c.base_confidence

    // Urgency: up to +30 if due within 2h
    if (c.due_date) {
      const msUntil = new Date(c.due_date).getTime() - now
      if (msUntil > 0 && msUntil < 2 * 3600_000) {
        breakdown.urgency = Math.round(30 * (1 - msUntil / (2 * 3600_000)))
      } else if (msUntil > 0 && msUntil < 24 * 3600_000) {
        breakdown.urgency = 10
      }
    }
    if (c.label === 'UPCOMING') breakdown.urgency = (breakdown.urgency || 0) + 20

    // Relevance: up to +20 from Stage-5 direct match
    breakdown.relevance = (c as any)._relevance_boost || 0

    // Actionability: +15 if has concrete button
    if (c.button) breakdown.actionability = 15

    // Staleness penalty: up to -20
    if (c.freshness_timestamp) {
      const ageDays = (now - new Date(c.freshness_timestamp).getTime()) / 86_400_000
      if (ageDays > 7) breakdown.staleness_penalty = -Math.min(20, Math.round(ageDays / 2))
    }

    // Learning: dismissal penalty (up to -20)
    const dismissCount = countLearning(learningRows, c, ['dismiss', 'dismissed', 'skip'])
    if (dismissCount > 0) breakdown.dismissal_penalty = -Math.min(20, dismissCount * 5)

    // Learning: action bonus (up to +10)
    const actCount = countLearning(learningRows, c, ['acted', 'action_taken', 'approved', 'approve'])
    if (actCount > 0) breakdown.action_bonus = Math.min(10, actCount * 3)

    const score = Object.values(breakdown).reduce((a, b) => a + b, 0)
    c._score = score
    c._score_breakdown = breakdown
    return c
  })

  // Apply Stage-10 hard suppressions
  const passed: Candidate[] = []
  for (const c of scored) {
    // Per-matcher raised-threshold rule
    const matcherDismisses = learningRows.filter((r: any) =>
      String(r.trigger_action || '').includes(`overlay:${c.type}`) &&
      String(r.user_action || '').includes('dismiss'),
    ).length
    const localThreshold = MIN_SCORE + (matcherDismisses >= LEARNING_DISMISS_THRESHOLD ? 10 : 0)

    // Per-entity suppression (2+ dismissals in 14 days)
    const entityDismisses14 = countEntityDismissals(learningRows, c, 14)
    if (entityDismisses14 >= 2) {
      suppress(c, 10, 'entity_dismissed_twice_last_14d'); continue
    }

    if ((c._score ?? 0) < localThreshold) {
      suppress(c, 8, `score_below_threshold_${localThreshold}`); continue
    }

    passed.push(c)
  }
  if (passed.length === 0) return null

  // Pick best; tiebreak by freshness_timestamp, then by matcher historical action rate.
  passed.sort((a, b) => {
    if ((b._score ?? 0) !== (a._score ?? 0)) return (b._score ?? 0) - (a._score ?? 0)
    const ta = a.freshness_timestamp ? new Date(a.freshness_timestamp).getTime() : 0
    const tb = b.freshness_timestamp ? new Date(b.freshness_timestamp).getTime() : 0
    if (tb !== ta) return tb - ta
    return matcherActionRate(learningRows, b.matcher_name) - matcherActionRate(learningRows, a.matcher_name)
  })
  return passed[0]
}

function countLearning(rows: any[], c: Candidate, actions: string[]): number {
  const matchEntity = entityTokensOf(c)
  if (matchEntity.length === 0) return 0
  return rows.filter((r: any) => {
    const ua = String(r.user_action || '').toLowerCase()
    if (!actions.some(a => ua.includes(a))) return false
    const ctx = String(r.trigger_context || '').toLowerCase()
    const trig = String(r.trigger_action || '').toLowerCase()
    return matchEntity.some(t => ctx.includes(t) || trig.includes(t))
  }).length
}

function countEntityDismissals(rows: any[], c: Candidate, windowDays: number): number {
  const windowMs = windowDays * 86_400_000
  const now = Date.now()
  const matchEntity = entityTokensOf(c)
  if (matchEntity.length === 0) return 0
  return rows.filter((r: any) => {
    const ts = r.timestamp ? new Date(r.timestamp).getTime() : 0
    if (now - ts > windowMs) return false
    const ua = String(r.user_action || '').toLowerCase()
    if (!/dismiss/.test(ua)) return false
    const ctx = String(r.trigger_context || '').toLowerCase()
    const trig = String(r.trigger_action || '').toLowerCase()
    return matchEntity.some(t => ctx.includes(t) || trig.includes(t))
  }).length
}

function matcherActionRate(rows: any[], matcher: string): number {
  const subset = rows.filter((r: any) => String(r.trigger_action || '').includes(matcher))
  if (subset.length === 0) return 0
  const acted = subset.filter((r: any) => /act|approve/.test(String(r.user_action || ''))).length
  return acted / subset.length
}

// ── Stage 9: Content rendering ───────────────────────────────────

export function stageRender(c: Candidate): { ok: true; candidate: Candidate } | { ok: false; reason: string } {
  // Enforce allowed label (fall back to COMMITMENT-family defaults).
  let label = String(c.label || '').toUpperCase()
  if (!ALLOWED_LABELS.has(label)) {
    // Map legacy types to allowed labels
    const remap: Record<string, string> = {
      'CONTACT': 'PERSON',
      'RELATIONSHIP': 'PERSON',
      'CHAIN PLAN': 'PATTERN',
    }
    label = remap[label] || 'PATTERN'
  }

  // Title: 5–8 words, ≤40 chars, specific, must include entity if present.
  let title = cleanText(c.raw_title || '')
  title = trimToWords(title, 8)
  title = title.slice(0, 40).trim()
  if (title.length < 5 || title.split(/\s+/).length < 3) return { ok: false, reason: 'title_too_short_after_clean' }

  // Body: 10–20 words, ≤140 chars.
  let body = cleanText(c.raw_body || '')
  body = trimToWords(body, 20)
  body = body.slice(0, 140).trim()
  if (body.length < 10) return { ok: false, reason: 'body_too_short_after_clean' }
  if (body.split(/\s+/).length < 4) return { ok: false, reason: 'body_too_few_words' }

  // Format dates & times naturally
  title = naturalizeDatesTimes(title)
  body = naturalizeDatesTimes(body)

  // Forbidden characters: em/en dashes, brackets, slashes, markdown
  for (const s of [title, body]) {
    if (/[—–\[\]\/*_`]/.test(s)) return { ok: false, reason: 'forbidden_char' }
  }

  // Allowed button labels
  if (c.button) {
    const allowed = new Set(['View', 'Reply', 'Draft', 'Done', 'Skip'])
    if (!allowed.has(c.button)) c.button = 'View'
  }

  c._final_title = title
  c._final_body = body
  c.label = label as OverlayLabel
  return { ok: true, candidate: c }
}

function cleanText(s: string): string {
  return s
    .replace(/[—–]/g, ' ')        // em/en dashes → space
    .replace(/[\[\]]/g, '')        // brackets
    .replace(/[\/]/g, ' ')         // slashes
    .replace(/[*_`]/g, '')         // markdown markers
    .replace(/\s+/g, ' ')
    .trim()
}

function trimToWords(s: string, maxWords: number): string {
  const words = s.split(/\s+/)
  if (words.length <= maxWords) return s
  return words.slice(0, maxWords).join(' ')
}

function naturalizeDatesTimes(s: string): string {
  let out = s

  // ISO date → "on April 20" / "today" / "tomorrow"
  out = out.replace(/\b(\d{4})-(\d{2})-(\d{2})\b/g, (_match, y, m, d) => {
    const target = new Date(`${y}-${m}-${d}`)
    if (Number.isNaN(target.getTime())) return _match
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    const diffDays = Math.round((target.getTime() - today.getTime()) / 86_400_000)
    if (diffDays === 0) return 'today'
    if (diffDays === 1) return 'tomorrow'
    if (diffDays === -1) return 'yesterday'
    if (diffDays > 1 && diffDays <= 6) {
      return `on ${target.toLocaleDateString('en-US', { weekday: 'long' })}`
    }
    return `on ${target.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}`
  })

  // 24-hour time (14:15) → 2:15 PM
  out = out.replace(/\b([01]?\d|2[0-3]):([0-5]\d)\b/g, (_m, hh, mm) => {
    const h = parseInt(hh, 10)
    const period = h >= 12 ? 'PM' : 'AM'
    const display = h === 0 ? 12 : h > 12 ? h - 12 : h
    return `${display}:${mm} ${period}`
  })

  return out
}

// ── Helper: build minimal overlay_log insert ─────────────────────

export async function logPipelineEvent(
  pool: Pool,
  outcome: 'fired' | 'suppressed',
  stage: number,
  reason: string,
  c: Pick<Candidate, 'entity_id' | 'type' | 'matcher_name' | 'raw_title' | 'raw_body'> & { _final_title?: string; _final_body?: string; _score?: number },
  signals: Signals,
): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO overlay_log
         (id, outcome, reason, entity_id, insight_type, matcher_name, stage, confidence, content_snapshot, signals_snapshot)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        crypto.randomUUID(),
        outcome === 'fired' ? 'fired' : `suppressed_stage_${stage}`,
        reason,
        c.entity_id || '',
        c.type,
        c.matcher_name,
        stage,
        Math.round(c._score ?? 0),
        `${c._final_title || c.raw_title} / ${c._final_body || c.raw_body}`.slice(0, 500),
        JSON.stringify({ app: signals.app, title: signals.title, url: signals.url, ocr_length: signals.ocr.length }),
      ],
    )
  } catch (err: any) {
    console.error('[pipeline/log] insert failed:', err.message)
  }
}

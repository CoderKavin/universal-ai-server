/**
 * Topic grounding — reject content that has no connection to the user's
 * known world.
 *
 * The Hegseth bug: an NYT breaking-news email was ingested, mis-classified
 * as a "new_project" life event, then fed into the chain reasoner which
 * asked Claude to produce a 3-step action plan. Claude happily invented
 * "Draft critical analysis of Hegseth military leadership changes" — a
 * topic Kavin has zero connection to. The chain reasoner had no grounding
 * check, so any text that parsed was fair game.
 *
 * Grounding check: before generating a chain / whisper / overlay card
 * for a trigger, verify the trigger references at least one token that
 * exists in the user's entity set (contacts, active commitments, known
 * projects, calendar participants, persona-declared topics).
 */

import type { Pool } from 'pg'
import { significantTokens } from './dismissal-learning'

// ── User entity set (cached in-process for 2 minutes) ────────────

interface UserEntitySet {
  tokens: Set<string>           // significant tokens (≥4 chars, lowercase)
  built_at: number               // Date.now() when built
  source_counts: Record<string, number>
}

let _cache: UserEntitySet | null = null
const CACHE_MS = 2 * 60 * 1000

/**
 * Build the set of tokens that represent things the user is actually
 * engaged with. Drawn from:
 *   - Contact names + email local-parts
 *   - Active commitments (significant nouns)
 *   - Upcoming calendar events + participants
 *   - Living profile (persona text, top 800 chars)
 *   - Known project names hardcoded for IRIS (Tattva, IRIS, Centrum, Airpoint,
 *     Extended Essay, IB subjects)
 */
export async function buildUserEntitySet(pool: Pool): Promise<UserEntitySet> {
  const now = Date.now()
  if (_cache && now - _cache.built_at < CACHE_MS) return _cache

  const tokens = new Set<string>()
  const counts: Record<string, number> = {}

  const add = (text: string | null | undefined, srcKey: string): void => {
    if (!text) return
    const toks = significantTokens(text)
    for (const t of toks) tokens.add(t)
    counts[srcKey] = (counts[srcKey] || 0) + toks.length
  }

  // 1. Relationships (names + email local-parts of top-50 closeness)
  try {
    const r = await pool.query(
      `SELECT name, email FROM relationships WHERE current_closeness > 5
       ORDER BY current_closeness DESC LIMIT 200`,
    )
    for (const row of r.rows) {
      add(row.name, 'relationships')
      if (row.email) add(String(row.email).split('@')[0], 'relationships')
    }
  } catch {}

  // 2. Active commitments
  try {
    const r = await pool.query(
      `SELECT description, committed_to FROM commitments
       WHERE status = 'active' LIMIT 200`,
    )
    for (const row of r.rows) {
      add(row.description, 'commitments')
      add(row.committed_to, 'commitments')
    }
  } catch {}

  // 3. Upcoming calendar events (next 14 days)
  try {
    const r = await pool.query(
      `SELECT summary, participants FROM calendar_events
       WHERE start_time::timestamptz > NOW() - INTERVAL '7 days'
         AND start_time::timestamptz < NOW() + INTERVAL '14 days'
         AND COALESCE(status, 'confirmed') != 'cancelled'
       LIMIT 100`,
    )
    for (const row of r.rows) {
      add(row.summary, 'calendar')
      try {
        const parts = typeof row.participants === 'string' ? JSON.parse(row.participants) : row.participants
        if (Array.isArray(parts)) for (const p of parts) add(String(p), 'calendar')
      } catch {}
    }
  } catch {}

  // 4. Living profile (persona)
  try {
    const r = await pool.query(`SELECT content FROM living_profile WHERE id='main' LIMIT 1`)
    if (r.rows[0]?.content) {
      add(String(r.rows[0].content).slice(0, 4000), 'persona')
    }
  } catch {}

  // 5. Recent centrum snapshots — things the user actively put on their board
  try {
    const r = await pool.query(
      `SELECT board_state, today_summary FROM centrum_snapshots
       ORDER BY timestamp DESC LIMIT 3`,
    )
    for (const row of r.rows) {
      add(row.today_summary, 'centrum')
      if (row.board_state) {
        try {
          const bs = typeof row.board_state === 'string' ? JSON.parse(row.board_state) : row.board_state
          add(JSON.stringify(bs).slice(0, 4000), 'centrum')
        } catch {}
      }
    }
  } catch {}

  // 6. Hardcoded IRIS/Kavin projects that might not yet appear in above queries
  // (we explicitly keep this list short — everything else should come from DB).
  const KNOWN_PROJECTS = [
    'tattva', 'iris', 'centrum', 'airpoint', 'chetana',
    'extended', 'essay', 'ibdp', 'trins', 'kavin', 'venkatesan',
    'physics', 'mathematics', 'economics', 'english', 'tok',
    'history', 'biology', 'chemistry',
  ]
  for (const p of KNOWN_PROJECTS) tokens.add(p)
  counts['hardcoded_projects'] = KNOWN_PROJECTS.length

  const built: UserEntitySet = { tokens, built_at: now, source_counts: counts }
  _cache = built
  return built
}

// Expose for testing / debugging
export function clearEntitySetCache(): void { _cache = null }

// ── The grounding check ──────────────────────────────────────────

export interface GroundingResult {
  grounded: boolean
  matched_tokens: string[]
  candidate_tokens: string[]
  reason: string
}

/**
 * Is the candidate text connected to the user's world?
 *
 * Returns grounded=true if AT LEAST ONE significant token from the
 * candidate appears in the user entity set. Returns grounded=false
 * with matched_tokens=[] if no overlap — callers MUST suppress in
 * this case (the failure mode is a news article becoming a fake
 * project).
 *
 * Edge cases:
 *   - Empty candidate text → grounded=false ('empty_candidate')
 *   - User entity set is empty (cold start) → grounded=true
 *     ('cold_start_no_entity_set') — we'd rather surface than
 *     suppress everything when the user has no data yet.
 */
export async function checkTopicGrounding(
  pool: Pool,
  candidateText: string | null | undefined,
): Promise<GroundingResult> {
  const candTokens = significantTokens(candidateText || '')
  if (candTokens.length === 0) {
    return { grounded: false, matched_tokens: [], candidate_tokens: [], reason: 'empty_candidate' }
  }

  const entitySet = await buildUserEntitySet(pool)
  if (entitySet.tokens.size === 0) {
    return {
      grounded: true,
      matched_tokens: [],
      candidate_tokens: candTokens,
      reason: 'cold_start_no_entity_set',
    }
  }

  const matched: string[] = []
  for (const t of candTokens) {
    if (entitySet.tokens.has(t)) matched.push(t)
  }

  if (matched.length === 0) {
    return {
      grounded: false,
      matched_tokens: [],
      candidate_tokens: candTokens,
      reason: 'topic_not_grounded_in_user_context',
    }
  }

  return {
    grounded: true,
    matched_tokens: matched,
    candidate_tokens: candTokens,
    reason: 'grounded',
  }
}

// ── Source whitelist for chain / whisper actor classification ──

/**
 * Does this observation represent something the USER did (a sent email,
 * an outgoing WhatsApp, a Centrum card they created), vs passive content
 * they consumed (a newsletter, a browser visit, an AI chat about
 * curiosity)?
 *
 * Passive content cannot seed action chains — it's input, not commitment.
 */
export function isUserActorObservation(obs: {
  source?: string | null
  event_type?: string | null
  raw_content?: string | null
}): boolean {
  const source = String(obs.source || '').toLowerCase()
  const eventType = String(obs.event_type || '').toLowerCase()
  const rc = String(obs.raw_content || '')

  // Explicit "user did this" sources
  if (source === 'centrum') return true
  if (source === 'chat' && eventType === 'queried') return true  // user typed into spotlight
  if (source === 'email_sent' || source === 'user_sent_email') return true
  if (source === 'whatsapp_sent' || source === 'user_whatsapp_sent') return true
  if (eventType === 'composed' || eventType === 'sent') return true

  // Email received — only count if it's a direct human reply, not broadcast.
  if (source === 'email' && eventType === 'received') {
    const fromMatch = rc.match(/^From:\s*(.+)$/m)
    if (!fromMatch) return false
    const fromLower = fromMatch[1].toLowerCase()
    if (/noreply|no-reply|updates-|notifications?@|newsletter|marketing@|breakingnews/.test(fromLower)) return false
    if (/nytimes|medium|substack|mailchimp|mailgun|sendgrid|linkedin/.test(fromLower)) return false
    // Direct human reply — treat as actor true only if the content is short
    // enough to be a human message (< 2000 chars) and doesn't look like HTML.
    if (rc.length > 5000) return false
    return true
  }

  // Browser / screen_capture / OCR are consumption signals, not actions.
  if (source === 'screen_capture' || source === 'browser') return false

  // WhatsApp received messages — direct, not broadcast, so treat as relevant
  // input (counts as user world even though user didn't send it).
  if (source === 'whatsapp' && eventType !== 'sent') return true

  // Default: conservative, assume passive.
  return false
}

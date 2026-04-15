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
  // STRICT: proper nouns that identify a specific person, org, or project
  // the user is actively engaged with. Matching one of these is strong
  // enough to ground a topic. Drawn from contact names, calendar
  // participants, hardcoded project names, commitment "committed_to" etc.
  strict_tokens: Set<string>
  // LOOSE: all other vocabulary (persona text, commitment descriptions,
  // centrum board contents). Matching these alone is NOT enough — they
  // count only as corroboration (need ≥2 to ground).
  loose_tokens: Set<string>
  built_at: number
  source_counts: Record<string, number>
  // Back-compat alias so older callers using .tokens don't break.
  tokens: Set<string>
}

// Generic words that appear in almost any persona/commitment but don't
// identify a specific user-engagement. These are SUBTRACTED from both
// strict and loose sets — matching them counts as zero.
const GENERIC_STOPLIST = new Set([
  // analytical
  'critical', 'analysis', 'research', 'discussion', 'review', 'study',
  'work', 'working', 'task', 'tasks', 'project', 'projects', 'plan',
  'planning', 'plans', 'strategy', 'approach', 'framework', 'process',
  'important', 'urgent', 'priority', 'priorities', 'focus', 'consider',
  'update', 'updates', 'progress', 'complete', 'completed', 'done',
  // leadership / org-generic
  'leadership', 'leader', 'team', 'teams', 'group', 'groups', 'member',
  'members', 'organization', 'management', 'manager', 'director',
  'department', 'community', 'network', 'committee',
  // time
  'today', 'tomorrow', 'yesterday', 'week', 'weekly', 'month', 'monthly',
  'year', 'yearly', 'daily', 'recent', 'current', 'future', 'past',
  // email / action generic (already in dismissal-learning but defensive here too)
  'reply', 'follow', 'draft', 'send', 'share', 'message', 'email', 'emails',
  'meeting', 'meetings', 'call', 'calls', 'zoom', 'invitation',
  // news-y verbs that show up everywhere
  'changes', 'change', 'changed', 'breaking', 'news', 'report', 'reports',
  'article', 'articles', 'headline', 'headlines', 'story', 'stories',
  'announcement', 'announce', 'statement', 'released', 'launch', 'launched',
])

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

  const strict = new Set<string>()
  const loose = new Set<string>()
  const counts: Record<string, number> = {}

  // STRICT tier: tokens we trust on their own (proper nouns identifying
  // a person/org/project the user is actively engaged with).
  const addStrict = (text: string | null | undefined, srcKey: string): void => {
    if (!text) return
    const toks = significantTokens(text)
    for (const t of toks) {
      if (GENERIC_STOPLIST.has(t)) continue
      strict.add(t)
    }
    counts[srcKey] = (counts[srcKey] || 0) + toks.length
  }
  // LOOSE tier: corroborative tokens (need 2+ matches to ground).
  const addLoose = (text: string | null | undefined, srcKey: string): void => {
    if (!text) return
    const toks = significantTokens(text)
    for (const t of toks) {
      if (GENERIC_STOPLIST.has(t)) continue
      if (strict.has(t)) continue  // don't duplicate into loose
      loose.add(t)
    }
    counts[srcKey] = (counts[srcKey] || 0) + toks.length
  }

  // 1. Relationships — contact names are strong identifiers. Email
  // local-parts same. Closeness > 5 keeps out one-off addresses.
  try {
    const r = await pool.query(
      `SELECT name, email FROM relationships WHERE current_closeness > 5
       ORDER BY current_closeness DESC LIMIT 300`,
    )
    for (const row of r.rows) {
      addStrict(row.name, 'relationships')
      if (row.email) addStrict(String(row.email).split('@')[0], 'relationships')
    }
  } catch {}

  // 2. Active commitments — committed_to is a strong identifier
  // (a named person/org). Description goes into LOOSE.
  try {
    const r = await pool.query(
      `SELECT description, committed_to FROM commitments
       WHERE status = 'active' LIMIT 200`,
    )
    for (const row of r.rows) {
      addStrict(row.committed_to, 'commitments')
      addLoose(row.description, 'commitments')
    }
  } catch {}

  // 3. Upcoming calendar events — participants are strong, summary loose.
  try {
    const r = await pool.query(
      `SELECT summary, participants FROM calendar_events
       WHERE start_time::timestamptz > NOW() - INTERVAL '7 days'
         AND start_time::timestamptz < NOW() + INTERVAL '14 days'
         AND COALESCE(status, 'confirmed') != 'cancelled'
       LIMIT 100`,
    )
    for (const row of r.rows) {
      addLoose(row.summary, 'calendar')
      try {
        const parts = typeof row.participants === 'string' ? JSON.parse(row.participants) : row.participants
        if (Array.isArray(parts)) for (const p of parts) addStrict(String(p), 'calendar')
      } catch {}
    }
  } catch {}

  // 4. Living profile — ALL loose. Persona text has too many generic
  // analytical words ("critical", "analysis", "leadership") that would
  // accidentally ground news articles if we treated them as strict.
  try {
    const r = await pool.query(`SELECT content FROM living_profile WHERE id='main' LIMIT 1`)
    if (r.rows[0]?.content) {
      addLoose(String(r.rows[0].content).slice(0, 4000), 'persona')
    }
  } catch {}

  // 5. Centrum — user explicitly put items on their board, so these
  // are strongly user-engaged; promote to strict.
  try {
    const r = await pool.query(
      `SELECT board_state, today_summary FROM centrum_snapshots
       ORDER BY timestamp DESC LIMIT 3`,
    )
    for (const row of r.rows) {
      addStrict(row.today_summary, 'centrum')
      if (row.board_state) {
        try {
          const bs = typeof row.board_state === 'string' ? JSON.parse(row.board_state) : row.board_state
          addStrict(JSON.stringify(bs).slice(0, 4000), 'centrum')
        } catch {}
      }
    }
  } catch {}

  // 6. Hardcoded project names — strong.
  const KNOWN_PROJECTS = [
    'tattva', 'iris', 'centrum', 'airpoint', 'chetana',
    'trins', 'kavin', 'venkatesan',
  ]
  for (const p of KNOWN_PROJECTS) strict.add(p)
  counts['hardcoded_projects'] = KNOWN_PROJECTS.length

  // Merged view for back-compat and quick lookup.
  const merged = new Set<string>([...strict, ...loose])

  const built: UserEntitySet = {
    strict_tokens: strict,
    loose_tokens: loose,
    tokens: merged,
    built_at: now,
    source_counts: counts,
  }
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
  const rawTokens = significantTokens(candidateText || '')
  // Filter out generic words from the candidate too — otherwise a news
  // article full of "analysis / leadership / research" would ground by
  // matching the generic words present in the persona.
  const candTokens = rawTokens.filter(t => !GENERIC_STOPLIST.has(t))

  if (candTokens.length === 0) {
    return { grounded: false, matched_tokens: [], candidate_tokens: rawTokens, reason: 'empty_or_all_generic' }
  }

  const entitySet = await buildUserEntitySet(pool)
  if (entitySet.strict_tokens.size === 0 && entitySet.loose_tokens.size === 0) {
    return {
      grounded: true,
      matched_tokens: [],
      candidate_tokens: candTokens,
      reason: 'cold_start_no_entity_set',
    }
  }

  const strictMatches: string[] = []
  const looseMatches: string[] = []
  for (const t of candTokens) {
    if (entitySet.strict_tokens.has(t)) strictMatches.push(t)
    else if (entitySet.loose_tokens.has(t)) looseMatches.push(t)
  }

  // Rule: one STRICT match is enough. Two LOOSE matches is enough.
  // Anything else = ungrounded.
  if (strictMatches.length >= 1) {
    return {
      grounded: true,
      matched_tokens: strictMatches,
      candidate_tokens: candTokens,
      reason: 'grounded_strict',
    }
  }
  if (looseMatches.length >= 2) {
    return {
      grounded: true,
      matched_tokens: looseMatches,
      candidate_tokens: candTokens,
      reason: 'grounded_loose_corroboration',
    }
  }

  return {
    grounded: false,
    matched_tokens: looseMatches,    // show what we *did* match, if anything
    candidate_tokens: candTokens,
    reason: looseMatches.length === 1 ? 'single_loose_token_insufficient' : 'topic_not_grounded_in_user_context',
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

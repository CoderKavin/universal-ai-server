/**
 * Dismissal learning — shared scoring helpers across overlay, feed, whisper.
 *
 * Reads correction_log to compute negative signals (dismissals) and positive
 * signals (approvals) for any candidate. Two precision improvements over the
 * original fuzzy-substring matching:
 *
 *   1. entity_id exact-match first. Stored on every correction_log row from
 *      now on; consumers prefer it over the fuzzy trigger_context substring.
 *
 *   2. ≥2-token overlap on the fallback path. The old code matched anything
 *      with one shared token (e.g., dismissing "Reply to Rajeev about EE
 *      interview" trained against the token "interview", causing every
 *      future card mentioning interviews to be suppressed). The fallback
 *      now requires at least two overlapping non-stopword tokens.
 *
 * Signal strength is also now first-class:
 *   - 'explicit' dismiss (clicked Dismiss) = 2x weight
 *   - 'implicit' ignore (auto-flip from ignored_count) = 1x weight
 */

import type { Pool } from 'pg'
import crypto from 'crypto'

// ── Token helpers ────────────────────────────────────────────────

const STOPWORDS = new Set([
  'a', 'an', 'and', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he', 'her',
  'hers', 'him', 'his', 'i', 'in', 'is', 'it', 'its', 'me', 'my', 'of', 'on',
  'or', 'our', 'ours', 'so', 'that', 'the', 'their', 'theirs', 'them', 'they',
  'this', 'to', 'us', 'was', 'we', 'were', 'will', 'with', 'you', 'your',
  'yours', 'about', 'into', 'over', 'out', 'up', 'down', 'just', 'not',
  // surface-internal labels — never useful for entity matching
  'reply', 'follow', 'draft', 'urgent', 'today', 'open', 'view', 'send',
])

export function significantTokens(s: string | null | undefined): string[] {
  if (!s) return []
  const tokens = String(s).toLowerCase().split(/[^a-z0-9]+/).filter(Boolean)
  const out: string[] = []
  for (const t of tokens) {
    if (t.length < 4) continue
    if (STOPWORDS.has(t)) continue
    out.push(t)
  }
  return out
}

export interface CorrectionRow {
  entity_id?: string | null
  trigger_action?: string
  trigger_context?: string
  user_action?: string
  signal_strength?: string
  dismiss_source?: string
  timestamp?: string
}

// ── Match a candidate to past corrections ────────────────────────

/**
 * Returns matching correction_log rows for a given entity_id + free-text
 * trigger context. Match precedence:
 *   1. exact entity_id (when available on row)
 *   2. ≥2 overlapping significant tokens between the candidate's trigger
 *      context and the row's trigger_context
 * Single-token coincidences are deliberately ignored.
 */
export function matchCorrections(
  rows: CorrectionRow[],
  candidate: { entity_id?: string | null; trigger_context?: string | null },
): CorrectionRow[] {
  const eid = candidate.entity_id || ''
  const candTokens = significantTokens(candidate.trigger_context || '')
  const candTokenSet = new Set(candTokens)

  const out: CorrectionRow[] = []
  for (const r of rows) {
    // Path A — exact entity_id match (newest data path)
    if (eid && r.entity_id && r.entity_id === eid) {
      out.push(r)
      continue
    }
    // Path B — fuzzy fallback. Require ≥2 overlapping significant tokens.
    if (candTokens.length === 0) continue
    const rowTokens = significantTokens(`${r.trigger_action || ''} ${r.trigger_context || ''}`)
    let overlap = 0
    for (const t of rowTokens) {
      if (candTokenSet.has(t)) {
        overlap++
        if (overlap >= 2) { out.push(r); break }
      }
    }
  }
  return out
}

// ── Weighted scoring ─────────────────────────────────────────────

/**
 * Returns a score adjustment for the given candidate, derived from
 * past corrections. Negative for dismissals, positive for approvals.
 *
 * Weights:
 *   - explicit dismiss: −10 each (max −30 from dismissals)
 *   - implicit ignore : −5 each (max −15)
 *   - explicit approve: +5 each (max +15)
 */
export function scoreFromCorrections(matches: CorrectionRow[]): {
  score: number
  dismiss_count: number
  approve_count: number
  breakdown: Record<string, number>
} {
  let dismissPts = 0
  let approvePts = 0
  let dismissCount = 0
  let approveCount = 0

  for (const r of matches) {
    const ua = String(r.user_action || '').toLowerCase()
    const explicit = (r.signal_strength || 'explicit') === 'explicit'
    if (/dismiss|reject|skip/.test(ua)) {
      dismissCount++
      dismissPts += explicit ? -10 : -5
    } else if (/act|approve|approved|action_taken/.test(ua)) {
      approveCount++
      approvePts += 5
    }
  }
  dismissPts = Math.max(-30, dismissPts)
  approvePts = Math.min(15, approvePts)

  return {
    score: dismissPts + approvePts,
    dismiss_count: dismissCount,
    approve_count: approveCount,
    breakdown: { dismiss: dismissPts, approve: approvePts },
  }
}

// ── Convenience: load + match in one pool query ──────────────────

export async function loadCorrectionsForCandidate(
  pool: Pool,
  candidate: { entity_id?: string | null; trigger_context?: string | null },
  windowDays: number = 90,
): Promise<CorrectionRow[]> {
  try {
    const r = await pool.query(
      `SELECT entity_id, trigger_action, trigger_context, user_action,
              signal_strength, dismiss_source, timestamp
       FROM correction_log
       WHERE timestamp > NOW() - ($1 || ' days')::interval`,
      [String(windowDays)],
    )
    return matchCorrections(r.rows as CorrectionRow[], candidate)
  } catch {
    return []
  }
}

// ── Insert with the new precision fields ─────────────────────────

export async function insertCorrectionWithEntity(
  pool: Pool,
  args: {
    entity_id?: string | null
    trigger_action: string
    trigger_context: string
    user_action: string
    correction_text?: string
    signal_strength?: 'explicit' | 'implicit'
    dismiss_source?: 'feed' | 'overlay' | 'whisper' | string
    dismiss_reason?: string | null
  },
): Promise<string> {
  const id = crypto.randomUUID()
  await pool.query(
    `INSERT INTO correction_log
       (id, entity_id, trigger_action, trigger_context, user_action,
        correction_text, signal_strength, dismiss_source, dismiss_reason,
        reasoning_chain, affected_claims)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '', '[]')`,
    [
      id,
      args.entity_id || null,
      args.trigger_action.slice(0, 500),
      String(args.trigger_context || '').slice(0, 2000),
      args.user_action,
      String(args.correction_text || '').slice(0, 2000),
      args.signal_strength || 'explicit',
      args.dismiss_source || null,
      args.dismiss_reason || null,
    ],
  )
  return id
}

/**
 * Active Learning Loop
 *
 * Captures every user correction, dismissal, edit, and contradiction.
 * Feeds corrections back into the persona writer so IRIS doesn't repeat
 * the same mistakes.
 *
 * Hooks:
 * - Corner feed dismissals
 * - Spotlight response edits / "that's wrong"
 * - Draft rewrites
 * - Clarifying question answers that contradict persona predictions
 *
 * On every persona rewrite, reads unapplied corrections from correction_log,
 * lowers confidence on affected claims, generates lessons learned, and
 * adds a "Lessons Learned" section to the persona.
 */

import {
  getRecentCorrections, markCorrectionsApplied,
  upsertLesson, getActiveLessons,
  getLivingProfile, setLivingProfile, listInsights, getSettings
} from './db'

// ── Process a new correction ──────────────────────────────────────

/**
 * Called when a user correction is logged. Immediately generates a lesson
 * and flags affected persona claims for confidence reduction.
 */
export async function processCorrection(
  triggerAction: string,
  userAction: string,
  correctionText: string,
  triggerContext: string
): Promise<{ lessonId: string; affectedClaims: string[] }> {
  // Identify which persona claims were likely involved
  const persona = await getLivingProfile() || ''
  const affectedClaims = findAffectedClaims(persona, triggerAction, triggerContext)

  // Generate the lesson: what did IRIS believe vs what's correct
  const originalBelief = inferOriginalBelief(triggerAction, triggerContext)
  const correctedBelief = correctionText || `User ${userAction}: ${triggerAction.slice(0, 100)}`

  const lessonId = await upsertLesson(
    originalBelief,
    correctedBelief,
    `${userAction} on ${new Date().toISOString().slice(0, 10)}`
  )

  console.log(`[learning] Correction processed: "${originalBelief.slice(0, 60)}" → "${correctedBelief.slice(0, 60)}"`)

  return { lessonId, affectedClaims }
}

// ── Detect "that's wrong" in spotlight queries ────────────────────

const CORRECTION_PATTERNS = [
  /\bthat'?s?\s+wrong\b/i,
  /\bthat'?s?\s+not\s+(?:right|correct|true|accurate)\b/i,
  /\bno[,.]?\s+(?:that's|it's|you're)\s+wrong\b/i,
  /\bwrong\s+(?:answer|response)\b/i,
  /\bincorrect\b/i,
  /\byou(?:'re| are)\s+(?:wrong|mistaken|confused)\b/i,
  /\bactually[,.]?\s+(?:it's|I|that|no)\b/i,
  /\bdon'?t\s+(?:say|do|suggest|recommend)\s+that\b/i,
  /\bstop\s+(?:doing|saying|suggesting)\b/i,
  /\bnever\s+(?:do|say|suggest)\s+that\b/i,
]

/**
 * Check if a spotlight query is a correction/complaint about a previous response.
 * Returns the correction text if detected, null otherwise.
 */
export function detectCorrectionInQuery(
  query: string,
  previousResponse: string | null
): { isCorrection: boolean; correctionText: string } {
  for (const pattern of CORRECTION_PATTERNS) {
    if (pattern.test(query)) {
      return {
        isCorrection: true,
        correctionText: query
      }
    }
  }

  return { isCorrection: false, correctionText: '' }
}

// ── Detect draft edits ────────────────────────────────────────────

/**
 * When user edits a draft (spotlight sends "original query → Edit"),
 * detect what changed and log it as a correction.
 */
export function detectDraftEdit(
  query: string,
  previousQuery: string | null,
  previousResponse: string | null
): { isDraftEdit: boolean; editedPart: string; originalDraft: string } {
  // Detect "→ Edit" pattern from spotlight action buttons
  const editMatch = query.match(/^(.+?)\s*→\s*Edit$/i)
  if (editMatch && previousResponse) {
    return {
      isDraftEdit: true,
      editedPart: editMatch[1],
      originalDraft: previousResponse
    }
  }
  return { isDraftEdit: false, editedPart: '', originalDraft: '' }
}

// ── Build lessons for persona writer ──────────────────────────────

/**
 * Called during persona rewrite. Returns a formatted section of lessons
 * learned that should be included in the persona, plus a list of
 * correction-informed adjustments.
 */
export async function buildLessonsForPersonaWriter(): Promise<{
  lessonsSection: string
  correctionAdjustments: string
  correctionIds: string[]
}> {
  // Get unapplied corrections
  const corrections = await getRecentCorrections(20)

  // Get all active lessons
  const lessons = await getActiveLessons()

  // Build the corrections adjustment block for the persona writer
  let correctionAdjustments = ''
  if (corrections.length > 0) {
    const adjustments = corrections.map((c: any) => {
      const affected = Array.isArray(c.affected_claims) ? c.affected_claims :
        (typeof c.affected_claims === 'string' ? JSON.parse(c.affected_claims) : [])
      return `- CORRECTION (${c.user_action}): IRIS did "${(c.trigger_action || '').slice(0, 80)}". ` +
        `User response: "${(c.correction_text || c.user_action || '').slice(0, 100)}". ` +
        `${affected.length > 0 ? `Affected claims: ${affected.join(', ')}. ` : ''}` +
        `LOWER confidence on related persona claims. Do NOT repeat this pattern.`
    })
    correctionAdjustments = `USER CORRECTIONS TO APPLY (${corrections.length} unapplied):\n${adjustments.join('\n')}\n\nFor each correction above, find the persona claim that caused the wrong behavior, lower its confidence by 20 points, and note the correction.`
  }

  // Build the lessons learned section
  let lessonsSection = ''
  if (lessons.length > 0) {
    const lessonLines = lessons.map((l: any) => {
      const reinforced = l.times_reinforced > 1 ? ` (reinforced ${l.times_reinforced}×)` : ''
      const date = new Date(l.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
      return `- I used to think: "${l.original_belief}" → Corrected to: "${l.corrected_to}" (${date}${reinforced})`
    })
    lessonsSection = `## Lessons Learned\n*Corrections from direct user feedback — never repeat these mistakes.*\n\n${lessonLines.join('\n')}`
  }

  return {
    lessonsSection,
    correctionAdjustments,
    correctionIds: corrections.map((c: any) => c.id)
  }
}

// ── Helpers ───────────────────────────────────────────────────────

function findAffectedClaims(persona: string, triggerAction: string, context: string): string[] {
  const claims: string[] = []

  // Extract key entities from the trigger action
  const keywords = extractKeywords(triggerAction + ' ' + context)

  // Find persona paragraphs that contain these keywords
  const paragraphs = persona.split('\n\n')
  for (const para of paragraphs) {
    const paraLower = para.toLowerCase()
    const matchCount = keywords.filter(k => paraLower.includes(k.toLowerCase())).length
    if (matchCount >= 2) {
      // This paragraph likely informed the wrong action
      const firstSentence = para.split(/[.!?]/)[0]?.trim()
      if (firstSentence && firstSentence.length > 10) {
        claims.push(firstSentence.slice(0, 100))
      }
    }
  }

  return claims.slice(0, 3)
}

function inferOriginalBelief(triggerAction: string, context: string): string {
  // Try to extract what IRIS believed from the trigger action
  if (triggerAction.includes('draft')) {
    return `IRIS drafted communication with a certain tone/style that the user rejected`
  }
  if (triggerAction.includes('recommend') || triggerAction.includes('suggest')) {
    return `IRIS recommended an action the user didn't want: ${triggerAction.slice(0, 100)}`
  }
  if (triggerAction.includes('priority') || triggerAction.includes('focus')) {
    return `IRIS prioritized incorrectly: ${triggerAction.slice(0, 100)}`
  }

  return `IRIS action rejected: ${triggerAction.slice(0, 120)}`
}

function extractKeywords(text: string): string[] {
  const caps = text.match(/\b[A-Z][a-zA-Z]{3,}\b/g) || []
  const lower = text.match(/\b[a-z]{5,}\b/g) || []
  const all = [...caps, ...lower]
  return all.filter((w, i) => all.indexOf(w) === i).slice(0, 10)
}

/**
 * Persona Confidence Scorer
 *
 * Tags every claim in the persona with a confidence score derived from:
 * - Number of supporting observations/insights
 * - Source diversity (how many different data sources)
 * - Recency of most recent evidence
 * - Contradiction detection
 *
 * Output format: Each persona section gets inline confidence annotations
 * that the context builder can parse for the spotlight prompt.
 *
 * Confidence levels:
 * - HIGH (>80): 5+ observations, 2+ sources, evidence within 7 days
 * - MEDIUM (50-80): 2-4 observations, or single source, or older evidence
 * - LOW (<50): 1 observation, no corroboration, or contradicted
 */

import { getPool } from './db'
import { getLivingProfile, listInsights, getSettings, setLivingProfile, markCorrectionsApplied } from './db'
import { buildLessonsForPersonaWriter } from './active-learning'
import { invalidateCache } from './context-cache'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface ClaimConfidence {
  claim: string
  score: number                    // 0-100
  supportingObservations: number
  sourceDiversity: number          // unique source types
  sources: string[]
  mostRecentEvidence: string       // ISO date
  contradicted: boolean
  level: 'high' | 'medium' | 'low'
}

// ── Main: rewrite persona with confidence tags ────────────────────

export async function rewritePersonaWithConfidence(): Promise<string | null> {
  const settings = await getSettings()
  const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
  if (!apiKey) return null

  const currentPersona = await getLivingProfile()
  if (!currentPersona) return null

  const pool = getPool()

  // Load all active insights with full metadata
  const allInsights = await listInsights('active')

  // Build insight evidence map: for each insight, compute confidence metadata
  const insightScores = await Promise.all(
    allInsights.map(async (insight: any) => {
      const evidence = parseEvidence(insight.evidence)
      const sourceDiversity = new Set(evidence).size || insight.source_count || 1

      // Check for contradicting insights
      const contradictions = allInsights.filter((other: any) =>
        other.id !== insight.id &&
        other.status === 'active' &&
        isContradiction(insight.statement, other.statement)
      )

      // Get most recent observation that supports this insight
      let recentDate = insight.last_confirmed || insight.first_detected || new Date().toISOString()

      // Compute confidence score
      const obsCount = insight.times_confirmed || 1
      const sources = sourceDiversity
      const recencyDays = daysSince(recentDate)

      let score = 0
      // Base: observation count (max 40 points)
      score += Math.min(40, obsCount * 8)
      // Source diversity (max 25 points)
      score += Math.min(25, sources * 10)
      // Recency bonus (max 20 points, decays over 30 days)
      score += Math.max(0, 20 - (recencyDays * 0.67))
      // Confirmation bonus (max 15 points)
      score += Math.min(15, (insight.times_confirmed || 0) * 3)
      // Contradiction penalty
      if (contradictions.length > 0) score = Math.max(10, score - 30)

      score = Math.min(100, Math.round(score))

      return {
        statement: insight.statement,
        category: insight.category,
        confidence: insight.confidence,
        computedScore: score,
        obsCount,
        sourceDiversity: sources,
        recencyDays: Math.round(recencyDays),
        contradicted: contradictions.length > 0,
        level: score > 80 ? 'high' as const : score >= 50 ? 'medium' as const : 'low' as const
      }
    })
  )

  // Group insights by category with scores
  const byCategory: Record<string, typeof insightScores> = {}
  for (const s of insightScores) {
    if (!byCategory[s.category]) byCategory[s.category] = []
    byCategory[s.category].push(s)
  }

  const insightBlock = Object.entries(byCategory).map(([cat, items]) => {
    const lines = items
      .sort((a, b) => b.computedScore - a.computedScore)
      .map(i => {
        const tag = `[CONF:${i.computedScore}|OBS:${i.obsCount}|SRC:${i.sourceDiversity}|AGE:${i.recencyDays}d${i.contradicted ? '|CONTRADICTED' : ''}]`
        return `${tag} ${i.statement}`
      })
    return `### ${cat.toUpperCase()}\n${lines.join('\n\n')}`
  }).join('\n\n')

  const today = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })

  // Load corrections and lessons learned
  const { lessonsSection, correctionAdjustments, correctionIds } = await buildLessonsForPersonaWriter()

  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const client = new Anthropic({ apiKey })

    const response = await client.messages.create({
      model: settings.model || 'claude-sonnet-4-20250514',
      max_tokens: 4000,
      system: `You are writing the definitive understanding of a person. Today is ${today}.

CRITICAL INSTRUCTION: Every factual claim in the persona MUST include an inline confidence tag.

Format: Each claim or observation you make should be followed by a confidence tag in this exact format:
<!--CONF:85|OBS:12|SRC:3|RECENT:2d-->

Where:
- CONF = confidence score 0-100 (derived from the insight evidence you're given)
- OBS = number of supporting observations
- SRC = number of distinct data sources
- RECENT = days since most recent evidence

Rules for setting confidence:
- If an insight has [CONF:90|OBS:5|SRC:3|AGE:1d], your claim based on it should be CONF:90
- If you're synthesizing multiple insights, take the LOWEST confidence of the components
- If you're speculating beyond what insights directly support, drop confidence by 20-30 points
- If an insight is marked CONTRADICTED, the claim should be ≤40 and noted as uncertain
- Never assert something at >80 confidence unless you have 3+ observations from 2+ sources
- If a USER CORRECTION exists for a claim, the corrected version should replace the old claim

${correctionAdjustments ? `\n${correctionAdjustments}\n` : ''}

Structure:
# My Understanding of Kavin
*Last rewritten: ${today}*

## Who He Really Is
## How He Operates
## His Relationships
## His Contradictions
## His Emotional Landscape
## His Trajectory
## What I Still Don't Understand
## Lessons Learned

The "Lessons Learned" section is MANDATORY. It lists every correction the user has made, formatted as:
"I used to think X → Corrected to Y (date)"
This section is always included and never shortened.

Write as "I believe..." not "Kavin is...". Be specific with evidence. Every paragraph should end with its confidence tag.`,
      messages: [{
        role: 'user',
        content: `CURRENT PERSONA:\n${currentPersona}\n\n---\n\nACTIVE INSIGHTS WITH EVIDENCE SCORES (${allInsights.length}):\n\n${insightBlock}${lessonsSection ? `\n\n---\n\nEXISTING LESSONS LEARNED (preserve and extend):\n${lessonsSection}` : ''}`
      }]
    })

    const newPersona = response.content[0].type === 'text' ? response.content[0].text : ''

    if (newPersona.length > 200 && newPersona.includes('Kavin')) {
      const stamped = newPersona.includes(today) ? newPersona : `*Last rewritten: ${today}*\n\n${newPersona}`
      await setLivingProfile(stamped)

      // Mark corrections as applied
      if (correctionIds.length > 0) {
        await markCorrectionsApplied(correctionIds)
        console.log(`[persona-conf] Applied ${correctionIds.length} corrections`)
      }

      invalidateCache('persona rewrite')
      console.log(`[persona-conf] Rewritten with confidence tags (${stamped.length} chars, ${allInsights.length} insights)`)
      return stamped
    }
  } catch (err: any) {
    console.error('[persona-conf] Rewrite failed:', err.message)
  }

  return null
}

// ── Extract confidence-tagged claims for context builder ──────────

export function extractConfidenceClaims(persona: string): {
  highConfidence: string[]
  mediumConfidence: string[]
  lowConfidence: string[]
  summary: string
} {
  const high: string[] = []
  const medium: string[] = []
  const low: string[] = []

  // Parse <!--CONF:XX|...--> tags from persona text
  const tagPattern = /([^<\n]+?)<!--CONF:(\d+)\|OBS:(\d+)\|SRC:(\d+)\|RECENT:(\d+)d-->/g
  let match: RegExpExecArray | null

  while ((match = tagPattern.exec(persona)) !== null) {
    const claim = match[1].trim()
    const conf = parseInt(match[2])
    const obs = parseInt(match[3])
    const src = parseInt(match[4])
    const recent = parseInt(match[5])

    const tagged = `${claim} [${conf}% confidence, ${obs} observations, ${src} sources, ${recent}d old]`

    if (conf > 80) high.push(tagged)
    else if (conf >= 50) medium.push(tagged)
    else low.push(tagged)
  }

  // If no tags found (old-format persona), treat everything as medium
  if (high.length === 0 && medium.length === 0 && low.length === 0) {
    return {
      highConfidence: [],
      mediumConfidence: [persona.slice(0, 2000)],
      lowConfidence: [],
      summary: persona.slice(0, 2000)
    }
  }

  // Build summary: high confidence facts stated directly, medium with hedge, low omitted
  const summaryParts = [
    ...high.slice(0, 15).map(c => c.split(' [')[0]), // Strip tags for the summary
    ...medium.slice(0, 10).map(c => `(likely) ${c.split(' [')[0]}`)
  ]

  return {
    highConfidence: high,
    mediumConfidence: medium,
    lowConfidence: low,
    summary: summaryParts.join('\n')
  }
}

// ── Build confidence-aware context string ─────────────────────────

export function buildConfidenceContext(persona: string): string {
  const { highConfidence, mediumConfidence, lowConfidence } = extractConfidenceClaims(persona)

  if (highConfidence.length === 0 && mediumConfidence.length === 0) {
    // No confidence tags — return persona as-is (backwards compatible)
    return persona.slice(0, 2000)
  }

  const parts: string[] = []

  if (highConfidence.length > 0) {
    parts.push(`KNOWN FACTS (high confidence, state directly):\n${highConfidence.slice(0, 15).map(c => `- ${c}`).join('\n')}`)
  }

  if (mediumConfidence.length > 0) {
    parts.push(`PROBABLE (medium confidence, hedge slightly — "seems to", "appears to"):\n${mediumConfidence.slice(0, 10).map(c => `- ${c}`).join('\n')}`)
  }

  if (lowConfidence.length > 0) {
    parts.push(`UNCERTAIN (low confidence — only mention if directly asked, with explicit uncertainty):\n${lowConfidence.slice(0, 5).map(c => `- ${c}`).join('\n')}`)
  }

  return parts.join('\n\n')
}

// ── Helpers ───────────────────────────────────────────────────────

function parseEvidence(evidence: any): string[] {
  if (Array.isArray(evidence)) return evidence
  if (typeof evidence === 'string') {
    try { return JSON.parse(evidence) } catch { return [] }
  }
  return []
}

function daysSince(isoDate: string): number {
  const d = new Date(isoDate)
  if (isNaN(d.getTime())) return 30
  return Math.max(0, (Date.now() - d.getTime()) / 86400000)
}

function isContradiction(a: string, b: string): boolean {
  // Simple heuristic: same topic area but opposite sentiment
  const aLower = a.toLowerCase()
  const bLower = b.toLowerCase()

  // Extract key entities (capitalized words 4+ chars)
  const aEntities = new Set((a.match(/\b[A-Z][a-zA-Z]{3,}\b/g) || []).map(e => e.toLowerCase()))
  const bEntities = new Set((b.match(/\b[A-Z][a-zA-Z]{3,}\b/g) || []).map(e => e.toLowerCase()))

  // Check entity overlap (same topic)
  let overlap = 0
  for (const e of aEntities) if (bEntities.has(e)) overlap++
  if (overlap < 1) return false // different topics, not a contradiction

  // Check for negation patterns
  const negations = [
    ['increase', 'decrease'], ['more', 'less'], ['active', 'inactive'],
    ['engaged', 'disengaged'], ['consistent', 'inconsistent'],
    ['improving', 'declining'], ['confident', 'anxious'],
    ['organized', 'disorganized'], ['proactive', 'reactive']
  ]

  for (const [pos, neg] of negations) {
    if ((aLower.includes(pos) && bLower.includes(neg)) ||
        (aLower.includes(neg) && bLower.includes(pos))) {
      return true
    }
  }

  return false
}

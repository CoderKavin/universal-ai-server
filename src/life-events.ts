/**
 * Life Event Detector — real-time detection of major life changes.
 *
 * Runs on EVERY new observation (not batched). Watches for patterns
 * that signal a status change: acceptances, rejections, role changes,
 * relationship shifts, location changes, medical events, new projects.
 *
 * When a life event is detected:
 * 1. Emit a high-priority life_event observation
 * 2. Store in life_events table with evidence trail
 * 3. Trigger an immediate persona rewrite (don't wait for 15-min cycle)
 *
 * Design principle: high precision, low recall. Better to miss an event
 * than to falsely flag one. Confidence thresholds are strict.
 */

import { getPool } from './db'
import { getLivingProfile, setLivingProfile, listInsights, insertObservation, getSettings } from './db'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface LifeEvent {
  id: string
  type: string
  summary: string
  confidence: number
  evidence: { source: string; content: string; timestamp: string }[]
  detected_at: string
}

type EventType =
  | 'acceptance'
  | 'rejection'
  | 'role_change'
  | 'relationship_shift'
  | 'location_change'
  | 'schedule_break'
  | 'loss'
  | 'new_project'
  | 'medical'
  | 'financial'
  | 'achievement'

// ── Pattern definitions ───────────────────────────────────────────

const ACCEPTANCE_PATTERNS = [
  /\bcongratulations?\b/i,
  /\bpleased to (?:offer|inform|announce|confirm)\b/i,
  /\baccepted\b.*\b(?:application|admission|offer|position|program|scholarship)\b/i,
  /\badmission decision\b/i,
  /\boffer (?:of|letter)\b/i,
  /\bwelcome (?:to|aboard)\b.*\b(?:class|cohort|program|team)\b/i,
  /\bscholarship\b.*\b(?:awarded|granted|received)\b/i,
  /\byou(?:'ve| have) been (?:selected|chosen|accepted|admitted)\b/i,
]

const REJECTION_PATTERNS = [
  /\bregret to inform\b/i,
  /\bunfortunately\b.*\b(?:unable|cannot|not able|not selected|not been)\b/i,
  /\bnot (?:been )?(?:selected|accepted|admitted|chosen|successful)\b/i,
  /\bafter careful (?:consideration|review)\b.*\b(?:unable|cannot|not)\b/i,
  /\bwe (?:are |were )?unable to offer\b/i,
  /\bapplication.*(?:unsuccessful|denied|declined)\b/i,
  /\bposition has been filled\b/i,
]

const RELATIONSHIP_PATTERNS = [
  { pattern: /\b(?:broke up|broken up|breaking up|we're done|it's over)\b/i, subtype: 'breakup' },
  { pattern: /\b(?:engaged|proposal|said yes|she said yes|he said yes)\b/i, subtype: 'engagement' },
  { pattern: /\b(?:got married|wedding|tied the knot|just married)\b/i, subtype: 'marriage' },
  { pattern: /\b(?:pregnant|expecting|baby on the way|going to be a)\b/i, subtype: 'pregnancy' },
  { pattern: /\b(?:divorced|separation|separated)\b/i, subtype: 'separation' },
]

const LOSS_PATTERNS = [
  /\b(?:passed away|passed on|no more|rest in peace|RIP|demise)\b/i,
  /\b(?:funeral|condolences|bereavement|mourning)\b/i,
  /\b(?:lost (?:my|his|her|our) (?:father|mother|dad|mom|mum|brother|sister|grandfather|grandmother|friend))\b/i,
  /\b(?:sadly|tragically).*\b(?:died|death|passed|lost)\b/i,
]

const MEDICAL_PATTERNS = [
  /\b(?:diagnosed with|diagnosis)\b/i,
  /\b(?:surgery|operation|procedure)\b.*\b(?:scheduled|underwent|completed|successful)\b/i,
  /\b(?:prescription|prescribed)\b.*\b(?:medication|medicine|treatment)\b/i,
  /\b(?:hospital|admitted|discharged|emergency room|ER visit)\b/i,
  /\b(?:test results|biopsy|MRI|CT scan|X-ray)\b.*\b(?:came back|results|positive|negative)\b/i,
]

const NEW_PROJECT_PATTERNS = [
  /\b(?:launching|just launched|starting|just started|founding|co-founding)\b.*\b(?:company|startup|project|venture|business|app)\b/i,
  /\b(?:new (?:role|position|job) at)\b/i,
  /\b(?:joined|joining)\b.*\b(?:as|team|company|startup)\b/i,
  /\b(?:promoted to|promotion)\b/i,
]

const FINANCIAL_PATTERNS = [
  /\b(?:funding|raised|investment|investor|seed round|series [A-D])\b.*\b(?:closed|received|secured|approved)\b/i,
  /\b(?:loan|mortgage)\b.*\b(?:approved|sanctioned|disbursed)\b/i,
  /\b(?:scholarship|grant|fellowship|award)\b.*\b(?:of|worth|amount|received).*(?:₹|\$|€|£|lakh|crore)\b/i,
]

const ACHIEVEMENT_PATTERNS = [
  /\b(?:won|winner|first (?:place|prize|rank)|gold medal|champion)\b/i,
  /\b(?:published|paper accepted|article published)\b/i,
  /\b(?:graduated|graduation|commencement|convocation)\b/i,
  /\b(?:certified|certification|license|licensure)\b.*\b(?:passed|received|earned)\b/i,
  /\b(?:record|milestone|breakthrough)\b.*\b(?:achieved|reached|hit|crossed)\b/i,
]

// ── Recent WiFi tracking for location changes ─────────────────────

const wifiHistory: Map<string, number> = new Map() // network → count in last 48h

// ── Core: analyze a single observation ────────────────────────────

export async function analyzeForLifeEvent(
  observationId: string,
  source: string,
  eventType: string,
  content: string,
  timestamp: string
): Promise<LifeEvent | null> {
  if (!content || content.length < 10) return null

  // Skip observations that are clearly not life-event candidates
  const skipSources = new Set(['daily_digest', 'weekly_digest', 'trend_detector', 'life_event', 'system'])
  if (skipSources.has(source)) return null

  const contentLower = content.toLowerCase()
  const evidence = [{ source, content: content.slice(0, 500), timestamp }]

  // ── Check each pattern category ─────────────────────────────────

  // 1. Acceptance emails
  if (source === 'email' && eventType === 'received') {
    for (const pattern of ACCEPTANCE_PATTERNS) {
      if (pattern.test(content)) {
        // Cross-validate: does it look like a real acceptance, not just someone using the word?
        const hasInstitution = /\b(?:university|college|school|program|position|role|company|institute|scholarship)\b/i.test(content)
        if (hasInstitution) {
          return createLifeEvent('acceptance', `Acceptance detected: ${extractSubject(content)}`, 0.85, evidence)
        }
      }
    }
  }

  // 2. Rejection emails
  if (source === 'email' && eventType === 'received') {
    for (const pattern of REJECTION_PATTERNS) {
      if (pattern.test(content)) {
        const hasInstitution = /\b(?:application|admission|position|role|program|candidacy)\b/i.test(content)
        if (hasInstitution) {
          return createLifeEvent('rejection', `Rejection detected: ${extractSubject(content)}`, 0.80, evidence)
        }
      }
    }
  }

  // 3. Relationship shifts (any message source)
  if (source === 'whatsapp' || source === 'email') {
    for (const { pattern, subtype } of RELATIONSHIP_PATTERNS) {
      if (pattern.test(content)) {
        // Require personal context — not a news article or movie quote
        const isPersonal = /\b(?:I|we|my|me|us|our)\b/i.test(content)
        if (isPersonal) {
          return createLifeEvent('relationship_shift', `Relationship change (${subtype}): ${content.slice(0, 100)}`, 0.70, evidence)
        }
      }
    }
  }

  // 4. Loss/death
  if (source === 'whatsapp' || source === 'email') {
    for (const pattern of LOSS_PATTERNS) {
      if (pattern.test(content)) {
        const isPersonal = /\b(?:I|we|my|our|his|her)\b/i.test(content)
        if (isPersonal) {
          return createLifeEvent('loss', `Loss detected: ${content.slice(0, 120)}`, 0.75, evidence)
        }
      }
    }
  }

  // 5. Medical events
  if (source === 'email' || source === 'whatsapp' || source === 'calendar') {
    for (const pattern of MEDICAL_PATTERNS) {
      if (pattern.test(content)) {
        return createLifeEvent('medical', `Medical event: ${content.slice(0, 120)}`, 0.65, evidence)
      }
    }
  }

  // 6. New project/role mentions appearing across multiple sources
  if (source === 'email' || source === 'whatsapp' || source === 'centrum') {
    for (const pattern of NEW_PROJECT_PATTERNS) {
      if (pattern.test(content)) {
        // Check if this is corroborated by observations in the last 48 hours
        const corroboration = await checkCorroboration(content, timestamp)
        const confidence = corroboration ? 0.80 : 0.55
        if (confidence >= 0.55) {
          return createLifeEvent('new_project', `New project/role: ${content.slice(0, 120)}`, confidence, evidence)
        }
      }
    }
  }

  // 7. Financial events
  if (source === 'email') {
    for (const pattern of FINANCIAL_PATTERNS) {
      if (pattern.test(content)) {
        return createLifeEvent('financial', `Financial event: ${extractSubject(content)}`, 0.75, evidence)
      }
    }
  }

  // 8. Achievement
  if (source === 'email' || source === 'whatsapp') {
    for (const pattern of ACHIEVEMENT_PATTERNS) {
      if (pattern.test(content)) {
        const isPersonal = /\b(?:I|we|my|our|you)\b/i.test(content)
        if (isPersonal) {
          return createLifeEvent('achievement', `Achievement: ${content.slice(0, 120)}`, 0.70, evidence)
        }
      }
    }
  }

  // 9. Location change (WiFi-based)
  if (source === 'wifi') {
    const network = content.trim()
    const count = (wifiHistory.get(network) || 0) + 1
    wifiHistory.set(network, count)

    // New network seen 3+ times = likely moved/traveling
    if (count === 3) {
      const pool = getPool()
      // Check if this is a genuinely new network (not seen in the last 30 days of observations)
      const existing = await pool.query(
        "SELECT COUNT(*)::int as c FROM observations WHERE source='wifi' AND raw_content=$1 AND timestamp < NOW() - INTERVAL '7 days'",
        [network]
      )
      if (existing.rows[0].c === 0) {
        return createLifeEvent('location_change', `New location detected: connected to "${network}" 3+ times (new network)`, 0.60, evidence)
      }
    }
  }

  // 10. Schedule pattern break (calendar events)
  if (source === 'calendar') {
    // Detect if a recurring event disappeared or a new recurring event appeared
    const isRecurring = /\brecurring\b/i.test(content) || /\bevery\b/i.test(content)
    const isNew = eventType === 'created'
    if (isRecurring && isNew) {
      return createLifeEvent('schedule_break', `New recurring schedule: ${content.slice(0, 120)}`, 0.50, evidence)
    }
  }

  return null
}

// ── Helpers ───────────────────────────────────────────────────────

function extractSubject(content: string): string {
  const subjectMatch = content.match(/Subject:\s*(.+?)(?:\n|$)/i)
  if (subjectMatch) return subjectMatch[1].trim().slice(0, 100)
  // First meaningful line
  const firstLine = content.split('\n').find(l => l.trim().length > 10)
  return firstLine?.trim().slice(0, 100) || content.slice(0, 80)
}

async function checkCorroboration(content: string, timestamp: string): Promise<boolean> {
  const pool = getPool()
  // Extract key nouns from content (words 5+ chars, capitalized)
  const keywords = content.match(/\b[A-Z][a-zA-Z]{4,}\b/g) || []
  if (keywords.length === 0) return false

  const cutoff = new Date(new Date(timestamp).getTime() - 48 * 3600 * 1000).toISOString()

  for (const keyword of keywords.slice(0, 5)) {
    const result = await pool.query(
      "SELECT COUNT(*)::int as c FROM observations WHERE timestamp >= $1 AND raw_content ILIKE $2 AND source != 'life_event'",
      [cutoff, `%${keyword}%`]
    )
    if (result.rows[0].c >= 2) return true // keyword appears in 2+ other observations
  }
  return false
}

// ── Create and store a life event ─────────────────────────────────

async function createLifeEvent(
  type: EventType,
  summary: string,
  confidence: number,
  evidence: { source: string; content: string; timestamp: string }[]
): Promise<LifeEvent> {
  const pool = getPool()
  const id = crypto.randomUUID()
  const now = new Date().toISOString()

  const event: LifeEvent = {
    id,
    type,
    summary,
    confidence,
    evidence,
    detected_at: now
  }

  // 1. Store in life_events table
  await pool.query(
    `INSERT INTO life_events (id, type, summary, confidence, evidence, detected_at)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [id, type, summary, confidence, JSON.stringify(evidence), now]
  )

  // 2. Emit high-priority observation
  await insertObservation(
    'life_event', type, summary,
    { type, confidence, evidence_count: evidence.length },
    confidence > 0.7 ? 'significant' : 'neutral',
    [], `LIFE EVENT: ${summary}`
  )

  // 3. Store as a high-confidence insight
  await pool.query(
    `INSERT INTO insights (id, statement, evidence, confidence, category, status, source_count)
     VALUES ($1, $2, $3, $4, 'life_event', 'active', $5)
     ON CONFLICT (id) DO UPDATE SET statement=$2, confidence=$4, last_confirmed=NOW()`,
    [`life-event-${id}`, `LIFE EVENT (${type}): ${summary}`, JSON.stringify(evidence.map(e => e.source)), confidence, evidence.length]
  )

  console.log(`[life-event] Detected: ${type} — "${summary}" (confidence: ${(confidence * 100).toFixed(0)}%)`)

  // 4. Invalidate context cache immediately
  import('./context-cache').then(m => m.invalidateCache(`life event: ${type}`)).catch(() => {})

  // 5. Trigger immediate persona rewrite
  triggerPersonaRewrite(type, summary).catch(err => {
    console.error('[life-event] Persona rewrite failed:', err.message)
  })

  // 5. Regenerate predicted actions (life event may invalidate existing predictions)
  import('./action-predictor').then(m => {
    m.regenerateOnEvent(`Life event: ${type} — ${summary}`).catch(() => {})
  }).catch(() => {})

  // 6. Generate reasoning chain for the life event
  import('./chain-reasoner').then(m => {
    m.generateChain(`Life event: ${summary}`, `life_event_${type}`, `life-${type}`).catch(() => {})
  }).catch(() => {})

  return event
}

// ── Persona rewrite (server-side) ─────────────────────────────────

async function triggerPersonaRewrite(eventType: string, eventSummary: string): Promise<void> {
  const settings = await getSettings()
  const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
  if (!apiKey) {
    console.log('[life-event] No API key — skipping persona rewrite')
    return
  }

  const currentPersona = await getLivingProfile()
  if (!currentPersona) {
    console.log('[life-event] No existing persona — skipping rewrite')
    return
  }

  const allInsights = await listInsights('active')
  const insightBlock = allInsights.slice(0, 30).map((i: any) =>
    `[${(i.confidence * 100).toFixed(0)}%] ${i.statement}`
  ).join('\n')

  const today = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })

  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const client = new Anthropic({ apiKey })

    const response = await client.messages.create({
      model: settings.model || 'claude-sonnet-4-20250514',
      max_tokens: 3000,
      system: `You are rewriting the definitive understanding of a person because a MAJOR LIFE EVENT just occurred.

Today is ${today}. A ${eventType} event was detected: "${eventSummary}"

This is significant. The persona must be updated to reflect this change. Preserve all existing understanding but integrate this new reality. The event should appear prominently in the relevant section (Trajectory, Emotional Landscape, or wherever it fits).

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

RULES: Write as "I believe..." not "Kavin is...". Be specific with evidence. Reference the life event explicitly. Preserve factual reference data.`,
      messages: [{
        role: 'user',
        content: `CURRENT PERSONA:\n${currentPersona}\n\n---\n\nLIFE EVENT JUST DETECTED:\nType: ${eventType}\nSummary: ${eventSummary}\n\n---\n\nACTIVE INSIGHTS (${allInsights.length}):\n${insightBlock}`
      }]
    })

    const newPersona = response.content[0].type === 'text' ? response.content[0].text : ''

    if (newPersona.length > 200 && (newPersona.includes('Kavin') || newPersona.includes('understand'))) {
      const stamped = newPersona.includes(today) ? newPersona : `*Last rewritten: ${today}*\n\n${newPersona}`
      await setLivingProfile(stamped)
      console.log(`[life-event] Persona rewritten (${stamped.length} chars) — triggered by ${eventType}`)
    }
  } catch (err: any) {
    console.error('[life-event] Persona rewrite Claude error:', err.message)
  }
}

// ── Periodic WiFi history cleanup ─────────────────────────────────

export function cleanupWifiHistory(): void {
  // Called periodically (e.g., every hour) to prevent memory leak
  // Reset counts so location detection stays fresh
  if (wifiHistory.size > 100) {
    wifiHistory.clear()
  }
}

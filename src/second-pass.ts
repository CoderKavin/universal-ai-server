/**
 * Second-Pass Reasoning — the unasked-context layer.
 *
 * After generating a literal response to a spotlight query, runs a
 * silent second pass that evaluates whether to append urgent unasked
 * context or a strong opinion.
 *
 * STRICT FILTER: most queries get literal-only responses. Additions
 * require evidence from observations. The worst failure mode is IRIS
 * derailing every query with irrelevant interjections.
 *
 * Structure:
 * 1. What did the user literally ask?
 * 2. What's the literal answer? (already generated)
 * 3. Is there something GENUINELY URGENT the user didn't mention?
 *    - Recent life event (last 48h)
 *    - High-severity routine break (3+ days)
 *    - Relationship anomaly (sudden silence from key contact)
 *    - Stale commitment about to escalate
 *    Must be backed by specific observation evidence.
 * 4. Do I have a STRONG opinion? (only if confidence > 80%)
 * 5. Final: literal answer + at most ONE unasked addition + at most ONE opinion.
 *
 * Returns null if no addition warranted (the common case).
 */

import { getPool, getSettings } from './db'

// ── Types ─────────────────────────────────────────────────────────

interface SecondPassInput {
  userQuery: string
  literalResponse: string
  context: string  // full context string already built
}

interface UrgentItem {
  type: 'life_event' | 'routine_break' | 'relationship_anomaly' | 'stale_escalation' | 'deadline'
  summary: string
  evidence: string
  urgencyScore: number // 0-10
}

// ── Main: evaluate whether to add unasked context ─────────────────

export async function runSecondPass(input: SecondPassInput): Promise<string | null> {
  const pool = getPool()

  // Step 1: Gather urgent items from the last 48 hours
  const urgentItems = await gatherUrgentItems(pool)

  // If nothing urgent, return null (literal answer only — the common case)
  if (urgentItems.length === 0) return null

  // Step 2: Filter — only items NOT already addressed in the literal response
  const unaddressed = urgentItems.filter(item => {
    const responseLower = input.literalResponse.toLowerCase()
    const summaryWords = item.summary.toLowerCase().split(/\s+/).filter(w => w.length > 4)
    // If 3+ key words from the urgent item appear in the response, it's already addressed
    const overlap = summaryWords.filter(w => responseLower.includes(w)).length
    return overlap < 3
  })

  if (unaddressed.length === 0) return null

  // Step 3: Pick the single most urgent unaddressed item
  unaddressed.sort((a, b) => b.urgencyScore - a.urgencyScore)
  const topUrgent = unaddressed[0]

  // Step 4: Only proceed if urgency is genuinely high (>= 6/10)
  if (topUrgent.urgencyScore < 6) return null

  // Step 5: Use Claude to compose the addition — must be brief, evidence-based
  const settings = await getSettings()
  const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
  if (!apiKey) return null

  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const { claudeWithFallback } = await import('./claude-fallback')
    const client = new Anthropic({ apiKey })

    const response = await claudeWithFallback(client, {
      model: settings.model || 'claude-sonnet-4-20250514',
      max_tokens: 150,
      system: `You are IRIS's second-pass filter. The user asked a question and got a literal answer. You must decide whether to append ONE brief piece of urgent context.

RULES:
- Return ONLY the addition text (1-2 sentences max), or return exactly "NONE" if no addition is warranted.
- The addition must be GENUINELY urgent — something that would change the user's next action.
- It must cite specific evidence (a date, a name, a number from the data).
- Do NOT repeat anything from the literal answer.
- Do NOT add generic advice, greetings, or filler.
- Format: start with a line break, then the addition. Use the same **entity** and {{metadata}} markup.
- If in doubt, return "NONE". Silence is better than noise.`,
      messages: [{
        role: 'user',
        content: `USER QUERY: "${input.userQuery}"

LITERAL ANSWER ALREADY GIVEN:
${input.literalResponse}

URGENT ITEM TO POTENTIALLY ADD:
Type: ${topUrgent.type}
Summary: ${topUrgent.summary}
Evidence: ${topUrgent.evidence}
Urgency: ${topUrgent.urgencyScore}/10

Should this be appended? If yes, compose the brief addition. If no, return NONE.`
      }]
    }, 'second-pass')

    const addition = response.content[0].type === 'text' ? response.content[0].text.trim() : 'NONE'

    if (addition === 'NONE' || addition.length < 5) return null

    return addition
  } catch (err: any) {
    console.error('[second-pass] Claude error:', err.message)
    return null
  }
}

// ── Gather urgent items from recent data ──────────────────────────

async function gatherUrgentItems(pool: any): Promise<UrgentItem[]> {
  const items: UrgentItem[] = []
  const day2ago = new Date(Date.now() - 48 * 3600000).toISOString()

  // 1. Recent life events (last 48h)
  const lifeEvents = await pool.query(
    "SELECT type, summary, confidence, evidence FROM life_events WHERE detected_at >= $1 ORDER BY detected_at DESC LIMIT 3",
    [day2ago]
  )
  for (const e of lifeEvents.rows) {
    items.push({
      type: 'life_event',
      summary: e.summary,
      evidence: `Life event detected: ${e.type} (${Math.round(e.confidence * 100)}% confidence)`,
      urgencyScore: e.confidence > 0.7 ? 9 : 7
    })
  }

  // 2. High-severity routine breaks
  const routineBreaks = await pool.query(
    "SELECT description, expected_behavior, actual_behavior, days_broken, severity FROM routine_breaks WHERE severity IN ('high', 'medium') AND detected_at >= $1 ORDER BY days_broken DESC LIMIT 3",
    [day2ago]
  )
  for (const b of routineBreaks.rows) {
    items.push({
      type: 'routine_break',
      summary: b.description,
      evidence: `Expected: ${b.expected_behavior}. Actual: ${b.actual_behavior}. Broken for ${b.days_broken} days.`,
      urgencyScore: b.severity === 'high' ? 8 : 6
    })
  }

  // 3. Relationship anomalies
  const anomalies = await pool.query(
    "SELECT name, anomaly, narrative_note, current_closeness FROM relationships WHERE anomaly IS NOT NULL AND anomaly != '' ORDER BY current_closeness DESC LIMIT 3"
  )
  for (const r of anomalies.rows) {
    const urgency = r.anomaly === 'sudden_silence' ? 7 :
                    r.anomaly === 'overdue_followup' && r.current_closeness > 40 ? 7 : 5
    items.push({
      type: 'relationship_anomaly',
      summary: `${r.name}: ${r.anomaly?.replace(/_/g, ' ')}`,
      evidence: r.narrative_note || `Anomaly: ${r.anomaly} (closeness: ${r.current_closeness}/100)`,
      urgencyScore: urgency
    })
  }

  // 4. Commitments about to escalate (stale > 10 days, high importance)
  const staleThreads = await pool.query(
    "SELECT subject, stale_days, last_message_from FROM email_threads WHERE stale_days > 10 ORDER BY stale_days DESC LIMIT 3"
  )
  for (const t of staleThreads.rows) {
    if (t.stale_days > 14) {
      items.push({
        type: 'stale_escalation',
        summary: `"${t.subject}" — ${t.stale_days} days stale`,
        evidence: `Email thread with ${t.last_message_from} has been unanswered for ${t.stale_days} days`,
        urgencyScore: t.stale_days > 20 ? 8 : 6
      })
    }
  }

  // 5. Calendar events in the next 2 hours
  const now = new Date()
  const soon = new Date(now.getTime() + 2 * 3600000).toISOString()
  const upcomingEvents = await pool.query(
    "SELECT summary, start_time FROM calendar_events WHERE start_time::timestamptz >= $1::timestamptz AND start_time::timestamptz <= $2::timestamptz AND status != 'cancelled' ORDER BY start_time::timestamptz LIMIT 2",
    [now.toISOString(), soon]
  )
  for (const e of upcomingEvents.rows) {
    const minutesAway = Math.round((new Date(e.start_time).getTime() - now.getTime()) / 60000)
    if (minutesAway > 0 && minutesAway < 120) {
      items.push({
        type: 'deadline',
        summary: `${e.summary} in ${minutesAway} minutes`,
        evidence: `Calendar event "${e.summary}" starts at ${e.start_time}`,
        urgencyScore: minutesAway < 30 ? 9 : minutesAway < 60 ? 7 : 5
      })
    }
  }

  return items
}

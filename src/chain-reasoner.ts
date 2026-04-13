/**
 * Chain Reasoning — three-steps-ahead planning.
 *
 * Thinks in causal chains: "if A happens, then B becomes urgent,
 * then C becomes possible."
 *
 * Triggered by:
 * - New life events
 * - New predicted actions with high confidence
 * - Stale threads crossing severity thresholds
 * - Calendar events approaching
 * - Explicit user request ("plan this out")
 *
 * For each trigger, generates a 3-step chain:
 * Step 1: Immediate correct action (what to do NOW)
 * Step 2: Consequence action (what becomes urgent AFTER step 1)
 * Step 3: Enablement action (what step 2 makes possible/required)
 *
 * Each step has:
 * - Title and description
 * - Draft content (email, message, document)
 * - Prerequisites (what must happen before this step)
 * - Dependency on previous step
 *
 * Chains feed back into the action predictor: steps 2-3 become
 * pre-staged predicted_actions that activate when step 1 completes.
 */

import { getPool, getSettings, getLivingProfile } from './db'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface ChainStep {
  stepNumber: number
  title: string
  description: string
  draftContent: string | null
  prerequisites: string
  dependsOnStep: string | null
}

interface ChainResult {
  chainId: string
  trigger: string
  steps: ChainStep[]
}

// ── Main: generate a reasoning chain for a trigger ────────────────

export async function generateChain(
  triggerEvent: string,
  triggerType: string,
  triggerEntity: string | null
): Promise<ChainResult | null> {
  const pool = getPool()
  const settings = await getSettings()
  const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
  if (!apiKey) return null

  // Don't duplicate: check if a chain already exists for this trigger entity
  if (triggerEntity) {
    const existing = await pool.query(
      "SELECT id FROM action_chains WHERE trigger_entity = $1 AND status = 'active' AND created_at > NOW() - INTERVAL '24 hours'",
      [triggerEntity]
    )
    if (existing.rows.length > 0) return null
  }

  // Gather rich context for the chain reasoning
  const contextParts = await gatherChainContext(pool, triggerEvent, triggerEntity)

  // Call Claude to reason three steps ahead
  const steps = await reasonChain(apiKey, settings.model, triggerEvent, triggerType, contextParts)
  if (!steps || steps.length === 0) return null

  // Store the chain
  const chainId = crypto.randomUUID()
  await pool.query(
    `INSERT INTO action_chains (id, trigger_event, trigger_type, trigger_entity, context_snapshot)
     VALUES ($1, $2, $3, $4, $5)`,
    [chainId, triggerEvent.slice(0, 500), triggerType, triggerEntity, contextParts.join('\n---\n').slice(0, 2000)]
  )

  // Store each step
  let prevStepId: string | null = null
  for (const step of steps) {
    const stepId = crypto.randomUUID()

    await pool.query(
      `INSERT INTO chain_steps (id, chain_id, step_number, title, description, draft_content, prerequisites, depends_on_step)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [stepId, chainId, step.stepNumber, step.title, step.description,
        step.draftContent, step.prerequisites, prevStepId]
    )

    // Feed steps 2 and 3 into predicted_actions
    if (step.stepNumber >= 2) {
      const predId = crypto.randomUUID()
      const urgency = step.stepNumber === 2 ? 'medium' : 'low'
      const expiresAt = new Date(Date.now() + (step.stepNumber === 2 ? 72 : 168) * 3600000).toISOString()

      await pool.query(
        `INSERT INTO predicted_actions (id, trigger_context, trigger_type, action_type, title, description,
          draft_content, confidence, urgency, expires_at, related_entity, evidence)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
        [predId, `Chain step ${step.stepNumber}: ${triggerEvent.slice(0, 100)}`,
          'chain_step', 'chain_action', step.title, step.description,
          step.draftContent, 0.6, urgency, expiresAt,
          `chain-${chainId}-step-${step.stepNumber}`,
          JSON.stringify([`Part of chain from: ${triggerEvent.slice(0, 100)}`, `Prerequisite: ${step.prerequisites.slice(0, 100)}`])]
      )

      // Link the predicted action back to the chain step
      await pool.query(
        "UPDATE chain_steps SET predicted_action_id = $1 WHERE id = $2",
        [predId, stepId]
      )
    }

    prevStepId = stepId
  }

  console.log(`[chain] Generated ${steps.length}-step chain for: ${triggerEvent.slice(0, 60)}`)

  return { chainId, trigger: triggerEvent, steps }
}

// ── Gather context for chain reasoning ────────────────────────────

async function gatherChainContext(pool: any, trigger: string, entity: string | null): Promise<string[]> {
  const parts: string[] = []

  // Persona (compressed)
  const persona = await getLivingProfile()
  if (persona) parts.push(`PERSONA (key facts):\n${persona.slice(0, 800)}`)

  // Related email threads
  if (entity) {
    const threads = await pool.query(
      "SELECT subject, stale_days, last_message_from, participants FROM email_threads WHERE subject ILIKE $1 OR thread_id = $1 LIMIT 3",
      [`%${entity.split('-').pop() || entity}%`]
    )
    if (threads.rows.length > 0) {
      parts.push(`RELATED THREADS:\n${threads.rows.map((t: any) =>
        `- "${t.subject}" (${t.stale_days}d stale, from: ${t.last_message_from})`
      ).join('\n')}`)
    }
  }

  // Related relationships
  const keywords = trigger.match(/\b[A-Z][a-zA-Z]{3,}\b/g) || []
  for (const kw of keywords.slice(0, 3)) {
    const rels = await pool.query(
      "SELECT name, current_closeness, trajectory, narrative_note FROM relationships WHERE name ILIKE $1 LIMIT 1",
      [`%${kw}%`]
    )
    if (rels.rows.length > 0) {
      const r = rels.rows[0]
      parts.push(`RELATIONSHIP — ${r.name}: closeness ${r.current_closeness}/100, ${r.trajectory}. ${r.narrative_note || ''}`)
    }
  }

  // Related commitments
  const commitments = await pool.query(
    "SELECT description, due_at, committed_to FROM commitments WHERE status = 'active' LIMIT 5"
  )
  if (commitments.rows.length > 0) {
    parts.push(`ACTIVE COMMITMENTS:\n${commitments.rows.map((c: any) =>
      `- ${c.description}${c.due_at ? ` (due: ${c.due_at})` : ''}${c.committed_to ? ` → ${c.committed_to}` : ''}`
    ).join('\n')}`)
  }

  // Upcoming calendar events
  const events = await pool.query(
    "SELECT summary, start_time, participants FROM calendar_events WHERE start_time::timestamptz >= NOW() AND start_time ~ '^\\d{4}-\\d{2}-\\d{2}' AND status != 'cancelled' ORDER BY start_time::timestamptz LIMIT 5"
  )
  if (events.rows.length > 0) {
    parts.push(`UPCOMING EVENTS:\n${events.rows.map((e: any) =>
      `- ${e.summary} at ${e.start_time}`
    ).join('\n')}`)
  }

  // Recent observations related to the trigger
  for (const kw of keywords.slice(0, 2)) {
    const obs = await pool.query(
      "SELECT source, raw_content, timestamp FROM observations WHERE raw_content ILIKE $1 ORDER BY timestamp DESC LIMIT 3",
      [`%${kw}%`]
    )
    if (obs.rows.length > 0) {
      parts.push(`RECENT OBSERVATIONS (${kw}):\n${obs.rows.map((o: any) =>
        `- [${o.source}] ${(o.raw_content || '').slice(0, 120)}`
      ).join('\n')}`)
    }
  }

  // Skills relevant to the trigger
  const skills = await pool.query(
    "SELECT skill_name, proficiency, trajectory FROM skills ORDER BY proficiency DESC LIMIT 5"
  )
  if (skills.rows.length > 0) {
    parts.push(`TOP SKILLS: ${skills.rows.map((s: any) => `${s.skill_name} ${s.proficiency}/100`).join(', ')}`)
  }

  return parts
}

// ── Claude chain reasoning ────────────────────────────────────────

async function reasonChain(
  apiKey: string,
  model: string,
  trigger: string,
  triggerType: string,
  contextParts: string[]
): Promise<ChainStep[] | null> {
  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const { claudeWithFallback } = await import('./claude-fallback')
    const client = new Anthropic({ apiKey })

    const response = await claudeWithFallback(client, {
      model: model || 'claude-sonnet-4-20250514',
      max_tokens: 2000,
      system: `You are IRIS's chain reasoning engine. Given a triggering event and full context about Kavin, think THREE steps ahead in a causal chain.

RULES:
- Each step must be SPECIFIC and ACTIONABLE — not vague advice.
- Each step must reference REAL people, dates, projects, or commitments from the context provided.
- Step 1 is the IMMEDIATE action (do this now).
- Step 2 is what becomes urgent AFTER step 1 is done (the consequence).
- Step 3 is what step 2 enables or requires (the next horizon).
- Each step should include draft content where appropriate (email text, document outline, message).
- Prerequisites must be concrete: "After Rajeev confirms the date" not "After things are settled".

Return ONLY valid JSON in this exact format:
[
  {
    "step": 1,
    "title": "short action title",
    "description": "what to do and why, with specific references to people/dates/projects",
    "draft": "actual draft content if applicable (email text, message, doc outline) — or null if no draft needed",
    "prerequisites": "what must be true before this step (or 'none' for step 1)"
  },
  { "step": 2, ... },
  { "step": 3, ... }
]

If you cannot generate a meaningful 3-step chain (the trigger is too simple or lacks context), return a shorter chain. Never pad with generic steps.`,
      messages: [{
        role: 'user',
        content: `TRIGGERING EVENT: ${trigger}\nTRIGGER TYPE: ${triggerType}\n\nCONTEXT:\n${contextParts.join('\n\n')}`
      }]
    }, 'chain-reasoner')

    const raw = response.content[0].type === 'text' ? response.content[0].text : '[]'

    try {
      const jsonMatch = raw.match(/\[[\s\S]*\]/)
      if (!jsonMatch) return null

      const parsed = JSON.parse(jsonMatch[0]) as {
        step: number
        title: string
        description: string
        draft: string | null
        prerequisites: string
      }[]

      return parsed.map(s => ({
        stepNumber: s.step,
        title: s.title,
        description: s.description,
        draftContent: s.draft,
        prerequisites: s.prerequisites || 'none',
        dependsOnStep: null
      }))
    } catch {
      console.error('[chain] Failed to parse Claude response')
      return null
    }
  } catch (err: any) {
    console.error('[chain] Claude error:', err.message)
    return null
  }
}

// ── Mark chain step as completed and activate next ────────────────

export async function completeChainStep(stepId: string): Promise<{ nextStep: any | null }> {
  const pool = getPool()

  await pool.query(
    "UPDATE chain_steps SET status = 'completed', completed_at = NOW() WHERE id = $1",
    [stepId]
  )

  // Find the next step in the same chain
  const current = await pool.query(
    "SELECT chain_id, step_number FROM chain_steps WHERE id = $1",
    [stepId]
  )
  if (current.rows.length === 0) return { nextStep: null }

  const { chain_id, step_number } = current.rows[0]
  const next = await pool.query(
    "SELECT * FROM chain_steps WHERE chain_id = $1 AND step_number = $2",
    [chain_id, step_number + 1]
  )

  if (next.rows.length > 0) {
    const nextStep = next.rows[0]
    // Promote the next step's predicted action to 'high' urgency
    if (nextStep.predicted_action_id) {
      await pool.query(
        "UPDATE predicted_actions SET urgency = 'high' WHERE id = $1",
        [nextStep.predicted_action_id]
      )
    }
    return { nextStep }
  }

  // No more steps — mark chain as completed
  await pool.query(
    "UPDATE action_chains SET status = 'completed' WHERE id = $1",
    [chain_id]
  )

  return { nextStep: null }
}

// ── Get active chains for context injection ───────────────────────

export async function getActiveChains(limit: number = 5): Promise<any[]> {
  const pool = getPool()
  const chains = await pool.query(
    "SELECT id, trigger_event, trigger_type, created_at FROM action_chains WHERE status = 'active' ORDER BY created_at DESC LIMIT $1",
    [limit]
  )

  const result = []
  for (const chain of chains.rows) {
    const steps = await pool.query(
      "SELECT step_number, title, description, status, prerequisites FROM chain_steps WHERE chain_id = $1 ORDER BY step_number",
      [chain.id]
    )
    result.push({
      ...chain,
      steps: steps.rows
    })
  }
  return result
}

export function formatChainsForContext(chains: any[]): string {
  if (chains.length === 0) return ''

  const lines = chains.map((c: any) => {
    const stepsText = (c.steps || []).map((s: any) => {
      const status = s.status === 'completed' ? '✅' : s.status === 'pending' ? '⬜' : '⏳'
      return `  ${status} Step ${s.step_number}: ${s.title}`
    }).join('\n')
    return `Chain: ${c.trigger_event.slice(0, 80)}\n${stepsText}`
  })

  return `ACTIVE REASONING CHAINS (multi-step plans in progress):\n${lines.join('\n\n')}`
}

// ── Batch: generate chains for all high-urgency triggers ──────────

export async function generateChainsForUrgentItems(): Promise<number> {
  const pool = getPool()
  let count = 0

  // Stale threads > 10 days with real contacts
  const stale = await pool.query(
    `SELECT thread_id, subject, stale_days, last_message_from FROM email_threads
     WHERE stale_days > 10 ORDER BY stale_days DESC LIMIT 5`
  )
  for (const t of stale.rows) {
    const result = await generateChain(
      `Stale thread: "${t.subject}" — ${t.stale_days} days without response from ${t.last_message_from}`,
      'stale_thread',
      `thread-${t.thread_id}`
    )
    if (result) count++
  }

  // Recent life events
  const events = await pool.query(
    "SELECT type, summary FROM life_events WHERE detected_at > NOW() - INTERVAL '48 hours' ORDER BY detected_at DESC LIMIT 3"
  )
  for (const e of events.rows) {
    const result = await generateChain(
      `Life event: ${e.summary}`,
      'life_event',
      `life-${e.type}`
    )
    if (result) count++
  }

  // Relationship anomalies
  const anomalies = await pool.query(
    "SELECT name, email, anomaly, narrative_note FROM relationships WHERE anomaly IS NOT NULL AND current_closeness > 30 LIMIT 3"
  )
  for (const r of anomalies.rows) {
    const result = await generateChain(
      `Relationship anomaly: ${r.name} — ${r.anomaly?.replace(/_/g, ' ')}. ${r.narrative_note || ''}`,
      'relationship_anomaly',
      `rel-${r.email}`
    )
    if (result) count++
  }

  console.log(`[chain] Generated ${count} reasoning chains`)
  return count
}

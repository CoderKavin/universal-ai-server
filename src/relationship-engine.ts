/**
 * Relationship Graph Engine
 *
 * Models how every relationship is evolving over time.
 * Runs hourly. Computes closeness, trajectory, anomalies.
 * Generates Claude-written narrative notes per contact.
 *
 * Data sources:
 * - Email threads (sent/received counts, stale days, reply patterns)
 * - WhatsApp observations (message counts, conversation frequency)
 * - Calendar events (shared events, meetings)
 * - Centrum (project-related contacts)
 *
 * Output:
 * - relationships table with per-contact scores and narratives
 * - Top relationships injected into spotlight context
 */

import { getPool, getSettings } from './db'
import { invalidateCache } from './context-cache'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface ContactData {
  id: string
  name: string
  email: string
  emailsSent: number
  emailsReceived: number
  relationshipScore: number
  lastInteraction: string | null
  staleDays: number
  awaitingReply: boolean
  whatsappMsgs30d: number
  calendarShared: number
  centrumMentions: number
}

interface RelationshipRow {
  id: string
  contact_id: string
  name: string
  email: string
  current_closeness: number
  trajectory: string
  last_interaction: string | null
  interaction_freq_30d: number
  interaction_freq_90d: number
  sentiment_trend: string
  topics_shared: string[]
  context_roles: string[]
  narrative_note: string
  whatsapp_msgs_30d: number
  email_count_30d: number
  initiator_ratio: number
  anomaly: string | null
}

// ── Main: run relationship analysis ───────────────────────────────

export async function runRelationshipEngine(): Promise<number> {
  const pool = getPool()
  const now = new Date()
  const day30ago = new Date(now.getTime() - 30 * 86400000).toISOString()
  const day90ago = new Date(now.getTime() - 90 * 86400000).toISOString()

  // ── 1. Load all contacts ────────────────────────────────────────

  // Base pool: top 100 contacts by relationship_score
  const contactsRes = await pool.query(
    `SELECT id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at
     FROM contacts WHERE name IS NOT NULL
     AND email NOT LIKE '%noreply%' AND email NOT LIKE '%no-reply%'
     AND email NOT LIKE '%notifications%' AND email NOT LIKE '%newsletter%'
     AND email NOT LIKE '%updates%' AND email NOT LIKE '%marketing%'
     AND email NOT LIKE '%promo%' AND email NOT LIKE '%alerts%'
     AND email NOT LIKE '%digest%' AND email NOT LIKE '%discover%'
     AND email NOT LIKE '%campaigns%' AND email NOT LIKE '%@e.%'
     AND email NOT LIKE '%@mail.%' AND email NOT LIKE '%@learn.%'
     AND emails_sent > 0
     ORDER BY relationship_score DESC LIMIT 100`
  )
  const contactsMap = new Map<string, any>()
  for (const c of contactsRes.rows) contactsMap.set(c.id, c)

  // ── Inclusion expansion: pull in contacts referenced by active brain state ──
  // We do this in JS instead of SQL to avoid cross-table cartesian product cost.

  async function addContactsFromQuery(sql: string, args: any[], reason: string): Promise<void> {
    const rows = await pool.query(sql, args)
    const before = contactsMap.size
    for (const extra of rows.rows) {
      if (!contactsMap.has(extra.id)) contactsMap.set(extra.id, extra)
    }
    const added = contactsMap.size - before
    if (added > 0) console.log(`[relationships] +${added} contacts via ${reason}`)
  }

  // 1a. Contacts referenced in correction_log (last 90 days)
  //     Strategy: fetch corrections, extract first-word tokens from trigger_action/trigger_context,
  //     then lookup matching contacts by first-name.
  const cLogRows = await pool.query(
    `SELECT trigger_action, trigger_context FROM correction_log
     WHERE timestamp > NOW() - INTERVAL '90 days' LIMIT 500`
  )
  const tokenSet = new Set<string>()
  for (const row of cLogRows.rows) {
    const text = `${row.trigger_action || ''} ${row.trigger_context || ''}`.toLowerCase()
    // Look for typical name-like tokens (3-20 chars, no numbers)
    const matches = text.match(/\b[a-z][a-z'.-]{2,19}\b/g) || []
    for (const t of matches) tokenSet.add(t)
  }
  if (tokenSet.size > 0) {
    const tokens = Array.from(tokenSet).slice(0, 200) // cap to avoid oversize IN clause
    await addContactsFromQuery(
      `SELECT id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at
       FROM contacts WHERE name IS NOT NULL AND LOWER(SPLIT_PART(name, ' ', 1)) = ANY($1::text[])`,
      [tokens],
      'correction_log'
    )
  }

  // 1b. Contacts referenced by pending predicted_actions
  const paRows = await pool.query(
    `SELECT title, description, related_entity FROM predicted_actions
     WHERE status = 'pending' LIMIT 200`
  )
  const paTokens = new Set<string>()
  for (const row of paRows.rows) {
    const text = `${row.title || ''} ${row.description || ''} ${row.related_entity || ''}`.toLowerCase()
    const matches = text.match(/\b[a-z][a-z'.-]{2,19}\b/g) || []
    for (const t of matches) paTokens.add(t)
  }
  if (paTokens.size > 0) {
    const tokens = Array.from(paTokens).slice(0, 300)
    await addContactsFromQuery(
      `SELECT id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at
       FROM contacts WHERE name IS NOT NULL AND LOWER(SPLIT_PART(name, ' ', 1)) = ANY($1::text[])`,
      [tokens],
      'predicted_actions'
    )
  }

  // 1c. Contacts mentioned 2+ times in recent observations (last 7 days)
  const obsRows = await pool.query(
    `SELECT raw_content FROM observations
     WHERE timestamp > NOW() - INTERVAL '7 days' LIMIT 2000`
  )
  const obsCounts = new Map<string, number>()
  for (const row of obsRows.rows) {
    const text = (row.raw_content || '').toLowerCase()
    const matches = text.match(/\b[a-z][a-z'.-]{2,19}\b/g) || []
    for (const t of matches) obsCounts.set(t, (obsCounts.get(t) || 0) + 1)
  }
  const obsTokens = Array.from(obsCounts.entries()).filter(([_, n]) => n >= 2).map(([t]) => t).slice(0, 300)
  if (obsTokens.length > 0) {
    await addContactsFromQuery(
      `SELECT id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at
       FROM contacts WHERE name IS NOT NULL AND LOWER(SPLIT_PART(name, ' ', 1)) = ANY($1::text[])`,
      [obsTokens],
      'observations'
    )
  }

  const contacts = Array.from(contactsMap.values())
  console.log(`[relationships] Candidate pool: ${contacts.length} contacts (base 100 + extras)`)

  if (contacts.length === 0) {
    console.log('[relationships] No contacts to analyze')
    return 0
  }

  // ── 2. Enrich with cross-source data ────────────────────────────

  const enriched: ContactData[] = []

  for (const c of contacts) {
    // WhatsApp message count (last 30 days)
    const waRes = await pool.query(
      `SELECT COUNT(*)::int as c FROM observations
       WHERE source = 'whatsapp' AND timestamp >= $1
       AND (raw_content ILIKE $2 OR raw_content ILIKE $3)`,
      [day30ago, `%${c.name}%`, `%${c.email?.split('@')[0]}%`]
    )

    // Calendar shared events
    const calRes = await pool.query(
      `SELECT COUNT(*)::int as c FROM calendar_events
       WHERE participants ILIKE $1 OR participants ILIKE $2`,
      [`%${c.name}%`, `%${c.email}%`]
    )

    // Centrum mentions
    const centRes = await pool.query(
      `SELECT COUNT(*)::int as c FROM observations
       WHERE source = 'centrum' AND timestamp >= $1
       AND raw_content ILIKE $2`,
      [day90ago, `%${c.name}%`]
    )

    // Email stale days for this contact
    const threadRes = await pool.query(
      `SELECT stale_days, awaiting_reply FROM email_threads
       WHERE participants ILIKE $1 OR last_message_from ILIKE $2
       ORDER BY stale_days DESC LIMIT 1`,
      [`%${c.email}%`, `%${c.email}%`]
    )

    enriched.push({
      id: c.id,
      name: c.name,
      email: c.email,
      emailsSent: c.emails_sent || 0,
      emailsReceived: c.emails_received || 0,
      relationshipScore: c.relationship_score || 0,
      lastInteraction: c.last_interaction_at,
      staleDays: threadRes.rows[0]?.stale_days || 0,
      awaitingReply: threadRes.rows[0]?.awaiting_reply === 1,
      whatsappMsgs30d: waRes.rows[0]?.c || 0,
      calendarShared: calRes.rows[0]?.c || 0,
      centrumMentions: centRes.rows[0]?.c || 0
    })
  }

  // ── 3. Compute closeness and trajectory for each contact ────────

  // Load previous relationship data for trajectory comparison
  const prevRelRes = await pool.query('SELECT contact_id, current_closeness, interaction_freq_30d FROM relationships')
  const prevMap = new Map(prevRelRes.rows.map((r: any) => [r.contact_id, r]))

  const relationships: {
    contact: ContactData
    closeness: number
    trajectory: string
    freq30d: number
    freq90d: number
    initiatorRatio: number
    contextRoles: string[]
    anomaly: string | null
  }[] = []

  for (const contact of enriched) {
    const totalEmails = contact.emailsSent + contact.emailsReceived
    const daysSinceLast = contact.lastInteraction
      ? Math.max(0, (now.getTime() - new Date(contact.lastInteraction).getTime()) / 86400000)
      : 90

    // Closeness score (0-100)
    let closeness = 0

    // Email frequency component (max 30)
    closeness += Math.min(30, totalEmails * 0.3)

    // WhatsApp component (max 25) — WhatsApp is more intimate
    closeness += Math.min(25, contact.whatsappMsgs30d * 2)

    // Recency component (max 20) — exponential decay
    closeness += Math.max(0, 20 * Math.exp(-daysSinceLast / 15))

    // Calendar shared component (max 10)
    closeness += Math.min(10, contact.calendarShared * 3)

    // Reciprocity bonus (max 10) — both parties engage
    const reciprocity = totalEmails > 0
      ? Math.min(contact.emailsSent, contact.emailsReceived) / Math.max(contact.emailsSent, contact.emailsReceived)
      : 0
    closeness += reciprocity * 10

    // Centrum mention bonus (max 5) — part of active projects
    closeness += Math.min(5, contact.centrumMentions)

    closeness = Math.min(100, Math.round(closeness))

    // Frequency: interactions per day over 30d and 90d windows
    const freq30d = (contact.emailsSent + contact.emailsReceived + contact.whatsappMsgs30d) / 30
    const freq90d = totalEmails / 90

    // Trajectory: compare 30d frequency to 90d frequency
    let trajectory = 'stable'
    if (freq90d > 0.01) {
      const delta = (freq30d - freq90d) / freq90d
      if (delta > 0.3) trajectory = 'rising'
      else if (delta < -0.3) trajectory = 'declining'
    } else if (freq30d > 0.1) {
      trajectory = 'rising' // new contact appearing
    }

    // Also check against previous relationship data
    const prev = prevMap.get(contact.id)
    if (prev && prev.current_closeness > 0) {
      const closenessDelta = (closeness - prev.current_closeness) / prev.current_closeness
      if (closenessDelta > 0.2 && trajectory === 'stable') trajectory = 'rising'
      if (closenessDelta < -0.2 && trajectory === 'stable') trajectory = 'declining'
    }

    // Initiator ratio (who reaches out more)
    const initiatorRatio = totalEmails > 0
      ? contact.emailsSent / totalEmails
      : 0.5

    // Context roles
    const roles: string[] = []
    if (contact.email?.includes('.edu') || contact.centrumMentions > 0) roles.push('academic')
    if (contact.calendarShared > 0) roles.push('professional')
    if (contact.whatsappMsgs30d > 5) roles.push('social')
    if (contact.emailsSent > 20 && contact.emailsReceived > 20) roles.push('work')
    if (roles.length === 0) roles.push('acquaintance')

    // Anomaly detection
    let anomaly: string | null = null
    if (prev && prev.interaction_freq_30d > 0.5 && freq30d < 0.05) {
      anomaly = 'sudden_silence'
    }
    if (!prev && closeness > 30) {
      anomaly = 'new_contact_rising_fast'
    }
    if (prev && prev.current_closeness < 10 && closeness > 25) {
      anomaly = 'dormant_reactivated'
    }
    if (contact.staleDays > 10 && contact.emailsSent > 10) {
      anomaly = 'overdue_followup'
    }

    relationships.push({
      contact,
      closeness,
      trajectory,
      freq30d,
      freq90d,
      initiatorRatio,
      contextRoles: roles,
      anomaly
    })
  }

  // Sort by closeness
  relationships.sort((a, b) => b.closeness - a.closeness)

  // ── 4. Generate narrative notes via Claude ──────────────────────

  const settings = await getSettings()
  const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
  let narratives: Map<string, string> = new Map()

  if (apiKey) {
    narratives = await generateNarratives(apiKey, settings.model, relationships.slice(0, 20))
  }

  // ── 5. Upsert into relationships table ─────────────────────────

  let upsertCount = 0
  for (const rel of relationships) {
    const narrative = narratives.get(rel.contact.id) || buildFallbackNarrative(rel)

    await pool.query(
      `INSERT INTO relationships (id, contact_id, name, email, current_closeness, trajectory,
        last_interaction, interaction_freq_30d, interaction_freq_90d, sentiment_trend,
        topics_shared, context_roles, narrative_note, whatsapp_msgs_30d, email_count_30d,
        initiator_ratio, anomaly, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
       ON CONFLICT (id) DO UPDATE SET
        current_closeness=$5, trajectory=$6, last_interaction=$7,
        interaction_freq_30d=$8, interaction_freq_90d=$9, sentiment_trend=$10,
        topics_shared=$11, context_roles=$12, narrative_note=$13,
        whatsapp_msgs_30d=$14, email_count_30d=$15, initiator_ratio=$16,
        anomaly=$17, updated_at=NOW()`,
      [
        `rel-${rel.contact.id}`, rel.contact.id, rel.contact.name, rel.contact.email,
        rel.closeness, rel.trajectory,
        rel.contact.lastInteraction, rel.freq30d, rel.freq90d,
        'neutral', // sentiment_trend — would need message content analysis
        JSON.stringify([]), // topics_shared — computed from observation content
        JSON.stringify(rel.contextRoles),
        narrative,
        rel.contact.whatsappMsgs30d,
        rel.contact.emailsSent + rel.contact.emailsReceived,
        rel.initiatorRatio,
        rel.anomaly
      ]
    )
    upsertCount++
  }

  invalidateCache('relationships updated')
  console.log(`[relationships] Updated ${upsertCount} relationships (${relationships.filter(r => r.trajectory === 'rising').length} rising, ${relationships.filter(r => r.trajectory === 'declining').length} declining, ${relationships.filter(r => r.anomaly).length} anomalies)`)

  return upsertCount
}

// ── Narrative generation via Claude ───────────────────────────────

async function generateNarratives(
  apiKey: string,
  model: string,
  relationships: {
    contact: ContactData
    closeness: number
    trajectory: string
    freq30d: number
    initiatorRatio: number
    contextRoles: string[]
    anomaly: string | null
  }[]
): Promise<Map<string, string>> {
  const result = new Map<string, string>()

  // Batch all contacts into one Claude call for efficiency
  const contactDescriptions = relationships.map((rel, i) => {
    const c = rel.contact
    const daysSinceLast = c.lastInteraction
      ? Math.round((Date.now() - new Date(c.lastInteraction).getTime()) / 86400000)
      : 999
    return `[${i + 1}] ${c.name} (${c.email})
  Closeness: ${rel.closeness}/100, Trajectory: ${rel.trajectory}
  Emails sent: ${c.emailsSent}, received: ${c.emailsReceived}
  WhatsApp messages (30d): ${c.whatsappMsgs30d}
  Calendar events shared: ${c.calendarShared}
  Last interaction: ${daysSinceLast} days ago
  Stale thread: ${c.staleDays > 0 ? c.staleDays + ' days' : 'none'}
  Awaiting reply from them: ${c.awaitingReply ? 'yes' : 'no'}
  Initiator ratio (Kavin sends/total): ${(rel.initiatorRatio * 100).toFixed(0)}%
  Roles: ${rel.contextRoles.join(', ')}
  ${rel.anomaly ? `ANOMALY: ${rel.anomaly}` : ''}`
  }).join('\n\n')

  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const { claudeWithFallback } = await import('./claude-fallback')
    const client = new Anthropic({ apiKey })

    const response = await claudeWithFallback(client, {
      model: model || 'claude-sonnet-4-20250514',
      max_tokens: 2000,
      system: `You are writing one-sentence narrative notes about Kavin's relationships. For each contact, write a single vivid, specific sentence that captures where the relationship is RIGHT NOW — not a summary, but a snapshot.

Good: "Kavin and Rajeev's correspondence has slowed significantly — the Extended Essay interview has been pending for 13 days with no movement."
Good: "Nisha Kurur feels like an active professional partner — steady back-and-forth on Chetana Trust deliverables."
Bad: "Kavin has a good relationship with Rajeev." (too generic)
Bad: "They communicate regularly." (no specificity)

Return ONLY a JSON array: [{"index": 1, "note": "..."}, ...]`,
      messages: [{
        role: 'user',
        content: contactDescriptions
      }]
    }, 'relationship-engine')

    const raw = response.content[0].type === 'text' ? response.content[0].text : '[]'
    try {
      const jsonMatch = raw.match(/\[[\s\S]*\]/)
      if (jsonMatch) {
        const notes = JSON.parse(jsonMatch[0]) as { index: number; note: string }[]
        for (const n of notes) {
          const rel = relationships[n.index - 1]
          if (rel) result.set(rel.contact.id, n.note)
        }
      }
    } catch {}
  } catch (err: any) {
    console.error('[relationships] Claude narrative generation failed:', err.message)
  }

  return result
}

// ── Fallback narrative (no Claude) ────────────────────────────────

function buildFallbackNarrative(rel: {
  contact: ContactData
  closeness: number
  trajectory: string
  anomaly: string | null
}): string {
  const c = rel.contact
  const daysSinceLast = c.lastInteraction
    ? Math.round((Date.now() - new Date(c.lastInteraction).getTime()) / 86400000)
    : 999

  if (rel.anomaly === 'sudden_silence') {
    return `Previously active contact — suddenly silent for ${daysSinceLast} days.`
  }
  if (rel.anomaly === 'overdue_followup') {
    return `${c.staleDays}-day overdue follow-up. ${c.emailsSent} emails sent historically.`
  }
  if (rel.trajectory === 'rising') {
    return `Rising connection — interaction frequency increasing. ${c.whatsappMsgs30d > 0 ? `Active on WhatsApp (${c.whatsappMsgs30d} msgs).` : ''}`
  }
  if (rel.trajectory === 'declining') {
    return `Declining — less frequent interaction. Last contact ${daysSinceLast} days ago.`
  }
  return `Stable relationship. Last interaction ${daysSinceLast} days ago.`
}

// ── Get top relationships for context injection ───────────────────

export async function getTopRelationships(limit: number = 10): Promise<any[]> {
  const pool = getPool()
  const r = await pool.query(
    `SELECT name, email, current_closeness, trajectory, narrative_note, anomaly,
            context_roles, whatsapp_msgs_30d, email_count_30d, initiator_ratio,
            last_interaction
     FROM relationships
     ORDER BY current_closeness DESC
     LIMIT $1`,
    [limit]
  )
  return r.rows
}

// ── Format for context injection ──────────────────────────────────

export function formatRelationshipsForContext(relationships: any[]): string {
  if (relationships.length === 0) return ''

  const lines = relationships.map((r: any) => {
    const roles = Array.isArray(r.context_roles) ? r.context_roles :
      (typeof r.context_roles === 'string' ? JSON.parse(r.context_roles) : [])
    const arrow = r.trajectory === 'rising' ? '↑' : r.trajectory === 'declining' ? '↓' : '→'
    const anomalyTag = r.anomaly ? ` [${r.anomaly.replace(/_/g, ' ')}]` : ''
    return `- ${r.name} (${arrow} ${r.current_closeness}/100, ${roles.join('/')})${anomalyTag}: ${r.narrative_note}`
  })

  return `RELATIONSHIP GRAPH (top ${relationships.length} by closeness):\n${lines.join('\n')}`
}

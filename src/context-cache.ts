/**
 * Context Cache — in-memory pre-assembled spotlight context bundle.
 *
 * Holds the full context per user so spotlight queries load from memory
 * instead of hitting PostgreSQL on every request.
 *
 * Invalidation triggers:
 * - Persona rewrite completes
 * - New life event detected
 * - Relationship graph updated
 * - Routine or skill model updated
 * - Calendar event created/updated
 * - New observation from top-10 relationship contact
 *
 * Cache hit → 0ms context load (vs ~50-200ms from DB)
 */

import {
  getLivingProfile, getLatestCentrumSnapshot,
  listStaleThreads, listThreadsAwaitingReply,
  listUpcomingEvents, listCommitments,
  listContacts, listRecentDocuments, getRecentQueryMemory
} from './db'
import { buildConfidenceContext } from './persona-confidence'
import { getTopRelationships, formatRelationshipsForContext } from './relationship-engine'
import { getActiveRoutines, getRecentBreaks, formatRoutinesForContext } from './routine-engine'
import { getSkills, formatSkillsForContext } from './skill-extractor'
import { getPendingActions, formatPredictedActionsForContext } from './action-predictor'
import { getActiveChains, formatChainsForContext } from './chain-reasoner'
import { getPool } from './db'

// ── Types ─────────────────────────────────────────────────────────

interface CachedContext {
  persona: string
  centrum: string
  calendar: string
  staleEmails: string
  awaitingReply: string
  commitments: string
  relationships: string
  documents: string
  digests: string
  trends: string
  routines: string
  skills: string
  predictedActions: string
  chains: string
  lifeEvents: string
  lessons: string
  assembledAt: number
  hitCount: number
}

// ── State ─────────────────────────────────────────────────────────

let cache: CachedContext | null = null
let buildingCache = false
let lastBuildTime = 0

const CACHE_TTL = 5 * 60 * 1000 // 5 minutes max age before forced rebuild

// ── Public: get cached context or rebuild ─────────────────────────

export async function getCachedContext(): Promise<CachedContext> {
  const now = Date.now()

  if (cache && (now - cache.assembledAt) < CACHE_TTL) {
    cache.hitCount++
    return cache
  }

  return rebuildCache()
}

export async function rebuildCache(): Promise<CachedContext> {
  if (buildingCache && cache) return cache // prevent concurrent rebuilds
  buildingCache = true

  try {
    const pool = getPool()
    const start = Date.now()

    // All queries in parallel — this is the key performance win
    const [
      profile,
      centrumSnap,
      stale,
      awaiting,
      upcoming,
      commitments,
      contacts,
      recentDocs,
      digestsRes,
      trendsRes,
      lifeEventsRes,
      lessonsRes,
      relationships,
      routines,
      breaks,
      skills,
      pendingActions,
      chains
    ] = await Promise.all([
      getLivingProfile(),
      getLatestCentrumSnapshot(),
      listStaleThreads(3),
      listThreadsAwaitingReply(),
      listUpcomingEvents(14),
      listCommitments('active'),
      listContacts(10),
      listRecentDocuments(7),
      pool.query("SELECT raw_content, extracted_entities FROM observations WHERE source IN ('daily_digest','weekly_digest') ORDER BY timestamp DESC LIMIT 3"),
      pool.query("SELECT statement FROM insights WHERE id LIKE 'trend-%' AND status='active' ORDER BY last_confirmed DESC LIMIT 5"),
      pool.query("SELECT type, summary, confidence, detected_at FROM life_events ORDER BY detected_at DESC LIMIT 3"),
      pool.query("SELECT original_belief, corrected_to, timestamp FROM lessons_learned WHERE active = true ORDER BY timestamp DESC LIMIT 10"),
      getTopRelationships(10),
      getActiveRoutines(),
      getRecentBreaks(5),
      getSkills(),
      getPendingActions(8),
      getActiveChains(3)
    ])

    // Format Centrum
    let centrumContext = ''
    if (centrumSnap) {
      const board = centrumSnap.board_state
      const cards = board.cards || []
      const clusters = cards.filter((c: any) => c.type === 'cluster')
      const tasks = cards.filter((c: any) => c.type === 'task')
      const todayIds = new Set(board.todayIds || [])
      const todayDone = new Set(board.todayDone || [])
      const sections: string[] = []

      const todayCards = cards.filter((c: any) => todayIds.has(c.id))
      if (todayCards.length > 0) {
        const items = todayCards.map((c: any) => {
          const cl = clusters.find((cl: any) => cl.id === c.clusterId)?.title || 'Unclustered'
          const done = todayDone.has(c.id) ? '✅' : '⬜'
          const subs = Array.isArray(c.content) ? c.content : []
          const doneN = subs.filter((t: any) => t.done).length
          return `${done} ${c.title} [${cl}]${subs.length ? ` (${doneN}/${subs.length} subtasks)` : ''}`
        })
        sections.push(`TODAY LIST (${todayDone.size}/${todayIds.size} done):\n${items.join('\n')}`)
      }

      for (const cluster of clusters) {
        const clTasks = tasks.filter((t: any) => t.clusterId === cluster.id && !todayIds.has(t.id))
        if (clTasks.length === 0) continue
        const items = clTasks.slice(0, 8).map((c: any) => {
          const subs = Array.isArray(c.content) ? c.content : []
          const doneN = subs.filter((t: any) => t.done).length
          return `- ${c.title} (${doneN}/${subs.length})${c.colorTag ? ` [${c.colorTag}]` : ''}`
        })
        sections.push(`${cluster.title} (${clTasks.length} cards):\n${items.join('\n')}`)
      }

      centrumContext = sections.length > 0
        ? `CENTRUM BOARDS (primary task source):\n${sections.join('\n\n')}`
        : ''
    }

    // Format commitments split by source
    const emailCommitments = commitments.filter((c: any) => c.source_type !== 'centrum')

    cache = {
      persona: buildConfidenceContext(profile ?? ''),

      centrum: centrumContext,

      calendar: upcoming.length > 0
        ? `CALENDAR (next 14 days):\n${upcoming.slice(0, 10).map((e: any) => `- ${e.summary} | ${new Date(e.start_time).toLocaleString('en-US', { weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}`).join('\n')}`
        : 'CALENDAR: No upcoming events.',

      staleEmails: stale.length > 0
        ? `STALE EMAILS:\n${stale.slice(0, 5).map((t: any) => `- "${t.subject}" — ${t.stale_days} days stale`).join('\n')}`
        : '',

      awaitingReply: awaiting.length > 0
        ? `AWAITING REPLY:\n${awaiting.slice(0, 5).map((t: any) => `- "${t.subject}" from ${t.last_message_from}`).join('\n')}`
        : '',

      commitments: emailCommitments.length > 0
        ? `EMAIL COMMITMENTS:\n${emailCommitments.slice(0, 8).map((c: any) => `- ${c.description}${c.due_at ? ' (due ' + new Date(c.due_at).toLocaleDateString() + ')' : ''}`).join('\n')}`
        : '',

      relationships: relationships.length > 0
        ? formatRelationshipsForContext(relationships)
        : contacts.length > 0 ? `KEY CONTACTS: ${contacts.slice(0, 8).map((c: any) => c.name).join(', ')}` : '',

      documents: recentDocs.length > 0
        ? `RECENT DOCUMENTS: ${recentDocs.slice(0, 5).map((d: any) => d.filename).join(', ')}`
        : '',

      digests: digestsRes.rows.length > 0
        ? `RECENT ACTIVITY DIGESTS:\n${digestsRes.rows.map((r: any) => {
            const ent = typeof r.extracted_entities === 'string' ? JSON.parse(r.extracted_entities) : r.extracted_entities || {}
            const date = ent.date || ent.endDate || '?'
            const total = ent.totalObservations || 0
            const highlights = (ent.highlights || []).slice(0, 3).join('; ')
            const people = (ent.people || ent.topPeople || []).slice(0, 5).join(', ')
            return `- ${date}: ${total} obs${highlights ? '. ' + highlights : ''}${people ? '. People: ' + people : ''}`
          }).join('\n')}`
        : '',

      trends: trendsRes.rows.length > 0
        ? `BEHAVIORAL TRENDS:\n${trendsRes.rows.map((r: any) => `- ${r.statement}`).join('\n')}`
        : '',

      routines: formatRoutinesForContext(routines, breaks),

      skills: skills.length > 0 ? formatSkillsForContext(skills) : '',

      predictedActions: pendingActions.length > 0 ? formatPredictedActionsForContext(pendingActions) : '',

      chains: chains.length > 0 ? formatChainsForContext(chains) : '',

      lifeEvents: lifeEventsRes.rows.length > 0
        ? `RECENT LIFE EVENTS:\n${lifeEventsRes.rows.map((r: any) => `- [${r.type}] ${r.summary} (${new Date(r.detected_at).toLocaleDateString()}, ${Math.round(r.confidence * 100)}%)`).join('\n')}`
        : '',

      lessons: lessonsRes.rows.length > 0
        ? `LESSONS LEARNED:\n${lessonsRes.rows.map((l: any) => `- "${l.original_belief}" → "${l.corrected_to}" (${new Date(l.timestamp).toLocaleDateString()})`).join('\n')}`
        : '',

      assembledAt: Date.now(),
      hitCount: 0
    }

    const elapsed = Date.now() - start
    console.log(`[cache] Context rebuilt in ${elapsed}ms (${Object.values(cache).filter(v => typeof v === 'string' && v.length > 0).length - 1} sections)`)

    return cache
  } finally {
    buildingCache = false
  }
}

// ── Invalidation ──────────────────────────────────────────────────

export function invalidateCache(reason: string): void {
  if (cache) {
    console.log(`[cache] Invalidated: ${reason} (was ${cache.hitCount} hits old)`)
  }
  cache = null
}

export function getCacheStats(): { cached: boolean; age: number; hits: number } {
  if (!cache) return { cached: false, age: 0, hits: 0 }
  return {
    cached: true,
    age: Date.now() - cache.assembledAt,
    hits: cache.hitCount
  }
}

// ── Assemble context string from cache by query type ──────────────

export type QueryType = 'factual' | 'analytical' | 'draft'

export function assembleContext(cached: CachedContext, queryType: QueryType, recentConversation: string): string {
  switch (queryType) {
    case 'factual':
      // Minimal: persona summary + calendar + stale emails + centrum today
      return [
        cached.persona.slice(0, 500),
        cached.centrum,
        cached.calendar,
        cached.staleEmails,
        recentConversation
      ].filter(Boolean).join('\n\n')

    case 'draft':
      // Writing-focused: persona + relationships + lessons + conversation
      return [
        cached.persona.slice(0, 800),
        cached.relationships,
        cached.staleEmails,
        cached.lessons,
        recentConversation
      ].filter(Boolean).join('\n\n')

    case 'analytical':
    default:
      // Full context for complex queries
      return [
        cached.persona,
        cached.centrum,
        cached.calendar,
        cached.staleEmails,
        cached.awaitingReply,
        cached.commitments,
        cached.relationships,
        cached.documents,
        cached.digests,
        cached.trends,
        cached.routines,
        cached.skills,
        cached.predictedActions,
        cached.chains,
        cached.lifeEvents,
        cached.lessons,
        recentConversation
      ].filter(Boolean).join('\n\n')
  }
}

// ── Query classifier ──────────────────────────────────────────────

export function classifyQuery(content: string): { type: QueryType; skipSecondPass: boolean } {
  const lower = content.toLowerCase().trim()

  // Draft requests — skip second pass
  if (/^(?:draft|write|email|message|reply|respond|send)\b/.test(lower)) {
    return { type: 'draft', skipSecondPass: true }
  }

  // Factual lookups — skip second pass
  if (/^(?:when|where|what time|how many|is |did |does |has |have |was |were |do i have|what's my next)\b/.test(lower)) {
    return { type: 'factual', skipSecondPass: true }
  }
  if (/^(?:what day|which|how long|how far|how old|how much)\b/.test(lower)) {
    return { type: 'factual', skipSecondPass: true }
  }

  // Analytical — full pipeline with second pass
  if (/^(?:what should|who is|who am|why am|what am i|how's my|am i|what's unusual|what pattern|summarize|what do you)\b/.test(lower)) {
    return { type: 'analytical', skipSecondPass: false }
  }
  if (/^(?:who's rising|what's my worst|what am i good|what should i|has anything|what's wrong|what am i getting)\b/.test(lower)) {
    return { type: 'analytical', skipSecondPass: false }
  }

  // Default: analytical (safe fallback — full context, with second pass)
  return { type: 'analytical', skipSecondPass: false }
}

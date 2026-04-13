/**
 * Universal AI Cloud Server.
 *
 * Standalone Node.js Express server with:
 *   - PostgreSQL database
 *   - Observation bus, insight extractor, persona engine
 *   - All API endpoints (chat, briefing, situation, glasses WebSocket)
 *   - Personal access token authentication
 *   - Centrum, Gmail, Calendar connectors
 */

import 'dotenv/config'
import express from 'express'
import { createServer } from 'http'
import { WebSocketServer, type WebSocket } from 'ws'
import crypto from 'crypto'
import { generateDailyDigest, generateWeeklyDigest, backfillDigests, startDigestScheduler } from './digest'
import { detectTrends } from './trends'
import { analyzeForLifeEvent, cleanupWifiHistory } from './life-events'
import { rewritePersonaWithConfidence, buildConfidenceContext } from './persona-confidence'
import { processCorrection, detectCorrectionInQuery, detectDraftEdit, buildLessonsForPersonaWriter } from './active-learning'
import {
  insertCorrection, markCorrectionsApplied, getActiveLessons, getAllCorrections,
  forgetLastQuery, forgetLastNQueries, forgetQueriesByKeyword, forgetQueriesInWindow, forgetAllQueries
} from './db'
import { runRelationshipEngine, getTopRelationships, formatRelationshipsForContext } from './relationship-engine'
import { extractRoutines, detectRoutineBreaks, getActiveRoutines, getRecentBreaks, formatRoutinesForContext } from './routine-engine'
import { extractSkills, getSkills, formatSkillsForContext } from './skill-extractor'
import { runSecondPass } from './second-pass'
import { runActionPredictor, getPendingActions, formatPredictedActionsForContext, regenerateOnEvent, markActionServed } from './action-predictor'
import { generateChain, generateChainsForUrgentItems, completeChainStep, getActiveChains, formatChainsForContext } from './chain-reasoner'
import { getCachedContext, invalidateCache, getCacheStats, assembleContext, classifyQuery, rebuildCache } from './context-cache'
import {
  initDB, migrate, getPool, getSettings, setSetting,
  getLivingProfile, setLivingProfile,
  listContacts, listStaleThreads, listThreadsAwaitingReply,
  listCommitments, listUpcomingEvents, listRecentDocuments,
  listProposedActions, updateActionStatus, insertProposedAction,
  getObservationCountBySource, totalObservationCount, getRecentObservations,
  listInsights, insertObservation,
  insertCentrumSnapshot, getLatestCentrumSnapshot, upsertCommitment,
  insertQueryMemory, getRecentQueryMemory, pruneQueryMemory
} from './db'

const PORT = parseInt(process.env.PORT || '3210', 10)

// ── Access token ──────────────────────────────────────────────────

let ACCESS_TOKEN = process.env.ACCESS_TOKEN || ''

async function ensureAccessToken(): Promise<void> {
  if (!ACCESS_TOKEN) {
    ACCESS_TOKEN = crypto.randomBytes(32).toString('hex')
    console.log(`[auth] Generated access token: ${ACCESS_TOKEN}`)
    console.log('[auth] Set ACCESS_TOKEN env var to persist this token')
  }
}

function authMiddleware(req: express.Request, res: express.Response, next: express.NextFunction): void {
  // Skip auth for health check
  if (req.path === '/health') { next(); return }

  const header = req.headers.authorization
  if (!header || header !== `Bearer ${ACCESS_TOKEN}`) {
    res.status(401).json({ error: 'Unauthorized — include Authorization: Bearer <token>' })
    return
  }
  next()
}

// ── Entity extraction from response text ─────────────────────────

// ── Forget command parser ─────────────────────────────────────────

async function handleForgetCommand(content: string): Promise<string | null> {
  const lower = content.toLowerCase().trim()

  // Must start with "forget"
  if (!lower.startsWith('forget')) return null

  const rest = lower.slice(6).trim()

  // "forget that" / "forget the last query" / "forget last"
  if (rest === 'that' || rest === 'the last query' || rest === 'last' || rest === 'it' || rest === '') {
    const deleted = await forgetLastQuery()
    return deleted > 0
      ? 'Forgotten: last query. Persona and observations unaffected.'
      : 'Nothing to forget — no recent queries in memory.'
  }

  // "forget everything" / "forget all" / "forget all queries"
  if (rest === 'everything' || rest === 'all' || rest === 'all queries' || rest === 'all memory') {
    const deleted = await forgetAllQueries()
    return `Forgotten: all ${deleted} queries from conversation memory. Persona and observations unaffected.`
  }

  // "forget the last N queries" / "forget last 3" / "forget last 5 queries"
  const lastNMatch = rest.match(/^(?:the )?last (\d+)\s*(?:queries|messages)?$/)
  if (lastNMatch) {
    const n = parseInt(lastNMatch[1])
    const deleted = await forgetLastNQueries(n)
    return `Forgotten: last ${deleted} queries. Persona and observations unaffected.`
  }

  // "forget everything from the last N minutes" / "forget last 10 minutes"
  const timeMatch = rest.match(/^(?:everything )?(?:from )?(?:the )?last (\d+)\s*min(?:utes?)?$/)
  if (timeMatch) {
    const minutes = parseInt(timeMatch[1])
    const deleted = await forgetQueriesInWindow(minutes)
    return `Forgotten: ${deleted} queries from the last ${minutes} minutes. Persona and observations unaffected.`
  }

  // "forget [keyword] stuff" / "forget Rajeev stuff" / "forget what I said about X"
  const keywordMatch = rest.match(/^(?:what I said about |about |)([\w\s]+?)(?:\s+stuff|\s+things|\s+queries)?$/)
  if (keywordMatch) {
    const keyword = keywordMatch[1].trim()
    if (keyword.length >= 2 && keyword.length <= 50) {
      const deleted = await forgetQueriesByKeyword(keyword)
      return deleted > 0
        ? `Forgotten: ${deleted} queries mentioning "${keyword}" from the last hour. Persona and observations unaffected.`
        : `No queries mentioning "${keyword}" found in the last hour.`
    }
  }

  // Didn't match any pattern — not a forget command, pass through to normal flow
  return null
}

function extractEntitiesFromText(responseText: string, queryText: string): string[] {
  const entities: Set<string> = new Set()
  const combined = responseText + ' ' + queryText

  // Extract **bold entities** from markup
  const boldMatches = combined.matchAll(/\*\*([^*]+)\*\*/g)
  for (const m of boldMatches) {
    entities.add(m[1].trim())
  }

  // Extract names: capitalized multi-word sequences (e.g., "Rajeev Kumar", "Nisha Kurur")
  const nameMatches = combined.matchAll(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b/g)
  for (const m of nameMatches) {
    const name = m[1].trim()
    // Filter out common non-name phrases
    if (!['Extended Essay', 'Google Chrome', 'Google Calendar', 'No Data'].includes(name)) {
      entities.add(name)
    }
  }

  // Extract times (2:15pm, 10:30 AM, etc.)
  const timeMatches = combined.matchAll(/\b(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)\b/g)
  for (const m of timeMatches) entities.add(m[1].trim())

  // Extract dates
  const dateMatches = combined.matchAll(/\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*(?:\s+\d{4})?)\b/gi)
  for (const m of dateMatches) entities.add(m[1].trim())

  // Extract project/topic names from known patterns
  const projectPatterns = [
    /Extended Essay/gi, /History IA/gi, /CAS Documentation/gi,
    /Kutty Olam/gi, /Chetana Trust/gi, /Centrum/gi, /IRIS/gi,
    /TATTVA/gi, /IBDP/gi, /Airpoint/gi
  ]
  for (const pat of projectPatterns) {
    const m = combined.match(pat)
    if (m) entities.add(m[0])
  }

  return Array.from(entities).slice(0, 20)
}

// ── Server setup ──────────────────────────────────────────────────

async function main(): Promise<void> {
  // Init database
  initDB()
  await migrate()
  await ensureAccessToken()

  const app = express()

  // CORS
  app.use((_req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*')
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
    if (_req.method === 'OPTIONS') { res.sendStatus(204); return }
    next()
  })

  app.use(express.json({ limit: '50mb' }))
  app.use(authMiddleware)

  // ── Health (no auth required) ───────────────────────────────

  app.get('/health', async (_req, res) => {
    const obsCount = await totalObservationCount()
    const profile = await getLivingProfile()
    res.json({
      status: 'ok',
      version: '0.1.0',
      observations: obsCount,
      persona_length: profile?.length ?? 0,
      uptime: process.uptime()
    })
  })

  // ── POST /api/chat/stream (SSE streaming spotlight) ──────────

  app.post('/api/chat/stream', async (req, res) => {
    const abortController = new AbortController()
    req.on('close', () => abortController.abort())

    try {
      const { content, mode } = req.body
      if (!content) { res.status(400).json({ error: 'content required' }); return }

      // Forget commands: handled without streaming
      const forgetResult = await handleForgetCommand(content)
      if (forgetResult) {
        res.setHeader('Content-Type', 'text/event-stream')
        res.setHeader('Cache-Control', 'no-cache')
        res.setHeader('Connection', 'keep-alive')
        res.write(`data: ${JSON.stringify({ type: 'text', content: forgetResult })}\n\n`)
        res.write(`data: ${JSON.stringify({ type: 'done' })}\n\n`)
        res.end()
        return
      }

      const settings = await getSettings()
      const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
      if (!apiKey) { res.status(400).json({ error: 'No Anthropic API key' }); return }

      insertObservation('chat', 'queried', content, {}, 'neutral', [], '').catch(() => {})
      pruneQueryMemory(30).catch(() => {})

      const Anthropic = (await import('@anthropic-ai/sdk')).default
      const client = new Anthropic({ apiKey })

      const recentQueries = await getRecentQueryMemory(10, 30)

      // Correction detection (same as non-streaming)
      const lastQuery = recentQueries.length > 0 ? recentQueries[recentQueries.length - 1] : null
      const { isCorrection, correctionText: corrText } = detectCorrectionInQuery(content, lastQuery?.response_text || null)
      if (isCorrection && lastQuery) {
        insertCorrection(lastQuery.response_text?.slice(0, 500) || '', lastQuery.query_text || '', 'corrected', corrText, '', []).catch(() => {})
        processCorrection(lastQuery.response_text?.slice(0, 200) || '', 'corrected', corrText, lastQuery.query_text || '').catch(() => {})
      }

      const queryClass = classifyQuery(content)
      const cached = await getCachedContext()

      const recentConversation = recentQueries.length > 0
        ? `RECENT CONVERSATION (last 30 minutes):\n${recentQueries.map((q: any) => {
            const entities = Array.isArray(q.entities_mentioned) ? q.entities_mentioned :
              (typeof q.entities_mentioned === 'string' ? JSON.parse(q.entities_mentioned) : [])
            return `User: "${q.query_text}"\nIRIS: "${(q.response_text || '').slice(0, 150)}"${entities.length ? ` [entities: ${entities.join(', ')}]` : ''}`
          }).join('\n\n')}`
        : ''

      const context = assembleContext(cached, queryClass.type, recentConversation)
      const isSpotlight = mode === 'spotlight'

      const IRIS_RULES = `You are IRIS — a sharp, informed personal assistant embedded in a macOS Spotlight bar. You already know everything about this person. You are NOT a chatbot or search engine.

RESPONSE RULES:
- Lead with the answer. Never lead with context or preamble.
- Default to the shortest honest answer. Simple facts: 1 line. Status checks: 2 lines max. Complex queries: 3-5 lines max.
- No greetings, no sign-offs, no filler. Just the answer.
- Prefer specific over vague. If no data, say what's missing. Never apologize.

CONFIDENCE RULES:
- High (>80%): state as fact. Medium (50-80%): hedge. Low (<50%): uncertainty or omit.

OPINION RULES:
- Recommendations: pick ONE answer with one-sentence defense. Commit, don't list.

FORMATTING:
- **bold** for entities. {{curly}} for metadata. [Button] for actions.`

      const systemBlocks: any[] = isSpotlight ? [
        { type: 'text', text: IRIS_RULES, cache_control: { type: 'ephemeral' } },
        { type: 'text', text: `\n\nCONTEXT:\n${context}`, cache_control: { type: 'ephemeral' } }
      ] : [
        { type: 'text', text: `You are Universal AI. Be concise and helpful.\n\n${context}` }
      ]

      // Set SSE headers
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no' // Disable nginx/render buffering
      })
      // Send initial ping to confirm connection
      res.write(`data: ${JSON.stringify({ type: 'ping' })}\n\n`)

      // Stream from Claude using the async iterator API
      let fullText = ''
      try {
        const systemStr = systemBlocks.map((b: any) => b.text).join('\n')

        // Use non-streaming Claude call, but write result immediately
        // Streaming through Render's proxy has buffering issues. Instead,
        // we call Claude normally and stream the RESULT to the client token by token
        const result = await client.messages.create({
          model: settings.model || 'claude-sonnet-4-20250514',
          max_tokens: isSpotlight ? 300 : 500,
          system: systemStr,
          messages: [{ role: 'user', content }]
        })

        fullText = result.content[0].type === 'text' ? result.content[0].text : ''

        // Stream the response to client character by character (simulated streaming)
        // This gives the client tokens progressively for the typing animation
        if (fullText.length > 0) {
          // Send in chunks of ~10 chars for smooth animation
          for (let i = 0; i < fullText.length; i += 10) {
            if (abortController.signal.aborted) break
            const chunk = fullText.slice(i, i + 10)
            res.write(`data: ${JSON.stringify({ type: 'text', content: chunk })}\n\n`)
          }
        }

        res.write(`data: ${JSON.stringify({ type: 'done' })}\n\n`)
      } catch (err: any) {
        console.error(`[stream] Error: ${err.message}`)
        try { res.write(`data: ${JSON.stringify({ type: 'error', content: err.message })}\n\n`) } catch {}
      }

      // Post-stream: store in query memory, run second-pass (fire-and-forget)
      if (fullText.length > 0) {
        const entities = extractEntitiesFromText(fullText, content)
        insertQueryMemory(content, fullText, entities).catch(() => {})
      }

      res.end()
    } catch (err: any) {
      if (!res.headersSent) {
        res.status(500).json({ error: err.message })
      } else {
        res.write(`data: ${JSON.stringify({ type: 'error', content: err.message })}\n\n`)
        res.end()
      }
    }
  })

  // ── POST /api/chat (Spotlight queries — non-streaming) ──────

  app.post('/api/chat', async (req, res) => {
    try {
      const { content, mode } = req.body
      if (!content) { res.status(400).json({ error: 'content required' }); return }

      // ── Forget commands: handled entirely in code, no Claude call ──
      const forgetResult = await handleForgetCommand(content)
      if (forgetResult) {
        res.json({ text: forgetResult })
        return
      }

      const settings = await getSettings()
      const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
      if (!apiKey) { res.status(400).json({ error: 'No Anthropic API key' }); return }

      // Log as observation (fire-and-forget)
      insertObservation('chat', 'queried', content, {}, 'neutral', [], '').catch(() => {})

      // Prune stale query memory (fire-and-forget)
      pruneQueryMemory(30).catch(() => {})

      const Anthropic = (await import('@anthropic-ai/sdk')).default
      const client = new Anthropic({ apiKey })

      // Load recent conversation context (last 10 queries within 30 minutes)
      const recentQueries = await getRecentQueryMemory(10, 30)

      // Check if this query is a correction of a previous response
      const lastQuery = recentQueries.length > 0 ? recentQueries[recentQueries.length - 1] : null
      const { isCorrection, correctionText: corrText } = detectCorrectionInQuery(content, lastQuery?.response_text || null)
      if (isCorrection && lastQuery) {
        insertCorrection(
          lastQuery.response_text?.slice(0, 500) || 'previous response',
          lastQuery.query_text || '',
          'corrected',
          corrText,
          '', // reasoning chain — we don't have it yet
          []
        ).catch(() => {})
        processCorrection(
          lastQuery.response_text?.slice(0, 200) || '',
          'corrected',
          corrText,
          lastQuery.query_text || ''
        ).catch(() => {})
      }

      // Check if this is a draft edit action
      const { isDraftEdit, editedPart, originalDraft } = detectDraftEdit(content, lastQuery?.query_text || null, lastQuery?.response_text || null)
      if (isDraftEdit) {
        insertCorrection(
          originalDraft.slice(0, 500),
          editedPart,
          'edited_draft',
          `User wants to edit the draft for: ${editedPart}`,
          '',
          []
        ).catch(() => {})
        processCorrection(
          `Draft: ${originalDraft.slice(0, 200)}`,
          'edited_draft',
          `User edited: ${editedPart}`,
          editedPart
        ).catch(() => {})
      }

      // ── Classify query and load context from cache ──────────────
      const queryClass = classifyQuery(content)
      const t0 = Date.now()

      // Get cached context (rebuilds if stale — all DB queries run in parallel)
      const cached = await getCachedContext()
      const cacheMs = Date.now() - t0

      // Format recent conversation (always fresh, not cached)
      const recentConversation = recentQueries.length > 0
        ? `RECENT CONVERSATION (last 30 minutes):\n${recentQueries.map((q: any) => {
            const entities = Array.isArray(q.entities_mentioned) ? q.entities_mentioned :
              (typeof q.entities_mentioned === 'string' ? JSON.parse(q.entities_mentioned) : [])
            return `User: "${q.query_text}"\nIRIS: "${(q.response_text || '').slice(0, 150)}"${entities.length ? ` [entities: ${entities.join(', ')}]` : ''}`
          }).join('\n\n')}`
        : ''

      // Assemble context sized by query type
      const context = assembleContext(cached, queryClass.type, recentConversation)

      const isSpotlight = mode === 'spotlight'

      // ── Build system prompt with cache_control for stable blocks ──
      // Stable blocks (persona, rules, context) get cache_control for
      // Anthropic prompt caching. Volatile parts (query) stay uncached.

      const IRIS_RULES = `You are IRIS — a sharp, informed personal assistant embedded in a macOS Spotlight bar. You already know everything about this person. You are NOT a chatbot or search engine.

RESPONSE RULES:
- Lead with the answer. Never lead with context or preamble.
- Default to the shortest honest answer. Simple facts: 1 line. Status checks: 2 lines max. Complex queries: 3-5 lines max.
- NEVER use: "Looking at your...", "Based on your...", "It seems like...", "I can see that...", "It appears...", "I'd recommend...", "You might want to...", "Feel free to...", "Hope this helps.", "Sure!", "Of course!", "I'd be happy to help."
- No greetings, no sign-offs, no filler. Just the answer.
- Prefer specific over vague: "14 days" not "a while ago", "2:15pm" not "this afternoon", names not "your contact".
- If you don't have data, say what's missing and offer a next step. Never apologize.

CONFIDENCE RULES (match your phrasing to the evidence strength):
- High confidence (>80%): state as fact.
- Medium (50-80%): hedge slightly. "You seem to prefer..."
- Low (<50%): explicit uncertainty or don't mention.
- NEVER confidently assert something backed by only 1 observation from 1 source.

OPINION RULES:
- When the user asks for a recommendation, pick ONE answer with one-sentence defense.
- Academic deadlines > social follow-ups > nice-to-haves.
- If user says "options" or "list", THEN give a list. Otherwise, commit.
- Never hedge with "you could". Say "Do X. Because Y."
- Centrum Today list items > email items.

FORMATTING:
- **bold** for entities. {{curly}} for metadata. [Button] for actions.
- Multi-item: one line each with — separator.
- >6 lines: truncate with {{...more}}
- Drafts: text then [Send] [Edit] [Cancel]`

      // Build system as array of content blocks with cache_control
      const systemBlocks: any[] = isSpotlight ? [
        // Block 1: IRIS behavior rules — highly stable, cache aggressively
        {
          type: 'text',
          text: IRIS_RULES,
          cache_control: { type: 'ephemeral' }
        },
        // Block 2: Persona + stable context — changes on persona rewrite (~every 15min)
        {
          type: 'text',
          text: `\n\nCONTEXT — this is who you're talking to and what you know:\n${context}`,
          cache_control: { type: 'ephemeral' }
        }
      ] : [
        { type: 'text', text: `You are Universal AI, a personal AI. Be concise and helpful.\n\n${context}` }
      ]

      // ── Fire main response (+ parallel second-pass for analytical) ──

      const mainCallPromise = client.messages.create({
        model: settings.model || 'claude-sonnet-4-20250514',
        max_tokens: isSpotlight ? 300 : 500,
        system: systemBlocks,
        messages: [{ role: 'user', content }]
      })

      // For analytical queries, fire second-pass in parallel
      let secondPassPromise: Promise<string | null> | null = null
      if (isSpotlight && !queryClass.skipSecondPass) {
        secondPassPromise = runSecondPass({
          userQuery: content,
          literalResponse: '', // second-pass runs independently with context only
          context
        }).catch(() => null)
      }

      // Wait for main response
      const response = await mainCallPromise
      let text = response.content[0].type === 'text' ? response.content[0].text : ''

      // Log cache performance from response headers
      const usage = (response as any).usage
      if (usage) {
        const cacheRead = usage.cache_read_input_tokens || 0
        const cacheCreate = usage.cache_creation_input_tokens || 0
        const totalInput = usage.input_tokens || 0
        if (cacheRead > 0 || cacheCreate > 0) {
          console.log(`[cache] Prompt cache: ${cacheRead} read, ${cacheCreate} created, ${totalInput} total input tokens`)
        }
      }

      // Merge second-pass result if it ran
      if (secondPassPromise) {
        try {
          const addition = await secondPassPromise
          if (addition && addition.length > 5 && addition !== 'NONE') {
            // Verify the addition doesn't repeat content already in the main response
            const mainWords = new Set(text.toLowerCase().split(/\s+/).filter((w: string) => w.length > 4))
            const addWords = addition.toLowerCase().split(/\s+/).filter((w: string) => w.length > 4)
            const overlap = addWords.filter((w: string) => mainWords.has(w)).length
            const overlapRatio = addWords.length > 0 ? overlap / addWords.length : 1

            if (overlapRatio < 0.5) {
              text = text + '\n\n' + addition
            }
          }
        } catch {
          // Second pass failure is non-critical
        }
      }

      // Extract entities from response and store in query memory
      const entities = extractEntitiesFromText(text, content)
      insertQueryMemory(content, text, entities).catch(() => {})

      res.json({ text })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── GET /api/situation ──────────────────────────────────────

  app.get('/api/situation', async (_req, res) => {
    try {
      const contacts = await listContacts(10)
      const stale = await listStaleThreads(3)
      const awaiting = await listThreadsAwaitingReply()
      const upcoming = await listUpcomingEvents(14)
      const commitments = await listCommitments('active')
      const recentDocs = await listRecentDocuments(7)
      const profile = await getLivingProfile()
      const obsCounts = await getObservationCountBySource()

      res.json({
        profile: profile?.slice(0, 500) ?? null,
        contacts: contacts.map((c: any) => ({ name: c.name, email: c.email, score: c.relationship_score })),
        stale_threads: stale.map((t: any) => ({ subject: t.subject, stale_days: t.stale_days })),
        awaiting_reply: awaiting.map((t: any) => ({ subject: t.subject, from: t.last_message_from })),
        upcoming_events: upcoming.map((e: any) => ({ summary: e.summary, start: e.start_time })),
        commitments: commitments.slice(0, 10).map((c: any) => ({ description: c.description, due: c.due_at })),
        recent_documents: recentDocs.slice(0, 10).map((d: any) => ({ filename: d.filename, type: d.type })),
        observation_counts: obsCounts
      })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── GET /api/briefing ───────────────────────────────────────

  app.get('/api/briefing', async (_req, res) => {
    try {
      const settings = await getSettings()
      const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
      if (!apiKey) { res.status(400).json({ error: 'No API key' }); return }

      const Anthropic = (await import('@anthropic-ai/sdk')).default
      const client = new Anthropic({ apiKey })

      const profile = await getLivingProfile() ?? ''
      const stale = await listStaleThreads(3)
      const upcoming = await listUpcomingEvents(1)
      const commitments = await listCommitments('active')

      const context = [
        profile.slice(0, 1500),
        `Stale: ${stale.slice(0, 3).map((t: any) => t.subject).join('; ')}`,
        `Events: ${upcoming.slice(0, 3).map((e: any) => e.summary).join('; ') || 'none'}`,
        `Commitments: ${commitments.slice(0, 3).map((c: any) => c.description.slice(0, 60)).join('; ') || 'none'}`
      ].join('\n')

      const response = await client.messages.create({
        model: settings.model || 'claude-sonnet-4-20250514',
        max_tokens: 400,
        system: 'You are Universal AI giving a concise daily briefing. Under 75 words. Conversational, not robotic.',
        messages: [{ role: 'user', content: context }]
      })

      const text = response.content[0].type === 'text' ? response.content[0].text : ''
      res.json({ text })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── GET /api/observations ───────────────────────────────────

  app.get('/api/observations', async (req, res) => {
    const limit = parseInt(req.query.limit as string || '20', 10)
    const obs = await getRecentObservations(limit)
    const counts = await getObservationCountBySource()
    res.json({ observations: obs, counts, total: await totalObservationCount() })
  })

  // ── GET /api/insights ───────────────────────────────────────

  app.get('/api/insights', async (_req, res) => {
    const insights = await listInsights('active')
    res.json({ insights, total: insights.length })
  })

  // ── GET /api/persona ────────────────────────────────────────

  app.get('/api/persona', async (_req, res) => {
    const persona = await getLivingProfile()
    res.json({ persona: persona ?? 'No persona yet', length: persona?.length ?? 0 })
  })

  // ── GET /api/feed (single endpoint for corner feed panel) ───

  app.get('/api/feed', async (_req, res) => {
    try {
      const pool = getPool()

      const [
        urgentActions,
        todayActions,
        ghostActions,
        routineBreaksRes,
        relationshipShifts,
        trendFlags,
        lifeEventsRecent,
        personaProfile,
        obsRate,
        sourceHealth
      ] = await Promise.all([
        // Urgent: predicted actions with urgency critical/high, deduped by title
        pool.query(`
          SELECT DISTINCT ON (LOWER(TRIM(title)))
            id, title, description, action_type, urgency, confidence, draft_content, trigger_context, related_entity
          FROM predicted_actions WHERE status='pending' AND expires_at > NOW()
          AND urgency IN ('critical', 'high')
          ORDER BY LOWER(TRIM(title)), confidence DESC LIMIT 10
        `),
        // Today: medium urgency, deduped by title
        pool.query(`
          SELECT DISTINCT ON (LOWER(TRIM(title)))
            id, title, description, action_type, urgency, confidence, draft_content, trigger_context, related_entity
          FROM predicted_actions WHERE status='pending' AND expires_at > NOW()
          AND urgency IN ('medium', 'low')
          ORDER BY LOWER(TRIM(title)), confidence DESC LIMIT 10
        `),
        // Ghost: recently expired or low-confidence filtered actions
        pool.query(`
          SELECT id, title, description, action_type, confidence, trigger_context
          FROM predicted_actions WHERE (status='expired' OR confidence < 0.5)
          AND predicted_at > NOW() - INTERVAL '2 hours'
          ORDER BY predicted_at DESC LIMIT 10
        `),
        // Routine breaks
        pool.query(`
          SELECT id, description, expected_behavior, actual_behavior, severity, days_broken, detected_at
          FROM routine_breaks WHERE detected_at > NOW() - INTERVAL '7 days'
          ORDER BY detected_at DESC LIMIT 5
        `),
        // Relationship shifts (with specific evidence data)
        pool.query(`
          SELECT name, current_closeness, trajectory, anomaly, narrative_note,
                 interaction_freq_30d, whatsapp_msgs_30d, email_count_30d,
                 initiator_ratio, last_interaction
          FROM relationships WHERE (trajectory != 'stable' OR anomaly IS NOT NULL)
          ORDER BY current_closeness DESC LIMIT 10
        `),
        // Trend flags
        pool.query(`
          SELECT statement FROM insights
          WHERE id LIKE 'trend-%' AND status='active'
          ORDER BY last_confirmed DESC LIMIT 5
        `),
        // Recent life events
        pool.query(`
          SELECT type, summary, confidence, detected_at
          FROM life_events WHERE detected_at > NOW() - INTERVAL '7 days'
          ORDER BY detected_at DESC LIMIT 3
        `),
        // Persona freshness
        pool.query("SELECT updated_at, version FROM living_profile WHERE id='main'"),
        // Observation rate (last hour)
        pool.query("SELECT COUNT(*)::int as c FROM observations WHERE timestamp > NOW() - INTERVAL '1 hour'"),
        // Source health (count per source in last hour)
        pool.query(`
          SELECT source, COUNT(*)::int as c FROM observations
          WHERE timestamp > NOW() - INTERVAL '1 hour'
          GROUP BY source ORDER BY c DESC
        `)
      ])

      // Compute persona freshness
      const personaRow = personaProfile.rows[0]
      const personaAge = personaRow ? Math.round((Date.now() - new Date(personaRow.updated_at).getTime()) / 60000) : 999
      const personaFreshness = personaAge < 15 ? 'fresh' : personaAge < 60 ? 'aging' : 'stale'

      // Active chains for urgent items
      const chainsRes = await pool.query(
        `SELECT ac.id, ac.trigger_event, cs.step_number, cs.title, cs.description, cs.status
         FROM action_chains ac JOIN chain_steps cs ON cs.chain_id = ac.id
         WHERE ac.status = 'active' ORDER BY ac.created_at DESC, cs.step_number LIMIT 15`
      )
      const chains: Record<string, any[]> = {}
      for (const row of chainsRes.rows) {
        if (!chains[row.id]) chains[row.id] = []
        chains[row.id].push({ step: row.step_number, title: row.title, description: row.description, status: row.status })
      }

      // ── PRIORITY SCORING ──────────────────────────────────────────
      // Score every item 0-100, then take top N per section.
      // Corner feed should fit one screen: max 3+3+2 = 8 cards total.

      // Load persona text for keyword matching
      const personaText = (await getLivingProfile() || '').toLowerCase()
      const topContactNames = relationshipShifts.rows.slice(0, 10).map((r: any) => r.name?.toLowerCase()).filter(Boolean)

      // Build relationship lookup for enriching TODAY cards
      const relLookup = new Map<string, any>()
      for (const r of relationshipShifts.rows) {
        if (r.name) relLookup.set(r.name.toLowerCase(), r)
      }

      // Enrich TODAY relationship cards with specific evidence (same as NOTICED)
      for (const a of todayActions.rows) {
        if (a.action_type === 'relationship_maintenance') {
          const name = (a.title || '').replace(/^Reach out to /i, '').trim()
          const rel = relLookup.get(name.toLowerCase())
          if (rel) {
            const freq30 = rel.interaction_freq_30d ? `${Math.round(rel.interaction_freq_30d * 30)} interactions this month` : ''
            const lastDays = rel.last_interaction ? `${Math.round((Date.now() - new Date(rel.last_interaction).getTime()) / 86400000)}d since last contact` : ''
            const waCount = rel.whatsapp_msgs_30d > 0 ? `${rel.whatsapp_msgs_30d} WhatsApp msgs` : ''
            const emailCount = rel.email_count_30d > 0 ? `${rel.email_count_30d} emails` : ''
            const initRatio = rel.initiator_ratio !== undefined ? `you initiate ${Math.round(rel.initiator_ratio * 100)}%` : ''
            const parts = [freq30, lastDays, waCount, emailCount, initRatio].filter(Boolean)
            if (parts.length > 0) a.trigger_context = parts.slice(0, 3).join(', ')
          }
        }
      }

      // Score urgent/today actions
      function scoreAction(a: any): number {
        let score = 0
        // Stale days signal (from trigger_context — e.g. "stale for 16 days")
        const staleMatch = (a.trigger_context || '').match(/(\d+)\s*days?\s*stale/i)
        if (staleMatch) score += Math.min(70, parseInt(staleMatch[1]) * 5)
        // Days since last contact (enriched relationship cards — e.g. "15d since last contact")
        const contactMatch = (a.trigger_context || '').match(/(\d+)d since last contact/)
        if (contactMatch) score += Math.min(30, parseInt(contactMatch[1]) * 2)
        // Top contact bonus
        const titleLower = (a.title || '').toLowerCase()
        if (topContactNames.some((n: string) => titleLower.includes(n))) score += 20
        // Deadline proximity
        if (a.urgency === 'critical') score += 15
        // Chain cascade bonus
        if (a.related_entity && chains[a.related_entity]) score += 10
        return Math.min(100, score)
      }

      const scoredUrgent = urgentActions.rows.map((a: any) => ({ ...a, priority_score: scoreAction(a) }))
        .sort((a: any, b: any) => b.priority_score - a.priority_score)
      const scoredToday = todayActions.rows.map((a: any) => ({ ...a, priority_score: scoreAction(a) }))
        .sort((a: any, b: any) => b.priority_score - a.priority_score)
        .filter((a: any) => a.priority_score >= 25)

      // Score noticed items
      const noticedCandidates: any[] = []

      // Routine breaks — filter noisy URL patterns
      for (const b of routineBreaksRes.rows) {
        let score = 0
        const desc = (b.description || '').toLowerCase()

        // Kill generic URL patterns
        const isGenericUrl = /visits\s+\S+\.\S+\s+most\s+on/i.test(b.description)
        if (isGenericUrl) { score = 0 } // hard zero for URL visit patterns
        else {
          // Person in top contacts
          if (topContactNames.some((n: string) => desc.includes(n))) score += 30
          // Project/commitment in persona
          const personaKeywords = ['extended essay', 'tattva', 'centrum', 'ibdp', 'airpoint', 'chetana', 'kutty olam', 'history ia', 'cas', 'tok']
          if (personaKeywords.some(k => desc.includes(k))) score += 30
          // Centrum tracked
          if (desc.includes('centrum') || desc.includes('today list')) score += 20
          // Severity-adjusted duration
          if (b.days_broken > 5) score += 15
          if (b.severity === 'high') score += 10
        }

        if (score > 50) {
          noticedCandidates.push({ type: 'routine_break', text: b.description, detail: `Expected: ${b.expected_behavior}. Actual: ${b.actual_behavior}`, severity: b.severity, days: b.days_broken, score })
        }
      }

      // Relationship shifts — specific evidence, not templated
      for (const r of relationshipShifts.rows) {
        if (r.trajectory === 'stable' && !r.anomaly) continue
        let score = 0
        const inTop10 = r.current_closeness > 25

        if (inTop10 && r.trajectory !== 'stable') score += 50
        if (r.anomaly === 'sudden_silence' || r.anomaly === 'overdue_followup') score += 20
        // Trajectory crossed direction
        if (r.trajectory === 'declining' && r.current_closeness > 30) score += 30
        if (r.trajectory === 'rising' && r.current_closeness > 40) score += 20

        if (score > 50) {
          const arrow = r.trajectory === 'rising' ? '↑' : r.trajectory === 'declining' ? '↓' : '→'
          // Specific evidence from relationship data
          const freq30 = r.interaction_freq_30d ? `${Math.round(r.interaction_freq_30d * 30)} interactions this month` : ''
          const lastDays = r.last_interaction ? `${Math.round((Date.now() - new Date(r.last_interaction).getTime()) / 86400000)}d since last contact` : ''
          const waCount = r.whatsapp_msgs_30d > 0 ? `${r.whatsapp_msgs_30d} WhatsApp msgs` : ''
          const emailCount = r.email_count_30d > 0 ? `${r.email_count_30d} emails` : ''
          const initRatio = r.initiator_ratio !== undefined ? `you initiate ${Math.round(r.initiator_ratio * 100)}%` : ''

          const evidenceParts = [freq30, lastDays, waCount, emailCount, initRatio].filter(Boolean)
          const detail = evidenceParts.length > 0 ? evidenceParts.slice(0, 3).join(', ') : (r.narrative_note || `Trajectory: ${r.trajectory}`)

          // Frame as an insight, not a metric
          const directionWord = r.trajectory === 'rising' ? 'is rising' : r.trajectory === 'declining' ? 'is fading' : 'shifted'
          const insightText = `${r.name} ${directionWord} — ${detail.split(',')[0]}`
          noticedCandidates.push({ type: 'relationship', text: insightText, detail, anomaly: r.anomaly, score })
        }
      }

      // Trend flags — only high-delta, persona-relevant
      for (const t of trendFlags.rows) {
        let score = 0
        const stmt = (t.statement || '').toLowerCase()
        const deltaMatch = stmt.match(/(\d+)%/)
        const delta = deltaMatch ? parseInt(deltaMatch[1]) : 0

        if (delta > 50) score += 40
        // Check if subject is in persona priorities
        const personaKeywords = ['extended essay', 'tattva', 'ibdp', 'software', 'coding', 'whatsapp', 'email']
        if (personaKeywords.some(k => stmt.includes(k))) score += 20
        if (delta > 100) score += 15

        if (score > 40) {
          noticedCandidates.push({ type: 'trend', text: t.statement, score })
        }
      }

      // Life events always win
      for (const e of lifeEventsRecent.rows) {
        noticedCandidates.push({ type: 'life_event', text: `${e.type}: ${e.summary}`, confidence: e.confidence, detected: e.detected_at, score: 90 })
      }

      // Sort all noticed by score, take top 2
      noticedCandidates.sort((a, b) => b.score - a.score)
      const noticed = noticedCandidates.slice(0, 2)
      const noticedOverflow = noticedCandidates.length - 2

      // Overflow counts (TODAY capped at 3 to keep feed scannable)
      const urgentOverflow = Math.max(0, scoredUrgent.length - 3)
      const todayOverflow = Math.min(3, Math.max(0, scoredToday.length - 3))

      res.json({
        status: {
          obsLastHour: obsRate.rows[0]?.c || 0,
          personaFreshness,
          personaAgeMinutes: personaAge,
          personaVersion: personaRow?.version || 0,
          sourcesActive: sourceHealth.rows.length,
          sourceBreakdown: sourceHealth.rows.reduce((acc: any, r: any) => { acc[r.source] = r.c; return acc }, {}),
          lastObservation: new Date().toISOString()
        },
        urgent: scoredUrgent.slice(0, 3),
        urgentOverflow,
        today: scoredToday.slice(0, 3),
        todayOverflow,
        noticed,
        noticedOverflow: Math.max(0, noticedOverflow),
        ghost: ghostActions.rows,
        chains
      })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── GET /api/actions ────────────────────────────────────────

  app.get('/api/actions', async (req, res) => {
    const status = req.query.status as string | undefined
    const actions = await listProposedActions(status)
    res.json({ actions })
  })

  // ── POST /api/actions/:id/approve ───────────────────────────

  app.post('/api/actions/:id/approve', async (req, res) => {
    await updateActionStatus(req.params.id, 'approved')
    res.json({ ok: true })
  })

  // ── POST /api/actions/:id/reject ────────────────────────────

  app.post('/api/actions/:id/reject', async (req, res) => {
    await updateActionStatus(req.params.id, 'rejected')
    const reason = req.body?.reason || ''

    // Log as correction for active learning
    const pool = getPool()
    const action = await pool.query('SELECT title, description FROM proposed_actions WHERE id=$1', [req.params.id])
    if (action.rows.length > 0) {
      const a = action.rows[0]
      insertCorrection(
        `Proposed action: ${a.title}`,
        a.description || '',
        'dismissed',
        reason,
        '',
        []
      ).catch(() => {})
      processCorrection(
        `Proposed: ${a.title} — ${(a.description || '').slice(0, 100)}`,
        'dismissed',
        reason || `User dismissed: ${a.title}`,
        a.description || ''
      ).catch(() => {})
    }

    res.json({ ok: true })
  })

  // ── Cache stats ─────────────────────────────────────────────

  app.get('/api/cache', (_req, res) => {
    res.json(getCacheStats())
  })

  app.post('/api/cache/invalidate', (_req, res) => {
    invalidateCache('manual')
    res.json({ ok: true })
  })

  // ── POST /api/observe (external observation ingestion) ──────

  app.post('/api/observe', async (req, res) => {
    const { source, event_type, content } = req.body
    if (!source || !event_type || !content) {
      res.status(400).json({ error: 'source, event_type, content required' })
      return
    }
    const id = await insertObservation(source, event_type, content, {}, 'neutral', [], '')

    // Run life event detector on every observation (async, non-blocking)
    analyzeForLifeEvent(id, source, event_type, content, new Date().toISOString()).catch(() => {})

    res.json({ id })
  })

  // ── Predicted actions ──────────────────────────────────────

  app.post('/api/actions/predict', async (_req, res) => {
    try {
      const count = await runActionPredictor()
      res.json({ ok: true, predicted: count })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/actions/predicted', async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 10
      const actions = await getPendingActions(limit)
      res.json({ actions })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/actions/predicted/:id/serve', async (req, res) => {
    try {
      await markActionServed(req.params.id, req.body?.query || '')
      res.json({ ok: true })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Chain reasoning ────────────────────────────────────────

  app.post('/api/chains/generate', async (req, res) => {
    try {
      const { trigger, trigger_type, trigger_entity } = req.body
      if (!trigger) { res.status(400).json({ error: 'trigger required' }); return }
      const result = await generateChain(trigger, trigger_type || 'manual', trigger_entity || null)
      if (result) {
        res.json({ ok: true, chain_id: result.chainId, steps: result.steps.length })
      } else {
        res.json({ ok: false, reason: 'No chain generated (duplicate or insufficient context)' })
      }
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/chains/generate-all', async (_req, res) => {
    try {
      const count = await generateChainsForUrgentItems()
      res.json({ ok: true, chains: count })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/chains', async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 5
      const chains = await getActiveChains(limit)
      res.json({ chains })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/chains/step/:id/complete', async (req, res) => {
    try {
      const result = await completeChainStep(req.params.id)
      res.json({ ok: true, nextStep: result.nextStep })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Routines ───────────────────────────────────────────────

  app.post('/api/routines/extract', async (_req, res) => {
    try {
      const count = await extractRoutines()
      const breaks = await detectRoutineBreaks()
      res.json({ ok: true, routines: count, breaks })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/routines', async (_req, res) => {
    try {
      const routines = await getActiveRoutines()
      const breaks = await getRecentBreaks(10)
      res.json({ routines, breaks })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Skills ─────────────────────────────────────────────────

  app.post('/api/skills/extract', async (_req, res) => {
    try {
      const count = await extractSkills()
      res.json({ ok: true, skills: count })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/skills', async (_req, res) => {
    try {
      const skills = await getSkills()
      res.json({ skills })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Active learning: corrections & lessons ─────────────────

  app.post('/api/correction', async (req, res) => {
    try {
      const { trigger, user_action, correction_text, context } = req.body
      if (!trigger || !user_action) {
        res.status(400).json({ error: 'trigger and user_action required' })
        return
      }
      const id = await insertCorrection(trigger, context || '', user_action, correction_text || '', '', [])
      const result = await processCorrection(trigger, user_action, correction_text || '', context || '')
      res.json({ ok: true, id, ...result })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/corrections', async (_req, res) => {
    try {
      const corrections = await getAllCorrections(50)
      res.json({ corrections })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/lessons', async (_req, res) => {
    try {
      const lessons = await getActiveLessons()
      res.json({ lessons })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Relationship graph ──────────────────────────────────────

  app.post('/api/relationships/update', async (_req, res) => {
    try {
      const count = await runRelationshipEngine()
      res.json({ ok: true, updated: count })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/relationships', async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 20
      const rels = await getTopRelationships(limit)
      res.json({ relationships: rels })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Persona confidence rewrite ──────────────────────────────

  app.post('/api/persona/rewrite', async (_req, res) => {
    try {
      const result = await rewritePersonaWithConfidence()
      if (result) {
        res.json({ ok: true, length: result.length })
      } else {
        res.json({ ok: false, error: 'Rewrite returned null — check API key and persona' })
      }
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Life events ────────────────────────────────────────────

  app.get('/api/life-events', async (_req, res) => {
    try {
      const pool = getPool()
      const r = await pool.query('SELECT * FROM life_events ORDER BY detected_at DESC LIMIT 20')
      res.json({ events: r.rows })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── POST /api/settings ─────────────────────────────────────

  app.post('/api/settings', async (req, res) => {
    for (const [key, value] of Object.entries(req.body)) {
      if (typeof value === 'string') await setSetting(key, value)
    }
    res.json({ ok: true })
  })

  // ── GET /api/settings ──────────────────────────────────────

  app.get('/api/settings', async (_req, res) => {
    const settings = await getSettings()
    // Mask sensitive values
    const masked: Record<string, string> = {}
    for (const [k, v] of Object.entries(settings)) {
      masked[k] = k.includes('key') || k.includes('secret') || k.includes('token')
        ? v.slice(0, 8) + '...' + v.slice(-4)
        : v
    }
    res.json(masked)
  })

  // ── Digest endpoints ────────────────────────────────────────

  app.post('/api/digest/backfill', async (req, res) => {
    try {
      const days = parseInt(req.query.days as string) || 30
      console.log(`[digest] Backfill requested: ${days} days`)
      const result = await backfillDigests(days)
      res.json({ ok: true, ...result })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/digest/generate', async (req, res) => {
    try {
      const { date } = req.body
      if (!date) { res.status(400).json({ error: 'date required (YYYY-MM-DD)' }); return }
      const result = await generateDailyDigest(date)
      res.json({ ok: true, generated: !!result, date })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/digests', async (_req, res) => {
    try {
      const pool = getPool()
      const daily = await pool.query(
        "SELECT id, timestamp, extracted_entities FROM observations WHERE source='daily_digest' ORDER BY timestamp DESC LIMIT 60"
      )
      const weekly = await pool.query(
        "SELECT id, timestamp, extracted_entities FROM observations WHERE source='weekly_digest' ORDER BY timestamp DESC LIMIT 10"
      )
      res.json({
        daily: daily.rows.map((r: any) => ({
          id: r.id,
          timestamp: r.timestamp,
          ...(typeof r.extracted_entities === 'string' ? JSON.parse(r.extracted_entities) : r.extracted_entities)
        })),
        weekly: weekly.rows.map((r: any) => ({
          id: r.id,
          timestamp: r.timestamp,
          ...(typeof r.extracted_entities === 'string' ? JSON.parse(r.extracted_entities) : r.extracted_entities)
        }))
      })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── POST /api/trends (run trend analysis) ───────────────────

  app.post('/api/trends', async (_req, res) => {
    try {
      const report = await detectTrends()
      res.json(report)
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/trends', async (_req, res) => {
    try {
      const pool = getPool()
      const r = await pool.query(
        "SELECT raw_content, extracted_entities, timestamp FROM observations WHERE source='trend_detector' ORDER BY timestamp DESC LIMIT 5"
      )
      res.json({ reports: r.rows })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── WHISPER NOTIFICATIONS ────────────────────────────────────────

  app.post('/api/whispers/check', async (_req, res) => {
    try {
      const pool = getPool()
      const now = new Date()
      const nowISO = now.toISOString()

      // ── Rate limit check ──────────────────────────────────────
      const rlRes = await pool.query(`SELECT * FROM settings WHERE key IN ('whisper_daily_count','whisper_daily_reset','whisper_hourly_count','whisper_hourly_reset','whisper_last_at','whisper_daily_limit','whisper_tomorrow_reduction')`)
      const rl: Record<string, string> = {}
      for (const r of rlRes.rows) rl[r.key] = r.value

      const dailyLimit = parseInt(rl.whisper_daily_limit || '5')
      const tomorrowReduction = parseInt(rl.whisper_tomorrow_reduction || '0')
      const effectiveLimit = Math.max(1, dailyLimit - tomorrowReduction)

      // Reset daily counter if new day
      const dailyReset = rl.whisper_daily_reset ? new Date(rl.whisper_daily_reset) : new Date(0)
      let dailyCount = parseInt(rl.whisper_daily_count || '0')
      if (now.toDateString() !== dailyReset.toDateString()) {
        dailyCount = 0
        await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_daily_count', '0') ON CONFLICT (key) DO UPDATE SET value = '0'`)
        await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_daily_reset', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, [nowISO])
        // Clear yesterday's reduction
        if (tomorrowReduction > 0) {
          await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_tomorrow_reduction', '0') ON CONFLICT (key) DO UPDATE SET value = '0'`)
        }
      }

      // Reset hourly counter
      const hourlyReset = rl.whisper_hourly_reset ? new Date(rl.whisper_hourly_reset) : new Date(0)
      let hourlyCount = parseInt(rl.whisper_hourly_count || '0')
      if (now.getTime() - hourlyReset.getTime() > 3600000) {
        hourlyCount = 0
        await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_hourly_count', '0') ON CONFLICT (key) DO UPDATE SET value = '0'`)
        await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_hourly_reset', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, [nowISO])
      }

      // 15-min cooldown
      const lastWhisperAt = rl.whisper_last_at ? new Date(rl.whisper_last_at) : new Date(0)
      const cooldownOk = now.getTime() - lastWhisperAt.getTime() > 900000 // 15 min

      // ── Gather trigger candidates ─────────────────────────────
      const candidates: any[] = []

      // 1. Life events (confidence > 80)
      const lifeEvents = await pool.query(
        `SELECT * FROM life_events WHERE confidence > 0.8 AND detected_at > NOW() - INTERVAL '2 hours' ORDER BY detected_at DESC LIMIT 3`
      )
      for (const le of lifeEvents.rows) {
        const severity = le.confidence > 0.9 ? 'critical' : 'normal'
        const ago = Math.round((now.getTime() - new Date(le.detected_at).getTime()) / 60000)
        candidates.push({
          type: 'life_event',
          severity,
          body: `Life event: ${(le.summary || '').slice(0, 65)}`,
          subtitle: `detected · ${ago}m ago`,
          trigger_hash: `life_event:${le.id}`,
          trigger_data: { life_event_id: le.id, type: le.type, confidence: le.confidence },
          actions: [{ label: 'View', action: 'view' }, { label: 'Dismiss', action: 'dismiss' }]
        })
      }

      // 2. Chain cascade alerts (3+ pending steps = cascade risk)
      const chains = await pool.query(`
        SELECT ac.id, ac.trigger_event, ac.trigger_entity, COUNT(cs.id)::int as pending_steps
        FROM action_chains ac
        JOIN chain_steps cs ON cs.chain_id = ac.id AND cs.status = 'pending'
        WHERE ac.status = 'active' AND ac.created_at > NOW() - INTERVAL '72 hours'
        GROUP BY ac.id HAVING COUNT(cs.id) >= 3
        LIMIT 2
      `)
      for (const c of chains.rows) {
        candidates.push({
          type: 'chain_cascade',
          severity: 'high',
          body: `Chain risk: ${(c.trigger_event || '').slice(0, 60)}`,
          subtitle: `${c.pending_steps} steps at risk`,
          trigger_hash: `chain:${c.id}`,
          trigger_data: { chain_id: c.id, pending_steps: c.pending_steps },
          actions: [{ label: 'View Chain', action: 'view_chain' }, { label: 'Dismiss', action: 'dismiss' }]
        })
      }

      // 3. Critical deadline crossing (<30 min to calendar event)
      const thirtyMin = new Date(now.getTime() + 30 * 60000).toISOString()
      const deadlines = await pool.query(
        `SELECT * FROM calendar_events WHERE start_time::timestamptz > $1::timestamptz AND start_time::timestamptz < $2::timestamptz AND status != 'cancelled' AND all_day = 0 LIMIT 3`,
        [nowISO, thirtyMin]
      )
      for (const ev of deadlines.rows) {
        const minsLeft = Math.round((new Date(ev.start_time).getTime() - now.getTime()) / 60000)
        candidates.push({
          type: 'deadline',
          severity: minsLeft < 15 ? 'high' : 'normal',
          body: `${(ev.summary || 'Event').slice(0, 50)} starts in ${minsLeft} minutes`,
          subtitle: `Calendar · ${ev.calendar_name || 'primary'}`,
          trigger_hash: `deadline:${ev.id}`,
          trigger_data: { event_id: ev.id, summary: ev.summary, minutes_left: minsLeft },
          actions: [{ label: 'Snooze 15min', action: 'snooze_15' }, { label: 'Open Calendar', action: 'open_calendar' }]
        })
      }

      // 4. Priority contact urgent signal
      const topContacts = await pool.query(`SELECT name, email, last_interaction, current_closeness FROM relationships ORDER BY current_closeness DESC LIMIT 5`)
      const top3 = topContacts.rows.slice(0, 3)
      const top5Names = topContacts.rows.map((r: any) => r.name?.toLowerCase()).filter(Boolean)
      const top3Names = top3.map((r: any) => r.name?.toLowerCase()).filter(Boolean)

      // Check for urgency keywords in recent observations from top 5
      if (top5Names.length > 0) {
        const urgentObs = await pool.query(
          `SELECT * FROM observations WHERE source IN ('email','whatsapp') AND timestamp > NOW() - INTERVAL '1 hour' AND (raw_content ILIKE '%urgent%' OR raw_content ILIKE '%ASAP%' OR raw_content ILIKE '%deadline%' OR raw_content ILIKE '%by EOD%' OR raw_content ILIKE '%important%') ORDER BY timestamp DESC LIMIT 5`
        )
        for (const obs of urgentObs.rows) {
          const content = (obs.raw_content || '').toLowerCase()
          const matchedContact = top5Names.find((n: string) => content.includes(n))
          if (matchedContact) {
            const ago = Math.round((now.getTime() - new Date(obs.timestamp).getTime()) / 60000)
            candidates.push({
              type: 'priority_contact',
              severity: 'normal',
              body: `${matchedContact.split(' ').map((w: string) => w[0].toUpperCase() + w.slice(1)).join(' ')} sent an urgent message`,
              subtitle: `via ${obs.source} · ${ago}m ago`,
              trigger_hash: `priority_contact:${obs.id}`,
              trigger_data: { observation_id: obs.id, contact: matchedContact, source: obs.source },
              actions: [{ label: 'Reply', action: 'reply' }, { label: 'View Thread', action: 'view' }]
            })
          }
        }

        // Top 3 contacts after 48h silence
        for (const c of top3) {
          if (!c.last_interaction) continue
          const hoursSinceContact = (now.getTime() - new Date(c.last_interaction).getTime()) / 3600000
          if (hoursSinceContact >= 48) {
            // Check if they sent something in the last hour
            const recentMsg = await pool.query(
              `SELECT id, source, timestamp FROM observations WHERE source IN ('email','whatsapp') AND timestamp > NOW() - INTERVAL '1 hour' AND raw_content ILIKE $1 LIMIT 1`,
              [`%${c.name}%`]
            )
            if (recentMsg.rows.length > 0) {
              const ago = Math.round((now.getTime() - new Date(recentMsg.rows[0].timestamp).getTime()) / 60000)
              candidates.push({
                type: 'priority_contact',
                severity: 'high',
                body: `${c.name} reached out after ${Math.round(hoursSinceContact / 24)}d of silence`,
                subtitle: `via ${recentMsg.rows[0].source} · ${ago}m ago`,
                trigger_hash: `priority_silence:${c.email}:${recentMsg.rows[0].id}`,
                trigger_data: { contact: c.name, email: c.email, hours_silent: hoursSinceContact },
                actions: [{ label: 'Reply', action: 'reply' }, { label: 'View Thread', action: 'view' }]
              })
            }
          }
        }
      }

      // 5. Severe routine break (3+ days, confidence > 80, severity > 0.7)
      const routineBreaks = await pool.query(`
        SELECT rb.*, r.confidence as routine_confidence
        FROM routine_breaks rb
        JOIN routines r ON r.id = rb.routine_id
        WHERE r.confidence > 0.8 AND rb.days_broken >= 3
        AND rb.detected_at > NOW() - INTERVAL '24 hours'
        ORDER BY rb.days_broken DESC LIMIT 2
      `)
      for (const rb of routineBreaks.rows) {
        // Parse severity as float — stored as text like 'high','medium','low' or float
        const sevVal = rb.severity === 'high' ? 0.9 : rb.severity === 'medium' ? 0.5 : rb.severity === 'low' ? 0.3 : parseFloat(rb.severity) || 0.3
        if (sevVal > 0.7) {
          candidates.push({
            type: 'routine_break',
            severity: 'normal',
            body: `${(rb.description || '').slice(0, 55)} — ${rb.days_broken}d break`,
            subtitle: `routine confidence ${Math.round(rb.routine_confidence * 100)}%`,
            trigger_hash: `routine_break:${rb.id}`,
            trigger_data: { routine_break_id: rb.id, days_broken: rb.days_broken },
            actions: [{ label: 'View', action: 'view' }, { label: 'Dismiss', action: 'dismiss' }]
          })
        }
      }

      // 6. Trend threshold alert (delta > 50%, persona top-3 priority)
      const personaText = (await getLivingProfile() || '').toLowerCase()
      const trendInsights = await pool.query(`SELECT * FROM insights WHERE id LIKE 'trend-%' AND status = 'active' ORDER BY last_confirmed DESC LIMIT 5`)
      for (const t of trendInsights.rows) {
        const stmt = (t.statement || '').toLowerCase()
        const deltaMatch = stmt.match(/(\d+)%/)
        const delta = deltaMatch ? parseInt(deltaMatch[1]) : 0
        if (delta <= 50) continue

        // Check if relates to persona top priorities
        const priorityKeywords = ['extended essay', 'tattva', 'ibdp', 'software', 'coding', 'iris']
        const relatesTopPriority = priorityKeywords.some(k => stmt.includes(k))
        if (!relatesTopPriority) continue

        candidates.push({
          type: 'trend_alert',
          severity: 'normal',
          body: `Trend: ${(t.statement || '').slice(0, 70)}`,
          subtitle: `${delta}% change detected`,
          trigger_hash: `trend:${t.id}`,
          trigger_data: { trend_id: t.id, delta },
          actions: [{ label: 'View', action: 'view' }, { label: 'Dismiss', action: 'dismiss' }]
        })
      }

      // 7. Stale thread hitting cascade threshold
      const staleThreads = await pool.query(`
        SELECT pa.id, pa.title, pa.trigger_context, pa.related_entity
        FROM predicted_actions pa
        WHERE pa.action_type = 'follow_up_draft' AND pa.status = 'pending' AND pa.expires_at > NOW()
        ORDER BY pa.confidence DESC LIMIT 5
      `)
      for (const st of staleThreads.rows) {
        // Check if there's an active chain depending on this thread
        const relatedChain = await pool.query(
          `SELECT ac.id FROM action_chains ac WHERE ac.status = 'active' AND (ac.trigger_entity = $1 OR ac.trigger_event ILIKE $2) LIMIT 1`,
          [st.related_entity || '', `%${(st.title || '').slice(0, 30)}%`]
        )
        if (relatedChain.rows.length > 0) {
          candidates.push({
            type: 'stale_thread',
            severity: 'normal',
            body: `Stale: ${(st.title || '').replace(/^Follow up: /i, '').slice(0, 55)} — blocks chain`,
            subtitle: `${st.trigger_context?.match(/(\d+)\s*days?/)?.[0] || 'overdue'}`,
            trigger_hash: `stale_thread:${st.id}`,
            trigger_data: { action_id: st.id, chain_id: relatedChain.rows[0].id },
            actions: [{ label: 'Approve Follow-up', action: 'approve' }, { label: 'Snooze', action: 'snooze' }]
          })
        }
      }

      // ── Apply filters ─────────────────────────────────────────
      // Clean expired suppressions
      await pool.query(`DELETE FROM whisper_suppressions WHERE suppressed_until < NOW()`)

      const readyWhispers: any[] = []

      for (const c of candidates) {
        // FILTER 1: Dedup — same trigger in last 4 hours?
        const recent = await pool.query(
          `SELECT id FROM whisper_log WHERE trigger_context = $1 AND fired_at > NOW() - INTERVAL '4 hours' AND suppressed = false LIMIT 1`,
          [c.trigger_hash]
        )
        if (recent.rows.length > 0) continue

        // FILTER 2: Suppression list
        const suppressed = await pool.query(
          `SELECT id FROM whisper_suppressions WHERE trigger_hash = $1 AND suppressed_until > NOW() LIMIT 1`,
          [c.trigger_hash]
        )
        if (suppressed.rows.length > 0) continue

        // FILTER 3: Dismissed this type 3+ times in 7 days (active learning)
        if (c.severity !== 'critical') {
          const dismissals = await pool.query(
            `SELECT COUNT(*)::int as c FROM whisper_log WHERE type = $1 AND user_action = 'dismiss' AND fired_at > NOW() - INTERVAL '7 days'`,
            [c.type]
          )
          if (dismissals.rows[0].c >= 3) continue
        }

        // FILTER 4: Rate limits (severity=critical bypasses daily limit but still counts)
        if (c.severity !== 'critical') {
          if (dailyCount >= effectiveLimit) continue
          if (hourlyCount >= 2) continue
          if (!cooldownOk) continue
        } else {
          // Critical past daily limit: fire anyway, reduce tomorrow
          if (dailyCount >= effectiveLimit) {
            await pool.query(
              `INSERT INTO settings (key, value) VALUES ('whisper_tomorrow_reduction', $1) ON CONFLICT (key) DO UPDATE SET value = $1`,
              [String(tomorrowReduction + 1)]
            )
          }
        }

        // Build whisper object
        const whisperObj = {
          id: crypto.randomUUID(),
          type: c.type,
          severity: c.severity,
          title: 'IRIS',
          body: c.body.slice(0, 80),
          subtitle: c.subtitle || '',
          actions: c.actions || [],
          trigger_context: c.trigger_hash,
          trigger_data: c.trigger_data || {}
        }

        readyWhispers.push(whisperObj)

        // Update rate limit counters
        dailyCount++
        hourlyCount++

        // Stop after first whisper per check (respect 15-min cooldown spirit)
        break
      }

      res.json({ whispers: readyWhispers })
    } catch (err: any) {
      console.error('[whispers] Check failed:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/whispers/fired', async (req, res) => {
    try {
      const pool = getPool()
      const { id, type, severity, body, subtitle, trigger_context, trigger_data, actions } = req.body

      // Log the whisper
      await pool.query(
        `INSERT INTO whisper_log (id, type, severity, title, body, subtitle, trigger_context, trigger_data, actions)
         VALUES ($1, $2, $3, 'IRIS', $4, $5, $6, $7, $8)`,
        [id, type, severity, body, subtitle || '', trigger_context || '', JSON.stringify(trigger_data || {}), JSON.stringify(actions || [])]
      )

      // Update rate limit counters
      const nowISO = new Date().toISOString()
      await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_last_at', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, [nowISO])
      await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_daily_count', (COALESCE((SELECT value::int FROM settings WHERE key = 'whisper_daily_count'), 0) + 1)::text) ON CONFLICT (key) DO UPDATE SET value = (COALESCE(settings.value::int, 0) + 1)::text`)
      await pool.query(`INSERT INTO settings (key, value) VALUES ('whisper_hourly_count', (COALESCE((SELECT value::int FROM settings WHERE key = 'whisper_hourly_count'), 0) + 1)::text) ON CONFLICT (key) DO UPDATE SET value = (COALESCE(settings.value::int, 0) + 1)::text`)

      res.json({ ok: true })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/whispers/action', async (req, res) => {
    try {
      const pool = getPool()
      const { whisper_id, action, time_to_action_ms } = req.body

      await pool.query(
        `UPDATE whisper_log SET user_action = $1, action_at = NOW(), time_to_action_ms = $2 WHERE id = $3`,
        [action, time_to_action_ms || 0, whisper_id]
      )

      // If snoozed, add to suppressions
      if (action === 'snooze' || action === 'snooze_15') {
        const whisper = await pool.query(`SELECT trigger_context, type FROM whisper_log WHERE id = $1`, [whisper_id])
        if (whisper.rows.length > 0) {
          const duration = action === 'snooze_15' ? 15 : 60 // minutes
          const until = new Date(Date.now() + duration * 60000).toISOString()
          await pool.query(
            `INSERT INTO whisper_suppressions (id, trigger_hash, whisper_type, suppressed_until) VALUES ($1, $2, $3, $4)`,
            [crypto.randomUUID(), whisper.rows[0].trigger_context, whisper.rows[0].type, until]
          )
        }
      }

      // If dismissed, log to correction_log for active learning
      if (action === 'dismiss') {
        const whisper = await pool.query(`SELECT type, trigger_context, trigger_data, body FROM whisper_log WHERE id = $1`, [whisper_id])
        if (whisper.rows.length > 0) {
          const w = whisper.rows[0]
          await pool.query(
            `INSERT INTO correction_log (id, trigger_action, trigger_context, user_action, correction_text, reasoning_chain, affected_claims)
             VALUES ($1, $2, $3, 'dismissed_whisper', $4, '', '[]')`,
            [crypto.randomUUID(), `whisper:${w.type}`, w.trigger_context || '', `User dismissed whisper: ${w.body}`]
          )
        }
      }

      res.json({ ok: true })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/whispers/history', async (_req, res) => {
    try {
      const pool = getPool()
      const history = await pool.query(
        `SELECT * FROM whisper_log WHERE fired_at > NOW() - INTERVAL '7 days' ORDER BY fired_at DESC LIMIT 50`
      )
      res.json({ whispers: history.rows })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── CONTEXTUAL OVERLAY ───────────────────────────────────────

  app.post('/api/overlay/evaluate', async (req, res) => {
    try {
      const pool = getPool()
      const { ocr_text, app: appName, window_title, url, file_path, mode } = req.body

      // ── UPGRADE 8: Multi-signal matching ───────────────────────
      const signals = {
        ocr: (ocr_text || '').toLowerCase(),
        title: (window_title || '').toLowerCase(),
        url: (url || '').toLowerCase(),
        file: (file_path || '').toLowerCase(),
        app: (appName || '').toLowerCase()
      }
      const allText = `${signals.ocr} ${signals.title} ${signals.url} ${signals.file}`.toLowerCase()

      // Detect compose/create state (used by multiple matchers)
      const isCompose = signals.title.includes('compose') || signals.title.includes('new message') ||
                        signals.url.includes('/compose') || signals.url.includes('mail.google.com/mail') ||
                        signals.ocr.includes('compose') || signals.ocr.includes('new message') ||
                        signals.ocr.includes('reply') || signals.app.includes('mail')
      const isEditor = signals.app.includes('code') || signals.app.includes('docs') ||
                       signals.app.includes('word') || signals.app.includes('pages') ||
                       signals.app.includes('notion') || signals.app.includes('obsidian')

      const insights: any[] = []

      // Load contacts once (shared across matchers)
      const contacts = await pool.query(
        `SELECT name, email, current_closeness, trajectory, anomaly, last_interaction,
                interaction_freq_30d, whatsapp_msgs_30d, email_count_30d
         FROM relationships WHERE current_closeness > 15 ORDER BY current_closeness DESC LIMIT 20`
      )

      // ── UPGRADE 1: Predictive pre-staging ──────────────────────
      // If compose state with no clear recipient yet, predict what they'll do
      const isBlankCompose = isCompose && signals.ocr.length < 100 &&
        !contacts.rows.some((c: any) => allText.includes((c.name || '').toLowerCase()))

      if (isBlankCompose) {
        // Find the most likely next action based on stale threads + pending actions
        const likelyAction = await pool.query(
          `SELECT pa.title, pa.description, pa.trigger_context, pa.related_entity, pa.action_type
           FROM predicted_actions pa
           WHERE pa.status = 'pending' AND pa.expires_at > NOW()
           AND pa.action_type IN ('follow_up_draft', 'relationship_maintenance')
           ORDER BY pa.confidence DESC LIMIT 1`
        )
        if (likelyAction.rows.length > 0) {
          const la = likelyAction.rows[0]
          const name = (la.title || '').replace(/^(Follow up:|Reach out to)\s*/i, '').slice(0, 25)
          insights.push({
            type: 'predictive', label: 'PREDICTION',
            title: `Probably replying to ${name}`,
            body: la.draft_content ? 'Draft ready' : (la.trigger_context || '').slice(0, 50),
            confidence: 76,
            entity_id: la.related_entity || name,
            button: la.draft_content ? 'Use Draft' : 'View',
            action_type: la.draft_content ? 'use_draft' : 'view',
            upgrade: 'predictive'
          })
        }
      }

      // ── Multi-signal entity matching function ──────────────────
      // UPGRADE 8: requires match in 2+ signals for high confidence
      function entityConfidence(name: string): number {
        const nameLower = name.toLowerCase()
        if (nameLower.length < 3) return 0
        let hits = 0
        if (signals.ocr.includes(nameLower)) hits++
        if (signals.title.includes(nameLower)) hits++
        if (signals.url.includes(nameLower.replace(/\s/g, ''))) hits++
        // Boost for multi-signal agreement
        return hits === 0 ? 0 : hits >= 2 ? 90 : 70
      }

      // ── Matcher 1: Contact name → actionable pending items ──
      for (const c of contacts.rows) {
        const conf = entityConfidence(c.name || '')
        if (conf === 0) continue

        const pendingAction = await pool.query(
          `SELECT id, title, trigger_context, draft_content FROM predicted_actions
           WHERE status = 'pending' AND expires_at > NOW()
           AND (title ILIKE $1 OR related_entity ILIKE $2) LIMIT 1`,
          [`%${c.name}%`, `%${c.email}%`]
        )
        if (pendingAction.rows.length > 0) {
          const pa = pendingAction.rows[0]
          insights.push({
            type: 'contact', label: 'CONTACT',
            title: `${c.name}: pending action`,
            body: (pa.trigger_context || pa.title || '').slice(0, 80),
            confidence: conf, entity_id: c.email
          })
          continue
        }

        const staleThread = await pool.query(
          `SELECT thread_id, subject, stale_days FROM email_threads
           WHERE participants LIKE $1 AND stale_days >= 3 ORDER BY stale_days DESC LIMIT 1`,
          [`%${c.email}%`]
        )
        if (staleThread.rows.length > 0) {
          insights.push({
            type: 'contact', label: 'EMAIL',
            title: `Stale thread with ${c.name}`,
            body: `"${(staleThread.rows[0].subject || '').slice(0, 50)}" — ${staleThread.rows[0].stale_days}d no reply`,
            confidence: conf - 5, entity_id: c.email
          })
        }
      }

      // ── Matcher 2: Calendar event approaching ──
      if (signals.app.includes('calendar') || allText.includes('calendar')) {
        const upcoming = await pool.query(
          `SELECT id, summary, start_time, participants FROM calendar_events
           WHERE start_time::timestamptz > NOW() AND start_time::timestamptz < NOW() + INTERVAL '2 hours'
           AND status != 'cancelled' AND all_day = 0 ORDER BY start_time::timestamptz LIMIT 3`
        )
        for (const ev of upcoming.rows) {
          const evName = (ev.summary || '').toLowerCase()
          if (!allText.includes(evName) && evName.length > 5) continue
          const participants = JSON.parse(ev.participants || '[]')
          const contactMatch = contacts.rows.find((c: any) =>
            participants.some((p: string) => p.toLowerCase().includes((c.name || '').toLowerCase()))
          )
          if (contactMatch) {
            const minsLeft = Math.round((new Date(ev.start_time).getTime() - Date.now()) / 60000)
            insights.push({
              type: 'calendar', label: 'MEETING',
              title: `${(ev.summary || '').slice(0, 40)} in ${minsLeft}m`,
              body: `${contactMatch.name} — ${contactMatch.trajectory || 'stable'} relationship`,
              confidence: 78, entity_id: ev.id
            })
          }
        }
      }

      // ── Matcher 3: Email compose with recipient ──
      if (isCompose && !isBlankCompose) {
        for (const c of contacts.rows) {
          const conf = entityConfidence(c.name || '')
          if (conf === 0) continue

          const commitment = await pool.query(
            `SELECT description, due_at FROM commitments
             WHERE status = 'active' AND (committed_to ILIKE $1 OR committed_by ILIKE $1)
             ORDER BY due_at ASC NULLS LAST LIMIT 1`,
            [`%${c.email}%`]
          )
          if (commitment.rows.length > 0) {
            const cm = commitment.rows[0]
            const dueInfo = cm.due_at ? ` (due ${cm.due_at})` : ''
            insights.push({
              type: 'email', label: 'COMMITMENT',
              title: `Open commitment with ${c.name}`,
              body: `${(cm.description || '').slice(0, 60)}${dueInfo}`,
              confidence: Math.min(conf + 5, 95), entity_id: c.email,
              button: 'View', action_type: 'view'
            })
            break
          }

          if (c.anomaly === 'sudden_silence' || c.anomaly === 'overdue_followup') {
            const daysSince = c.last_interaction
              ? Math.round((Date.now() - new Date(c.last_interaction).getTime()) / 86400000) : 0
            if (daysSince >= 7) {
              insights.push({
                type: 'email', label: 'RELATIONSHIP',
                title: `${c.name} — ${daysSince}d since last contact`,
                body: c.anomaly === 'sudden_silence' ? 'Unusual silence detected' : 'Follow-up overdue',
                confidence: conf - 5, entity_id: c.email
              })
              break
            }
          }
        }
      }

      // ── Matcher 4: File/document ──
      if (isEditor) {
        const recentDocs = await pool.query(
          `SELECT id, filename, path, summary, last_modified FROM documents
           WHERE last_modified > NOW() - INTERVAL '30 days' ORDER BY last_modified DESC LIMIT 20`
        )
        for (const doc of recentDocs.rows) {
          const fnLower = (doc.filename || '').toLowerCase()
          if (fnLower.length < 4) continue
          // UPGRADE 8: check across all signals
          const inOcr = signals.ocr.includes(fnLower)
          const inTitle = signals.title.includes(fnLower)
          const inFile = signals.file.includes(fnLower)
          const hits = [inOcr, inTitle, inFile].filter(Boolean).length
          if (hits === 0) continue

          const relCommitment = await pool.query(
            `SELECT description FROM commitments WHERE status = 'active' AND description ILIKE $1 LIMIT 1`,
            [`%${doc.filename.slice(0, 20)}%`]
          )
          if (relCommitment.rows.length > 0) {
            insights.push({
              type: 'file', label: 'DOCUMENT',
              title: doc.filename.slice(0, 40),
              body: `Related: ${(relCommitment.rows[0].description || '').slice(0, 60)}`,
              confidence: 65 + hits * 10, entity_id: doc.id
            })
          }
        }
      }

      // ── Matcher 5: Centrum card ──
      if (allText.includes('centrum') || allText.includes('spoika') || allText.includes('today list')) {
        const chains = await pool.query(
          `SELECT ac.id, ac.trigger_event, COUNT(cs.id)::int as pending
           FROM action_chains ac JOIN chain_steps cs ON cs.chain_id = ac.id AND cs.status = 'pending'
           WHERE ac.status = 'active' GROUP BY ac.id HAVING COUNT(cs.id) >= 2 LIMIT 3`
        )
        for (const ch of chains.rows) {
          const triggerLower = (ch.trigger_event || '').toLowerCase()
          const words = triggerLower.split(/\s+/).filter((w: string) => w.length > 4)
          const matchedWords = words.filter((w: string) => allText.includes(w))
          if (matchedWords.length >= 2) {
            insights.push({
              type: 'centrum', label: 'CHAIN PLAN',
              title: (ch.trigger_event || '').slice(0, 45),
              body: `${ch.pending} steps pending`,
              confidence: 76, entity_id: ch.id,
              button: 'View Chain', action_type: 'view_chain'
            })
          }
        }
      }

      // ── UPGRADE 4: Intent inference from activity sequence ─────
      if (mode === 'proactive' || insights.length === 0) {
        const recentActivity = await pool.query(
          `SELECT source, event_type, raw_content FROM observations
           WHERE timestamp > NOW() - INTERVAL '10 minutes'
           ORDER BY timestamp DESC LIMIT 15`
        )
        if (recentActivity.rows.length >= 3) {
          // Look for focused topic across recent activity
          const activityText = recentActivity.rows.map((r: any) => r.raw_content || '').join(' ').toLowerCase()
          // Find topics that appear in 3+ recent observations
          const topicCandidates = contacts.rows.filter((c: any) =>
            (c.name || '').length > 3 && activityText.split(c.name.toLowerCase()).length > 2
          )
          for (const topic of topicCandidates) {
            const pa = await pool.query(
              `SELECT title, trigger_context FROM predicted_actions
               WHERE status = 'pending' AND expires_at > NOW()
               AND (title ILIKE $1 OR related_entity ILIKE $2) LIMIT 1`,
              [`%${topic.name}%`, `%${topic.email}%`]
            )
            if (pa.rows.length > 0) {
              insights.push({
                type: 'sequence', label: 'PATTERN',
                title: `You're focused on ${topic.name}`,
                body: (pa.rows[0].trigger_context || pa.rows[0].title || '').slice(0, 60),
                confidence: 72, entity_id: topic.email,
                upgrade: 'sequence'
              })
              break
            }
          }
        }
      }

      // ── UPGRADE 6: Proactive time-based triggering ─────────────
      if (mode === 'proactive') {
        // Check for approaching calendar events needing prep
        const soonEvents = await pool.query(
          `SELECT id, summary, start_time, participants FROM calendar_events
           WHERE start_time::timestamptz > NOW() + INTERVAL '5 minutes'
           AND start_time::timestamptz < NOW() + INTERVAL '30 minutes'
           AND status != 'cancelled' AND all_day = 0 LIMIT 1`
        )
        if (soonEvents.rows.length > 0) {
          const ev = soonEvents.rows[0]
          const minsLeft = Math.round((new Date(ev.start_time).getTime() - Date.now()) / 60000)
          insights.push({
            type: 'proactive', label: 'UPCOMING',
            title: `${(ev.summary || '').slice(0, 40)} in ${minsLeft}m`,
            body: 'No prep detected — review now?',
            confidence: 78, entity_id: ev.id,
            button: 'View', action_type: 'view',
            upgrade: 'proactive'
          })
        }

        // Check for stale high-priority commitments
        const urgentCommitments = await pool.query(
          `SELECT description, due_at, committed_to FROM commitments
           WHERE status = 'active' AND due_at IS NOT NULL
           AND due_at::text <= $1 ORDER BY due_at LIMIT 1`,
          [new Date(Date.now() + 24 * 3600000).toISOString().slice(0, 10)]
        )
        if (urgentCommitments.rows.length > 0) {
          const uc = urgentCommitments.rows[0]
          insights.push({
            type: 'proactive', label: 'DEADLINE',
            title: `Due ${uc.due_at}: ${(uc.description || '').slice(0, 35)}`,
            body: uc.committed_to ? `For ${uc.committed_to}` : 'Review before deadline',
            confidence: 80, entity_id: `commitment:${uc.description?.slice(0, 20)}`,
            upgrade: 'proactive'
          })
        }
      }

      if (insights.length === 0) return res.json({ show: false })

      insights.sort((a, b) => b.confidence - a.confidence)
      let best = insights[0]

      // ── UPGRADE 3: Cross-source enrichment ─────────────────────
      if (best.entity_id) {
        const entitySearch = best.entity_id
        // Check calendar for upcoming events with this entity
        const calMatch = await pool.query(
          `SELECT summary, start_time FROM calendar_events
           WHERE start_time::timestamptz > NOW() AND start_time::timestamptz < NOW() + INTERVAL '7 days'
           AND (summary ILIKE $1 OR participants ILIKE $1) AND status != 'cancelled'
           ORDER BY start_time::timestamptz LIMIT 1`,
          [`%${entitySearch.split('@')[0].split(':').pop()?.slice(0, 15)}%`]
        )
        // Check chains for this entity
        const chainMatch = await pool.query(
          `SELECT ac.trigger_event, cs.title as step_title, cs2.title as step2_title, cs3.title as step3_title
           FROM action_chains ac
           LEFT JOIN chain_steps cs ON cs.chain_id = ac.id AND cs.step_number = 1
           LEFT JOIN chain_steps cs2 ON cs2.chain_id = ac.id AND cs2.step_number = 2
           LEFT JOIN chain_steps cs3 ON cs3.chain_id = ac.id AND cs3.step_number = 3
           WHERE ac.status = 'active'
           AND (ac.trigger_event ILIKE $1 OR ac.trigger_entity ILIKE $2)
           LIMIT 1`,
          [`%${entitySearch.split('@')[0].split(':').pop()?.slice(0, 15)}%`, `%${entitySearch}%`]
        )

        // UPGRADE 7: Attach chain hint
        if (chainMatch.rows.length > 0) {
          const ch = chainMatch.rows[0]
          const steps = [ch.step_title, ch.step2_title, ch.step3_title].filter(Boolean)
          if (steps.length >= 2) {
            const chainHint = steps.map((s: string) => s.slice(0, 20)).join(' → ')
            best.chain_hint = chainHint
          }
        }

        // UPGRADE 3: Add calendar context if it enriches
        if (calMatch.rows.length > 0) {
          const calEv = calMatch.rows[0]
          const daysUntil = Math.round((new Date(calEv.start_time).getTime() - Date.now()) / 86400000)
          if (daysUntil <= 3 && !best.body.includes(calEv.summary)) {
            best.body = `${calEv.summary} in ${daysUntil}d. ${best.body}`.slice(0, 90)
            best.confidence = Math.min(best.confidence + 5, 95)
          }
        }
      }

      // ── UPGRADE 5: Pattern comparison (past similar items) ─────
      if (best.entity_id && best.type !== 'predictive') {
        const pastSimilar = await pool.query(
          `SELECT trigger_action, trigger_context, user_action, correction_text
           FROM correction_log
           WHERE trigger_context ILIKE $1 AND timestamp > NOW() - INTERVAL '30 days'
           ORDER BY timestamp DESC LIMIT 3`,
          [`%${best.entity_id.split('@')[0].split(':').pop()?.slice(0, 12)}%`]
        )
        const dismissed = pastSimilar.rows.filter((r: any) => r.user_action?.includes('dismiss'))
        if (dismissed.length >= 2 && pastSimilar.rows.length >= 3) {
          // Past pattern: user dismisses this type — lower confidence
          best.confidence -= 15
        }
        const acted = pastSimilar.rows.filter((r: any) => r.user_action?.includes('acted'))
        if (acted.length >= 2) {
          // Past pattern: user acts on this type — boost confidence
          best.confidence += 5
        }
      }

      // Final confidence gate
      if (best.confidence < 65) return res.json({ show: false })

      // Ensure total content < 140 chars
      const totalLen = (best.label || '').length + (best.title || '').length + (best.body || '').length
      if (totalLen > 140) {
        best.body = (best.body || '').slice(0, 140 - (best.label || '').length - (best.title || '').length - 3) + '...'
      }

      res.json({
        show: true,
        id: `overlay-${Date.now()}`,
        ...best
      })
    } catch (err: any) {
      console.error('[overlay] Evaluate failed:', err.message)
      res.json({ show: false })
    }
  })

  // ── GOOGLE SYNC (server-side) ─────────────────────────────────

  app.post('/api/google/connect', async (req, res) => {
    try {
      const { refresh_token, email, google_client_id, google_client_secret, account_type } = req.body
      if (!refresh_token || !email) return res.status(400).json({ error: 'refresh_token and email required' })

      const account = (account_type === 'school' ? 'school' : 'primary') as 'primary' | 'school'
      const provider = account === 'school' ? 'google_school' : 'google'

      const pool = getPool()
      // Store client credentials in settings (shared across accounts)
      if (google_client_id) await pool.query(`INSERT INTO settings (key, value) VALUES ('google_client_id', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, [google_client_id])
      if (google_client_secret) await pool.query(`INSERT INTO settings (key, value) VALUES ('google_client_secret', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, [google_client_secret])

      // Validate token by doing a refresh
      const clientId = google_client_id || (await pool.query(`SELECT value FROM settings WHERE key = 'google_client_id'`)).rows[0]?.value
      const clientSecret = google_client_secret || (await pool.query(`SELECT value FROM settings WHERE key = 'google_client_secret'`)).rows[0]?.value

      const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ refresh_token, client_id: clientId, client_secret: clientSecret, grant_type: 'refresh_token' })
      })

      if (!tokenRes.ok) return res.status(400).json({ error: 'Token refresh failed — invalid credentials' })

      const tokens = await tokenRes.json() as { access_token: string; expires_in?: number }
      const expiryDate = tokens.expires_in ? Date.now() + tokens.expires_in * 1000 : null

      await pool.query(
        `INSERT INTO oauth_tokens (provider, access_token, refresh_token, expiry_date, email, updated_at)
         VALUES ($5, $1, $2, $3, $4, NOW())
         ON CONFLICT (provider) DO UPDATE SET access_token=$1, refresh_token=$2, expiry_date=$3, email=$4, updated_at=NOW()`,
        [tokens.access_token, refresh_token, expiryDate, email, provider]
      )

      // Start sync scheduler if not already running
      const { startSyncScheduler } = await import('./sync-scheduler')
      startSyncScheduler()

      res.json({ ok: true, email, account_type: account })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/google/disconnect', async (_req, res) => {
    try {
      const pool = getPool()
      await pool.query(`DELETE FROM oauth_tokens WHERE provider = 'google'`)
      const { stopSyncScheduler } = await import('./sync-scheduler')
      stopSyncScheduler()
      res.json({ ok: true })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/google/status', async (_req, res) => {
    try {
      const pool = getPool()
      const tokenRes = await pool.query(`SELECT provider, email FROM oauth_tokens WHERE provider IN ('google', 'google_school')`)

      const accounts: any[] = []
      let primaryConnected = false
      let primaryEmail: string | null = null
      for (const row of tokenRes.rows) {
        const type = row.provider === 'google_school' ? 'school' : 'primary'
        accounts.push({ type, email: row.email })
        if (type === 'primary') { primaryConnected = true; primaryEmail = row.email }
      }

      const syncRes = await pool.query(
        `SELECT integration, last_sync_at, status, total_processed FROM sync_state
         WHERE integration IN ('gmail', 'calendar', 'drive', 'gmail_school', 'calendar_school', 'drive_school')`
      )
      const syncs: Record<string, any> = {}
      for (const r of syncRes.rows) syncs[r.integration] = { last_sync: r.last_sync_at, status: r.status, total: r.total_processed }

      res.json({ connected: primaryConnected, email: primaryEmail, accounts, ...syncs })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/sync/trigger', async (req, res) => {
    try {
      const service = req.body?.service as string | undefined
      const { triggerSync } = await import('./sync-scheduler')
      await triggerSync(service as any)
      const { getSyncStatus } = await import('./sync-scheduler')
      const status = await getSyncStatus()
      res.json({ ok: true, triggered: service || 'all', status })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/sync/status', async (_req, res) => {
    try {
      const { getSyncStatus } = await import('./sync-scheduler')
      const status = await getSyncStatus()
      res.json(status)
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/gmail/send', async (req, res) => {
    try {
      const { to, subject, body } = req.body
      if (!to || !subject) return res.status(400).json({ error: 'to and subject required' })
      const { sendEmail } = await import('./gmail-sync')
      const messageId = await sendEmail(to, subject, body || '')
      res.json({ ok: true, messageId })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/gmail/trash', async (req, res) => {
    try {
      const { messageId } = req.body
      if (!messageId) return res.status(400).json({ error: 'messageId required' })
      const { trashMessage } = await import('./gmail-sync')
      const ok = await trashMessage(messageId)
      res.json({ ok })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.post('/api/calendar/create', async (req, res) => {
    try {
      const { summary, startTime, endTime, description } = req.body
      if (!summary || !startTime || !endTime) return res.status(400).json({ error: 'summary, startTime, endTime required' })
      const { createCalendarEvent } = await import('./calendar-sync')
      const eventId = await createCalendarEvent(summary, startTime, endTime, description)
      res.json({ ok: true, eventId })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── POST /api/centrum (board snapshot + commitment extraction) ──

  app.post('/api/centrum', async (req, res) => {
    try {
      const board = req.body
      if (!board || !board.cards) { res.status(400).json({ error: 'Invalid board state' }); return }

      // Build cluster summary
      const cards = board.cards as any[]
      const clusters = cards.filter((c: any) => c.type === 'cluster')
      const tasks = cards.filter((c: any) => c.type === 'task')
      const clusterSummary: Record<string, any> = {}

      for (const cluster of clusters) {
        const clusterTasks = tasks.filter((t: any) => t.clusterId === cluster.id)
        const subtasks = clusterTasks.reduce((s: number, t: any) => s + (Array.isArray(t.content) ? t.content.length : 0), 0)
        const done = clusterTasks.reduce((s: number, t: any) => s + (Array.isArray(t.content) ? t.content.filter((st: any) => st.done).length : 0), 0)
        clusterSummary[cluster.title] = { cards: clusterTasks.length, subtasks, done }
      }

      // Build today summary text
      const todayIds = new Set(board.todayIds || [])
      const todayDone = new Set(board.todayDone || [])
      const todayDeferred = board.todayDeferred || {}
      const todayCards = cards.filter((c: any) => todayIds.has(c.id))
      const todayLines = todayCards.map((c: any) => {
        const cluster = clusters.find((cl: any) => cl.id === c.clusterId)?.title || 'Unclustered'
        const isDone = todayDone.has(c.id)
        const deferred = todayDeferred[c.id]
        const subtasks = Array.isArray(c.content) ? c.content : []
        const doneCount = subtasks.filter((t: any) => t.done).length
        let line = `${isDone ? '✅' : '⬜'} ${c.title} [${cluster}]`
        if (subtasks.length > 0) line += ` (${doneCount}/${subtasks.length})`
        if (deferred) line += ` [deferred${deferred.date ? ' to ' + deferred.date : ''}]`
        return line
      })
      const todaySummary = `TODAY (${todayDone.size}/${todayIds.size} done):\n${todayLines.join('\n')}`

      // Store snapshot
      const snapId = await insertCentrumSnapshot(board, clusterSummary, todaySummary)

      // Extract commitments from Centrum cards
      let commitCount = 0
      for (const card of tasks) {
        const cluster = clusters.find((cl: any) => cl.id === card.clusterId)?.title || 'Unclustered'
        const subtasks = Array.isArray(card.content) ? card.content : []
        const isToday = todayIds.has(card.id)
        const isDone = todayDone.has(card.id)

        // Every card in Today that isn't done → active commitment
        if (isToday && !isDone) {
          await upsertCommitment(
            `centrum-${card.id}`,
            `${card.title} [${cluster}]`,
            'centrum', card.id, null, null, 'active', 0.9
          )
          commitCount++
        }

        // Every undone subtask in a Today card → commitment
        if (isToday && subtasks.length > 0) {
          for (const st of subtasks) {
            if (!st.done) {
              const stId = `centrum-${card.id}-${st.text?.slice(0, 30).replace(/[^a-zA-Z0-9]/g, '_')}`
              await upsertCommitment(
                stId,
                `${st.text} (in "${card.title}" [${cluster}])`,
                'centrum', card.id, null, null, 'active', 0.85
              )
              commitCount++
            }
          }
        }
      }

      // Mark centrum commitments not in current board as completed
      const pool = getPool()
      const existing = await pool.query("SELECT id, source_ref FROM commitments WHERE source_type='centrum' AND status='active'")
      const activeCardIds = new Set(tasks.map((t: any) => t.id))
      for (const row of existing.rows) {
        if (row.source_ref && !activeCardIds.has(row.source_ref) && !todayIds.has(row.source_ref)) {
          await pool.query("UPDATE commitments SET status='completed' WHERE id=$1", [row.id])
        }
      }

      console.log(`[centrum] Snapshot ${snapId}: ${cards.length} cards, ${commitCount} commitments extracted`)
      res.json({ ok: true, snapshot_id: snapId, cards: cards.length, commitments_extracted: commitCount, clusters: clusterSummary })
    } catch (err: any) {
      console.error('[centrum] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // ── POST /api/import (bulk data migration from local SQLite) ──

  app.post('/api/import', async (req, res) => {
    try {
      const { contacts, email_threads, calendar_events, commitments, insights, living_profile } = req.body
      const pool = getPool()
      const counts: Record<string, number> = {}

      if (contacts?.length) {
        for (const c of contacts) {
          await pool.query(
            `INSERT INTO contacts (id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (email) DO UPDATE SET
             name=$2, emails_sent=$4, emails_received=$5, relationship_score=$6, last_interaction_at=$7`,
            [c.id, c.name, c.email, c.emails_sent || 0, c.emails_received || 0, c.relationship_score || 0, c.last_interaction_at]
          )
        }
        counts.contacts = contacts.length
      }

      if (email_threads?.length) {
        for (const t of email_threads) {
          await pool.query(
            `INSERT INTO email_threads (thread_id, subject, last_message_from, last_message_date, awaiting_reply, stale_days, participants, message_count)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (thread_id) DO UPDATE SET
             subject=$2, last_message_from=$3, last_message_date=$4, awaiting_reply=$5, stale_days=$6, participants=$7, message_count=$8`,
            [t.thread_id, t.subject, t.last_message_from, t.last_message_date, t.awaiting_reply, t.stale_days, t.participants, t.message_count]
          )
        }
        counts.email_threads = email_threads.length
      }

      if (calendar_events?.length) {
        for (const e of calendar_events) {
          await pool.query(
            `INSERT INTO calendar_events (id, summary, start_time, end_time, all_day, location, participants, recurring, status, calendar_name)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO UPDATE SET
             summary=$2, start_time=$3, end_time=$4, all_day=$5, location=$6, participants=$7, recurring=$8, status=$9, calendar_name=$10`,
            [e.id, e.summary, e.start_time, e.end_time, e.all_day, e.location, e.participants, e.recurring, e.status, e.calendar_name]
          )
        }
        counts.calendar_events = calendar_events.length
      }

      if (commitments?.length) {
        for (const c of commitments) {
          await pool.query(
            `INSERT INTO commitments (id, description, source_type, source_ref, committed_to, committed_by, due_at, status, ai_confidence)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO UPDATE SET
             description=$2, source_type=$3, source_ref=$4, committed_to=$5, committed_by=$6, due_at=$7, status=$8, ai_confidence=$9`,
            [c.id, c.description, c.source_type, c.source_ref, c.committed_to, c.committed_by, c.due_at, c.status, c.ai_confidence]
          )
        }
        counts.commitments = commitments.length
      }

      if (insights?.length) {
        for (const i of insights) {
          await pool.query(
            `INSERT INTO insights (id, statement, evidence, confidence, category, status, source_count, times_confirmed)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO UPDATE SET
             statement=$2, evidence=$3, confidence=$4, category=$5, status=$6, source_count=$7, times_confirmed=$8`,
            [i.id, i.statement, i.evidence, i.confidence, i.category, i.status, i.source_count, i.times_confirmed]
          )
        }
        counts.insights = insights.length
      }

      if (req.body.documents?.length) {
        for (const d of req.body.documents) {
          await pool.query(
            `INSERT INTO documents (id, path, filename, type, summary, tags, size_bytes, last_modified)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO UPDATE SET
             path=$2, filename=$3, type=$4, summary=$5, tags=$6, size_bytes=$7, last_modified=$8`,
            [d.id, d.path, d.filename, d.type, d.summary || '', d.tags || '[]', d.size_bytes || 0, d.last_modified]
          )
        }
        counts.documents = req.body.documents.length
      }

      if (living_profile) {
        await setLivingProfile(living_profile)
        counts.living_profile = 1
      }

      console.log('[import] Migration complete:', counts)
      res.json({ ok: true, counts })
    } catch (err: any) {
      console.error('[import] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // ── WebSocket: /ws/glasses ──────────────────────────────────

  const server = createServer(app)

  const wss = new WebSocketServer({ noServer: true })

  server.on('upgrade', (request, socket, head) => {
    if (request.url === '/ws/glasses') {
      // Check auth for WebSocket
      const url = new URL(request.url, `http://localhost`)
      const token = request.headers.authorization?.replace('Bearer ', '') ||
                    new URLSearchParams(url.search).get('token')
      if (token !== ACCESS_TOKEN && ACCESS_TOKEN) {
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n')
        socket.destroy()
        return
      }
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request)
      })
    } else {
      socket.destroy()
    }
  })

  let connectedDevices = 0

  wss.on('connection', (ws: WebSocket) => {
    connectedDevices++
    console.log(`[ws] Device connected (${connectedDevices} total)`)
    ws.send(JSON.stringify({ type: 'ready', version: '0.1.0-cloud' }))

    ws.on('close', () => {
      connectedDevices--
      console.log(`[ws] Device disconnected (${connectedDevices} total)`)
    })

    ws.on('message', async (data: Buffer) => {
      // Same binary protocol as Electron version
      if (!Buffer.isBuffer(data) || data.length < 1) return
      const op = data[0]
      const payload = data.length > 1 ? data.subarray(1) : Buffer.alloc(0)

      try {
        if (op === 0x04 && payload.length === 0) {
          // Briefing request
          const settings = await getSettings()
          const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY || ''
          if (!apiKey) { ws.send(JSON.stringify({ error: 'No API key' })); return }

          const Anthropic = (await import('@anthropic-ai/sdk')).default
          const client = new Anthropic({ apiKey })
          const profile = await getLivingProfile() ?? ''

          const response = await client.messages.create({
            model: settings.model || 'claude-sonnet-4-20250514',
            max_tokens: 300,
            system: 'Concise daily briefing. Under 60 words. Conversational.',
            messages: [{ role: 'user', content: profile.slice(0, 1500) }]
          })

          const text = response.content[0].type === 'text' ? response.content[0].text : ''
          ws.send(JSON.stringify({ type: 'response', mode: 'briefing', text }))
        }
      } catch (err: any) {
        ws.send(JSON.stringify({ error: err.message }))
      }
    })
  })

  // ── Update /health with device count ────────────────────────

  app.get('/health', async (_req, res) => {
    const obsCount = await totalObservationCount()
    const profile = await getLivingProfile()
    res.json({
      status: 'ok',
      version: '0.1.0-cloud',
      connected_devices: connectedDevices,
      observations: obsCount,
      persona_length: profile?.length ?? 0,
      persona_last_updated: 'see /api/persona',
      uptime: Math.floor(process.uptime())
    })
  })

  // ── Start ───────────────────────────────────────────────────

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n${'='.repeat(60)}`)
    console.log(`  Universal AI Cloud Server`)
    console.log(`  http://0.0.0.0:${PORT}`)
    console.log(`  WebSocket: ws://0.0.0.0:${PORT}/ws/glasses`)
    console.log(`  Access Token: ${ACCESS_TOKEN.slice(0, 12)}...`)
    console.log(`${'='.repeat(60)}\n`)

    // Start nightly digest scheduler (00:15 IST)
    startDigestScheduler()

    // Periodic WiFi history cleanup for location detection (every hour)
    setInterval(cleanupWifiHistory, 3600_000)

    // Relationship engine — run hourly
    setTimeout(() => {
      runRelationshipEngine().catch(err => console.error('[relationships] Initial run failed:', err.message))
    }, 30_000)
    setInterval(() => {
      runRelationshipEngine().catch(err => console.error('[relationships] Hourly run failed:', err.message))
    }, 3600_000)

    // Action predictor — run every 10 minutes
    setTimeout(() => {
      runActionPredictor().catch(err => console.error('[predictor] Initial run failed:', err.message))
    }, 20_000) // 20s after startup
    setInterval(() => {
      runActionPredictor().catch(err => console.error('[predictor] Periodic run failed:', err.message))
    }, 10 * 60_000) // every 10 minutes

    // Skill extraction — run on startup + weekly
    setTimeout(() => {
      extractSkills().catch(err => console.error('[skills] Initial extraction failed:', err.message))
    }, 60_000) // 60s after startup
    setInterval(() => {
      extractSkills().catch(err => console.error('[skills] Weekly extraction failed:', err.message))
    }, 7 * 24 * 3600_000) // weekly

    // Routine extraction — run on startup + daily, exception detection hourly
    setTimeout(async () => {
      try {
        await extractRoutines()
        await detectRoutineBreaks()
      } catch (err: any) {
        console.error('[routines] Initial extraction failed:', err.message)
      }
    }, 45_000) // 45s after startup
    setInterval(() => {
      detectRoutineBreaks().catch(err => console.error('[routines] Exception detection failed:', err.message))
    }, 3600_000) // hourly exception checks

    // Google sync scheduler — Gmail 10m, Calendar 15m, Drive 15m
    import('./sync-scheduler').then(({ startSyncScheduler }) => {
      startSyncScheduler()
    }).catch(err => console.error('[sync-scheduler] Failed to start:', err.message))
  })
}

main().catch((err) => {
  console.error('Fatal:', err)
  process.exit(1)
})

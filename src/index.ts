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
import { claudeWithFallback, claudeStreamWithFallback, getDegradationWhisper } from './claude-fallback'
import { generateChain, generateChainsForUrgentItems, completeChainStep, getActiveChains, formatChainsForContext } from './chain-reasoner'
import { getCachedContext, invalidateCache, getCacheStats, assembleContext, classifyQuery, rebuildCache } from './context-cache'
import { runPipeline, stageRender, logPipelineEvent, type Candidate, type Signals } from './overlay-pipeline'
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

  // ── Screen context helper for spotlight ─────────────────────

  async function getScreenContext(query: string): Promise<string> {
    try {
      const pool = getPool()

      // Only inject screen context if query seems to reference "this" or current context
      const lower = query.toLowerCase()
      const needsScreen = /\bthis\b|\bit\b|\bthe\b.*\b(email|doc|card|page|file|event|message|thread)\b|\bwhat('s| is) on\b|\bhelp me (with|reply|respond|write|draft|summarize)\b|\bcurrent\b|\bscreen\b|\blooking at\b/i.test(lower)

      if (!needsScreen) return ''

      const screenObs = await pool.query(
        `SELECT raw_content, timestamp FROM observations WHERE source = 'screen_capture' ORDER BY timestamp DESC LIMIT 1`
      )
      const appObs = await pool.query(
        `SELECT raw_content, timestamp FROM observations WHERE source = 'app_activity' ORDER BY timestamp DESC LIMIT 1`
      )

      const screen = screenObs.rows[0]
      const appActivity = appObs.rows[0]

      if (!screen) return ''

      // Freshness gate: only inject if < 30 seconds old
      const age = Date.now() - new Date(screen.timestamp).getTime()
      if (age > 30_000) return ''

      // Parse app name
      let focusedApp = ''
      let windowTitle = ''
      if (appActivity?.raw_content) {
        const m = appActivity.raw_content.match(/(?:Switched to|App):\s*(.+?)(?:\s*—\s*(.+))?$/)
        if (m) { focusedApp = m[1]?.trim() || ''; windowTitle = m[2]?.trim() || '' }
      }

      // Parse OCR text
      let ocrText = ''
      if (screen.raw_content) {
        const parts = screen.raw_content.split('Text:\n')
        ocrText = (parts[1] || screen.raw_content).slice(0, 500)
      }

      // Detect entities
      const entities: string[] = []
      if (ocrText.length > 20) {
        const contacts = await pool.query(
          `SELECT name FROM relationships WHERE current_closeness > 15 ORDER BY current_closeness DESC LIMIT 15`
        )
        for (const c of contacts.rows) {
          if (c.name?.length > 3 && ocrText.toLowerCase().includes(c.name.toLowerCase())) {
            entities.push(c.name)
          }
        }
      }

      const sections = [
        `The user is currently looking at ${focusedApp || 'unknown app'}${windowTitle ? ` — "${windowTitle}"` : ''}.`,
        ocrText ? `Visible content on screen: "${ocrText.slice(0, 300)}"` : '',
        entities.length > 0 ? `Detected people/entities: ${entities.join(', ')}` : '',
        `When the user says "this", "it", or references something without specifying, they mean what's on this screen.`
      ].filter(Boolean)

      return `\n\nCURRENT SCREEN CONTEXT (live, ${Math.round(age / 1000)}s ago):\n${sections.join('\n')}`
    } catch {
      return ''
    }
  }

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

      // Screen context injection (only when query references "this" or current context)
      const screenCtx = isSpotlight ? await getScreenContext(content) : ''

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
        { type: 'text', text: `\n\nCONTEXT:\n${context}${screenCtx}`, cache_control: { type: 'ephemeral' } }
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
        const result = await claudeWithFallback(client, {
          model: settings.model || 'claude-sonnet-4-20250514',
          max_tokens: isSpotlight ? 300 : 500,
          system: systemStr,
          messages: [{ role: 'user', content }]
        }, 'spotlight-stream')

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

      // Screen context injection (only when query references "this" or current context)
      const screenCtx = isSpotlight ? await getScreenContext(content) : ''

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
        // Block 2: Persona + stable context + screen context
        {
          type: 'text',
          text: `\n\nCONTEXT — this is who you're talking to and what you know:\n${context}${screenCtx}`,
          cache_control: { type: 'ephemeral' }
        }
      ] : [
        { type: 'text', text: `You are Universal AI, a personal AI. Be concise and helpful.\n\n${context}` }
      ]

      // ── Fire main response (+ parallel second-pass for analytical) ──

      const mainCallPromise = claudeWithFallback(client, {
        model: settings.model || 'claude-sonnet-4-20250514',
        max_tokens: isSpotlight ? 300 : 500,
        system: systemBlocks,
        messages: [{ role: 'user', content }]
      }, 'spotlight-chat')

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

  // ── GET /api/current-context (screen awareness for spotlight) ──

  app.get('/api/current-context', async (_req, res) => {
    try {
      const pool = getPool()

      // Get most recent screen_capture observation
      const screenObs = await pool.query(
        `SELECT raw_content, timestamp FROM observations
         WHERE source = 'screen_capture' ORDER BY timestamp DESC LIMIT 1`
      )

      // Get most recent app_activity observation
      const appObs = await pool.query(
        `SELECT raw_content, timestamp FROM observations
         WHERE source = 'app_activity' ORDER BY timestamp DESC LIMIT 1`
      )

      // Get most recent browser URL
      const browserObs = await pool.query(
        `SELECT raw_content, timestamp FROM observations
         WHERE source IN ('browser_page', 'browser') ORDER BY timestamp DESC LIMIT 1`
      )

      const screen = screenObs.rows[0]
      const appActivity = appObs.rows[0]
      const browser = browserObs.rows[0]

      // Parse app name and window title from app_activity
      let focusedApp = ''
      let windowTitle = ''
      if (appActivity?.raw_content) {
        const appMatch = appActivity.raw_content.match(/(?:Switched to|App):\s*(.+?)(?:\s*—\s*(.+))?$/)
        if (appMatch) {
          focusedApp = appMatch[1]?.trim() || ''
          windowTitle = appMatch[2]?.trim() || ''
        }
      }

      // Parse URL from browser observation
      let url = ''
      if (browser?.raw_content) {
        const urlMatch = browser.raw_content.match(/https?:\/\/[^\s]+/)
        if (urlMatch) url = urlMatch[0]
      }

      // OCR text from screen capture (last 500 chars)
      let ocrText = ''
      if (screen?.raw_content) {
        // Screen capture format: "App: X\nWindow: Y\nURL: Z\nText:\n..."
        const textSection = screen.raw_content.split('Text:\n')
        ocrText = (textSection[1] || screen.raw_content).slice(0, 500)
      }

      // Detect entities in OCR
      const detectedEntities: string[] = []
      if (ocrText.length > 20) {
        const contacts = await pool.query(
          `SELECT name FROM relationships WHERE current_closeness > 15 ORDER BY current_closeness DESC LIMIT 20`
        )
        for (const c of contacts.rows) {
          if (c.name && c.name.length > 3 && ocrText.toLowerCase().includes(c.name.toLowerCase())) {
            detectedEntities.push(c.name)
          }
        }
      }

      // Freshness check
      const screenAge = screen ? Date.now() - new Date(screen.timestamp).getTime() : 999999
      const isFresh = screenAge < 60_000 // < 60 seconds

      res.json({
        fresh: isFresh,
        focused_app: focusedApp,
        window_title: windowTitle,
        url,
        ocr_text: isFresh ? ocrText : '',
        detected_entities: detectedEntities,
        timestamp: screen?.timestamp || null,
        age_seconds: Math.round(screenAge / 1000)
      })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

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

      const response = await claudeWithFallback(client, {
        model: settings.model || 'claude-sonnet-4-20250514',
        max_tokens: 400,
        system: 'You are Universal AI giving a concise daily briefing. Under 75 words. Conversational, not robotic.',
        messages: [{ role: 'user', content: context }]
      }, 'briefing')

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

  // ── POST /api/observations (phone notification batch ingest) ──
  // The phone app queues notification observations locally and flushes
  // them here when it has connectivity. Accepts a single object or an
  // array of up to 100; dedupes by (device_id, app_package, posted_at_ms,
  // title) within a 5-minute ingest window; fans out the normal life-event
  // analyzer per row.
  app.post('/api/observations', async (req, res) => {
    try {
      const pool = getPool()
      const raw = req.body
      const items: any[] = Array.isArray(raw) ? raw : (raw && typeof raw === 'object' ? [raw] : [])

      if (items.length === 0) {
        return res.status(400).json({ error: 'body must be an observation object or array' })
      }
      if (items.length > 100) {
        return res.status(400).json({ error: 'max 100 observations per call' })
      }

      let ingested = 0
      let deduped = 0
      const errors: Array<{ index: number; error: string }> = []

      for (let i = 0; i < items.length; i++) {
        const obs = items[i] || {}
        try {
          const source: string = String(obs.source || 'phone_notification')
          const appPackage: string = String(obs.app_package || '')
          const title: string = String(obs.title || '')
          const postedAtMs: number | null = typeof obs.posted_at_ms === 'number' ? obs.posted_at_ms : null
          const deviceId: string = String(obs.device_id || '')
          const body: string = String(obs.body || '')
          const subtext: string = obs.subtext == null ? '' : String(obs.subtext)
          const category: string = String(obs.category || '')
          const appName: string = String(obs.app_name || '')
          const isGroup: boolean = !!obs.is_group
          const priority: number = typeof obs.priority === 'number' ? obs.priority : 0

          // Dedup: same (device_id + app_package + posted_at_ms + title) already
          // ingested in the last 5 minutes → skip silently.
          if (postedAtMs != null && deviceId && appPackage) {
            const dup = await pool.query(
              `SELECT 1 FROM observations
               WHERE source = 'phone_notification'
                 AND timestamp > NOW() - INTERVAL '5 minutes'
                 AND extracted_entities::jsonb->>'device_id' = $1
                 AND extracted_entities::jsonb->>'app_package' = $2
                 AND (extracted_entities::jsonb->>'posted_at_ms')::bigint = $3
                 AND extracted_entities::jsonb->>'title' = $4
               LIMIT 1`,
              [deviceId, appPackage, postedAtMs, title],
            )
            if (dup.rows.length > 0) { deduped++; continue }
          }

          // Build raw_content: greppable plain text that downstream extractors
          // (whatsapp, life-event, persona writer) already know how to read.
          const rawText = String(obs.raw_content || '').trim() || [
            appName && `App: ${appName}`,
            title && `From: ${title}`,
            body && `Message: ${body}`,
            subtext && `Context: ${subtext}`,
          ].filter(Boolean).join('\n')

          const entities = {
            app_package: appPackage,
            app_name: appName,
            title,
            body,
            subtext: subtext || null,
            category,
            is_group: isGroup,
            posted_at_ms: postedAtMs,
            priority,
            device_id: deviceId,
            ...(obs.extracted_entities && typeof obs.extracted_entities === 'object' ? obs.extracted_entities : {}),
          }

          // Event type: messaging / email / call / system — match existing
          // convention used by screen_capture ('viewed') and chat ('queried').
          const eventType = category || 'received'

          const id = await insertObservation(
            source,
            eventType,
            rawText,
            entities,
            'neutral',
            [],
            '',
          )

          // Fan out to existing per-observation processors (non-blocking).
          const tsIso = obs.timestamp || new Date().toISOString()
          analyzeForLifeEvent(id, source, eventType, rawText, tsIso).catch(() => {})

          ingested++
        } catch (err: any) {
          errors.push({ index: i, error: err.message || String(err) })
        }
      }

      res.json({ ok: true, ingested, deduped, errors })
    } catch (err: any) {
      console.error('[observations] batch ingest failed:', err.message)
      res.status(500).json({ error: err.message })
    }
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
  //
  // Decision-making lives in overlay-pipeline.ts. This endpoint is the
  // candidate-generation stage only: it runs 13 matchers, each producing
  // zero or more Candidate objects, then hands the list to runPipeline()
  // which enforces quality / freshness / duplication / relevance /
  // actionability / rate-limit / scoring / rendering / learning stages.
  //
  // Target fire rate: 3-10/day. Silence is the default.

  app.post('/api/overlay/evaluate', async (req, res) => {
    try {
      const pool = getPool()
      const { ocr_text, app: appName, window_title, url, file_path, mode, typing_rate, app_switched_at } = req.body

      // ── UPGRADE 8: Multi-signal matching ───────────────────────
      const signals: Signals = {
        ocr: (ocr_text || '').toLowerCase(),
        title: (window_title || '').toLowerCase(),
        url: (url || '').toLowerCase(),
        file: (file_path || '').toLowerCase(),
        app: (appName || '').toLowerCase(),
        mode: mode === 'proactive' ? 'proactive' : 'reactive',
        typing_rate: typeof typing_rate === 'number' ? typing_rate : undefined,
        app_switched_at: typeof app_switched_at === 'number' ? app_switched_at : undefined,
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

      const candidates: Candidate[] = []
      const nowMs = Date.now()

      // Load contacts once (shared across matchers).
      // Use signal-aware filter: if any name in signals, pull that specific contact;
      // otherwise fall back to top-50 by closeness.
      // Tokenize aggressively: split on any non-alpha char so "rajeev.kumartk" → ["rajeev","kumartk"].
      const signalNames = new Set<string>()
      const nameWords = allText.split(/[^a-z]+/).filter(w => w.length >= 3 && w.length <= 25)
      for (const w of nameWords) signalNames.add(w)

      const contacts = signalNames.size > 0
        ? await pool.query(
            `SELECT name, email, current_closeness, trajectory, anomaly, last_interaction,
                    interaction_freq_30d, whatsapp_msgs_30d, email_count_30d,
                    CASE
                      WHEN LOWER(SPLIT_PART(name, ' ', 1)) = ANY($1::text[]) THEN 1000
                      WHEN LOWER(SPLIT_PART(email, '@', 1)) = ANY($1::text[]) THEN 999
                      ELSE current_closeness
                    END AS _priority
             FROM relationships
             WHERE current_closeness > 5
             AND (LOWER(SPLIT_PART(name, ' ', 1)) = ANY($1::text[])
                  OR LOWER(SPLIT_PART(email, '@', 1)) = ANY($1::text[])
                  OR current_closeness >= 20)
             ORDER BY _priority DESC, current_closeness DESC LIMIT 50`,
            [Array.from(signalNames)]
          )
        : await pool.query(
            `SELECT name, email, current_closeness, trajectory, anomaly, last_interaction,
                    interaction_freq_30d, whatsapp_msgs_30d, email_count_30d
             FROM relationships WHERE current_closeness > 5 ORDER BY current_closeness DESC LIMIT 30`
          )

      // Helper: extract first name for matching (handles "Rajeev kumar t k" → "rajeev")
      const firstName = (fullName: string): string =>
        (fullName || '').trim().split(/\s+/)[0].toLowerCase()

      // Common words that look like names but aren't useful for contact matching
      const NAME_STOPLIST = new Set([
        'mail', 'gmail', 'inbox', 'compose', 'new', 'reply', 'sent', 'draft',
        'team', 'admin', 'info', 'support', 'help', 'service', 'system',
        'noreply', 'updates', 'news', 'alerts', 'today', 'weekly',
        'play', 'apps', 'games', 'pinterest', 'instagram', 'google'
      ])
      const isUsefulName = (n: string): boolean =>
        n.length >= 4 && !NAME_STOPLIST.has(n)

      // ── UPGRADE 1: Predictive pre-staging ──────────────────────
      // If compose state with no clear recipient yet, predict what they'll do.
      // Match against first name so short names work.
      const isBlankCompose = isCompose && signals.ocr.length < 100 &&
        !contacts.rows.some((c: any) => {
          const fn = firstName(c.name || '')
          if (!isUsefulName(fn)) return false
          // Match with word boundary to avoid 'mail' matching inside 'gmail'
          const re = new RegExp(`\\b${fn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`)
          return re.test(allText)
        })

      if (isBlankCompose) {
        // Find the most likely next action based on stale threads + pending actions
        const likelyAction = await pool.query(
          `SELECT pa.id, pa.title, pa.description, pa.trigger_context, pa.related_entity, pa.action_type, pa.predicted_at, pa.draft_content
           FROM predicted_actions pa
           WHERE pa.status = 'pending' AND pa.expires_at > NOW()
           AND pa.action_type IN ('follow_up_draft', 'relationship_maintenance')
           ORDER BY pa.confidence DESC LIMIT 1`
        )
        if (likelyAction.rows.length > 0) {
          const la = likelyAction.rows[0]
          const name = (la.title || '').replace(/^(Follow up:|Reach out to)\s*/i, '').slice(0, 25)
          candidates.push({
            matcher_name: 'predictive',
            type: 'predictive',
            label: la.draft_content ? 'DRAFT READY' : 'PREDICTION',
            raw_title: `Probably replying to ${name}`,
            raw_body: la.draft_content ? 'Draft ready to send' : (la.trigger_context || '').slice(0, 120),
            raw_content: la.description || la.trigger_context || '',
            base_confidence: 76,
            entity_id: la.related_entity || name,
            button: la.draft_content ? 'Draft' : 'View',
            action_type: la.draft_content ? 'use_draft' : 'view',
            upgrade: 'predictive',
            source_table: 'predicted_actions',
            source_id: la.id,
            freshness_timestamp: la.predicted_at,
          })
        }
      }

      // ── Multi-signal entity matching function ──────────────────
      // UPGRADE 8: requires match in 2+ signals for high confidence.
      // Matches on full name OR first name (handles "Rajeev kumar t k" → "rajeev").
      function entityConfidence(name: string, email?: string): number {
        const nameLower = (name || '').toLowerCase()
        const fn = firstName(name)
        if (!isUsefulName(fn)) return 0
        const emailUser = (email || '').split('@')[0].toLowerCase()

        const contains = (hay: string): boolean => {
          if (!hay) return false
          if (nameLower.length >= 3 && hay.includes(nameLower)) return true
          if (fn.length >= 3 && new RegExp(`\\b${fn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`).test(hay)) return true
          if (emailUser.length >= 3 && hay.includes(emailUser)) return true
          return false
        }

        let hits = 0
        if (contains(signals.ocr)) hits++
        if (contains(signals.title)) hits++
        if (contains(signals.url.replace(/\s/g, ''))) hits++
        return hits === 0 ? 0 : hits >= 2 ? 90 : 70
      }

      // ── Matcher 1: Contact name → actionable pending items ──
      for (const c of contacts.rows) {
        const conf = entityConfidence(c.name || '', c.email)
        if (conf === 0) continue

        const pendingAction = await pool.query(
          `SELECT id, title, trigger_context, draft_content, predicted_at FROM predicted_actions
           WHERE status = 'pending' AND expires_at > NOW()
           AND (title ILIKE $1 OR related_entity ILIKE $2) LIMIT 1`,
          [`%${c.name}%`, `%${c.email}%`]
        )
        if (pendingAction.rows.length > 0) {
          const pa = pendingAction.rows[0]
          candidates.push({
            matcher_name: 'contact',
            type: 'contact',
            label: 'PERSON',
            raw_title: `${c.name} needs follow-up`,
            raw_body: (pa.trigger_context || pa.title || '').slice(0, 130),
            raw_content: pa.trigger_context || '',
            base_confidence: conf,
            entity_id: c.email,
            button: 'View', action_type: 'view',
            source_table: 'predicted_actions',
            source_id: pa.id,
            freshness_timestamp: pa.predicted_at,
          })
          continue
        }

        const staleThread = await pool.query(
          `SELECT thread_id, subject, stale_days, last_message_date FROM email_threads
           WHERE participants LIKE $1 AND stale_days >= 3 AND awaiting_reply = 1
           ORDER BY stale_days DESC LIMIT 1`,
          [`%${c.email}%`]
        )
        if (staleThread.rows.length > 0) {
          const st = staleThread.rows[0]
          // Apply freshness rule: thread > 30d old only fires for top-20 relationships
          const ageDays = st.last_message_date
            ? (nowMs - new Date(st.last_message_date).getTime()) / 86_400_000
            : st.stale_days
          if (ageDays > 30 && (c.current_closeness ?? 0) < 40) continue
          candidates.push({
            matcher_name: 'contact',
            type: 'email',
            label: 'REPLY',
            raw_title: `Reply to ${c.name} about this thread`,
            raw_body: `"${(st.subject || '').slice(0, 60)}" — ${st.stale_days} days with no reply from you`,
            raw_content: st.subject || '',
            base_confidence: conf - 5,
            entity_id: c.email,
            button: 'Reply', action_type: 'reply',
            source_table: 'email_threads',
            source_id: st.thread_id,
            freshness_timestamp: st.last_message_date,
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
            candidates.push({
              matcher_name: 'calendar',
              type: 'calendar',
              label: 'MEETING',
              raw_title: `${(ev.summary || 'Meeting').slice(0, 35)} in ${minsLeft}m`,
              raw_body: `With ${contactMatch.name}. Relationship ${contactMatch.trajectory || 'stable'}.`,
              raw_content: ev.summary || '',
              base_confidence: 78,
              entity_id: ev.id,
              button: 'View', action_type: 'view',
              source_table: 'calendar_events',
              source_id: ev.id,
              freshness_timestamp: ev.start_time,
              critical: minsLeft <= 10,
            })
          }
        }
      }

      // ── Matcher 3: Email compose with recipient ──
      if (isCompose && !isBlankCompose) {
        for (const c of contacts.rows) {
          const conf = entityConfidence(c.name || '', c.email)
          if (conf === 0) continue

          const commitment = await pool.query(
            `SELECT id, description, due_at, created_at FROM commitments
             WHERE status = 'active' AND (committed_to ILIKE $1 OR committed_by ILIKE $1)
             ORDER BY due_at ASC NULLS LAST LIMIT 1`,
            [`%${c.email}%`]
          )
          if (commitment.rows.length > 0) {
            const cm = commitment.rows[0]
            const descClean = String(cm.description || '').trim()
            const dueLine = cm.due_at ? ` Due ${String(cm.due_at).slice(0, 10)}.` : ''
            candidates.push({
              matcher_name: 'email_commitment',
              type: 'email',
              label: 'COMMITMENT',
              raw_title: `Commitment with ${c.name}`,
              raw_body: `${descClean.slice(0, 100)}.${dueLine}`.trim(),
              raw_content: descClean,
              base_confidence: Math.min(conf + 5, 95),
              entity_id: c.email,
              button: 'View', action_type: 'view',
              source_table: 'commitments',
              source_id: cm.id,
              freshness_timestamp: cm.created_at,
              due_date: cm.due_at,
            })
            break
          }

          if (c.anomaly === 'sudden_silence' || c.anomaly === 'overdue_followup') {
            const daysSince = c.last_interaction
              ? Math.round((Date.now() - new Date(c.last_interaction).getTime()) / 86400000) : 0
            if (daysSince >= 7) {
              candidates.push({
                matcher_name: 'relationship',
                type: 'email',
                label: 'PERSON',
                raw_title: `${c.name} went quiet ${daysSince} days ago`,
                raw_body: c.anomaly === 'sudden_silence'
                  ? `Unusual silence for this contact. Worth a quick check-in.`
                  : `Follow-up is overdue by ${daysSince} days.`,
                raw_content: c.anomaly,
                base_confidence: conf - 5,
                entity_id: c.email,
                button: 'View', action_type: 'view',
                source_table: 'relationships',
                source_id: c.email,
                freshness_timestamp: c.last_interaction,
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
            `SELECT id, description, due_at FROM commitments
             WHERE status = 'active' AND description ILIKE $1 LIMIT 1`,
            [`%${doc.filename.slice(0, 20)}%`]
          )
          if (relCommitment.rows.length > 0) {
            const rc = relCommitment.rows[0]
            candidates.push({
              matcher_name: 'file',
              type: 'file',
              label: 'DOCUMENT',
              raw_title: `Open commitment for ${doc.filename.slice(0, 30)}`,
              raw_body: `Related: ${String(rc.description || '').slice(0, 110)}`,
              raw_content: rc.description || '',
              base_confidence: 65 + hits * 10,
              entity_id: doc.id,
              button: 'View', action_type: 'view',
              source_table: 'commitments',
              source_id: rc.id,
              freshness_timestamp: doc.last_modified,
              due_date: rc.due_at,
            })
          }
        }
      }

      // ── Matcher 5: Centrum card ──
      if (allText.includes('centrum') || allText.includes('spoika') || allText.includes('today list')) {
        const chains = await pool.query(
          `SELECT ac.id, ac.trigger_event, ac.created_at, COUNT(cs.id)::int as pending
           FROM action_chains ac JOIN chain_steps cs ON cs.chain_id = ac.id AND cs.status = 'pending'
           WHERE ac.status = 'active' GROUP BY ac.id, ac.trigger_event, ac.created_at
           HAVING COUNT(cs.id) >= 2 LIMIT 3`
        )
        for (const ch of chains.rows) {
          const triggerLower = (ch.trigger_event || '').toLowerCase()
          const words = triggerLower.split(/\s+/).filter((w: string) => w.length > 4)
          const matchedWords = words.filter((w: string) => allText.includes(w))
          if (matchedWords.length >= 2) {
            candidates.push({
              matcher_name: 'centrum',
              type: 'centrum',
              label: 'PATTERN',
              raw_title: `${(ch.trigger_event || 'Plan').slice(0, 35)} has steps to do`,
              raw_body: `${ch.pending} steps are still pending in this chain.`,
              raw_content: ch.trigger_event || '',
              base_confidence: 76,
              entity_id: ch.id,
              button: 'View', action_type: 'view_chain',
              source_table: 'action_chains',
              source_id: ch.id,
              freshness_timestamp: ch.created_at,
            })
          }
        }
      }

      // ── UPGRADE 4: Intent inference from activity sequence ─────
      if (mode === 'proactive' || candidates.length === 0) {
        const recentActivity = await pool.query(
          `SELECT source, event_type, raw_content FROM observations
           WHERE timestamp > NOW() - INTERVAL '15 minutes'
           ORDER BY timestamp DESC LIMIT 30`
        )
        if (recentActivity.rows.length >= 3) {
          const activityText = recentActivity.rows.map((r: any) => r.raw_content || '').join(' ').toLowerCase()
          // Extract candidate name tokens from recent activity and find matching contacts
          const tokens = new Set<string>()
          for (const w of activityText.split(/[^a-z]+/)) {
            if (w.length >= 3 && w.length <= 20) tokens.add(w)
          }
          // Only keep tokens appearing 3+ times (with word boundary)
          const focusedTokens: string[] = []
          for (const t of tokens) {
            const re = new RegExp(`\\b${t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'g')
            const hits = (activityText.match(re) || []).length
            if (hits >= 3) focusedTokens.push(t)
          }
          if (focusedTokens.length > 0) {
            const focusedContacts = await pool.query(
              `SELECT name, email FROM relationships
               WHERE current_closeness > 5
               AND LOWER(SPLIT_PART(name, ' ', 1)) = ANY($1::text[])
               LIMIT 5`,
              [focusedTokens]
            )
            for (const topic of focusedContacts.rows) {
              const fn = firstName(topic.name)
              const pa = await pool.query(
                `SELECT id, title, trigger_context, predicted_at FROM predicted_actions
                 WHERE status = 'pending' AND expires_at > NOW()
                 AND (title ILIKE $1 OR related_entity ILIKE $2 OR description ILIKE $1 OR trigger_context ILIKE $1) LIMIT 1`,
                [`%${fn}%`, `%${topic.email}%`]
              )
              if (pa.rows.length > 0) {
                const p = pa.rows[0]
                candidates.push({
                  matcher_name: 'intent',
                  type: 'sequence',
                  label: 'FOCUS',
                  raw_title: `You keep returning to ${topic.name}`,
                  raw_body: (p.trigger_context || p.title || 'Pending item waiting.').slice(0, 120),
                  raw_content: p.trigger_context || '',
                  base_confidence: 72,
                  entity_id: topic.email,
                  button: 'View', action_type: 'view',
                  upgrade: 'sequence',
                  source_table: 'predicted_actions',
                  source_id: p.id,
                  freshness_timestamp: p.predicted_at,
                })
                break
              }
            }
          }
        }
      }

      // ── UPGRADE 6: Proactive time-based triggering ─────────────
      if (mode === 'proactive') {
        // Approaching calendar events needing prep.
        const soonEvents = await pool.query(
          `SELECT id, summary, start_time, participants FROM calendar_events
           WHERE start_time::timestamptz > NOW() + INTERVAL '5 minutes'
           AND start_time::timestamptz < NOW() + INTERVAL '30 minutes'
           AND status != 'cancelled' AND all_day = 0 LIMIT 1`
        )
        if (soonEvents.rows.length > 0) {
          const ev = soonEvents.rows[0]
          const minsLeft = Math.round((new Date(ev.start_time).getTime() - Date.now()) / 60000)
          candidates.push({
            matcher_name: 'proactive_calendar',
            type: 'proactive',
            label: 'UPCOMING',
            raw_title: `${(ev.summary || 'Meeting').slice(0, 35)} in ${minsLeft}m`,
            raw_body: `No prep detected. Review or join now.`,
            raw_content: ev.summary || '',
            base_confidence: 78,
            entity_id: ev.id,
            button: 'View', action_type: 'view',
            upgrade: 'proactive',
            source_table: 'calendar_events',
            source_id: ev.id,
            freshness_timestamp: ev.start_time,
            critical: minsLeft <= 10,
          })
        }

        // Urgent commitments: due within 7 days, not dismissed-via-inaction.
        // Freshness (overdue >24h) and quality (truncation etc.) are
        // handled by the pipeline. We only load the top 3 candidates here.
        const urgentCommitments = await pool.query(
          `SELECT id, description, due_at, committed_to, created_at, auto_surface_count, auto_surface_clicked
           FROM commitments
           WHERE status = 'active'
             AND (dismissed_via_inaction IS NOT TRUE)
             AND due_at IS NOT NULL
             AND due_at::text >= (NOW() - INTERVAL '1 day')::text
             AND due_at::text <= (NOW() + INTERVAL '7 days')::text
           ORDER BY due_at ASC LIMIT 3`
        )
        for (const uc of urgentCommitments.rows) {
          const dueMs = new Date(uc.due_at).getTime()
          const overdue = dueMs < nowMs
          const label = overdue ? 'OVERDUE' : 'DEADLINE'
          const descClean = String(uc.description || '').trim()
          candidates.push({
            matcher_name: 'proactive_commitment',
            type: 'proactive',
            label,
            raw_title: overdue ? `Overdue: ${descClean.slice(0, 30)}` : `Due soon: ${descClean.slice(0, 30)}`,
            raw_body: uc.committed_to
              ? `${descClean.slice(0, 90)} for ${String(uc.committed_to).slice(0, 40)}.`
              : `${descClean.slice(0, 110)}.`,
            raw_content: descClean,
            base_confidence: overdue ? 82 : 80,
            entity_id: `commitment:${uc.id}`,
            button: 'Done', action_type: 'view',
            upgrade: 'proactive',
            source_table: 'commitments',
            source_id: uc.id,
            freshness_timestamp: uc.created_at,
            due_date: uc.due_at,
            critical: !overdue && (dueMs - nowMs) < 2 * 3600_000,
          })
        }
      }

      if (candidates.length === 0) return res.json({ show: false })

      // ── Pipeline: Stages 2-10 ─────────────────────────────────
      // Everything beyond Stage 1 (candidate generation) is enforced by
      // overlay-pipeline.ts. That module logs every suppression with its
      // stage and reason so we can debug why nothing fires.
      const outcome = await runPipeline(pool, candidates, signals)

      // Log every suppressed candidate to overlay_log for telemetry.
      for (const s of outcome.suppressed) {
        await logPipelineEvent(
          pool, 'suppressed', s.stage, s.reason,
          {
            entity_id: s.entity_id, type: s.type, matcher_name: s.matcher,
            raw_title: s.title, raw_body: '',
          },
          signals,
        )
      }

      const best = outcome.fired
      if (!best) return res.json({ show: false })

      // ── UPGRADE 3 + UPGRADE 7: post-selection enrichment ──────
      // Attach a chain hint and a calendar sidebar if the winning entity
      // maps to one. These are presentation-only — do not re-score.
      if (best.entity_id) {
        const afterColon = best.entity_id.split(':').pop() || best.entity_id
        const shortToken = (afterColon.split('@')[0] || '').split(/[^a-zA-Z0-9-]/)[0].slice(0, 20)
        if (shortToken.length >= 3) {
          try {
            const chainMatch = await pool.query(
              `SELECT ac.trigger_event, cs.title as s1, cs2.title as s2, cs3.title as s3
               FROM action_chains ac
               LEFT JOIN chain_steps cs ON cs.chain_id = ac.id AND cs.step_number = 1
               LEFT JOIN chain_steps cs2 ON cs2.chain_id = ac.id AND cs2.step_number = 2
               LEFT JOIN chain_steps cs3 ON cs3.chain_id = ac.id AND cs3.step_number = 3
               WHERE ac.status = 'active'
               AND (ac.trigger_event ILIKE $1 OR ac.trigger_entity ILIKE $1)
               LIMIT 1`,
              [`%${shortToken}%`],
            )
            if (chainMatch.rows[0]) {
              const ch = chainMatch.rows[0]
              const steps = [ch.s1, ch.s2, ch.s3].filter(Boolean)
              if (steps.length >= 2) {
                best.chain_hint = steps.map((s: string) => s.slice(0, 20)).join(' → ')
              }
            }
          } catch {}
        }
      }

      // ── Commitment auto-surface cap (3 surfaces without a click → quiet forever) ──
      if (best.entity_id.startsWith('commitment:')) {
        const commitmentId = best.entity_id.slice('commitment:'.length)
        try {
          const upd = await pool.query(
            `UPDATE commitments
             SET auto_surface_count = COALESCE(auto_surface_count, 0) + 1,
                 last_surfaced_at = NOW(),
                 dismissed_via_inaction = CASE
                   WHEN COALESCE(auto_surface_count, 0) + 1 >= 3 AND COALESCE(auto_surface_clicked, 0) = 0 THEN TRUE
                   ELSE dismissed_via_inaction
                 END
             WHERE id = $1
             RETURNING dismissed_via_inaction`,
            [commitmentId],
          )
          if (upd.rows[0]?.dismissed_via_inaction === true) {
            await logPipelineEvent(pool, 'suppressed', 7, 'auto_surface_cap_3', best, signals)
            return res.json({ show: false })
          }
        } catch (err: any) {
          console.error('[overlay] commitment surface tracking failed:', err.message)
        }
      }

      // Log the fire.
      await logPipelineEvent(pool, 'fired', 10, 'ok', best, signals)

      res.json({
        show: true,
        id: `overlay-${Date.now()}`,
        type: best.type,
        label: best.label,
        title: best._final_title || best.raw_title,
        body: best._final_body || best.raw_body,
        confidence: Math.round(best._score ?? best.base_confidence),
        entity_id: best.entity_id,
        button: best.button,
        action_type: best.action_type,
        chain_hint: best.chain_hint,
        pattern_score: best.pattern_score,
        upgrade: best.upgrade,
      })
    } catch (err: any) {
      console.error('[overlay] Evaluate failed:', err.message, err.stack)
      res.json({ show: false })
    }
  })

  // Track overlay action clicks (for auto-surface counter)
  app.post('/api/overlay/action', async (req, res) => {
    try {
      const pool = getPool()
      const { entity_id } = req.body || {}
      if (typeof entity_id === 'string' && entity_id.startsWith('commitment:')) {
        const commitmentId = entity_id.slice('commitment:'.length)
        await pool.query(
          `UPDATE commitments SET auto_surface_clicked = COALESCE(auto_surface_clicked, 0) + 1 WHERE id = $1`,
          [commitmentId]
        )
      }
      res.json({ ok: true })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── Surface coordination ─────────────────────────────────────
  // Other IRIS surfaces heartbeat here when they become visible or dismiss.
  // The overlay pipeline queries surface_state (Stage 5) to avoid competing.
  app.post('/api/surface/visibility', async (req, res) => {
    try {
      const pool = getPool()
      const { surface, visible, device_id, metadata } = req.body || {}
      if (typeof surface !== 'string' || typeof visible !== 'boolean') {
        return res.status(400).json({ error: 'surface and visible required' })
      }
      // Whitelist to avoid arbitrary-key injection.
      const ALLOWED = new Set(['overlay', 'feed', 'spotlight', 'whisper', 'glasses_voice'])
      if (!ALLOWED.has(surface)) {
        return res.status(400).json({ error: 'unknown surface' })
      }
      await pool.query(
        `INSERT INTO surface_state (surface, visible, device_id, last_event_at, metadata)
         VALUES ($1, $2, $3, NOW(), $4)
         ON CONFLICT (surface) DO UPDATE SET
           visible = EXCLUDED.visible,
           device_id = EXCLUDED.device_id,
           last_event_at = NOW(),
           metadata = EXCLUDED.metadata`,
        [surface, visible, device_id || null, metadata ? JSON.stringify(metadata) : '{}'],
      )
      res.json({ ok: true })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/surface/state', async (_req, res) => {
    try {
      const pool = getPool()
      const r = await pool.query(
        `SELECT surface, visible, device_id, last_event_at, metadata FROM surface_state
         ORDER BY last_event_at DESC`,
      )
      res.json({ surfaces: r.rows })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // ── GOOGLE SYNC (server-side) ─────────────────────────────────

  app.post('/api/google/connect', async (req, res) => {
    try {
      const { refresh_token, email, google_client_id, google_client_secret, account_type, expected_email } = req.body
      if (!refresh_token || !email) return res.status(400).json({ error: 'refresh_token and email required' })

      const account = (account_type === 'school' ? 'school' : 'primary') as 'primary' | 'school'
      const provider = account === 'school' ? 'google_school' : 'google'

      const pool = getPool()

      // Guard: prevent accidental overwrite of primary with wrong email
      if (account === 'primary') {
        const existing = await pool.query(`SELECT email FROM oauth_tokens WHERE provider = 'google'`)
        if (existing.rows.length > 0 && existing.rows[0].email) {
          const existingEmail = existing.rows[0].email
          if (existingEmail !== email && expected_email && expected_email !== email) {
            return res.status(400).json({
              error: `Email mismatch — signed in as ${email} but primary is ${existingEmail}. Use Connect School Account for secondary accounts.`
            })
          }
        }
      }

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
      const service = (req.body?.service || req.body?.source) as string | undefined
      const { triggerSync } = await import('./sync-scheduler')
      await triggerSync(service as any)
      const { getSyncStatus } = await import('./sync-scheduler')
      const status = await getSyncStatus()
      res.json({ ok: true, triggered: service || 'all', status })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // Reclassify the 'primary' oauth slot as 'school' without re-auth,
  // so the personal account can be connected to the primary slot.
  app.post('/api/admin/reclassify-oauth', async (req, res) => {
    try {
      const pool = getPool()
      const { from_provider, to_provider } = req.body || {}
      if (!from_provider || !to_provider) {
        res.status(400).json({ error: 'from_provider and to_provider required' }); return
      }
      if (!['google', 'google_school'].includes(from_provider) || !['google', 'google_school'].includes(to_provider)) {
        res.status(400).json({ error: 'provider must be google or google_school' }); return
      }
      // Ensure destination slot is empty, else the user would clobber it
      const dest = await pool.query(`SELECT email FROM oauth_tokens WHERE provider = $1`, [to_provider])
      if (dest.rows.length > 0) {
        res.status(409).json({ error: `${to_provider} slot already has ${dest.rows[0].email} — cannot reclassify without overwrite` }); return
      }
      const src = await pool.query(`SELECT email FROM oauth_tokens WHERE provider = $1`, [from_provider])
      if (src.rows.length === 0) {
        res.status(404).json({ error: `${from_provider} slot is empty` }); return
      }
      await pool.query(`UPDATE oauth_tokens SET provider = $1 WHERE provider = $2`, [to_provider, from_provider])
      // Also rename sync_state rows
      if (from_provider === 'google' && to_provider === 'google_school') {
        for (const int of ['gmail', 'calendar', 'drive']) {
          await pool.query(
            `UPDATE sync_state SET integration = $1 WHERE integration = $2`,
            [`${int}_school`, int]
          )
        }
      } else if (from_provider === 'google_school' && to_provider === 'google') {
        for (const int of ['gmail_school', 'calendar_school', 'drive_school']) {
          const base = int.replace('_school', '')
          await pool.query(
            `UPDATE sync_state SET integration = $1 WHERE integration = $2`,
            [base, int]
          )
        }
      }
      res.json({ ok: true, moved_email: src.rows[0].email, from: from_provider, to: to_provider })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // Commitments audit: garbled content, overdue, surfacing history
  app.get('/api/admin/commitments-audit', async (_req, res) => {
    try {
      const pool = getPool()
      const total = await pool.query(`SELECT COUNT(*)::int AS c FROM commitments`)
      const overdue = await pool.query(
        `SELECT COUNT(*)::int AS c FROM commitments
         WHERE due_at IS NOT NULL AND due_at::text < NOW()::text AND status = 'active'`
      )
      const active = await pool.query(
        `SELECT COUNT(*)::int AS c FROM commitments WHERE status = 'active'`
      )
      const upcoming7d = await pool.query(
        `SELECT COUNT(*)::int AS c FROM commitments
         WHERE status = 'active' AND due_at IS NOT NULL
         AND due_at::text >= NOW()::text
         AND due_at::text <= (NOW() + INTERVAL '7 days')::text`
      )
      // Find likely-garbled rows: ending with single char + space, unbalanced parens,
      // truncation indicators, too-short or missing
      const garbled = await pool.query(
        `SELECT id, description, committed_to, committed_by, due_at, source_type, source_ref, created_at
         FROM commitments
         WHERE status = 'active'
         AND (
           description ~ '\\s[A-Z]\\.?$'           -- ends with single capital letter ('by W')
           OR description ~ '\\s\\S$'              -- ends mid-word (single char after space)
           OR description LIKE '%(%' AND description NOT LIKE '%)%'  -- unbalanced open paren
           OR description LIKE '%[missing]%'
           OR description LIKE '%[...]%'
           OR LENGTH(description) < 10
           OR LENGTH(description) > 300
         )
         ORDER BY created_at DESC LIMIT 30`
      )
      const kahilCas = await pool.query(
        `SELECT id, description, source_type, source_ref, due_at, created_at
         FROM commitments
         WHERE description ILIKE '%Kahil%' OR description ILIKE '%CAS WLFLO%' OR description ILIKE '%by W%'
         LIMIT 10`
      )
      res.json({
        total: total.rows[0].c,
        active: active.rows[0].c,
        overdue: overdue.rows[0].c,
        upcoming_next_7_days: upcoming7d.rows[0].c,
        garbled_candidates_count: garbled.rows.length,
        garbled_samples: garbled.rows,
        kahil_or_cas_matches: kahilCas.rows,
      })
    } catch (err: any) {
      console.error('[commitments-audit] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // ── OVERLAY PIPELINE DIAGNOSTICS ─────────────────────────────
  // Full audit of upstream data quality + pipeline-relevant state.
  // Hit this once after deploy to see the actual state of the DB and
  // decide whether further cleanup is warranted before enabling the new
  // decision pipeline for live use.
  app.get('/api/admin/overlay-audit', async (_req, res) => {
    try {
      const pool = getPool()

      const q = async (sql: string, params: any[] = []) => (await pool.query(sql, params)).rows

      const commitmentsOverdue = await q(
        `SELECT id, description, due_at, created_at FROM commitments
         WHERE status = 'active' AND due_at IS NOT NULL AND due_at::text < NOW()::text
         ORDER BY due_at ASC LIMIT 20`,
      )
      const commitmentsGarbled = await q(
        `SELECT id, description, due_at FROM commitments
         WHERE status = 'active'
         AND (description ~ '\\s[A-Z]\\.?$'
              OR description ~ '\\s[A-Za-z]{1,2}$'
              OR (description LIKE '%(%' AND description NOT LIKE '%)%')
              OR description LIKE '%[missing]%'
              OR description LIKE '%[...]%'
              OR LENGTH(description) < 10
              OR LENGTH(description) > 300)
         LIMIT 30`,
      )
      const commitmentsGarbledCount = (await q(
        `SELECT COUNT(*)::int AS c FROM commitments
         WHERE status = 'active'
         AND (description ~ '\\s[A-Z]\\.?$'
              OR description ~ '\\s[A-Za-z]{1,2}$'
              OR (description LIKE '%(%' AND description NOT LIKE '%)%')
              OR LENGTH(description) < 10)`,
      ))[0].c
      const predictedActionsStale = (await q(
        `SELECT COUNT(*)::int AS c FROM predicted_actions
         WHERE predicted_at < NOW() - INTERVAL '7 days'`,
      ))[0].c
      const emailThreadsStaleAwaiting = (await q(
        `SELECT COUNT(*)::int AS c FROM email_threads
         WHERE awaiting_reply = 1 AND stale_days > 30`,
      ))[0].c
      const actionChainsInactive = (await q(
        `SELECT COUNT(*)::int AS c FROM action_chains
         WHERE status = 'active' AND created_at < NOW() - INTERVAL '14 days'`,
      ))[0].c
      const correctionCountsByMatcher = await q(
        `SELECT trigger_action, user_action, COUNT(*)::int AS c
         FROM correction_log
         WHERE timestamp > NOW() - INTERVAL '90 days'
         GROUP BY trigger_action, user_action
         ORDER BY c DESC LIMIT 30`,
      )
      const quarantined = await q(
        `SELECT source_table, source_id, failure_count, last_reason
         FROM overlay_data_quality_issues WHERE reviewed = FALSE
         ORDER BY failure_count DESC LIMIT 30`,
      )
      const firesLast24h = (await q(
        `SELECT COUNT(*)::int AS c FROM overlay_log
         WHERE outcome = 'fired' AND timestamp > NOW() - INTERVAL '24 hours'`,
      ))[0].c
      const suppressionTopReasons = await q(
        `SELECT reason, COUNT(*)::int AS c FROM overlay_log
         WHERE outcome LIKE 'suppressed_%' AND timestamp > NOW() - INTERVAL '24 hours'
         GROUP BY reason ORDER BY c DESC LIMIT 20`,
      )
      const firesByDay = await q(
        `SELECT timestamp::date AS day, COUNT(*)::int AS c FROM overlay_log
         WHERE outcome = 'fired' AND timestamp > NOW() - INTERVAL '7 days'
         GROUP BY 1 ORDER BY 1 DESC`,
      )

      res.json({
        commitments: {
          overdue_count: commitmentsOverdue.length,
          overdue_top20: commitmentsOverdue,
          garbled_total: commitmentsGarbledCount,
          garbled_samples: commitmentsGarbled,
        },
        predicted_actions: { stale_older_than_7d: predictedActionsStale },
        email_threads: { awaiting_reply_but_over_30d: emailThreadsStaleAwaiting },
        action_chains: { inactive_over_14d: actionChainsInactive },
        correction_log: { by_action_last_90d: correctionCountsByMatcher },
        quarantined_rows: quarantined,
        overlay_log: {
          fires_last_24h: firesLast24h,
          suppression_top_reasons_24h: suppressionTopReasons,
          fires_by_day_last_7: firesByDay,
        },
      })
    } catch (err: any) {
      console.error('[overlay-audit] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // Ten canonical pipeline tests. Each builds synthetic candidates +
  // signals and reports whether the pipeline behaved as specified.
  app.get('/api/admin/overlay-test', async (_req, res) => {
    try {
      const pool = getPool()
      const results: Array<{ test: string; expected: string; actual: string; pass: boolean; details?: any }> = []

      const baseSignals: Signals = {
        ocr: '', title: '', url: '', file: '', app: '', mode: 'reactive',
      }

      const makeCandidate = (overrides: Partial<Candidate>): Candidate => ({
        matcher_name: 'test',
        type: 'email',
        label: 'COMMITMENT',
        raw_title: 'A reasonable title for testing',
        raw_body: 'A reasonable body with enough words to satisfy the render stage.',
        base_confidence: 80,
        entity_id: 'test@example.com',
        button: 'View',
        action_type: 'view',
        ...overrides,
      })

      // Test 1: Kahil/CAS garbled commitment — must be suppressed at Stage 2.
      const kahilCandidate = makeCandidate({
        matcher_name: 'proactive_commitment',
        type: 'proactive',
        label: 'DEADLINE',
        raw_title: 'Open commitment with Kahil',
        raw_body: 'Inform all TRINS students about CAS WLFLO Collaboration by W',
        entity_id: 'commitment:test-kahil',
        source_table: 'commitments',
        source_id: 'test-kahil',
        due_date: '2026-02-04',
        freshness_timestamp: '2026-02-01',
      })
      const t1 = await runPipeline(pool, [kahilCandidate], { ...baseSignals, app: 'chrome' })
      const kahilSuppressed = t1.suppressed.some(s => s.stage === 2 || s.stage === 3)
      results.push({
        test: '1. Kahil/CAS garbled commitment',
        expected: 'suppressed at Stage 2 (quality) or Stage 3 (freshness)',
        actual: t1.fired ? 'FIRED' : t1.suppressed.map(s => `stage${s.stage}:${s.reason}`).join('; '),
        pass: !t1.fired && kahilSuppressed,
      })

      // Test 2: Upcoming meeting in 15 min while browsing — should fire with UPCOMING.
      const mtgStart = new Date(Date.now() + 15 * 60_000).toISOString()
      const mtgCandidate = makeCandidate({
        matcher_name: 'proactive_calendar',
        type: 'proactive',
        label: 'UPCOMING',
        raw_title: 'Standup with Rajeev in 15m',
        raw_body: 'Weekly sync starting soon in your calendar.',
        entity_id: 'cal-test-standup',
        source_table: 'calendar_events',
        source_id: 'cal-test-standup',
        freshness_timestamp: mtgStart,
      })
      const t2 = await runPipeline(pool, [mtgCandidate], { ...baseSignals, app: 'chrome', title: 'rajeev' })
      results.push({
        test: '2. Upcoming meeting in 15m, user browsing',
        expected: 'fires with UPCOMING label',
        actual: t2.fired ? `FIRED: ${t2.fired.label}` : `suppressed: ${t2.suppressed.map(s => s.reason).join(';')}`,
        pass: !!t2.fired && String(t2.fired.label).toUpperCase() === 'UPCOMING',
      })

      // Test 3: Overdue commitment 3 days ago — should fire once with OVERDUE.
      const threeDaysAgo = new Date(Date.now() - 3 * 86_400_000).toISOString().slice(0, 10)
      const overdueCandidate = makeCandidate({
        matcher_name: 'proactive_commitment',
        type: 'proactive',
        label: 'OVERDUE',
        raw_title: 'Overdue send the Q2 deck to team',
        raw_body: 'You said you would send the Q2 deck three days ago.',
        entity_id: 'commitment:test-overdue-3d-sim',
        source_table: 'commitments',
        source_id: 'test-overdue-3d-sim',
        due_date: threeDaysAgo,
        freshness_timestamp: threeDaysAgo,
      })
      const t3 = await runPipeline(pool, [overdueCandidate], { ...baseSignals, app: 'chrome' })
      // Spec carve-out: 1-7 day overdue fires ONCE with OVERDUE label.
      results.push({
        test: '3. Overdue commitment 3 days ago',
        expected: 'fires with OVERDUE label',
        actual: t3.fired ? `FIRED: ${t3.fired.label}` : t3.suppressed.map(s => `stage${s.stage}:${s.reason}`).join('; '),
        pass: !!t3.fired && String(t3.fired.label).toUpperCase() === 'OVERDUE',
      })

      // Test 4: Overdue by 90 days — suppressed at Stage 3.
      const ninetyDaysAgo = new Date(Date.now() - 90 * 86_400_000).toISOString().slice(0, 10)
      const t4Candidate = makeCandidate({
        matcher_name: 'proactive_commitment',
        label: 'OVERDUE',
        raw_title: 'Overdue write the annual report draft',
        raw_body: 'You committed to writing the report but it is very old now.',
        entity_id: 'commitment:test-overdue-90d-sim',
        source_table: 'commitments',
        source_id: 'test-overdue-90d-sim',
        due_date: ninetyDaysAgo,
        freshness_timestamp: ninetyDaysAgo,
      })
      const t4 = await runPipeline(pool, [t4Candidate], { ...baseSignals, app: 'chrome' })
      results.push({
        test: '4. Overdue commitment 90 days ago',
        expected: 'suppressed at Stage 3 (freshness)',
        actual: t4.fired ? `FIRED` : t4.suppressed.map(s => `stage${s.stage}:${s.reason}`).join('; '),
        pass: !t4.fired && t4.suppressed.some(s => s.stage === 3),
      })

      // Test 5: VS Code + unrelated email candidate — Stage 5 deep_focus rejects.
      const t5Candidate = makeCandidate({
        matcher_name: 'contact',
        type: 'email',
        label: 'REPLY',
        raw_title: 'Reply to Sarah about the dinner plans',
        raw_body: 'Sarah asked about Friday. You have not responded in four days.',
        entity_id: 'sarah@example.com',
      })
      const t5 = await runPipeline(pool, [t5Candidate], { ...baseSignals, app: 'visual studio code', title: 'index.ts' })
      results.push({
        test: '5. Unrelated email while in VS Code',
        expected: 'suppressed at Stage 5 (deep focus)',
        actual: t5.fired ? 'FIRED' : t5.suppressed.map(s => `stage${s.stage}:${s.reason}`).join('; '),
        pass: !t5.fired && t5.suppressed.some(s => s.stage === 5),
      })

      // Test 6: Gmail + calendar candidate for today — topical adjacency.
      const t6Candidate = makeCandidate({
        matcher_name: 'calendar',
        type: 'calendar',
        label: 'MEETING',
        raw_title: 'Team retro at 2 PM today',
        raw_body: 'Retro meeting on your calendar. Agenda not yet shared.',
        entity_id: 'cal-test-retro',
        source_table: 'calendar_events',
        freshness_timestamp: new Date(Date.now() + 3 * 3600_000).toISOString(),
      })
      const t6 = await runPipeline(pool, [t6Candidate], { ...baseSignals, app: 'mail', title: 'inbox' })
      results.push({
        test: '6. Calendar candidate while in Gmail',
        expected: 'fires (topical match via calendar+email adjacency) OR explains',
        actual: t6.fired ? `FIRED: ${t6.fired.label}` : t6.suppressed.map(s => s.reason).join(';'),
        pass: !!t6.fired || t6.suppressed.every(s => s.stage <= 5),
      })

      // Test 7: Same entity twice — second is dedup-suppressed (via batched DB state).
      // We cannot modify DB state inside the test without disturbing production logs;
      // instead, verify that the pipeline's dedup logic queries overlay_log by
      // submitting the same candidate after a direct insert.
      try {
        await pool.query(
          `INSERT INTO overlay_log (id, outcome, reason, entity_id, insight_type, matcher_name, stage, confidence, content_snapshot)
           VALUES ($1, 'fired', 'test', $2, 'email', 'contact', 10, 80, 'test')`,
          [crypto.randomUUID(), 'dedup-test@example.com'],
        )
        const t7Candidate = makeCandidate({
          matcher_name: 'contact', type: 'email', label: 'REPLY',
          raw_title: 'Reply to dedup test user about stuff',
          raw_body: 'This candidate should be dedup-suppressed by Stage 4.',
          entity_id: 'dedup-test@example.com',
        })
        const t7 = await runPipeline(pool, [t7Candidate], { ...baseSignals, app: 'mail' })
        results.push({
          test: '7. Same entity fired recently — dedup',
          expected: 'suppressed at Stage 4',
          actual: t7.fired ? 'FIRED' : t7.suppressed.map(s => `stage${s.stage}:${s.reason}`).join(';'),
          pass: !t7.fired && t7.suppressed.some(s => s.stage === 4),
        })
        // Clean up the synthetic log row.
        await pool.query(`DELETE FROM overlay_log WHERE entity_id = $1 AND reason = 'test'`, ['dedup-test@example.com'])
      } catch (err: any) {
        results.push({ test: '7. Dedup', expected: 'suppressed at Stage 4', actual: `error: ${err.message}`, pass: false })
      }

      // Test 8: 11 fires in a day — rate limit. We simulate by inserting
      // 10 synthetic fires in the last 24h, then submit an 11th.
      try {
        const bulk = Array.from({ length: 10 }, () => crypto.randomUUID())
        for (const id of bulk) {
          await pool.query(
            `INSERT INTO overlay_log (id, outcome, reason, entity_id, insight_type, matcher_name, stage, confidence, content_snapshot, timestamp)
             VALUES ($1, 'fired', 'test_bulk', $2, 'email', 'contact', 10, 80, 'test', NOW() - INTERVAL '3 hours')`,
            [id, `bulk-${id.slice(0, 6)}@example.com`],
          )
        }
        const t8Candidate = makeCandidate({
          matcher_name: 'contact', type: 'email',
          raw_title: 'Reply to eleventh test user today',
          raw_body: 'This is the eleventh candidate and should be rate-limited.',
          entity_id: 'rate-limit-test@example.com',
        })
        const t8 = await runPipeline(pool, [t8Candidate], { ...baseSignals, app: 'mail' })
        results.push({
          test: '8. 11th fire in a day — rate limit',
          expected: 'suppressed at Stage 7',
          actual: t8.fired ? 'FIRED' : t8.suppressed.map(s => `stage${s.stage}:${s.reason}`).join(';'),
          pass: !t8.fired && t8.suppressed.some(s => s.stage === 7),
        })
        // Clean up synthetic fires.
        await pool.query(`DELETE FROM overlay_log WHERE reason = 'test_bulk'`)
      } catch (err: any) {
        results.push({ test: '8. Rate limit', expected: 'suppressed at Stage 7', actual: `error: ${err.message}`, pass: false })
      }

      // Test 9: Rapid typing — suppress all at Stage 5.
      const t9Candidate = makeCandidate({
        raw_title: 'Reply to someone about a thing today',
        raw_body: 'This should be suppressed because the user is typing rapidly.',
        entity_id: 'typing-test@example.com',
      })
      const t9 = await runPipeline(pool, [t9Candidate], { ...baseSignals, app: 'mail', typing_rate: 5 })
      results.push({
        test: '9. Rapid typing (>2 changes/sec)',
        expected: 'suppressed at Stage 5 (rapid_typing_suppress)',
        actual: t9.fired ? 'FIRED' : t9.suppressed.map(s => `stage${s.stage}:${s.reason}`).join(';'),
        pass: !t9.fired && t9.suppressed.some(s => s.stage === 5 && /typing/.test(s.reason)),
      })

      // Test 10: Morning session — first fire of day, recent session start.
      // Relies on live DB state; reports whatever happens.
      const t10Candidate = makeCandidate({
        matcher_name: 'proactive',
        type: 'proactive',
        label: 'FOCUS',
        raw_title: 'A morning focus prompt to start the day',
        raw_body: 'You have three commitments due this week. Worth a quick look.',
        entity_id: 'morning-session-test',
        base_confidence: 82,
      })
      const t10 = await runPipeline(pool, [t10Candidate], { ...baseSignals, app: 'chrome' })
      results.push({
        test: '10. Morning session heuristic',
        expected: 'fires via Test D if conditions met; otherwise explains',
        actual: t10.fired ? 'FIRED' : t10.suppressed.map(s => `stage${s.stage}:${s.reason}`).join(';'),
        pass: true, // informational — always passes but shows outcome
      })

      const passed = results.filter(r => r.pass).length
      res.json({
        summary: { passed, total: results.length },
        results,
      })
    } catch (err: any) {
      console.error('[overlay-test] Error:', err.message, err.stack)
      res.status(500).json({ error: err.message })
    }
  })

  // Calendar diagnostic: counts + next upcoming events + tokens/sync_state
  app.get('/api/admin/calendar-diag', async (_req, res) => {
    try {
      const pool = getPool()
      const total = await pool.query(`SELECT COUNT(*)::int AS c FROM calendar_events`)
      const upcoming = await pool.query(
        `SELECT COUNT(*)::int AS c FROM calendar_events
         WHERE start_time::timestamptz > NOW() AND COALESCE(status,'confirmed') != 'cancelled'`
      )
      const next5 = await pool.query(
        `SELECT id, summary, start_time, end_time, calendar_name, participants, status
         FROM calendar_events WHERE start_time::timestamptz > NOW()
         ORDER BY start_time::timestamptz LIMIT 5`
      )
      const byCalendar = await pool.query(
        `SELECT calendar_name, COUNT(*)::int AS c FROM calendar_events
         GROUP BY calendar_name ORDER BY c DESC`
      )
      const tokens = await pool.query(
        `SELECT provider, email, expiry_date, updated_at FROM oauth_tokens ORDER BY updated_at DESC`
      )
      const syncState = await pool.query(
        `SELECT integration, last_sync_at, status, total_processed FROM sync_state ORDER BY integration`
      )
      res.json({
        calendar_events: {
          total: total.rows[0].c,
          upcoming_from_now: upcoming.rows[0].c,
          next_5_upcoming: next5.rows,
          by_calendar_name: byCalendar.rows,
        },
        oauth_tokens: tokens.rows,
        sync_state: syncState.rows,
      })
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

  // ── Devices (phone app registration / heartbeat / listing) ─────

  app.post('/api/devices/register', async (req, res) => {
    try {
      const pool = getPool()
      const { device_type, device_id, device_name, os, os_version, app_version, user_id } = req.body || {}
      if (!device_type || !device_id) {
        res.status(400).json({ error: 'device_type and device_id required' }); return
      }
      const existing = await pool.query(`SELECT id FROM devices WHERE device_id = $1`, [device_id])
      const id = existing.rows[0]?.id || crypto.randomUUID()
      await pool.query(
        `INSERT INTO devices (id, user_id, device_type, device_id, device_name, os, os_version, app_version, last_seen, is_active)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), TRUE)
         ON CONFLICT (device_id) DO UPDATE SET
           user_id = COALESCE(EXCLUDED.user_id, devices.user_id),
           device_type = EXCLUDED.device_type,
           device_name = COALESCE(EXCLUDED.device_name, devices.device_name),
           os = COALESCE(EXCLUDED.os, devices.os),
           os_version = COALESCE(EXCLUDED.os_version, devices.os_version),
           app_version = COALESCE(EXCLUDED.app_version, devices.app_version),
           last_seen = NOW(),
           is_active = TRUE,
           updated_at = NOW()`,
        [id, user_id || null, device_type, device_id, device_name || null, os || null, os_version || null, app_version || null]
      )
      res.json({ ok: true, device_id })
    } catch (err: any) {
      console.error('[devices/register] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  app.delete('/api/devices/:device_id', async (req, res) => {
    try {
      const pool = getPool()
      const r = await pool.query(`DELETE FROM devices WHERE device_id = $1`, [req.params.device_id])
      res.json({ ok: true, deleted: r.rowCount })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.patch('/api/devices/:device_id/heartbeat', async (req, res) => {
    try {
      const pool = getPool()
      const deviceId = req.params.device_id
      const r = await pool.query(
        `UPDATE devices SET last_seen = NOW(), updated_at = NOW(), is_active = TRUE
         WHERE device_id = $1 RETURNING last_seen`,
        [deviceId]
      )
      if (r.rowCount === 0) { res.status(404).json({ error: 'device not found' }); return }
      res.json({ ok: true, last_seen: r.rows[0].last_seen })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/devices', async (_req, res) => {
    try {
      const pool = getPool()
      const r = await pool.query(
        `SELECT id, user_id, device_type, device_id, device_name, os, os_version, app_version,
                last_seen, is_active, created_at, updated_at,
                EXTRACT(EPOCH FROM (NOW() - last_seen))::int AS seconds_since_seen
         FROM devices ORDER BY last_seen DESC`
      )
      const devices = r.rows.map((d: any) => {
        const s = Number(d.seconds_since_seen || 0)
        let status: 'online' | 'away' | 'offline'
        if (s < 5 * 60) status = 'online'
        else if (s < 60 * 60) status = 'away'
        else status = 'offline'
        return { ...d, status }
      })
      res.json({ devices })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/status', async (_req, res) => {
    try {
      const pool = getPool()

      // Devices with status
      const devRes = await pool.query(
        `SELECT device_type, device_id, device_name, os, last_seen,
                EXTRACT(EPOCH FROM (NOW() - last_seen))::int AS seconds_since_seen
         FROM devices ORDER BY last_seen DESC`
      )
      const devices = devRes.rows.map((d: any) => {
        const s = Number(d.seconds_since_seen || 0)
        let status: 'online' | 'away' | 'offline'
        if (s < 5 * 60) status = 'online'
        else if (s < 60 * 60) status = 'away'
        else status = 'offline'
        return { ...d, status }
      })

      // Sync state
      let sync_state: any = {}
      try {
        const { getSyncStatus } = await import('./sync-scheduler')
        sync_state = await getSyncStatus()
      } catch {}

      // Observation rate (last hour)
      const rateRes = await pool.query(
        `SELECT COUNT(*)::int AS c FROM observations WHERE timestamp > NOW() - INTERVAL '1 hour'`
      )
      const observation_rate = rateRes.rows[0].c

      // Last observation
      const lastRes = await pool.query(
        `SELECT timestamp FROM observations ORDER BY timestamp DESC LIMIT 1`
      )
      const last_observation_ts = lastRes.rows[0]?.timestamp || null

      // Persona freshness: rewrite age
      const personaRes = await pool.query(
        `SELECT updated_at, EXTRACT(EPOCH FROM (NOW() - updated_at))::int AS age_s
         FROM living_profile LIMIT 1`
      )
      let persona_freshness: 'fresh' | 'aging' | 'stale' = 'stale'
      const ageS = Number(personaRes.rows[0]?.age_s || Number.MAX_SAFE_INTEGER)
      if (ageS < 6 * 3600) persona_freshness = 'fresh'
      else if (ageS < 48 * 3600) persona_freshness = 'aging'

      res.json({
        devices,
        sync_state,
        observation_rate,
        persona_freshness,
        last_observation_ts,
      })
    } catch (err: any) {
      console.error('[status] Error:', err.message)
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

  // ── Admin endpoints for test data inspection and management ────

  // Search contacts by name or email
  app.get('/api/admin/contacts', async (req, res) => {
    try {
      const pool = getPool()
      const search = ((req.query.search as string) || '').toLowerCase()
      const rows = await pool.query(
        `SELECT id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at
         FROM contacts WHERE LOWER(COALESCE(name,'')) LIKE $1 OR LOWER(email) LIKE $1
         ORDER BY relationship_score DESC LIMIT 50`,
        [`%${search}%`]
      )
      res.json({ contacts: rows.rows })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // Search email threads
  app.get('/api/admin/email-threads', async (req, res) => {
    try {
      const pool = getPool()
      const search = ((req.query.search as string) || '').toLowerCase()
      const rows = await pool.query(
        `SELECT thread_id, subject, participants, last_message_from, last_message_date, stale_days, awaiting_reply, message_count
         FROM email_threads
         WHERE LOWER(COALESCE(participants,'')) LIKE $1 OR LOWER(COALESCE(subject,'')) LIKE $1 OR LOWER(COALESCE(last_message_from,'')) LIKE $1
         ORDER BY last_message_date DESC LIMIT 30`,
        [`%${search}%`]
      )
      res.json({ threads: rows.rows })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // Search observations for a keyword in last N hours
  app.get('/api/admin/obs-search', async (req, res) => {
    try {
      const pool = getPool()
      const search = ((req.query.search as string) || '').toLowerCase()
      const hours = parseInt((req.query.hours as string) || '24', 10)
      const rows = await pool.query(
        `SELECT id, timestamp, source, event_type, raw_content
         FROM observations
         WHERE LOWER(raw_content) LIKE $1
         AND timestamp > NOW() - ($2 || ' hours')::interval
         ORDER BY timestamp DESC LIMIT 50`,
        [`%${search}%`, String(hours)]
      )
      res.json({ observations: rows.rows, count: rows.rows.length })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // Seed synthetic test data (marked with test-overlay- id prefix)
  app.post('/api/admin/seed-test-data', async (req, res) => {
    try {
      const pool = getPool()
      const { calendarEvents, actionChains, corrections, contacts, observations, predictedActions } = req.body
      const counts: Record<string, number> = {}

      for (const c of contacts || []) {
        await pool.query(
          `INSERT INTO contacts (id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (email) DO UPDATE SET
           name=$2, emails_sent=$4, emails_received=$5, relationship_score=$6, last_interaction_at=$7`,
          [c.id, c.name, c.email, c.emails_sent || 0, c.emails_received || 0, c.relationship_score || 0, c.last_interaction_at || new Date().toISOString()]
        )
      }
      counts.contacts = (contacts || []).length

      for (const ev of calendarEvents || []) {
        await pool.query(
          `INSERT INTO calendar_events (id, summary, start_time, end_time, all_day, location, participants, recurring, status, calendar_name)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
           ON CONFLICT (id) DO UPDATE SET summary=$2, start_time=$3, end_time=$4, participants=$7, status=$9`,
          [ev.id, ev.summary, ev.start_time, ev.end_time, ev.all_day || 0, ev.location || '', ev.participants || '', ev.recurring || 0, ev.status || 'confirmed', ev.calendar_name || 'primary']
        )
      }
      counts.calendar_events = (calendarEvents || []).length

      for (const ac of actionChains || []) {
        await pool.query(
          `INSERT INTO action_chains (id, trigger_event, trigger_type, trigger_entity, status, context_snapshot)
           VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET trigger_entity=$4, status=$5`,
          [ac.id, ac.trigger_event, ac.trigger_type || 'test', ac.trigger_entity || '', ac.status || 'active', ac.context_snapshot || '']
        )
        for (const step of ac.steps || []) {
          await pool.query(
            `INSERT INTO chain_steps (id, chain_id, step_number, title, description, status)
             VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET title=$4, description=$5, status=$6`,
            [step.id, ac.id, step.step_number, step.title, step.description || '', step.status || 'pending']
          )
        }
      }
      counts.action_chains = (actionChains || []).length

      for (const c of corrections || []) {
        await pool.query(
          `INSERT INTO correction_log (id, trigger_action, trigger_context, user_action, correction_text)
           VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO UPDATE SET trigger_action=$2, trigger_context=$3, user_action=$4, correction_text=$5`,
          [c.id, c.trigger_action, c.trigger_context || '', c.user_action, c.correction_text || '']
        )
      }
      counts.corrections = (corrections || []).length

      for (const o of observations || []) {
        await pool.query(
          `INSERT INTO observations (id, source, event_type, raw_content, extracted_entities, emotional_valence)
           VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING`,
          [o.id, o.source, o.event_type, o.raw_content, o.extracted_entities || '{}', o.emotional_valence || 'neutral']
        )
      }
      counts.observations = (observations || []).length

      for (const p of predictedActions || []) {
        await pool.query(
          `INSERT INTO predicted_actions (id, trigger_context, trigger_type, action_type, title, description, confidence, urgency, expires_at, status, related_entity)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (id) DO UPDATE SET
            title=$5, description=$6, related_entity=$11, status=$10, expires_at=$9`,
          [p.id, p.trigger_context || '', p.trigger_type || 'test', p.action_type || 'follow_up_draft',
           p.title, p.description || '', p.confidence || 0.7, p.urgency || 'medium',
           p.expires_at || new Date(Date.now() + 7 * 86400000).toISOString(),
           p.status || 'pending', p.related_entity || '']
        )
      }
      counts.predicted_actions = (predictedActions || []).length

      res.json({ ok: true, counts })
    } catch (err: any) {
      console.error('[admin/seed] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // Trigger WhatsApp extractor (backfill or incremental)
  app.post('/api/whatsapp/extract', async (req, res) => {
    try {
      const { runWhatsAppExtractor } = await import('./whatsapp-extractor')
      const backfillAll = req.body?.backfillAll === true
      const limit = typeof req.body?.limit === 'number' ? req.body.limit : 2000
      const result = await runWhatsAppExtractor({ limit, backfillAll })
      res.json({ ok: true, ...result })
    } catch (err: any) {
      console.error('[whatsapp/extract] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // Get WhatsApp extractor status
  app.get('/api/whatsapp/status', async (_req, res) => {
    try {
      const pool = getPool()
      const [total, parsed, unparsed] = await Promise.all([
        pool.query(`SELECT COUNT(*)::int AS c FROM observations WHERE source = 'whatsapp'`),
        pool.query(`SELECT COUNT(*)::int AS c FROM observations WHERE source = 'whatsapp' AND parsed_whatsapp IS NOT NULL`),
        pool.query(`SELECT COUNT(*)::int AS c FROM observations WHERE source = 'whatsapp' AND parsed_whatsapp IS NULL`),
      ])
      const contactsByChannel = await pool.query(
        `SELECT primary_channel, COUNT(*)::int AS c FROM contacts GROUP BY primary_channel`
      )
      res.json({
        observations: {
          total: total.rows[0].c,
          parsed: parsed.rows[0].c,
          unparsed: unparsed.rows[0].c,
        },
        contacts_by_channel: contactsByChannel.rows,
      })
    } catch (err: any) {
      res.status(500).json({ error: err.message })
    }
  })

  // Diagnostic: WhatsApp / contacts / relationships breakdown
  app.get('/api/admin/whatsapp-diag', async (_req, res) => {
    try {
      const pool = getPool()

      const tablesRes = await pool.query(
        `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`
      )
      const allTables = tablesRes.rows.map((r: any) => r.tablename)
      const whatsappTables = allTables.filter((t: string) => t.toLowerCase().includes('whatsapp'))

      const contactsTotal = await pool.query(`SELECT COUNT(*)::int AS c FROM contacts`)
      const contactsWithEmail = await pool.query(`SELECT COUNT(*)::int AS c FROM contacts WHERE email IS NOT NULL AND email != ''`)
      const contactsEmailNull = await pool.query(`SELECT COUNT(*)::int AS c FROM contacts WHERE email IS NULL OR email = ''`)
      const contactColumns = await pool.query(
        `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'contacts' ORDER BY ordinal_position`
      )

      const relationshipsTotal = await pool.query(`SELECT COUNT(*)::int AS c FROM relationships`)
      const relWithEmail = await pool.query(`SELECT COUNT(*)::int AS c FROM relationships WHERE email IS NOT NULL AND email != ''`)
      const relNoEmail = await pool.query(`SELECT COUNT(*)::int AS c FROM relationships WHERE email IS NULL OR email = ''`)
      const relWithWa = await pool.query(`SELECT COUNT(*)::int AS c FROM relationships WHERE whatsapp_msgs_30d > 0`)
      const relWaOnly = await pool.query(
        `SELECT COUNT(*)::int AS c FROM relationships
         WHERE whatsapp_msgs_30d > 0 AND email_count_30d = 0`
      )
      const relColumns = await pool.query(
        `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'relationships' ORDER BY ordinal_position`
      )

      const waObsTotal = await pool.query(`SELECT COUNT(*)::int AS c FROM observations WHERE source = 'whatsapp'`)
      const waObs30d = await pool.query(
        `SELECT COUNT(*)::int AS c FROM observations WHERE source = 'whatsapp' AND timestamp > NOW() - INTERVAL '30 days'`
      )
      const waObsSample = await pool.query(
        `SELECT id, timestamp, source, event_type, raw_content, extracted_entities
         FROM observations WHERE source = 'whatsapp'
         ORDER BY timestamp DESC LIMIT 5`
      )

      // Targeted lookups
      const searchContact = async (q: string) => (
        await pool.query(
          `SELECT id, name, email, emails_sent, emails_received, relationship_score, last_interaction_at
           FROM contacts WHERE LOWER(COALESCE(name,'')) LIKE $1 OR LOWER(COALESCE(email,'')) LIKE $1 LIMIT 10`,
          [`%${q.toLowerCase()}%`]
        )
      ).rows
      const searchRel = async (q: string) => (
        await pool.query(
          `SELECT name, email, current_closeness, trajectory, interaction_freq_30d, interaction_freq_90d,
                  whatsapp_msgs_30d, email_count_30d, last_interaction
           FROM relationships WHERE LOWER(COALESCE(name,'')) LIKE $1 OR LOWER(COALESCE(email,'')) LIKE $1 LIMIT 10`,
          [`%${q.toLowerCase()}%`]
        )
      ).rows

      const [ananyaContact, ananyaRel, sanjitContact, sanjitRel, rajeevContact, rajeevRel] = await Promise.all([
        searchContact('ananya'), searchRel('ananya'),
        searchContact('sanjit'), searchRel('sanjit'),
        searchContact('rajeev'), searchRel('rajeev'),
      ])

      // Extract frequent name-like tokens from WhatsApp raw_content
      const waAll = await pool.query(
        `SELECT raw_content FROM observations WHERE source = 'whatsapp' LIMIT 2000`
      )
      const waNameCounts = new Map<string, number>()
      for (const row of waAll.rows) {
        const text = (row.raw_content || '').toLowerCase()
        const toks = text.split(/[^a-z]+/).filter((w: string) => w.length >= 4 && w.length <= 20)
        const seen = new Set<string>()
        for (const t of toks) {
          if (seen.has(t)) continue
          seen.add(t)
          waNameCounts.set(t, (waNameCounts.get(t) || 0) + 1)
        }
      }
      // Only keep tokens appearing in 3+ WA rows
      const frequentWaTokens = Array.from(waNameCounts.entries())
        .filter(([_, n]) => n >= 3)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 50)

      // Among frequent WA tokens, which do NOT have a contact record?
      const contactFirstNames = new Set<string>()
      const allContactsRes = await pool.query(`SELECT name FROM contacts WHERE name IS NOT NULL`)
      for (const r of allContactsRes.rows) {
        contactFirstNames.add(((r.name || '').trim().split(/\s+/)[0] || '').toLowerCase())
      }
      const STOPLIST = new Set([
        'http','https','com','www','the','and','you','your','this','that','with','from','have','will','would','could','should','about','been','just','like','want','need','make','take','what','when','where','know','good','time','back','over','well','some','other','those','these','much','many','more','most','only','into','after','before','still','again','also','very','really','okay','today','there','here','they','their','them','then','than','which','while','were','was','are','for','but','not','all','any','can','who','how','why','our','out','off','did','get','got','put','let','see','use','one','two','ten','pls','hai','okay','yes','no'
      ])
      const waOnlyTokens = frequentWaTokens
        .filter(([t]) => !contactFirstNames.has(t) && !STOPLIST.has(t))
        .slice(0, 25)

      res.json({
        all_tables: allTables,
        whatsapp_tables_found: whatsappTables,
        contacts: {
          total: contactsTotal.rows[0].c,
          with_email: contactsWithEmail.rows[0].c,
          email_null_or_empty: contactsEmailNull.rows[0].c,
          columns: contactColumns.rows,
        },
        relationships: {
          total: relationshipsTotal.rows[0].c,
          with_email: relWithEmail.rows[0].c,
          no_email: relNoEmail.rows[0].c,
          with_whatsapp_msgs: relWithWa.rows[0].c,
          whatsapp_only: relWaOnly.rows[0].c,
          columns: relColumns.rows,
        },
        whatsapp_observations: {
          total: waObsTotal.rows[0].c,
          last_30d: waObs30d.rows[0].c,
          sample_5_recent: waObsSample.rows,
        },
        targeted_people: {
          ananya: { contact: ananyaContact, relationship: ananyaRel },
          sanjit: { contact: sanjitContact, relationship: sanjitRel },
          rajeev: { contact: rajeevContact, relationship: rajeevRel },
        },
        wa_name_discovery: {
          description: 'Tokens appearing in 3+ WhatsApp observations',
          total_frequent_tokens: frequentWaTokens.length,
          frequent_but_no_contact_match: waOnlyTokens,
        }
      })
    } catch (err: any) {
      console.error('[admin/whatsapp-diag] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

  // Delete all test data (id LIKE 'test-overlay-%')
  app.post('/api/admin/delete-test-data', async (req, res) => {
    try {
      const pool = getPool()
      const prefix = 'test-overlay-'
      const results: Record<string, number> = {}
      const ops: [string, string][] = [
        ['calendar_events',    `DELETE FROM calendar_events WHERE id LIKE $1`],
        ['chain_steps',        `DELETE FROM chain_steps WHERE id LIKE $1 OR chain_id LIKE $1`],
        ['action_chains',      `DELETE FROM action_chains WHERE id LIKE $1`],
        ['correction_log',     `DELETE FROM correction_log WHERE id LIKE $1`],
        ['observations',       `DELETE FROM observations WHERE id LIKE $1`],
        ['predicted_actions',  `DELETE FROM predicted_actions WHERE id LIKE $1`],
      ]
      for (const [name, sql] of ops) {
        const r = await pool.query(sql, [`${prefix}%`])
        results[name] = r.rowCount || 0
      }
      res.json({ ok: true, deleted: results })
    } catch (err: any) {
      console.error('[admin/delete] Error:', err.message)
      res.status(500).json({ error: err.message })
    }
  })

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
    const url = new URL(request.url || '/', `http://localhost`)
    if (url.pathname === '/ws/glasses') {
      // Check auth for WebSocket — Bearer header or ?token= query param
      const token = request.headers.authorization?.replace('Bearer ', '') ||
                    url.searchParams.get('token')
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

  // ── Glasses helpers: Whisper transcription (Groq) ──────────────

  function wrapPCMasWAV(pcm: Buffer, sampleRate: number, channels: number, bitsPerSample: number): Buffer {
    const byteRate = sampleRate * channels * (bitsPerSample / 8)
    const blockAlign = channels * (bitsPerSample / 8)
    const header = Buffer.alloc(44)
    header.write('RIFF', 0)
    header.writeUInt32LE(36 + pcm.length, 4)
    header.write('WAVE', 8)
    header.write('fmt ', 12)
    header.writeUInt32LE(16, 16)
    header.writeUInt16LE(1, 20)
    header.writeUInt16LE(channels, 22)
    header.writeUInt32LE(sampleRate, 24)
    header.writeUInt32LE(byteRate, 28)
    header.writeUInt16LE(blockAlign, 32)
    header.writeUInt16LE(bitsPerSample, 34)
    header.write('data', 36)
    header.writeUInt32LE(pcm.length, 40)
    return Buffer.concat([header, pcm])
  }

  async function transcribeAudio(groqKey: string, pcmBuffer: Buffer): Promise<string> {
    const Groq = (await import('groq-sdk')).default
    const wavBuffer = wrapPCMasWAV(pcmBuffer, 16000, 1, 16)
    const groq = new Groq({ apiKey: groqKey })
    const file = new File([wavBuffer], 'audio.wav', { type: 'audio/wav' })
    const transcription = await groq.audio.transcriptions.create({
      file,
      model: 'whisper-large-v3-turbo',
      response_format: 'text'
    })
    return typeof transcription === 'string' ? transcription : (transcription as any).text ?? ''
  }

  // ── Glasses helpers: Cartesia TTS (REST fallback + WebSocket streaming) ──

  const CARTESIA_VOICE_ID = '1463a4e1-56a1-4b41-b257-728d56e93605'
  const CARTESIA_MODEL = 'sonic-2'
  const CARTESIA_VERSION = '2024-06-10'
  const CARTESIA_OUTPUT_FORMAT = { container: 'raw' as const, sample_rate: 16000, encoding: 'pcm_s16le' as const }

  // REST fallback for briefing or when WS is unavailable
  async function generateTTSRaw(cartesiaKey: string, text: string): Promise<Buffer | null> {
    const response = await fetch('https://api.cartesia.ai/tts/bytes', {
      method: 'POST',
      headers: {
        'X-API-Key': cartesiaKey,
        'Cartesia-Version': CARTESIA_VERSION,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model_id: CARTESIA_MODEL,
        transcript: text.slice(0, 5000),
        voice: { mode: 'id', id: CARTESIA_VOICE_ID },
        output_format: CARTESIA_OUTPUT_FORMAT
      })
    })
    if (!response.ok) {
      console.error(`[TTS] Cartesia error: ${response.status} ${response.statusText}`)
      return null
    }
    return Buffer.from(await response.arrayBuffer())
  }

  function sendTTSFrame(ws: WebSocket, pcmAudio: Buffer): void {
    const frame = Buffer.alloc(1 + pcmAudio.length)
    frame[0] = 0x10 // MSG_AUDIO_RESPONSE
    pcmAudio.copy(frame, 1)
    ws.send(frame)
  }

  // ── Glasses helpers: text sanitization for TTS ─────────────────

  /** Strip forbidden characters so the user never hears them spoken. */
  function sanitizeForTTS(text: string): string {
    return text
      // Em dash, en dash, figure dash, horizontal bar → period+space
      .replace(/[—–‒―]/g, '. ')
      // Strip markdown/visual characters entirely
      .replace(/[*_`[\]{}|\\<>]/g, '')
      // Strip parentheses (keep the content)
      .replace(/[()]/g, '')
      // Bullet points and list markers
      .replace(/^[\s]*[•·▪▫●○]\s*/gm, '')
      // Slashes between words → "or" feels unnatural; use space
      .replace(/(\w)\/(\w)/g, '$1 or $2')
      // Collapse multiple spaces
      .replace(/\s+/g, ' ')
      // Collapse ". ." repeats from the em dash replacement
      .replace(/\.\s*\./g, '.')
      .trim()
  }

  // ── Glasses helpers: query classification for token budgeting ──

  type GlassesQueryType = 'command' | 'factual' | 'analytical'

  function classifyGlassesQuery(transcript: string): GlassesQueryType {
    const lower = transcript.toLowerCase().trim().replace(/[.!?,]+$/, '')
    const wordCount = lower.split(/\s+/).filter(Boolean).length

    // Short commands: "send it", "approve", "skip", "remind me later"
    if (wordCount <= 4 &&
        /^(send|approve|cancel|skip|yes|no|done|reject|accept|ok|okay|dismiss|snooze|remind|confirm|go|stop|pause|resume|next|more|delete|save|archive|mute|unmute)\b/.test(lower)) {
      return 'command'
    }

    // Factual: yes/no questions, specific-info queries
    if (/^(what time|what's the time|when is|when's|when does|where is|where's|who is|who's|who was|did |does |is |are |was |were |do i|did i|have i|has |how many|how much|how long|what day|what date|what's my|what are my|how's)/.test(lower)) {
      return 'factual'
    }

    return 'analytical'
  }

  function maxTokensFor(type: GlassesQueryType): number {
    switch (type) {
      case 'command':    return 15
      case 'factual':    return 40
      case 'analytical': return 80
    }
  }

  // ── Cartesia WebSocket streaming TTS ──────────────────────────

  /**
   * Stream text chunks to Cartesia WS and forward audio to glasses in real-time.
   * Creates a fresh WS connection per call — simpler and more reliable than
   * pooling. WS connection overhead is ~100ms, negligible compared to TTFB wins.
   */
  function createCartesiaStream(cartesiaKey: string, glassesWs: WebSocket): {
    sendChunk: (text: string) => void
    finish: () => Promise<{ audioBytes: number; chunks: number }>
    contextId: string
    ready: Promise<void>
  } {
    const contextId = `glasses-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    let audioBytes = 0
    let audioChunks = 0
    let resolveReady: () => void
    let rejectReady: (err: Error) => void
    let resolveDone: (val: { audioBytes: number; chunks: number }) => void
    let wsInstance: import('ws').WebSocket | null = null
    let firstChunkSent = false
    let finished = false
    let doneReceived = false

    const readyPromise = new Promise<void>((res, rej) => { resolveReady = res; rejectReady = rej })
    const donePromise = new Promise<{ audioBytes: number; chunks: number }>((res) => { resolveDone = res })

    const completeDone = () => {
      if (finished) return
      finished = true
      if (wsInstance) {
        try { wsInstance.close() } catch {}
      }
      resolveDone({ audioBytes, chunks: audioChunks })
    }

    // Connect fresh WS for this stream
    const WS = require('ws') as typeof import('ws')
    const wsUrl = `wss://api.cartesia.ai/tts/websocket?api_key=${encodeURIComponent(cartesiaKey)}&cartesia_version=${CARTESIA_VERSION}`
    const ws = new WS.WebSocket(wsUrl)

    const connectTimeout = setTimeout(() => {
      console.error(`[TTS-WS] Connection timeout (${contextId})`)
      try { ws.close() } catch {}
      rejectReady!(new Error('Cartesia WS connection timeout'))
      completeDone()
    }, 10000)

    ws.on('open', () => {
      clearTimeout(connectTimeout)
      wsInstance = ws
      console.log(`[TTS-WS] Cartesia WS connected (${contextId})`)
      resolveReady!()
    })

    ws.on('message', (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString())
        if (msg.context_id && msg.context_id !== contextId) return

        if (msg.type === 'chunk' && msg.data) {
          const pcm = Buffer.from(msg.data, 'base64')
          audioBytes += pcm.length
          audioChunks++
          if (glassesWs.readyState === 1 /* OPEN */) {
            sendTTSFrame(glassesWs, pcm)
          }
        } else if (msg.type === 'done') {
          doneReceived = true
          completeDone()
        } else if (msg.type === 'error') {
          console.error(`[TTS-WS] Cartesia stream error (${contextId}):`, msg.error)
          completeDone()
        }
      } catch (err) {
        console.error(`[TTS-WS] Parse error (${contextId}):`, err)
      }
    })

    ws.on('close', () => {
      console.log(`[TTS-WS] WS closed (${contextId}) audio=${audioBytes}b chunks=${audioChunks} done=${doneReceived}`)
      completeDone()
    })

    ws.on('error', (err: Error) => {
      clearTimeout(connectTimeout)
      console.error(`[TTS-WS] WS error (${contextId}):`, err.message)
      rejectReady!(err)
      completeDone()
    })

    return {
      contextId,
      ready: readyPromise,
      sendChunk(text: string) {
        if (!wsInstance || !text.trim() || finished) return
        if (wsInstance.readyState !== 1 /* OPEN */) return
        try {
          wsInstance.send(JSON.stringify({
            model_id: CARTESIA_MODEL,
            transcript: text,
            voice: { mode: 'id', id: CARTESIA_VOICE_ID },
            output_format: CARTESIA_OUTPUT_FORMAT,
            context_id: contextId,
            continue: true,
            language: 'en'
          }))
          if (!firstChunkSent) {
            firstChunkSent = true
            console.log(`[TTS-WS] First text chunk sent (${contextId})`)
          }
        } catch (err: any) {
          console.error(`[TTS-WS] sendChunk error (${contextId}):`, err.message)
        }
      },
      async finish(): Promise<{ audioBytes: number; chunks: number }> {
        if (finished) return { audioBytes, chunks: audioChunks }
        if (wsInstance && wsInstance.readyState === 1 /* OPEN */) {
          try {
            wsInstance.send(JSON.stringify({
              model_id: CARTESIA_MODEL,
              transcript: '',
              voice: { mode: 'id', id: CARTESIA_VOICE_ID },
              output_format: CARTESIA_OUTPUT_FORMAT,
              context_id: contextId,
              continue: false,
              language: 'en'
            }))
          } catch (err: any) {
            console.error(`[TTS-WS] finish send error (${contextId}):`, err.message)
            completeDone()
          }
        }

        // Hard timeout: don't wait forever for done signal
        const timeoutMs = 20000
        return Promise.race([
          donePromise,
          new Promise<{ audioBytes: number; chunks: number }>((res) => {
            setTimeout(() => {
              if (!finished) {
                console.warn(`[TTS-WS] finish timeout (${contextId}) — forcing close after ${timeoutMs}ms`)
                completeDone()
              }
              res({ audioBytes, chunks: audioChunks })
            }, timeoutMs)
          })
        ])
      }
    }
  }

  // ── Glasses helpers: IRIS glasses system prompt ────────────────

  const GLASSES_IRIS_RULES = `You are IRIS, a personal AI assistant. You are being spoken aloud through TTS on glasses hardware. Your response will be converted directly to speech without any visual rendering. Follow these rules absolutely.

LENGTH:
- Factual questions: one sentence, five to fifteen words. Lead with the answer.
- Analytical questions: two to three sentences max, twenty to forty words. Answer first, brief evidence, stop.
- Command acknowledgment: one to five words. "Sent." "Approved." "Done."
- Error responses: five words max. No apologies. "I don't know that one."

LANGUAGE:
- Use contractions always: don't, can't, won't, haven't, hasn't, you're, they're, it's, there's, would've, should've, I've, you've. Never use the uncontracted form.
- FORBIDDEN CHARACTERS: em dash (—), en dash (–), bullet points, parentheses, brackets, asterisks, slashes, pipes, backticks, underscores. If you need to separate two thoughts, use a period and start a new sentence.
- No numbered lists unless the user explicitly asks for a list. No bullet points or visual formatting.
- Numbers under ten: spell as words. "three days" not "3 days". Numbers over ten can stay numeric.
- Times: speak naturally. "two fifteen" not "14:15" or "2:15 PM".
- Dates: speak naturally. "April twentieth" not "Apr 20" or "20/04".
- Currency: spell out. "six thousand rupees" not "₹6000".
- IB abbreviations: say "Extended Essay" when context allows, otherwise "E E". "I B" letter by letter. "C A S" letter by letter. "T O K" letter by letter. "I A" letter by letter.

TOKEN BUDGET:
- You have a strict token budget. Every word must earn its place. If you can't say it in the budget, pick the single most important thing and say only that.
- Do not pad. Do not explain. Do not list more than you must.

TONE:
- Warm but not sycophantic. Use "I" and "you" directly. "I checked your calendar" is fine.
- No filler phrases ever: no "I hope this helps", no "let me know if you need anything else", no "feel free to", no "of course", no "I'd be happy to".
- No thinking-out-loud: no "let me check", no "one moment", no "looking at that now".
- Lead with the answer, not context about the answer.
- Opinionated when asked for a recommendation. Pick one answer and commit.
- Brutally honest about follow-through problems, bad patterns, avoidance. No softening.

METADATA:
- Never read numerical confidence scores, stale day counts as raw numbers over thirty, or internal metrics aloud.
- Convert to natural language: "about two weeks ago", "pretty sure", "probably", "I'm guessing".
- Don't say action type labels like "URGENT NOW" or "predicted action". Just state the content.

CONTINUATION:
- When there's genuinely more depth available and the user would likely want it, end with a natural question: "Want the full list?" "I have more if you want it." "Ask me who specifically."
- Do NOT use continuation hooks on every response. Factual queries end cleanly with no hook.

PRONUNCIATION AND PACING:
- Use periods and commas deliberately for pacing. Short sentences sound faster and more urgent. Longer sentences with commas sound calmer.
- Match pacing to content: urgent things should be choppy, briefings should flow.

EXAMPLES:
Bad: "You have not replied to Rajeev Kumar in 13 days — your Extended Essay interview is scheduled for April 20 at 2:15 PM."
Good: "You haven't heard from Rajeev in about two weeks. Your Extended Essay interview is April twentieth at two fifteen."

Bad: "Based on your priorities, I would recommend focusing on the following: 1) Rajeev follow-up, 2) History IA section B, 3) CAS documentation."
Good: "Focus on Rajeev first. He's the blocker for your E E interview this week. After that, History I A section B, you haven't touched it in six days."

Bad: "I have a draft ready for your review — would you like me to send it or should I modify it first?"
Good: "Draft's ready. Say send, or tell me what to change."

Bad: "Sent successfully! The email has been delivered to Rajeev."
Good: "Sent."

Bad: "I'm sorry, I don't have any information about that topic in my current database."
Good: "I don't know that one."`

  const GLASSES_BRIEFING_RULES = `You are IRIS delivering a spoken briefing through glasses TTS. Same voice rules as all glasses responses apply.

BRIEFING LENGTH:
- Morning: three to four sentences, twenty-five to forty words total. Conversational and complete.
- Evening: three to four sentences, twenty-five to forty words total. Reflective but forward-looking.

BRIEFING EXAMPLES:
Morning: "Today's your E E interview window. Two classes before lunch, then free afternoon. Rajeev still hasn't replied, I have a follow-up ready when you want it. Otherwise, quiet day."
Evening: "Solid day. You cleared the Physics assignment and two Centrum cards. Still no word from Rajeev, I'll check again tomorrow morning. Tomorrow looks lighter, good time for E E work."`

  // ── Glasses helpers: voice query pipeline ──────────────────────

  async function handleGlassesVoiceQuery(ws: WebSocket, pcmBuffer: Buffer): Promise<void> {
    const t0 = Date.now()
    const settings = await getSettings()
    const groqKey = settings.groq_api_key || ''
    const cartesiaKey = settings.cartesia_api_key || ''
    const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY || ''

    if (!groqKey) { ws.send(JSON.stringify({ error: 'No Groq API key configured' })); return }
    if (!apiKey) { ws.send(JSON.stringify({ error: 'No Anthropic API key configured' })); return }
    if (!cartesiaKey) { ws.send(JSON.stringify({ error: 'No Cartesia API key configured' })); return }

    // Step 1: Transcribe via Groq Whisper
    const transcript = await transcribeAudio(groqKey, pcmBuffer)
    const t1 = Date.now()
    console.log(`[glasses] transcribed in ${t1 - t0}ms: "${transcript.slice(0, 80)}"`)

    if (!transcript || transcript.trim().length < 2) {
      ws.send(JSON.stringify({ type: 'response', mode: 'voice', text: '' }))
      return
    }

    // Log as observation
    insertObservation('glasses_voice', 'queried', transcript, {}, 'neutral', [], '').catch(() => {})

    // Step 2+3: Stream Claude → Cartesia WS → glasses speaker (pipelined)
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const client = new Anthropic({ apiKey })

    const recentQueries = await getRecentQueryMemory(5, 30)
    const queryClass = classifyQuery(transcript)
    const cached = await getCachedContext()

    const recentConversation = recentQueries.length > 0
      ? `RECENT CONVERSATION:\n${recentQueries.map((q: any) => {
          const entities = Array.isArray(q.entities_mentioned) ? q.entities_mentioned :
            (typeof q.entities_mentioned === 'string' ? JSON.parse(q.entities_mentioned) : [])
          return `User: "${q.query_text}"\nIRIS: "${(q.response_text || '').slice(0, 100)}"${entities.length ? ` [${entities.join(', ')}]` : ''}`
        }).join('\n')}`
      : ''

    const context = assembleContext(cached, queryClass.type, recentConversation)

    // Start Cartesia WS stream in parallel with Claude call
    const ttsStream = createCartesiaStream(cartesiaKey, ws)

    // Use Claude streaming so we can pipe tokens to Cartesia as they arrive
    let fullText = ''
    let textBuffer = '' // Buffer text until we hit a sentence boundary
    let tFirstAudio = 0
    const tClaudeStart = Date.now()

    // Classify query type to enforce hard max_tokens budget
    const glassesType = classifyGlassesQuery(transcript)
    const budgetedMaxTokens = maxTokensFor(glassesType)
    console.log(`[glasses] query type: ${glassesType} (max_tokens: ${budgetedMaxTokens})`)

    try {
      // Wait for Cartesia WS to be ready before starting Claude stream
      await ttsStream.ready

      const stream = client.messages.stream({
        model: settings.model || 'claude-sonnet-4-20250514',
        max_tokens: budgetedMaxTokens,
        system: [
          { type: 'text', text: GLASSES_IRIS_RULES, cache_control: { type: 'ephemeral' } as any },
          { type: 'text', text: `\nCONTEXT:\n${context}`, cache_control: { type: 'ephemeral' } as any }
        ],
        messages: [{ role: 'user', content: transcript }]
      })

      // Sentence-boundary regex: split on . ! ? followed by space or end
      const sentenceBoundary = /[.!?](?:\s|$)/

      for await (const event of stream) {
        if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
          const token = event.delta.text
          fullText += token
          textBuffer += token

          // Send to Cartesia when we accumulate a sentence boundary or 80+ chars
          if (sentenceBoundary.test(textBuffer) || textBuffer.length >= 80) {
            const clean = sanitizeForTTS(textBuffer)
            if (clean) ttsStream.sendChunk(clean)
            if (!tFirstAudio) tFirstAudio = Date.now()
            textBuffer = ''
          }
        }
      }

      // Flush any remaining text
      if (textBuffer.trim()) {
        const clean = sanitizeForTTS(textBuffer)
        if (clean) ttsStream.sendChunk(clean)
        if (!tFirstAudio) tFirstAudio = Date.now()
      }
    } catch (err: any) {
      // On Claude streaming error, try fallback with non-streaming
      console.error(`[glasses] Claude stream error: ${err.message} — trying fallback`)
      try {
        const fallbackResponse = await claudeWithFallback(client, {
          model: settings.model || 'claude-sonnet-4-20250514',
          max_tokens: budgetedMaxTokens,
          system: [
            { type: 'text', text: GLASSES_IRIS_RULES, cache_control: { type: 'ephemeral' } },
            { type: 'text', text: `\nCONTEXT:\n${context}`, cache_control: { type: 'ephemeral' } }
          ],
          messages: [{ role: 'user', content: transcript }]
        }, 'glasses-voice-fallback')
        fullText = fallbackResponse.content?.[0]?.type === 'text' ? fallbackResponse.content[0].text ?? '' : ''
        if (fullText) {
          const clean = sanitizeForTTS(fullText)
          if (clean) ttsStream.sendChunk(clean)
          if (!tFirstAudio) tFirstAudio = Date.now()
        }
      } catch (fallbackErr: any) {
        console.error(`[glasses] All Claude models failed: ${fallbackErr.message}`)
        ws.send(JSON.stringify({ type: 'response', mode: 'voice', text: '', error: 'AI temporarily unavailable' }))
        return
      }
    }

    const t2 = Date.now()
    console.log(`[glasses] Claude streamed in ${t2 - tClaudeStart}ms (${fullText.length} chars)`)

    if (!fullText || fullText.trim().length === 0) {
      ws.send(JSON.stringify({ type: 'response', mode: 'voice', text: '', error: 'Empty response from AI' }))
      return
    }

    // Sanitize for display/storage (match what the user actually heard)
    const cleanFullText = sanitizeForTTS(fullText)

    // Store in query memory
    const entities = extractEntitiesFromText(cleanFullText, transcript)
    insertQueryMemory(transcript, cleanFullText, entities).catch(() => {})

    // Send text response (for logging/display on companion app)
    const degradationNote = getDegradationWhisper()
    ws.send(JSON.stringify({
      type: 'response', mode: 'voice', text: cleanFullText,
      conversation_active: true,
      listen_after: 5.0,
      ...(degradationNote ? { whisper: degradationNote } : {})
    }))

    // Wait for Cartesia to finish sending all audio
    const ttsResult = await ttsStream.finish()
    const t3 = Date.now()

    const ttfb = tFirstAudio ? tFirstAudio - t0 : t3 - t0
    console.log(`[glasses] voice query total: ${t3 - t0}ms (whisper: ${t1 - t0}ms, claude-stream: ${t2 - tClaudeStart}ms, tts-stream: ${t3 - t2}ms, TTFB: ${ttfb}ms, audio: ${ttsResult.audioBytes} bytes / ${ttsResult.chunks} chunks)`)
  }

  // ── Glasses helpers: briefing generator ────────────────────────

  async function handleGlassesBriefing(ws: WebSocket): Promise<void> {
    const settings = await getSettings()
    const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY || ''
    const cartesiaKey = settings.cartesia_api_key || ''
    if (!apiKey) { ws.send(JSON.stringify({ error: 'No API key' })); return }

    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const client = new Anthropic({ apiKey })
    const pool = getPool()

    // Determine morning vs evening
    const hour = new Date().getHours()
    const isMorning = hour >= 5 && hour < 14

    // Gather briefing context
    const [profileRaw, cached, predictedActions, recentBreaks, lifeEvents] = await Promise.all([
      getLivingProfile(),
      getCachedContext(),
      pool.query(
        `SELECT title, description, urgency FROM predicted_actions WHERE status = 'pending' AND (expires_at IS NULL OR expires_at > NOW()) ORDER BY urgency DESC, predicted_at DESC LIMIT 5`
      ).then(r => r.rows).catch(() => []),
      pool.query(
        `SELECT description, severity FROM routine_breaks WHERE detected_at > NOW() - INTERVAL '24 hours' ORDER BY detected_at DESC LIMIT 3`
      ).then(r => r.rows).catch(() => []),
      pool.query(
        `SELECT summary FROM life_events WHERE detected_at > NOW() - INTERVAL '24 hours' ORDER BY detected_at DESC LIMIT 1`
      ).then(r => r.rows).catch(() => [])
    ])
    const profile = profileRaw ?? ''

    // Get upcoming events
    const events = await pool.query(
      `SELECT summary, start_time FROM calendar_events WHERE start_time > NOW() AND start_time < NOW() + INTERVAL '12 hours' ORDER BY start_time LIMIT 3`
    )

    // Get daily digest (for evening)
    let digestSummary = ''
    if (!isMorning) {
      const digest = await pool.query(
        `SELECT raw_content FROM observations WHERE source = 'daily_digest' ORDER BY timestamp DESC LIMIT 1`
      )
      digestSummary = digest.rows[0]?.raw_content?.slice(0, 500) || ''
    }

    const briefingContext = [
      profile ? `PROFILE:\n${(typeof profile === 'string' ? profile : '').slice(0, 800)}` : '',
      predictedActions.length ? `PRIORITIES:\n${predictedActions.map((a: any) => `- [${a.urgency}] ${a.title}: ${a.description}`).join('\n')}` : '',
      events.rows.length ? `UPCOMING:\n${events.rows.map((e: any) => `- ${e.summary} at ${e.start_time}`).join('\n')}` : '',
      recentBreaks.length ? `ROUTINE BREAKS:\n${recentBreaks.map((b: any) => `- ${b.description} (${b.severity})`).join('\n')}` : '',
      lifeEvents.length ? `LIFE EVENT: ${lifeEvents[0].summary}` : '',
      !isMorning && digestSummary ? `TODAY'S ACTIVITY:\n${digestSummary}` : ''
    ].filter(Boolean).join('\n\n')

    const briefingPrompt = isMorning
      ? `Generate a 15-second morning briefing (about 40-50 words). Include:
- Top 1-2 priorities for today
- Any critical calendar events in the next few hours
- Any notable routine breaks
- Optional: a life event if one happened overnight
Deliver as natural sentences, like a personal assistant briefing you. Conversational but efficient. No lists, no bullet points.`
      : `Generate a 20-second evening briefing (about 55-65 words). Include:
- What was accomplished today
- What didn't get done that should have
- One sentence on tomorrow's outlook
- Optional: a pattern observation
Deliver as natural sentences, like a personal assistant wrapping up the day. Reflective but forward-looking. No lists, no bullet points.`

    // Stream Claude → Cartesia WS → glasses for low-latency briefing
    const ttsStream = cartesiaKey ? createCartesiaStream(cartesiaKey, ws) : null
    let fullText = ''

    try {
      if (ttsStream) await ttsStream.ready

      const stream = client.messages.stream({
        model: settings.model || 'claude-sonnet-4-20250514',
        max_tokens: 100,
        system: `${GLASSES_IRIS_RULES}\n\n${GLASSES_BRIEFING_RULES}\n\n${briefingContext}`,
        messages: [{ role: 'user', content: briefingPrompt }]
      })

      let textBuffer = ''
      const sentenceBoundary = /[.!?](?:\s|$)/

      for await (const event of stream) {
        if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
          fullText += event.delta.text
          textBuffer += event.delta.text

          if (ttsStream && (sentenceBoundary.test(textBuffer) || textBuffer.length >= 80)) {
            const clean = sanitizeForTTS(textBuffer)
            if (clean) ttsStream.sendChunk(clean)
            textBuffer = ''
          }
        }
      }

      if (ttsStream && textBuffer.trim()) {
        const clean = sanitizeForTTS(textBuffer)
        if (clean) ttsStream.sendChunk(clean)
      }
    } catch (err: any) {
      console.error(`[glasses] Briefing stream error: ${err.message} — trying fallback`)
      try {
        const fallbackResponse = await claudeWithFallback(client, {
          model: settings.model || 'claude-sonnet-4-20250514',
          max_tokens: 100,
          system: `${GLASSES_IRIS_RULES}\n\n${GLASSES_BRIEFING_RULES}\n\n${briefingContext}`,
          messages: [{ role: 'user', content: briefingPrompt }]
        }, 'glasses-briefing')
        fullText = fallbackResponse.content?.[0]?.type === 'text' ? fallbackResponse.content[0].text ?? '' : ''
        if (ttsStream && fullText) {
          const clean = sanitizeForTTS(fullText)
          if (clean) ttsStream.sendChunk(clean)
        }
      } catch (fallbackErr: any) {
        console.error(`[glasses] Briefing fallback failed: ${fallbackErr.message}`)
      }
    }

    const cleanFullText = sanitizeForTTS(fullText)
    console.log(`[glasses] briefing (${isMorning ? 'morning' : 'evening'}): ${cleanFullText.slice(0, 100)}...`)

    // Send text response
    ws.send(JSON.stringify({ type: 'response', mode: 'briefing', text: cleanFullText }))

    // Wait for TTS to finish
    if (ttsStream) {
      await ttsStream.finish()
    }
  }

  // ── Glasses helpers: ambient audio processing ──────────────────

  const ambientBuffers = new Map<WebSocket, { chunks: Buffer[], lastProcess: number, enabled: boolean, processing: boolean }>()

  async function processAmbientAudio(ws: WebSocket, pcmChunk: Buffer): Promise<void> {
    let state = ambientBuffers.get(ws)
    if (!state) {
      state = { chunks: [], lastProcess: Date.now(), enabled: true, processing: false }
      ambientBuffers.set(ws, state)
    }
    if (!state.enabled || state.processing) return

    state.chunks.push(pcmChunk)

    // Process every 30 seconds
    const elapsed = Date.now() - state.lastProcess
    if (elapsed < 30000) return

    const settings = await getSettings()
    const groqKey = settings.groq_api_key || ''
    if (!groqKey) return

    // Combine chunks and transcribe — lock to prevent race
    state.processing = true
    const combined = Buffer.concat(state.chunks)
    state.chunks = []
    state.lastProcess = Date.now()

    // Skip if too short (< 1 second of audio)
    if (combined.length < 32000) return

    try {
      const transcript = await transcribeAudio(groqKey, combined)
      if (!transcript || transcript.trim().length < 5) return

      console.log(`[glasses-ambient] transcribed ${combined.length} bytes: "${transcript.slice(0, 100)}"`)

      // Store as observation tagged with glasses_ambient source
      await insertObservation(
        'glasses_ambient',
        'ambient_speech',
        transcript,
        {},
        'neutral',
        [],
        ''
      )

      // Check for commitments or actionable items
      const commitmentPatterns = /\b(remember|remind me|don't forget|need to|have to|should|must|gotta|gonna)\b/i
      if (commitmentPatterns.test(transcript)) {
        await insertObservation(
          'glasses_ambient',
          'commitment_detected',
          transcript,
          { source: 'spoken_aloud' },
          'neutral',
          [],
          'Spoken commitment detected via glasses ambient listening'
        )
        console.log(`[glasses-ambient] commitment detected: "${transcript.slice(0, 80)}"`)
      }
    } catch (err: any) {
      console.error(`[glasses-ambient] transcription error:`, err)
    } finally {
      state.processing = false
    }
  }

  // ── WebSocket connection handler ───────────────────────────────

  wss.on('connection', (ws: WebSocket) => {
    connectedDevices++
    console.log(`[ws] Device connected (${connectedDevices} total)`)
    ws.send(JSON.stringify({ type: 'ready', version: '0.2.0-cloud-voice' }))

    ws.on('close', () => {
      connectedDevices--
      ambientBuffers.delete(ws)
      console.log(`[ws] Device disconnected (${connectedDevices} total)`)
    })

    ws.on('message', async (data: Buffer) => {
      if (!Buffer.isBuffer(data) || data.length < 1) return
      const op = data[0]
      const payload = data.length > 1 ? data.subarray(1) : Buffer.alloc(0)

      try {
        switch (op) {
          case 0x02: {
            // Voice query — touch-initiated recording (PCM audio)
            console.log(`[glasses] voice query: ${payload.length} bytes audio`)
            await handleGlassesVoiceQuery(ws, payload)
            break
          }

          case 0x03: {
            // Ambient listening — passive background audio chunk
            await processAmbientAudio(ws, payload)
            break
          }

          case 0x04: {
            if (payload.length === 0) {
              // Briefing request (empty payload)
              console.log(`[glasses] briefing request`)
              await handleGlassesBriefing(ws)
            } else {
              // Follow-up voice in conversation window (has audio payload)
              console.log(`[glasses] follow-up voice: ${payload.length} bytes`)
              await handleGlassesVoiceQuery(ws, payload)
            }
            break
          }

          case 0x05: {
            // Paired image + audio — extract audio, skip image for Phase 2
            if (payload.length < 5) break
            const imgLen5 = (payload[0] << 24) | (payload[1] << 16) | (payload[2] << 8) | payload[3]
            if (imgLen5 > payload.length - 4) {
              console.log(`[glasses] invalid image length: ${imgLen5} > ${payload.length - 4}`)
              break
            }
            const audioStart5 = 4 + imgLen5
            if (audioStart5 < payload.length) {
              const audioPcm = payload.subarray(audioStart5)
              console.log(`[glasses] paired msg — skipping ${imgLen5}B image, processing ${audioPcm.length}B audio`)
              await handleGlassesVoiceQuery(ws, audioPcm)
            }
            break
          }

          case 0x08: {
            // Passive image + audio — extract audio, skip image for Phase 2
            if (payload.length < 5) break
            const imgLen8 = (payload[0] << 24) | (payload[1] << 16) | (payload[2] << 8) | payload[3]
            if (imgLen8 > payload.length - 4) {
              console.log(`[glasses] invalid image length: ${imgLen8} > ${payload.length - 4}`)
              break
            }
            const audioStart8 = 4 + imgLen8
            if (audioStart8 < payload.length) {
              const audioPcm = payload.subarray(audioStart8)
              console.log(`[glasses] passive paired — skipping image, processing ${audioPcm.length}B audio`)
              await processAmbientAudio(ws, audioPcm)
            }
            break
          }

          case 0x01:
          case 0x09: {
            // JPEG frame / scene snapshot — skip for Phase 2
            console.log(`[glasses] image received (${payload.length}B) — skipped (Phase 2 voice-only)`)
            break
          }

          default:
            console.log(`[glasses] unknown op: 0x${op.toString(16)} (${payload.length} bytes)`)
        }
      } catch (err: any) {
        console.error(`[glasses] error handling op 0x${op.toString(16)}: ${err.message}`)
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
      version: '0.2.0-cloud-voice',
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

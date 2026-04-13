/**
 * Claude API fallback chain.
 *
 * Automatically retries on 529 (overloaded) and 503 (service unavailable)
 * errors using a model fallback chain:
 *   Primary:    claude-sonnet-4-20250514
 *   Fallback 1: claude-3-5-sonnet-20241022
 *   Fallback 2: claude-haiku-4-5
 *
 * After 5 minutes on a fallback, automatically retries primary.
 * Logs every fallback with timestamp, query type, and error.
 */

import Anthropic from '@anthropic-ai/sdk'

const MODEL_CHAIN = [
  'claude-sonnet-4-20250514',
  'claude-3-5-sonnet-20241022',
  'claude-haiku-4-5'
] as const

// ── Fallback state tracking ────────────────────────────────────

interface FallbackEvent {
  timestamp: number
  fromModel: string
  toModel: string
  error: string
  queryType: string
}

let currentModelIndex = 0
let lastFallbackTime = 0
let fallbackLog: FallbackEvent[] = []

const PRIMARY_RETRY_INTERVAL = 5 * 60 * 1000 // 5 minutes

function pruneOldEvents(): void {
  const oneHourAgo = Date.now() - 60 * 60 * 1000
  fallbackLog = fallbackLog.filter(e => e.timestamp > oneHourAgo)
}

function getFallbackRate(): number {
  pruneOldEvents()
  if (fallbackLog.length === 0) return 0
  return fallbackLog.length // count of fallback events in last hour
}

/** Check if we should try primary again (5 min cooldown expired) */
function shouldRetryPrimary(): boolean {
  if (currentModelIndex === 0) return false
  return Date.now() - lastFallbackTime >= PRIMARY_RETRY_INTERVAL
}

function getEffectiveModel(requestedModel?: string): string {
  // If a specific non-default model was requested, use it directly (no fallback)
  if (requestedModel && !MODEL_CHAIN.includes(requestedModel as any)) {
    return requestedModel
  }

  // If cooldown expired, try primary again
  if (shouldRetryPrimary()) {
    console.log(`[fallback] 5-minute cooldown expired — retrying primary model (${MODEL_CHAIN[0]})`)
    currentModelIndex = 0
  }

  return MODEL_CHAIN[currentModelIndex]
}

function isRetryableError(err: any): boolean {
  const status = err?.status ?? err?.statusCode ?? err?.error?.status
  return status === 529 || status === 503
}

// ── Main wrapper ───────────────────────────────────────────────

type NonStreamingParams = Anthropic.MessageCreateParamsNonStreaming

export async function claudeWithFallback(
  client: Anthropic,
  params: NonStreamingParams,
  queryType: string = 'unknown'
): Promise<Anthropic.Message> {
  const requestedModel = params.model as string
  const usesFallbackChain = !requestedModel || MODEL_CHAIN.includes(requestedModel as any)

  if (!usesFallbackChain) {
    return client.messages.create(params) as Promise<Anthropic.Message>
  }

  let startIndex = currentModelIndex
  if (shouldRetryPrimary()) {
    startIndex = 0
  }

  for (let i = startIndex; i < MODEL_CHAIN.length; i++) {
    const model = MODEL_CHAIN[i]
    try {
      const result = await client.messages.create({ ...params, model }) as Anthropic.Message

      if (i === 0 && currentModelIndex > 0) {
        console.log(`[fallback] Primary model recovered — switching back to ${MODEL_CHAIN[0]}`)
        currentModelIndex = 0
      }

      return result
    } catch (err: any) {
      if (!isRetryableError(err) || i === MODEL_CHAIN.length - 1) {
        // Non-retryable error or last model in chain — throw
        throw err
      }

      const nextModel = MODEL_CHAIN[i + 1]
      const errorMsg = `${err?.status || 'unknown'}: ${err?.message?.slice(0, 100) || 'unknown error'}`
      console.warn(`[fallback] ${model} returned ${errorMsg} — falling back to ${nextModel} [${queryType}]`)

      fallbackLog.push({
        timestamp: Date.now(),
        fromModel: model,
        toModel: nextModel,
        error: errorMsg,
        queryType
      })

      currentModelIndex = i + 1
      lastFallbackTime = Date.now()

      // Check degradation threshold
      const rate = getFallbackRate()
      if (rate > 0 && rate % 5 === 0) {
        console.warn(`[fallback] ⚠ ${rate} fallback events in the last hour — Claude API may be degraded`)
      }
    }
  }

  // Should never reach here, but TypeScript needs it
  throw new Error('All models in fallback chain exhausted')
}

// ── Streaming variant ──────────────────────────────────────────

export async function claudeStreamWithFallback(
  client: Anthropic,
  params: NonStreamingParams,
  queryType: string = 'unknown'
): Promise<any> {
  const requestedModel = params.model as string
  const usesFallbackChain = !requestedModel || MODEL_CHAIN.includes(requestedModel as any)

  if (!usesFallbackChain) {
    return client.messages.stream(params)
  }

  let startIndex = currentModelIndex
  if (shouldRetryPrimary()) {
    startIndex = 0
  }

  for (let i = startIndex; i < MODEL_CHAIN.length; i++) {
    const model = MODEL_CHAIN[i]
    try {
      const stream = client.messages.stream({ ...params, model })

      if (i === 0 && currentModelIndex > 0) {
        console.log(`[fallback] Primary model recovered — switching back to ${MODEL_CHAIN[0]}`)
        currentModelIndex = 0
      }

      return stream
    } catch (err: any) {
      if (!isRetryableError(err) || i === MODEL_CHAIN.length - 1) {
        throw err
      }

      const nextModel = MODEL_CHAIN[i + 1]
      const errorMsg = `${err?.status || 'unknown'}: ${err?.message?.slice(0, 100) || 'unknown error'}`
      console.warn(`[fallback] stream: ${model} returned ${errorMsg} — falling back to ${nextModel} [${queryType}]`)

      fallbackLog.push({
        timestamp: Date.now(),
        fromModel: model,
        toModel: nextModel,
        error: errorMsg,
        queryType
      })

      currentModelIndex = i + 1
      lastFallbackTime = Date.now()
    }
  }

  throw new Error('All models in fallback chain exhausted (stream)')
}

// ── Status helpers ─────────────────────────────────────────────

export function getFallbackStatus(): {
  currentModel: string
  fallbackActive: boolean
  eventsLastHour: number
  degraded: boolean
} {
  pruneOldEvents()
  return {
    currentModel: MODEL_CHAIN[currentModelIndex],
    fallbackActive: currentModelIndex > 0,
    eventsLastHour: fallbackLog.length,
    degraded: fallbackLog.length >= 5
  }
}

/** Get whisper text if API is degraded, or null if healthy */
export function getDegradationWhisper(): string | null {
  const status = getFallbackStatus()
  if (status.degraded) {
    return `Claude API degraded — running on fallback model (${status.currentModel})`
  }
  return null
}

/**
 * Trend Detector — identifies behavioral changes over time.
 *
 * Compares 30-day rolling baseline against last-7-day values for
 * tracked dimensions. Emits trend insights when delta exceeds 30%.
 *
 * Dimensions tracked:
 * - Contact response times (via stale thread days)
 * - Topic frequencies (from daily digests)
 * - Activity volume per source (observations per day)
 * - Centrum task completion rate
 * - Email output volume (sent count)
 * - WhatsApp activity levels
 *
 * Runs after daily digest generation. Trends feed into the persona
 * writer's "current state" section.
 */

import { getPool } from './db'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface TrendDimension {
  name: string
  baseline30d: number
  current7d: number
  delta: number    // percent change: (current - baseline) / baseline * 100
  direction: 'up' | 'down' | 'stable'
  description: string
}

interface TrendReport {
  timestamp: string
  dimensions: TrendDimension[]
  significantTrends: TrendDimension[]  // |delta| > 30%
  summary: string
}

// ── Main: detect trends ───────────────────────────────────────────

export async function detectTrends(): Promise<TrendReport> {
  const pool = getPool()
  const now = new Date()
  const day7ago = new Date(now.getTime() - 7 * 86400000).toISOString()
  const day30ago = new Date(now.getTime() - 30 * 86400000).toISOString()

  const dimensions: TrendDimension[] = []

  // ── 1. Activity volume per source ───────────────────────────────

  const sources = ['browser', 'whatsapp', 'email', 'screen_capture', 'app_activity', 'clipboard', 'chat', 'centrum']

  for (const source of sources) {
    const baseline = await pool.query(
      "SELECT COUNT(*)::float / GREATEST(1, EXTRACT(DAY FROM NOW() - $2::timestamptz))::float as daily_avg FROM observations WHERE source=$1 AND timestamp >= $2",
      [source, day30ago]
    )
    const recent = await pool.query(
      "SELECT COUNT(*)::float / 7.0 as daily_avg FROM observations WHERE source=$1 AND timestamp >= $2",
      [source, day7ago]
    )

    const base = baseline.rows[0]?.daily_avg || 0
    const curr = recent.rows[0]?.daily_avg || 0

    if (base > 1) { // Only track sources with meaningful volume
      const delta = base > 0 ? ((curr - base) / base) * 100 : 0
      dimensions.push({
        name: `${source}_volume`,
        baseline30d: Math.round(base * 10) / 10,
        current7d: Math.round(curr * 10) / 10,
        delta: Math.round(delta),
        direction: delta > 10 ? 'up' : delta < -10 ? 'down' : 'stable',
        description: `${source} activity: ${Math.round(curr)}/day (was ${Math.round(base)}/day)`
      })
    }
  }

  // ── 2. Email sent volume ────────────────────────────────────────

  const emailBase = await pool.query(
    "SELECT COUNT(*)::float / GREATEST(1, EXTRACT(DAY FROM NOW() - $1::timestamptz))::float as daily_avg FROM observations WHERE source='email' AND event_type='sent' AND timestamp >= $1",
    [day30ago]
  )
  const emailRecent = await pool.query(
    "SELECT COUNT(*)::float / 7.0 as daily_avg FROM observations WHERE source='email' AND event_type='sent' AND timestamp >= $1",
    [day7ago]
  )

  const emailBaseline = emailBase.rows[0]?.daily_avg || 0
  const emailCurrent = emailRecent.rows[0]?.daily_avg || 0
  if (emailBaseline > 0.1) {
    const emailDelta = ((emailCurrent - emailBaseline) / emailBaseline) * 100
    dimensions.push({
      name: 'email_sent_volume',
      baseline30d: Math.round(emailBaseline * 10) / 10,
      current7d: Math.round(emailCurrent * 10) / 10,
      delta: Math.round(emailDelta),
      direction: emailDelta > 10 ? 'up' : emailDelta < -10 ? 'down' : 'stable',
      description: `Emails sent: ${Math.round(emailCurrent * 7)} this week (avg was ${Math.round(emailBaseline * 7)}/week)`
    })
  }

  // ── 3. Contact response staleness ───────────────────────────────

  const staleThreads = await pool.query(
    "SELECT AVG(stale_days)::float as avg_stale, MAX(stale_days)::int as max_stale, COUNT(*)::int as total FROM email_threads WHERE stale_days > 0"
  )
  const avgStale = staleThreads.rows[0]?.avg_stale || 0
  const maxStale = staleThreads.rows[0]?.max_stale || 0
  const totalStale = staleThreads.rows[0]?.total || 0

  if (totalStale > 0) {
    // Compare against a reasonable baseline (7 days is "normal" response time)
    const staleDelta = ((avgStale - 7) / 7) * 100
    dimensions.push({
      name: 'response_staleness',
      baseline30d: 7,
      current7d: Math.round(avgStale * 10) / 10,
      delta: Math.round(staleDelta),
      direction: staleDelta > 30 ? 'up' : staleDelta < -30 ? 'down' : 'stable',
      description: `Average email response time: ${Math.round(avgStale)} days (${totalStale} stale threads, worst: ${maxStale} days)`
    })
  }

  // ── 4. Centrum task completion rate ─────────────────────────────

  const centrumSnap = await pool.query(
    "SELECT board_state FROM centrum_snapshots ORDER BY timestamp DESC LIMIT 1"
  )
  if (centrumSnap.rows.length > 0) {
    const board = centrumSnap.rows[0].board_state
    const tasks = (board.cards || []).filter((c: any) => c.type === 'task')
    const totalSubtasks = tasks.reduce((s: number, t: any) =>
      s + (Array.isArray(t.content) ? t.content.length : 0), 0)
    const doneSubtasks = tasks.reduce((s: number, t: any) =>
      s + (Array.isArray(t.content) ? t.content.filter((st: any) => st.done).length : 0), 0)

    if (totalSubtasks > 0) {
      const completionRate = (doneSubtasks / totalSubtasks) * 100
      dimensions.push({
        name: 'centrum_completion',
        baseline30d: 50, // Assume 50% as a reasonable baseline
        current7d: Math.round(completionRate),
        delta: Math.round(completionRate - 50),
        direction: completionRate > 60 ? 'up' : completionRate < 40 ? 'down' : 'stable',
        description: `Centrum task completion: ${doneSubtasks}/${totalSubtasks} subtasks done (${Math.round(completionRate)}%)`
      })
    }

    // Today list tracking
    const todayTotal = (board.todayIds || []).length
    const todayDone = (board.todayDone || []).length
    if (todayTotal > 0) {
      const todayRate = (todayDone / todayTotal) * 100
      dimensions.push({
        name: 'centrum_today_completion',
        baseline30d: 50,
        current7d: Math.round(todayRate),
        delta: Math.round(todayRate - 50),
        direction: todayRate > 60 ? 'up' : todayRate < 40 ? 'down' : 'stable',
        description: `Centrum Today list: ${todayDone}/${todayTotal} items done (${Math.round(todayRate)}%)`
      })
    }
  }

  // ── 5. WhatsApp engagement ──────────────────────────────────────

  const waBase = await pool.query(
    "SELECT COUNT(*)::float / GREATEST(1, EXTRACT(DAY FROM NOW() - $1::timestamptz))::float as daily_avg FROM observations WHERE source='whatsapp' AND event_type='sent' AND timestamp >= $1",
    [day30ago]
  )
  const waRecent = await pool.query(
    "SELECT COUNT(*)::float / 7.0 as daily_avg FROM observations WHERE source='whatsapp' AND event_type='sent' AND timestamp >= $1",
    [day7ago]
  )

  const waBaseline = waBase.rows[0]?.daily_avg || 0
  const waCurrent = waRecent.rows[0]?.daily_avg || 0
  if (waBaseline > 0.5) {
    const waDelta = ((waCurrent - waBaseline) / waBaseline) * 100
    dimensions.push({
      name: 'whatsapp_sent',
      baseline30d: Math.round(waBaseline * 10) / 10,
      current7d: Math.round(waCurrent * 10) / 10,
      delta: Math.round(waDelta),
      direction: waDelta > 10 ? 'up' : waDelta < -10 ? 'down' : 'stable',
      description: `WhatsApp messages sent: ${Math.round(waCurrent)}/day (was ${Math.round(waBaseline)}/day)`
    })
  }

  // ── 6. IRIS usage ───────────────────────────────────────────────

  const chatBase = await pool.query(
    "SELECT COUNT(*)::float / GREATEST(1, EXTRACT(DAY FROM NOW() - $1::timestamptz))::float as daily_avg FROM observations WHERE source='chat' AND timestamp >= $1",
    [day30ago]
  )
  const chatRecent = await pool.query(
    "SELECT COUNT(*)::float / 7.0 as daily_avg FROM observations WHERE source='chat' AND timestamp >= $1",
    [day7ago]
  )

  const chatBaseline = chatBase.rows[0]?.daily_avg || 0
  const chatCurrent = chatRecent.rows[0]?.daily_avg || 0
  if (chatBaseline > 0) {
    const chatDelta = chatBaseline > 0 ? ((chatCurrent - chatBaseline) / chatBaseline) * 100 : 0
    dimensions.push({
      name: 'iris_usage',
      baseline30d: Math.round(chatBaseline * 10) / 10,
      current7d: Math.round(chatCurrent * 10) / 10,
      delta: Math.round(chatDelta),
      direction: chatDelta > 10 ? 'up' : chatDelta < -10 ? 'down' : 'stable',
      description: `IRIS queries: ${Math.round(chatCurrent)}/day (was ${Math.round(chatBaseline)}/day)`
    })
  }

  // ── Find significant trends (|delta| > 30%) ────────────────────

  const significantTrends = dimensions.filter(d => Math.abs(d.delta) >= 30)

  // ── Store as insights ──────────────────────────────────────────

  for (const trend of significantTrends) {
    const direction = trend.direction === 'up' ? '↑' : '↓'
    const statement = `TREND: ${trend.description} (${direction} ${Math.abs(trend.delta)}% vs 30-day average)`
    const id = `trend-${trend.name}-${new Date().toISOString().slice(0, 10)}`

    await pool.query(
      `INSERT INTO insights (id, statement, evidence, confidence, category, status, source_count)
       VALUES ($1, $2, '[]', $3, 'behavioral', 'active', 1)
       ON CONFLICT (id) DO UPDATE SET statement=$2, confidence=$3, last_confirmed=NOW()`,
      [id, statement, Math.min(0.9, 0.5 + Math.abs(trend.delta) / 200)]
    )
  }

  // ── Build summary ──────────────────────────────────────────────

  let summary = ''
  if (significantTrends.length === 0) {
    summary = 'No significant behavioral changes detected (all dimensions within 30% of baseline).'
  } else {
    const trendDescriptions = significantTrends.map(t => {
      const arrow = t.direction === 'up' ? '↑' : '↓'
      return `${arrow} ${t.description}`
    })
    summary = `${significantTrends.length} significant trend(s) detected:\n${trendDescriptions.join('\n')}`
  }

  // Store trend report as observation
  const reportId = crypto.randomUUID()
  await pool.query(
    `INSERT INTO observations (id, timestamp, source, event_type, raw_content, extracted_entities, emotional_valence, related_observations, persona_impact)
     VALUES ($1, NOW(), 'trend_detector', 'analysis', $2, $3, 'neutral', '[]', '')`,
    [reportId, summary, JSON.stringify({
      dimensions: dimensions.map(d => ({ name: d.name, delta: d.delta, direction: d.direction })),
      significantCount: significantTrends.length
    })]
  )

  console.log(`[trends] Analysis complete: ${dimensions.length} dimensions, ${significantTrends.length} significant changes`)

  return {
    timestamp: now.toISOString(),
    dimensions,
    significantTrends,
    summary
  }
}

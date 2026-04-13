/**
 * Skill Model — behavior-based skill assessment.
 *
 * Analyzes observations to determine what Kavin is actually good at
 * vs where he struggles — based on what he DOES, not what he says.
 *
 * Skill categories:
 * - Technical: coding, tool proficiency, system building
 * - Communication: email quality, reply patterns, social engagement
 * - Academic: IB subject work, research, writing
 * - Project management: follow-through, deadline adherence, task completion
 * - Social: relationship maintenance, network building, responsiveness
 * - Creative: content creation, design work, media production
 *
 * Runs weekly. Each skill gets a 0-100 score, trajectory, and evidence.
 */

import { getPool, getSettings } from './db'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface SkillAssessment {
  name: string
  category: string
  proficiency: number
  trajectory: 'improving' | 'stable' | 'declining'
  evidenceCount: number
  examples: string[]
}

// ── Main: extract skills from observations ────────────────────────

export async function extractSkills(): Promise<number> {
  const pool = getPool()
  const day60ago = new Date(Date.now() - 60 * 86400000).toISOString()
  const day30ago = new Date(Date.now() - 30 * 86400000).toISOString()

  console.log('[skills] Extracting skill model from observations...')

  const skills: SkillAssessment[] = []

  // ── 1. Technical: Coding & system building ──────────────────────

  const codeApps = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'app_activity' AND timestamp >= $1
     AND (raw_content ILIKE '%Terminal%' OR raw_content ILIKE '%Visual Studio%'
       OR raw_content ILIKE '%Xcode%' OR raw_content ILIKE '%Cursor%'
       OR raw_content ILIKE '%WebStorm%' OR raw_content ILIKE '%IntelliJ%')`,
    [day60ago]
  )
  const codeSessions = codeApps.rows[0]?.c || 0

  const codeFiles = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'file' AND timestamp >= $1
     AND (raw_content ILIKE '%.ts%' OR raw_content ILIKE '%.js%'
       OR raw_content ILIKE '%.py%' OR raw_content ILIKE '%.swift%'
       OR raw_content ILIKE '%.tsx%' OR raw_content ILIKE '%.jsx%')`,
    [day60ago]
  )
  const codeFileCount = codeFiles.rows[0]?.c || 0

  const codeBrowsing = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'browser' AND timestamp >= $1
     AND (raw_content ILIKE '%github.com%' OR raw_content ILIKE '%stackoverflow%'
       OR raw_content ILIKE '%docs.google%' OR raw_content ILIKE '%developer%'
       OR raw_content ILIKE '%npmjs.com%' OR raw_content ILIKE '%claude.ai%')`,
    [day60ago]
  )
  const codeResearch = codeBrowsing.rows[0]?.c || 0

  // Screen captures showing code
  const codeScreen = await pool.query(
    `SELECT raw_content FROM observations
     WHERE source = 'screen_capture' AND timestamp >= $1
     AND (raw_content ILIKE '%function%' OR raw_content ILIKE '%const %'
       OR raw_content ILIKE '%import %' OR raw_content ILIKE '%export %')
     LIMIT 5`,
    [day60ago]
  )

  const totalCodeSignals = codeSessions + codeFileCount + Math.min(codeResearch, 100)
  if (totalCodeSignals > 0) {
    const proficiency = Math.min(95, 30 + Math.log2(totalCodeSignals + 1) * 10)

    // Recent vs older to compute trajectory
    const recentCode = await pool.query(
      `SELECT COUNT(*)::int as c FROM observations
       WHERE source = 'app_activity' AND timestamp >= $1
       AND (raw_content ILIKE '%Terminal%' OR raw_content ILIKE '%Visual Studio%'
         OR raw_content ILIKE '%Cursor%')`,
      [day30ago]
    )
    const recentRatio = codeSessions > 0 ? (recentCode.rows[0]?.c || 0) / codeSessions : 0
    const trajectory = recentRatio > 0.6 ? 'improving' : recentRatio < 0.3 ? 'declining' : 'stable'

    const examples = [
      `${codeSessions} coding sessions across Terminal, VS Code, Cursor in 60 days`,
      `${codeFileCount} code files created/modified (.ts, .js, .py, .swift)`,
      `${codeResearch} developer-related browser visits (GitHub, StackOverflow, Claude)`
    ]
    if (codeScreen.rows.length > 0) {
      const snippet = (codeScreen.rows[0].raw_content || '').slice(0, 100)
      examples.push(`Screen capture showing active coding: "${snippet.replace(/\n/g, ' ').trim()}"`)
    }

    skills.push({
      name: 'Software Development',
      category: 'technical',
      proficiency: Math.round(proficiency),
      trajectory: trajectory as any,
      evidenceCount: totalCodeSignals,
      examples: examples.slice(0, 3)
    })
  }

  // ── 2. Communication: Email quality & responsiveness ────────────

  const emailSent = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'email' AND event_type = 'sent' AND timestamp >= $1`,
    [day60ago]
  )
  const sentCount = emailSent.rows[0]?.c || 0

  const emailReceived = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'email' AND event_type = 'received' AND timestamp >= $1`,
    [day60ago]
  )
  const receivedCount = emailReceived.rows[0]?.c || 0

  const staleThreads = await pool.query(
    "SELECT AVG(stale_days)::float as avg, COUNT(*)::int as c FROM email_threads WHERE stale_days > 0"
  )
  const avgStale = staleThreads.rows[0]?.avg || 0
  const staleCount = staleThreads.rows[0]?.c || 0

  if (sentCount + receivedCount > 0) {
    // Response rate: lower stale = better communication
    const responseScore = Math.max(20, 80 - avgStale * 3)
    // Volume: more sent = more active communicator
    const volumeScore = Math.min(30, sentCount * 0.3)
    const proficiency = Math.min(95, responseScore * 0.6 + volumeScore + 10)

    skills.push({
      name: 'Email Communication',
      category: 'communication',
      proficiency: Math.round(proficiency),
      trajectory: avgStale > 10 ? 'declining' : 'stable',
      evidenceCount: sentCount + receivedCount,
      examples: [
        `${sentCount} emails sent, ${receivedCount} received in 60 days`,
        `Average response delay on stale threads: ${Math.round(avgStale)} days (${staleCount} threads pending)`,
        avgStale > 7 ? `Response time is above average — ${staleCount} threads awaiting follow-up` : `Maintaining reasonable response times`
      ]
    })
  }

  // ── 3. Social engagement: WhatsApp patterns ─────────────────────

  const waSent = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'whatsapp' AND event_type = 'sent' AND timestamp >= $1`,
    [day60ago]
  )
  const waViewed = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'whatsapp' AND event_type = 'viewed' AND timestamp >= $1`,
    [day60ago]
  )
  const waSentCount = waSent.rows[0]?.c || 0
  const waViewedCount = waViewed.rows[0]?.c || 0

  // Get unique contacts from WhatsApp
  const waContacts = await pool.query(
    `SELECT COUNT(DISTINCT SUBSTRING(raw_content FROM 'To ([^:]+):'))::int as c
     FROM observations WHERE source = 'whatsapp' AND event_type = 'sent' AND timestamp >= $1`,
    [day60ago]
  )
  const uniqueWaContacts = waContacts.rows[0]?.c || 0

  if (waSentCount > 0) {
    const engagementScore = Math.min(90, 20 + Math.log2(waSentCount + 1) * 8 + uniqueWaContacts * 2)

    skills.push({
      name: 'Social Engagement',
      category: 'social',
      proficiency: Math.round(engagementScore),
      trajectory: 'stable',
      evidenceCount: waSentCount + waViewedCount,
      examples: [
        `${waSentCount} WhatsApp messages sent to ${uniqueWaContacts} contacts in 60 days`,
        `${waViewedCount} conversation check-ins (active monitoring)`,
        waSentCount > 100 ? 'Highly active social communicator' : 'Moderate social engagement'
      ]
    })
  }

  // ── 4. Academic: IB subject engagement ──────────────────────────

  const academicBrowsing = await pool.query(
    `SELECT raw_content FROM observations
     WHERE source = 'browser' AND timestamp >= $1
     AND (raw_content ILIKE '%essay%' OR raw_content ILIKE '%IA %'
       OR raw_content ILIKE '%IB %' OR raw_content ILIKE '%physics%'
       OR raw_content ILIKE '%economics%' OR raw_content ILIKE '%history%'
       OR raw_content ILIKE '%chemistry%' OR raw_content ILIKE '%math%'
       OR raw_content ILIKE '%TOK%' OR raw_content ILIKE '%CAS %'
       OR raw_content ILIKE '%extended essay%')
     LIMIT 200`,
    [day60ago]
  )

  const academicDocs = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'file' AND timestamp >= $1
     AND (raw_content ILIKE '%essay%' OR raw_content ILIKE '%IA%'
       OR raw_content ILIKE '%economics%' OR raw_content ILIKE '%chemistry%'
       OR raw_content ILIKE '%math%' OR raw_content ILIKE '%worksheet%'
       OR raw_content ILIKE '%exam%')`,
    [day60ago]
  )

  const academicCount = academicBrowsing.rows.length + (academicDocs.rows[0]?.c || 0)
  if (academicCount > 0) {
    // Detect which subjects appear most
    const subjectCounts: Record<string, number> = {}
    const subjects = ['physics', 'economics', 'history', 'chemistry', 'math', 'tok', 'cas', 'extended essay']

    for (const row of academicBrowsing.rows) {
      const content = (row.raw_content || '').toLowerCase()
      for (const subj of subjects) {
        if (content.includes(subj)) subjectCounts[subj] = (subjectCounts[subj] || 0) + 1
      }
    }

    const topSubjects = Object.entries(subjectCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)

    // Check for EE/IA activity specifically
    const eeActivity = subjectCounts['extended essay'] || 0
    const iaActivity = Object.entries(subjectCounts)
      .filter(([k]) => k !== 'extended essay' && k !== 'tok' && k !== 'cas')
      .reduce((s, [, c]) => s + c, 0)

    const proficiency = Math.min(85, 30 + Math.log2(academicCount + 1) * 8)

    skills.push({
      name: 'Academic Work (IB)',
      category: 'academic',
      proficiency: Math.round(proficiency),
      trajectory: eeActivity > 5 ? 'improving' : 'stable',
      evidenceCount: academicCount,
      examples: [
        `${academicCount} academic-related observations (browsing + documents)`,
        topSubjects.length > 0 ? `Most active subjects: ${topSubjects.map(([s, c]) => `${s} (${c})`).join(', ')}` : 'General academic activity',
        `${academicDocs.rows[0]?.c || 0} academic documents created/modified`
      ]
    })
  }

  // ── 5. Project management: Follow-through ───────────────────────

  const totalCommitments = await pool.query(
    "SELECT COUNT(*)::int as total, SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END)::int as completed, SUM(CASE WHEN status='active' THEN 1 ELSE 0 END)::int as active FROM commitments"
  )
  const total = totalCommitments.rows[0]?.total || 0
  const completed = totalCommitments.rows[0]?.completed || 0
  const active = totalCommitments.rows[0]?.active || 0
  const completionRate = total > 0 ? completed / total : 0

  // Centrum task completion
  const centrumSnap = await pool.query(
    "SELECT board_state FROM centrum_snapshots ORDER BY timestamp DESC LIMIT 1"
  )
  let centrumCompletion = 0
  let centrumTotal = 0
  if (centrumSnap.rows.length > 0) {
    const board = centrumSnap.rows[0].board_state
    const tasks = (board.cards || []).filter((c: any) => c.type === 'task' || c.type === 'note')
    centrumTotal = tasks.length
    // Check todayDone ratio
    const todayDone = (board.todayDone || []).length
    const todayTotal = (board.todayIds || []).length
    centrumCompletion = todayTotal > 0 ? todayDone / todayTotal : 0
  }

  if (total > 0 || centrumTotal > 0) {
    const followThrough = Math.round(
      (completionRate * 40) + // email commitments
      (centrumCompletion * 30) + // centrum today-list
      20 // base
    )

    skills.push({
      name: 'Follow-through',
      category: 'project_management',
      proficiency: Math.min(85, Math.max(15, followThrough)),
      trajectory: avgStale > 10 ? 'declining' : 'stable',
      evidenceCount: total + centrumTotal,
      examples: [
        `${completed}/${total} email commitments completed (${Math.round(completionRate * 100)}%)`,
        `${active} active commitments pending`,
        centrumTotal > 0 ? `Centrum: ${centrumTotal} tasks tracked, Today list completion: ${Math.round(centrumCompletion * 100)}%` : 'No Centrum data',
        `${staleCount} stale email threads averaging ${Math.round(avgStale)} days — ${avgStale > 7 ? 'needs improvement' : 'acceptable'}`
      ].slice(0, 3)
    })
  }

  // ── 6. Time management: Calendar adherence ──────────────────────

  const calEvents = await pool.query(
    `SELECT COUNT(*)::int as c FROM calendar_events WHERE status != 'cancelled'`
  )
  const eventCount = calEvents.rows[0]?.c || 0

  // Compare scheduled vs actual activity
  const centrumTodayHistory = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'centrum' AND raw_content ILIKE '%Today%' AND timestamp >= $1`,
    [day60ago]
  )

  if (eventCount > 0 || centrumTodayHistory.rows[0]?.c > 0) {
    const timeScore = Math.min(80, 30 + eventCount * 2 + (centrumTodayHistory.rows[0]?.c || 0) * 3)

    skills.push({
      name: 'Time Management',
      category: 'project_management',
      proficiency: Math.round(timeScore),
      trajectory: 'stable',
      evidenceCount: eventCount + (centrumTodayHistory.rows[0]?.c || 0),
      examples: [
        `${eventCount} calendar events scheduled`,
        `${centrumTodayHistory.rows[0]?.c || 0} Centrum "Today" list interactions`,
        avgStale > 10 ? 'Email response delays suggest time pressure' : 'Managing deadlines within normal bounds'
      ]
    })
  }

  // ── 7. Research & information gathering ─────────────────────────

  const researchSites = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'browser' AND timestamp >= $1
     AND (raw_content ILIKE '%scholar.google%' OR raw_content ILIKE '%jstor%'
       OR raw_content ILIKE '%wikipedia%' OR raw_content ILIKE '%arxiv%'
       OR raw_content ILIKE '%research%' OR raw_content ILIKE '%journal%'
       OR raw_content ILIKE '%pubmed%' OR raw_content ILIKE '%scite%')`,
    [day60ago]
  )

  const clipboardResearch = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'clipboard' AND timestamp >= $1
     AND LENGTH(raw_content) > 100`,
    [day60ago]
  )

  const researchCount = (researchSites.rows[0]?.c || 0) + (clipboardResearch.rows[0]?.c || 0)
  if (researchCount > 5) {
    skills.push({
      name: 'Research & Information Gathering',
      category: 'academic',
      proficiency: Math.min(85, 30 + Math.log2(researchCount + 1) * 12),
      trajectory: 'stable',
      evidenceCount: researchCount,
      examples: [
        `${researchSites.rows[0]?.c || 0} academic/research site visits`,
        `${clipboardResearch.rows[0]?.c || 0} substantial clipboard copies (research notes)`,
        `Active across Google Scholar, Wikipedia, Scite, and other research tools`
      ]
    })
  }

  // ── 8. Creative / Media production ──────────────────────────────

  const creativeApps = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source IN ('app_activity', 'screen_capture') AND timestamp >= $1
     AND (raw_content ILIKE '%Premiere%' OR raw_content ILIKE '%Final Cut%'
       OR raw_content ILIKE '%Photoshop%' OR raw_content ILIKE '%Figma%'
       OR raw_content ILIKE '%Canva%' OR raw_content ILIKE '%DaVinci%'
       OR raw_content ILIKE '%GarageBand%' OR raw_content ILIKE '%Logic%')`,
    [day60ago]
  )

  const creativeBrowsing = await pool.query(
    `SELECT COUNT(*)::int as c FROM observations
     WHERE source = 'browser' AND timestamp >= $1
     AND (raw_content ILIKE '%behance%' OR raw_content ILIKE '%dribbble%'
       OR raw_content ILIKE '%unsplash%' OR raw_content ILIKE '%pexels%'
       OR raw_content ILIKE '%vimeo%' OR raw_content ILIKE '%soundcloud%')`,
    [day60ago]
  )

  const creativeCount = (creativeApps.rows[0]?.c || 0) + (creativeBrowsing.rows[0]?.c || 0)
  if (creativeCount > 3) {
    skills.push({
      name: 'Creative & Media Production',
      category: 'creative',
      proficiency: Math.min(80, 25 + Math.log2(creativeCount + 1) * 10),
      trajectory: 'stable',
      evidenceCount: creativeCount,
      examples: [
        `${creativeApps.rows[0]?.c || 0} creative app sessions`,
        `${creativeBrowsing.rows[0]?.c || 0} creative resource visits`,
        `Active in media production tools`
      ]
    })
  }

  // ── 9. Networking / relationship building ───────────────────────

  const relationships = await pool.query(
    "SELECT COUNT(*)::int as total, SUM(CASE WHEN trajectory='rising' THEN 1 ELSE 0 END)::int as rising FROM relationships"
  )
  const relTotal = relationships.rows[0]?.total || 0
  const relRising = relationships.rows[0]?.rising || 0

  const contacts = await pool.query(
    "SELECT COUNT(*)::int as c FROM contacts WHERE emails_sent > 0"
  )
  const activeContacts = contacts.rows[0]?.c || 0

  if (activeContacts > 0) {
    const networkScore = Math.min(85, 20 + Math.log2(activeContacts + 1) * 10 + relRising * 5)

    skills.push({
      name: 'Network Building',
      category: 'social',
      proficiency: Math.round(networkScore),
      trajectory: relRising > 3 ? 'improving' : 'stable',
      evidenceCount: activeContacts,
      examples: [
        `${activeContacts} active email contacts (bidirectional)`,
        `${relTotal} tracked relationships, ${relRising} currently rising`,
        `Maintains cross-domain network: academic, professional, social, family`
      ]
    })
  }

  // ── Use Claude for nuanced assessment on top skills ─────────────

  const settings = await getSettings()
  const apiKey = settings.api_key || process.env.ANTHROPIC_API_KEY
  if (apiKey && skills.length > 0) {
    await enrichWithClaude(apiKey, settings.model, skills)
  }

  // ── Store skills ────────────────────────────────────────────────

  // Clear old skills
  await pool.query("DELETE FROM skills")

  let stored = 0
  for (const s of skills) {
    await pool.query(
      `INSERT INTO skills (id, skill_name, category, proficiency, trajectory, evidence_count, examples)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        `skill-${s.category}-${s.name.toLowerCase().replace(/\s+/g, '-').slice(0, 30)}`,
        s.name, s.category, s.proficiency, s.trajectory,
        s.evidenceCount, JSON.stringify(s.examples)
      ]
    )
    stored++
  }

  console.log(`[skills] Evaluated ${stored} skills`)
  return stored
}

// ── Claude enrichment ─────────────────────────────────────────────

async function enrichWithClaude(apiKey: string, model: string, skills: SkillAssessment[]): Promise<void> {
  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default
    const { claudeWithFallback } = await import('./claude-fallback')
    const client = new Anthropic({ apiKey })

    const skillDescriptions = skills.map((s, i) =>
      `[${i + 1}] ${s.name} (${s.category}): ${s.proficiency}/100, ${s.trajectory}\n  Evidence: ${s.examples.join('; ')}`
    ).join('\n\n')

    const response = await claudeWithFallback(client, {
      model: model || 'claude-sonnet-4-20250514',
      max_tokens: 1500,
      system: `You are assessing a student's real skills based on behavioral evidence. For each skill, provide a one-sentence honest assessment that acknowledges both strengths and weaknesses visible in the data.

Return JSON array: [{"index": 1, "assessment": "honest one-sentence evaluation", "adjusted_score": 75}]

Be brutally honest. If someone has 9 stale email threads averaging 10+ days, their communication follow-through is poor regardless of volume. If they're coding daily, that's a strong signal. Adjust scores up or down based on quality signals, not just quantity.`,
      messages: [{ role: 'user', content: skillDescriptions }]
    }, 'skill-extractor')

    const raw = response.content[0].type === 'text' ? response.content[0].text : '[]'
    try {
      const jsonMatch = raw.match(/\[[\s\S]*\]/)
      if (jsonMatch) {
        const assessments = JSON.parse(jsonMatch[0]) as { index: number; assessment: string; adjusted_score: number }[]
        for (const a of assessments) {
          const skill = skills[a.index - 1]
          if (skill) {
            if (a.assessment) skill.examples.push(`Claude assessment: ${a.assessment}`)
            if (a.adjusted_score) skill.proficiency = a.adjusted_score
          }
        }
      }
    } catch {}
  } catch (err: any) {
    console.error('[skills] Claude enrichment failed:', err.message)
  }
}

// ── Get skills for context injection ──────────────────────────────

export async function getSkills(): Promise<any[]> {
  const pool = getPool()
  const r = await pool.query('SELECT * FROM skills ORDER BY proficiency DESC')
  return r.rows
}

export function formatSkillsForContext(skills: any[]): string {
  if (skills.length === 0) return ''

  const lines = skills.map((s: any) => {
    const arrow = s.trajectory === 'improving' ? '↑' : s.trajectory === 'declining' ? '↓' : '→'
    const examples = Array.isArray(s.examples) ? s.examples :
      (typeof s.examples === 'string' ? JSON.parse(s.examples) : [])
    const evidence = examples.slice(0, 2).join('; ')
    return `- ${s.skill_name}: ${s.proficiency}/100 ${arrow} (${s.evidence_count} evidence points) — ${evidence.slice(0, 120)}`
  })

  return `SKILL MODEL (behavior-based assessment):\n${lines.join('\n')}`
}

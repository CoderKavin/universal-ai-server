/**
 * Daily & Weekly Digest Engine
 *
 * Aggregates raw observations into structured daily summaries that IRIS
 * can use to answer questions like "what did I do yesterday?" or
 * "how was my week?"
 *
 * Runs nightly at 00:15 IST. Generates weekly rollups on Sundays.
 * Can backfill retroactively for any date range.
 *
 * Output is stored as observations with source='daily_digest' or 'weekly_digest'.
 */

import { getPool } from './db'
import crypto from 'crypto'

// ── Types ─────────────────────────────────────────────────────────

interface SourceSummary {
  count: number
  eventTypes: Record<string, number>
  details: string[]
  people: string[]
  topics: string[]
}

interface DigestResult {
  date: string
  dayOfWeek: string
  totalObservations: number
  sources: Record<string, SourceSummary>
  allPeople: { name: string; mentions: number; contexts: string[] }[]
  allTopics: string[]
  highlights: string[]
  readableSummary: string
}

// ── Configuration ─────────────────────────────────────────────────

const USER_TIMEZONE = 'Asia/Kolkata'

// Common words to filter out of topic extraction
const STOP_WORDS = new Set([
  'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
  'should', 'may', 'might', 'shall', 'can', 'need', 'dare', 'ought',
  'used', 'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from',
  'as', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
  'between', 'out', 'off', 'over', 'under', 'again', 'further', 'then',
  'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'both',
  'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor',
  'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'just',
  'don', 'now', 'and', 'but', 'or', 'if', 'while', 'about', 'up',
  'its', 'it', 'he', 'she', 'they', 'them', 'his', 'her', 'their',
  'this', 'that', 'these', 'those', 'what', 'which', 'who', 'whom',
  'i', 'me', 'my', 'we', 'our', 'you', 'your', 'him', 'app', 'text',
  'screen', 'switched', 'active', 'last', 'ago', 'msgs', 'msg', 'new',
  'one', 'two', 'also', 'like', 'get', 'got', 'see', 'let', 'com',
  'www', 'https', 'http', 'google', 'gmail', 'null', 'undefined'
])

// ── Core: Generate digest for a single date ───────────────────────

export async function generateDailyDigest(dateStr: string): Promise<DigestResult | null> {
  const pool = getPool()

  // Convert date string to UTC range for the user's timezone
  // dateStr is 'YYYY-MM-DD' in IST
  const dayStart = new Date(`${dateStr}T00:00:00+05:30`)
  const dayEnd = new Date(`${dateStr}T23:59:59.999+05:30`)

  // Check if digest already exists for this date
  const existing = await pool.query(
    "SELECT id FROM observations WHERE source='daily_digest' AND raw_content LIKE $1",
    [`## Daily Digest: ${dateStr}%`]
  )
  if (existing.rows.length > 0) {
    console.log(`[digest] Skipping ${dateStr} — already exists`)
    return null
  }

  // Fetch all observations for this date
  const obs = await pool.query(
    'SELECT * FROM observations WHERE timestamp >= $1 AND timestamp < $2 ORDER BY timestamp ASC',
    [dayStart.toISOString(), dayEnd.toISOString()]
  )

  const observations = obs.rows
  if (observations.length === 0) {
    console.log(`[digest] ${dateStr}: No observations`)
    // Store an empty digest so we don't re-process
    await storeDigest(dateStr, `## Daily Digest: ${dateStr}\n\nNo activity recorded.`, {
      date: dateStr, totalObservations: 0
    })
    return null
  }

  // Load contacts for person matching
  const contactsRes = await pool.query('SELECT name, email FROM contacts WHERE name IS NOT NULL')
  const contactNames = contactsRes.rows
    .map((c: any) => c.name?.trim())
    .filter((n: string) => n && n.length > 2)

  // Group observations by source
  const bySource: Record<string, any[]> = {}
  for (const o of observations) {
    if (!bySource[o.source]) bySource[o.source] = []
    bySource[o.source].push(o)
  }

  // Process each source
  const sources: Record<string, SourceSummary> = {}
  const globalPeopleMap: Record<string, { mentions: number; contexts: Set<string> }> = {}
  const globalTopicWords: Record<string, number> = {}

  for (const [source, items] of Object.entries(bySource)) {
    const summary = processSource(source, items, contactNames)
    sources[source] = summary

    // Merge people
    for (const person of summary.people) {
      if (!globalPeopleMap[person]) globalPeopleMap[person] = { mentions: 0, contexts: new Set() }
      globalPeopleMap[person].mentions++
      globalPeopleMap[person].contexts.add(source)
    }

    // Merge topics
    for (const topic of summary.topics) {
      globalTopicWords[topic] = (globalTopicWords[topic] || 0) + 1
    }
  }

  // Build people list sorted by mentions
  const allPeople = Object.entries(globalPeopleMap)
    .map(([name, data]) => ({
      name,
      mentions: data.mentions,
      contexts: Array.from(data.contexts)
    }))
    .sort((a, b) => b.mentions - a.mentions)
    .slice(0, 15)

  // Build topic list sorted by frequency
  const allTopics = Object.entries(globalTopicWords)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([topic]) => topic)

  // Generate highlights (most significant events)
  const highlights = generateHighlights(sources, allPeople)

  // Build day of week
  const dayOfWeek = new Date(dayStart).toLocaleDateString('en-US', {
    weekday: 'long',
    timeZone: USER_TIMEZONE
  })

  // Build readable summary
  const readableSummary = buildReadableSummary({
    date: dateStr,
    dayOfWeek,
    totalObservations: observations.length,
    sources,
    allPeople,
    allTopics,
    highlights,
    readableSummary: ''
  })

  const result: DigestResult = {
    date: dateStr,
    dayOfWeek,
    totalObservations: observations.length,
    sources,
    allPeople,
    allTopics,
    highlights,
    readableSummary
  }

  // Store as observation
  await storeDigest(dateStr, readableSummary, {
    date: dateStr,
    dayOfWeek,
    totalObservations: observations.length,
    sourceCounts: Object.fromEntries(Object.entries(sources).map(([k, v]) => [k, v.count])),
    people: allPeople.slice(0, 10).map(p => p.name),
    topics: allTopics,
    highlights
  })

  console.log(`[digest] ${dateStr} (${dayOfWeek}): ${observations.length} observations, ${allPeople.length} people, ${allTopics.length} topics`)
  return result
}

// ── Source-specific processors ────────────────────────────────────

function processSource(source: string, items: any[], contactNames: string[]): SourceSummary {
  const eventTypes: Record<string, number> = {}
  const details: string[] = []
  const people: string[] = []
  const topics: string[] = []

  for (const item of items) {
    const et = item.event_type || 'unknown'
    eventTypes[et] = (eventTypes[et] || 0) + 1
  }

  switch (source) {
    case 'browser':
      processBrowser(items, details, topics)
      break
    case 'whatsapp':
      processWhatsApp(items, details, people, topics, contactNames)
      break
    case 'email':
      processEmail(items, details, people, topics)
      break
    case 'screen_capture':
      processScreenCapture(items, details, topics)
      break
    case 'app_activity':
      processAppActivity(items, details, topics)
      break
    case 'clipboard':
      processClipboard(items, details, topics)
      break
    case 'chat':
      processChat(items, details, topics)
      break
    case 'centrum':
      processCentrum(items, details, topics)
      break
    case 'calendar':
      processCalendar(items, details, people, topics)
      break
    case 'wifi':
      processWifi(items, details)
      break
    default:
      // Generic: just extract content
      for (const item of items.slice(0, 10)) {
        const content = (item.raw_content || '').slice(0, 100)
        if (content) details.push(content)
      }
  }

  // Extract people from all raw content
  const allContent = items.map(i => i.raw_content || '').join(' ')
  for (const name of contactNames) {
    if (allContent.toLowerCase().includes(name.toLowerCase())) {
      people.push(name)
    }
  }

  // Extract topics from all content
  const contentTopics = extractTopics(allContent)
  topics.push(...contentTopics)

  return {
    count: items.length,
    eventTypes,
    details: details.slice(0, 20),
    people: [...new Set(people)],
    topics: [...new Set(topics)]
  }
}

function processBrowser(items: any[], details: string[], topics: string[]): void {
  // Extract unique domains and page titles
  const domains: Record<string, number> = {}
  const titles: string[] = []

  for (const item of items) {
    const content = item.raw_content || ''
    // Format: "Page Title — domain.com" or just URL
    const domainMatch = content.match(/—\s*([^\s]+)$/)
    if (domainMatch) {
      const domain = domainMatch[1].replace(/^www\./, '')
      domains[domain] = (domains[domain] || 0) + 1
    }
    const titleMatch = content.match(/^(.+?)\s*—/)
    if (titleMatch) {
      titles.push(titleMatch[1].trim())
    }
  }

  // Top domains
  const topDomains = Object.entries(domains)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
  for (const [domain, count] of topDomains) {
    details.push(`${domain} (${count} pages)`)
  }

  // Extract topics from page titles
  for (const title of titles.slice(0, 20)) {
    const words = extractTopics(title)
    topics.push(...words)
  }
}

function processWhatsApp(items: any[], details: string[], people: string[], topics: string[], contactNames: string[]): void {
  const sent = items.filter(i => i.event_type === 'sent')
  const conversations = items.filter(i => i.event_type === 'viewed')

  if (sent.length > 0) {
    details.push(`${sent.length} messages sent`)
    for (const msg of sent.slice(0, 5)) {
      const content = msg.raw_content || ''
      // Extract recipient: "To +91...: message"
      const toMatch = content.match(/^To\s+([^:]+):/)
      if (toMatch) {
        details.push(`→ ${toMatch[1].trim()}: "${content.slice(content.indexOf(':') + 2, content.indexOf(':') + 52).trim()}"`)
      }
    }
  }

  // Parse conversation list format: "Name: N msgs, last Xm ago"
  for (const conv of conversations) {
    const content = conv.raw_content || ''
    const lines = content.split('\n').filter((l: string) => l.includes('msgs'))
    for (const line of lines.slice(0, 10)) {
      const nameMatch = line.match(/^([^:]+):\s*(\d+)\s*msgs/)
      if (nameMatch) {
        const name = nameMatch[1].trim()
        const msgCount = parseInt(nameMatch[2])
        if (msgCount > 0 && !name.includes('Active WhatsApp')) {
          people.push(name)
          if (msgCount > 5) details.push(`${name}: ${msgCount} messages`)
        }
      }
    }
  }
}

function processEmail(items: any[], details: string[], people: string[], topics: string[]): void {
  const sent = items.filter(i => i.event_type === 'sent')
  const received = items.filter(i => i.event_type === 'received')

  if (sent.length > 0) details.push(`${sent.length} emails sent`)
  if (received.length > 0) details.push(`${received.length} emails received`)

  for (const email of items.slice(0, 10)) {
    const content = email.raw_content || ''
    // Extract subject and people from email observation content
    const subjectMatch = content.match(/Subject:\s*(.+?)(?:\n|$)/i)
    const fromMatch = content.match(/From:\s*(.+?)(?:\n|$)/i)
    const toMatch = content.match(/To:\s*(.+?)(?:\n|$)/i)

    if (subjectMatch) {
      const direction = email.event_type === 'sent' ? '→' : '←'
      const person = email.event_type === 'sent' ? toMatch?.[1] : fromMatch?.[1]
      details.push(`${direction} "${subjectMatch[1].trim()}"${person ? ` (${person.trim().slice(0, 40)})` : ''}`)
      if (person) people.push(person.trim().split(/[<@]/)[0].trim())
    }

    topics.push(...extractTopics(content))
  }
}

function processScreenCapture(items: any[], details: string[], topics: string[]): void {
  // Extract app names and key text from OCR
  const apps: Record<string, number> = {}

  for (const item of items) {
    const content = item.raw_content || ''
    const appMatch = content.match(/^App:\s*(.+?)$/m)
    if (appMatch) {
      const app = appMatch[1].trim()
      apps[app] = (apps[app] || 0) + 1
    }
  }

  const topApps = Object.entries(apps).sort((a, b) => b[1] - a[1]).slice(0, 5)
  for (const [app, count] of topApps) {
    details.push(`${app}: ${count} captures`)
  }
}

function processAppActivity(items: any[], details: string[], topics: string[]): void {
  // Track app switches
  const apps: Record<string, number> = {}

  for (const item of items) {
    const content = item.raw_content || ''
    const appMatch = content.match(/Switched to:\s*(.+)/)
    if (appMatch) {
      const app = appMatch[1].trim()
      apps[app] = (apps[app] || 0) + 1
    }
  }

  const topApps = Object.entries(apps).sort((a, b) => b[1] - a[1]).slice(0, 8)
  for (const [app, count] of topApps) {
    details.push(`${app} (${count} switches)`)
  }
}

function processClipboard(items: any[], details: string[], topics: string[]): void {
  details.push(`${items.length} clipboard copies`)
  for (const item of items.slice(0, 3)) {
    const content = (item.raw_content || '').slice(0, 80)
    if (content) details.push(`Copied: "${content}${content.length >= 80 ? '...' : ''}"`)
  }
  // Extract topics from clipboard content
  for (const item of items) {
    topics.push(...extractTopics(item.raw_content || ''))
  }
}

function processChat(items: any[], details: string[], topics: string[]): void {
  details.push(`${items.length} IRIS queries`)
  for (const item of items.slice(0, 5)) {
    details.push(`Q: "${(item.raw_content || '').slice(0, 80)}"`)
  }
}

function processCentrum(items: any[], details: string[], topics: string[]): void {
  const created = items.filter(i => i.event_type === 'created')
  const modified = items.filter(i => i.event_type === 'modified')
  const viewed = items.filter(i => i.event_type === 'viewed')

  if (created.length > 0) details.push(`${created.length} cards/tasks created`)
  if (modified.length > 0) details.push(`${modified.length} changes/completions`)

  for (const item of [...created, ...modified].slice(0, 8)) {
    details.push((item.raw_content || '').slice(0, 100))
  }

  // Board summary from viewed events
  for (const item of viewed.slice(0, 1)) {
    const content = item.raw_content || ''
    if (content.startsWith('Board loaded:')) {
      details.push(content.slice(0, 120))
    }
  }
}

function processCalendar(items: any[], details: string[], people: string[], topics: string[]): void {
  for (const item of items) {
    const content = item.raw_content || ''
    details.push(content.slice(0, 120))
    // Extract participant names
    const participantMatch = content.match(/participants?:\s*(.+)/i)
    if (participantMatch) {
      const names = participantMatch[1].split(',').map((n: string) => n.trim())
      people.push(...names)
    }
    topics.push(...extractTopics(content))
  }
}

function processWifi(items: any[], details: string[]): void {
  const networks = new Set<string>()
  for (const item of items) {
    const content = item.raw_content || ''
    if (content) networks.add(content.trim())
  }
  if (networks.size > 0) {
    details.push(`Networks: ${Array.from(networks).join(', ')}`)
  }
}

// ── Topic extraction ──────────────────────────────────────────────

function extractTopics(text: string): string[] {
  if (!text) return []

  // Tokenize: split on non-alphanumeric, keep words 4+ chars
  const words = text.toLowerCase()
    .replace(/[^a-zA-Z\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 4 && !STOP_WORDS.has(w))

  // Return unique words (deduped per call)
  return [...new Set(words)].slice(0, 10)
}

// ── Highlights generation ─────────────────────────────────────────

function generateHighlights(
  sources: Record<string, SourceSummary>,
  people: { name: string; mentions: number }[]
): string[] {
  const highlights: string[] = []

  // High-activity sources
  for (const [source, summary] of Object.entries(sources)) {
    if (source === 'browser' && summary.count > 50) {
      highlights.push(`Heavy browsing day: ${summary.count} pages visited`)
    }
    if (source === 'whatsapp' && summary.count > 20) {
      highlights.push(`Active WhatsApp day: ${summary.count} observations`)
    }
    if (source === 'email') {
      const sent = summary.eventTypes['sent'] || 0
      const received = summary.eventTypes['received'] || 0
      if (sent > 0 || received > 0) {
        highlights.push(`Email: ${sent} sent, ${received} received`)
      }
    }
    if (source === 'centrum') {
      const created = summary.eventTypes['created'] || 0
      const modified = summary.eventTypes['modified'] || 0
      if (created > 0) highlights.push(`Created ${created} Centrum cards/tasks`)
      if (modified > 0) highlights.push(`${modified} Centrum task updates`)
    }
    if (source === 'chat' && summary.count > 0) {
      highlights.push(`${summary.count} IRIS queries`)
    }
  }

  // Top person interaction
  if (people.length > 0 && people[0].mentions >= 3) {
    highlights.push(`Most-mentioned person: ${people[0].name} (${people[0].mentions} mentions)`)
  }

  return highlights
}

// ── Readable summary builder ──────────────────────────────────────

function buildReadableSummary(digest: DigestResult): string {
  const lines: string[] = []
  lines.push(`## Daily Digest: ${digest.date} (${digest.dayOfWeek})`)
  lines.push('')

  // Overview
  const sourceSummary = Object.entries(digest.sources)
    .map(([s, d]) => `${s}: ${d.count}`)
    .join(', ')
  lines.push(`**${digest.totalObservations} observations** across ${Object.keys(digest.sources).length} sources (${sourceSummary})`)
  lines.push('')

  // Highlights
  if (digest.highlights.length > 0) {
    lines.push('### Highlights')
    for (const h of digest.highlights) {
      lines.push(`- ${h}`)
    }
    lines.push('')
  }

  // Per-source details
  const sourceOrder = ['centrum', 'email', 'calendar', 'whatsapp', 'browser', 'app_activity', 'screen_capture', 'clipboard', 'chat', 'wifi']
  const sourceLabels: Record<string, string> = {
    browser: 'Web Browsing', whatsapp: 'WhatsApp', email: 'Email',
    screen_capture: 'Screen Activity', app_activity: 'App Usage',
    clipboard: 'Clipboard', chat: 'IRIS Queries', centrum: 'Centrum Tasks',
    calendar: 'Calendar', wifi: 'Location/WiFi'
  }

  for (const source of sourceOrder) {
    const summary = digest.sources[source]
    if (!summary) continue

    lines.push(`### ${sourceLabels[source] || source} (${summary.count})`)
    for (const detail of summary.details.slice(0, 10)) {
      lines.push(`- ${detail}`)
    }
    lines.push('')
  }

  // Any remaining sources not in the order list
  for (const [source, summary] of Object.entries(digest.sources)) {
    if (sourceOrder.includes(source)) continue
    lines.push(`### ${source} (${summary.count})`)
    for (const detail of summary.details.slice(0, 5)) {
      lines.push(`- ${detail}`)
    }
    lines.push('')
  }

  // People
  if (digest.allPeople.length > 0) {
    lines.push('### Key People')
    const personList = digest.allPeople.slice(0, 10)
      .map(p => `${p.name} (${p.mentions}× via ${p.contexts.join(', ')})`)
    lines.push(personList.join(', '))
    lines.push('')
  }

  // Topics
  if (digest.allTopics.length > 0) {
    lines.push('### Topics')
    lines.push(digest.allTopics.join(', '))
    lines.push('')
  }

  return lines.join('\n')
}

// ── Store digest as observation ───────────────────────────────────

async function storeDigest(date: string, readableSummary: string, entities: object): Promise<string> {
  const pool = getPool()
  const id = crypto.randomUUID()
  await pool.query(
    `INSERT INTO observations (id, timestamp, source, event_type, raw_content, extracted_entities, emotional_valence, related_observations, persona_impact)
     VALUES ($1, $2, 'daily_digest', 'digest', $3, $4, 'neutral', '[]', '')`,
    [id, `${date}T23:59:00+05:30`, readableSummary.slice(0, 8000), JSON.stringify(entities)]
  )
  return id
}

// ── Weekly digest ─────────────────────────────────────────────────

export async function generateWeeklyDigest(endDate: string): Promise<string | null> {
  const pool = getPool()

  // Check if already exists
  const existing = await pool.query(
    "SELECT id FROM observations WHERE source='weekly_digest' AND raw_content LIKE $1",
    [`## Weekly Digest: Week ending ${endDate}%`]
  )
  if (existing.rows.length > 0) {
    console.log(`[digest] Weekly digest for ${endDate} already exists`)
    return null
  }

  // Get the 7 daily digests leading up to endDate
  const end = new Date(`${endDate}T23:59:59+05:30`)
  const start = new Date(end.getTime() - 7 * 86400000)

  const digests = await pool.query(
    "SELECT raw_content, extracted_entities FROM observations WHERE source='daily_digest' AND timestamp >= $1 AND timestamp <= $2 ORDER BY timestamp ASC",
    [start.toISOString(), end.toISOString()]
  )

  if (digests.rows.length === 0) {
    console.log(`[digest] No daily digests found for week ending ${endDate}`)
    return null
  }

  // Aggregate across the week
  let totalObs = 0
  const weekSourceCounts: Record<string, number> = {}
  const weekPeople: Record<string, number> = {}
  const weekTopics: Record<string, number> = {}
  const dailySummaries: string[] = []
  const dayActivities: { date: string; count: number }[] = []

  for (const row of digests.rows) {
    const entities = typeof row.extracted_entities === 'string'
      ? JSON.parse(row.extracted_entities)
      : row.extracted_entities || {}

    const dayTotal = entities.totalObservations || 0
    totalObs += dayTotal
    dayActivities.push({ date: entities.date || '?', count: dayTotal })

    // Merge source counts
    const sc = entities.sourceCounts || {}
    for (const [src, cnt] of Object.entries(sc)) {
      weekSourceCounts[src] = (weekSourceCounts[src] || 0) + (cnt as number)
    }

    // Merge people
    const ppl = entities.people || []
    for (const name of ppl) {
      weekPeople[name] = (weekPeople[name] || 0) + 1
    }

    // Merge topics
    const tps = entities.topics || []
    for (const topic of tps) {
      weekTopics[topic] = (weekTopics[topic] || 0) + 1
    }

    // Short daily summary
    const highlights = entities.highlights || []
    if (highlights.length > 0) {
      dailySummaries.push(`**${entities.date}** (${entities.dayOfWeek || '?'}): ${highlights.slice(0, 3).join('; ')}`)
    } else {
      dailySummaries.push(`**${entities.date}**: ${dayTotal} observations`)
    }
  }

  // Find busiest and quietest days
  const sorted = [...dayActivities].sort((a, b) => b.count - a.count)
  const busiest = sorted[0]
  const quietest = sorted[sorted.length - 1]

  // Top people and topics for the week
  const topPeople = Object.entries(weekPeople).sort((a, b) => b[1] - a[1]).slice(0, 10)
  const topTopics = Object.entries(weekTopics).sort((a, b) => b[1] - a[1]).slice(0, 10)

  // Build weekly summary
  const lines: string[] = []
  lines.push(`## Weekly Digest: Week ending ${endDate}`)
  lines.push('')
  lines.push(`**${totalObs} total observations** across ${digests.rows.length} days`)
  lines.push('')

  // Source breakdown
  lines.push('### Activity by Source')
  const srcEntries = Object.entries(weekSourceCounts).sort((a, b) => b[1] - a[1])
  for (const [src, cnt] of srcEntries) {
    lines.push(`- ${src}: ${cnt}`)
  }
  lines.push('')

  // Day-by-day
  lines.push('### Day by Day')
  for (const ds of dailySummaries) {
    lines.push(`- ${ds}`)
  }
  lines.push('')

  if (busiest && quietest && busiest.date !== quietest.date) {
    lines.push(`Busiest: ${busiest.date} (${busiest.count} obs). Quietest: ${quietest.date} (${quietest.count} obs).`)
    lines.push('')
  }

  // People
  if (topPeople.length > 0) {
    lines.push('### Key People This Week')
    lines.push(topPeople.map(([name, cnt]) => `${name} (${cnt} days)`).join(', '))
    lines.push('')
  }

  // Topics
  if (topTopics.length > 0) {
    lines.push('### Recurring Topics')
    lines.push(topTopics.map(([topic, cnt]) => `${topic} (${cnt}×)`).join(', '))
    lines.push('')
  }

  const summary = lines.join('\n')

  // Store as observation
  const id = crypto.randomUUID()
  await pool.query(
    `INSERT INTO observations (id, timestamp, source, event_type, raw_content, extracted_entities, emotional_valence, related_observations, persona_impact)
     VALUES ($1, $2, 'weekly_digest', 'digest', $3, $4, 'neutral', '[]', '')`,
    [id, `${endDate}T23:59:30+05:30`, summary.slice(0, 8000), JSON.stringify({
      endDate,
      totalObservations: totalObs,
      daysCount: digests.rows.length,
      sourceCounts: weekSourceCounts,
      topPeople: topPeople.map(([n]) => n),
      topTopics: topTopics.map(([t]) => t)
    })]
  )

  console.log(`[digest] Weekly digest for week ending ${endDate}: ${totalObs} observations across ${digests.rows.length} days`)
  return id
}

// ── Backfill: generate digests for past N days ────────────────────

export async function backfillDigests(days: number = 30): Promise<{ daily: number; weekly: number }> {
  console.log(`[digest] Backfilling last ${days} days...`)
  let dailyCount = 0
  let weeklyCount = 0

  // Generate daily digests from oldest to newest
  for (let i = days; i >= 1; i--) {
    const date = new Date(Date.now() - i * 86400000)
    const dateStr = date.toLocaleDateString('en-CA', { timeZone: USER_TIMEZONE }) // YYYY-MM-DD format
    try {
      const result = await generateDailyDigest(dateStr)
      if (result) dailyCount++
    } catch (err: any) {
      console.error(`[digest] Error generating digest for ${dateStr}:`, err.message)
    }
  }

  // Generate weekly digests for each completed week
  for (let i = days; i >= 7; i -= 7) {
    const endDate = new Date(Date.now() - (i - 6) * 86400000)
    const endStr = endDate.toLocaleDateString('en-CA', { timeZone: USER_TIMEZONE })
    // Only generate if it's a Sunday or we're doing the first pass
    try {
      const result = await generateWeeklyDigest(endStr)
      if (result) weeklyCount++
    } catch (err: any) {
      console.error(`[digest] Error generating weekly for ${endStr}:`, err.message)
    }
  }

  console.log(`[digest] Backfill complete: ${dailyCount} daily, ${weeklyCount} weekly digests created`)
  return { daily: dailyCount, weekly: weeklyCount }
}

// ── Scheduler: run nightly at 00:15 IST ───────────────────────────

let schedulerInterval: ReturnType<typeof setInterval> | null = null
let lastDigestDate: string | null = null

export function startDigestScheduler(): void {
  if (schedulerInterval) clearInterval(schedulerInterval)

  // Check every 60 seconds
  schedulerInterval = setInterval(async () => {
    try {
      const now = new Date()
      const istNow = new Date(now.toLocaleString('en-US', { timeZone: USER_TIMEZONE }))
      const hour = istNow.getHours()
      const minute = istNow.getMinutes()
      const todayStr = istNow.toLocaleDateString('en-CA') // YYYY-MM-DD

      // Run daily digest at 00:15 IST
      if (hour === 0 && minute >= 15 && minute < 16 && lastDigestDate !== todayStr) {
        lastDigestDate = todayStr

        // Generate yesterday's digest
        const yesterday = new Date(istNow.getTime() - 86400000)
        const yesterdayStr = yesterday.toLocaleDateString('en-CA')
        console.log(`[digest] Scheduled: generating digest for ${yesterdayStr}`)
        await generateDailyDigest(yesterdayStr)

        // If Sunday, also generate weekly
        if (istNow.getDay() === 0) {
          console.log(`[digest] Sunday: generating weekly digest`)
          await generateWeeklyDigest(yesterdayStr)
        }
      }
    } catch (err: any) {
      console.error('[digest] Scheduler error:', err.message)
    }
  }, 60_000)

  console.log('[digest] Scheduler started (checking every 60s for 00:15 IST)')
}

export function stopDigestScheduler(): void {
  if (schedulerInterval) { clearInterval(schedulerInterval); schedulerInterval = null }
}

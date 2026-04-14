/**
 * WhatsApp contact extractor.
 *
 * Runs every 15 minutes (scheduled elsewhere). Processes WhatsApp
 * observations whose `parsed_whatsapp` column is NULL:
 *   1. Parse raw_content → structured fields
 *   2. Write structured fields back to observations.parsed_whatsapp
 *   3. Upsert unique senders (received) and recipients (sent) into contacts
 *   4. Fuzzy-merge high-confidence matches with existing email contacts
 */

import crypto from 'crypto'
import { getPool } from './db'
import { parseWhatsAppObservation, isPersonalName, type ParsedWhatsApp } from './whatsapp-parser'

interface ExtractorResult {
  observationsProcessed: number
  messagesParsed: number
  contactsCreated: number
  contactsUpdated: number
  contactsMerged: number
  ambiguousFlagged: number
  batches: number
}

/**
 * Main extractor. Processes in batches to avoid long DB transactions.
 * By default processes all unparsed rows (use batch size to control).
 */
export async function runWhatsAppExtractor(options: {
  limit?: number           // max observations per call; default 2000
  backfillAll?: boolean    // if true, keep looping until done; default false
} = {}): Promise<ExtractorResult> {
  const pool = getPool()
  const limit = options.limit ?? 2000
  const result: ExtractorResult = {
    observationsProcessed: 0,
    messagesParsed: 0,
    contactsCreated: 0,
    contactsUpdated: 0,
    contactsMerged: 0,
    ambiguousFlagged: 0,
    batches: 0,
  }

  // Accumulate per-contact daily message counts so we can increment
  // whatsapp_msgs_total and set last_whatsapp_at once at the end.
  // Map key: contactId → { displayName, phone, messages, lastTs, direction }

  while (true) {
    const rows = await pool.query(
      `SELECT id, timestamp, raw_content
       FROM observations
       WHERE source = 'whatsapp' AND parsed_whatsapp IS NULL
       ORDER BY timestamp ASC
       LIMIT $1`,
      [limit]
    )
    if (rows.rows.length === 0) break
    result.batches++

    // Group by sender / recipient to bulk-upsert
    // Maps: key = "name:<displayName>" or "phone:<E164>"
    const touched = new Map<string, {
      kind: 'name' | 'phone'
      displayName?: string
      phone?: string
      rawName?: string
      is_group: boolean
      msgs: number
      latestTs: string
    }>()

    for (const obs of rows.rows) {
      const parsed = parseWhatsAppObservation(obs.raw_content || '')
      await pool.query(
        `UPDATE observations SET parsed_whatsapp = $1 WHERE id = $2`,
        [parsed ? JSON.stringify({ ...parsed, processed_at: new Date().toISOString() }) : JSON.stringify({ parsed: false, processed_at: new Date().toISOString() }), obs.id]
      )
      result.observationsProcessed++

      if (!parsed || parsed.summary_only) continue
      result.messagesParsed++

      // Decide key: prefer name (received) > phone (sent)
      let key: string | null = null
      if (parsed.direction === 'received' && isPersonalName(parsed.display_name)) {
        key = `name:${(parsed.display_name || '').toLowerCase()}`
      } else if (parsed.phone) {
        key = `phone:${parsed.phone}`
      }
      if (!key) continue

      const existing = touched.get(key)
      if (existing) {
        existing.msgs++
        if (obs.timestamp > existing.latestTs) existing.latestTs = obs.timestamp
      } else {
        touched.set(key, {
          kind: key.startsWith('name:') ? 'name' : 'phone',
          displayName: parsed.display_name || undefined,
          rawName: parsed.raw_name || undefined,
          phone: parsed.phone || undefined,
          is_group: parsed.is_group,
          msgs: 1,
          latestTs: obs.timestamp
        })
      }
    }

    // Upsert contacts for each touched key
    for (const [_key, info] of touched) {
      // Skip group-only contacts (group names aren't individual people)
      if (info.kind === 'name' && info.is_group) {
        // Group messages by individual senders would need different parsing.
        // For now, skip group-labeled rows to avoid polluting the contacts
        // table with group names.
        continue
      }

      const upserted = await upsertWhatsAppContact(info)
      if (upserted.action === 'created') result.contactsCreated++
      else if (upserted.action === 'merged') result.contactsMerged++
      else if (upserted.action === 'updated') result.contactsUpdated++
      else if (upserted.action === 'ambiguous') result.ambiguousFlagged++
    }

    if (!options.backfillAll) break
    if (rows.rows.length < limit) break // drained
  }

  console.log(`[wa-extractor] done: ${JSON.stringify(result)}`)
  return result
}

/**
 * Upsert a WhatsApp-derived contact into the contacts table.
 * Fuzzy-merges with existing email contacts when the first name matches
 * exactly AND the email contact has no phone yet.
 */
async function upsertWhatsAppContact(info: {
  kind: 'name' | 'phone'
  displayName?: string
  rawName?: string
  phone?: string
  msgs: number
  latestTs: string
}): Promise<{ action: 'created' | 'updated' | 'merged' | 'ambiguous' | 'skipped' }> {
  const pool = getPool()

  // Try to find existing contact:
  // 1. By exact phone match (strongest)
  // 2. By whatsapp_display_name equality
  // 3. By first-name match with an email-only contact (merge candidate)

  if (info.kind === 'phone' && info.phone) {
    const byPhone = await pool.query(
      `SELECT id, primary_channel, whatsapp_msgs_total FROM contacts WHERE phone = $1 LIMIT 1`,
      [info.phone]
    )
    if (byPhone.rows.length > 0) {
      const row = byPhone.rows[0]
      await pool.query(
        `UPDATE contacts
         SET whatsapp_msgs_total = COALESCE(whatsapp_msgs_total, 0) + $1,
             last_whatsapp_at = GREATEST(COALESCE(last_whatsapp_at, $2::timestamptz), $2::timestamptz),
             updated_at = NOW()
         WHERE id = $3`,
        [info.msgs, info.latestTs, row.id]
      )
      return { action: 'updated' }
    }
    // New phone-only contact
    const id = crypto.randomUUID()
    await pool.query(
      `INSERT INTO contacts (id, name, phone, primary_channel, whatsapp_msgs_total, last_whatsapp_at, last_interaction_at, needs_manual_merge)
       VALUES ($1, $2, $3, 'whatsapp', $4, $5, $5, TRUE)
       ON CONFLICT (phone) DO NOTHING`,
      [id, `Unknown (${info.phone})`, info.phone, info.msgs, info.latestTs]
    )
    return { action: 'created' }
  }

  if (info.kind === 'name' && info.displayName) {
    const nameLower = info.displayName.toLowerCase().trim()
    const firstName = nameLower.split(/\s+/)[0]

    // Look for existing WA-derived contact with same display name
    const byDisplay = await pool.query(
      `SELECT id, primary_channel FROM contacts
       WHERE LOWER(whatsapp_display_name) = $1 LIMIT 1`,
      [nameLower]
    )
    if (byDisplay.rows.length > 0) {
      await pool.query(
        `UPDATE contacts
         SET whatsapp_msgs_total = COALESCE(whatsapp_msgs_total, 0) + $1,
             last_whatsapp_at = GREATEST(COALESCE(last_whatsapp_at, $2::timestamptz), $2::timestamptz),
             last_interaction_at = $2,
             updated_at = NOW()
         WHERE id = $3`,
        [info.msgs, info.latestTs, byDisplay.rows[0].id]
      )
      return { action: 'updated' }
    }

    // Merge candidate: email-only contact with matching first name, no phone yet.
    // Only merge when the match is unambiguous (exactly one such contact).
    const mergeCandidates = await pool.query(
      `SELECT id, name, email, primary_channel, phone FROM contacts
       WHERE LOWER(SPLIT_PART(name, ' ', 1)) = $1
         AND (whatsapp_display_name IS NULL)
         AND (phone IS NULL)
         AND email IS NOT NULL`,
      [firstName]
    )
    if (mergeCandidates.rows.length === 1) {
      // Merge into the existing email contact
      const existing = mergeCandidates.rows[0]
      await pool.query(
        `UPDATE contacts
         SET whatsapp_display_name = $1,
             whatsapp_msgs_total = COALESCE(whatsapp_msgs_total, 0) + $2,
             last_whatsapp_at = GREATEST(COALESCE(last_whatsapp_at, $3::timestamptz), $3::timestamptz),
             primary_channel = 'both',
             updated_at = NOW()
         WHERE id = $4`,
        [info.displayName, info.msgs, info.latestTs, existing.id]
      )
      return { action: 'merged' }
    }
    if (mergeCandidates.rows.length > 1) {
      // Ambiguous — multiple existing contacts with same first name. Create new
      // and flag for manual merge.
      const id = crypto.randomUUID()
      await pool.query(
        `INSERT INTO contacts (id, name, whatsapp_display_name, primary_channel, whatsapp_msgs_total, last_whatsapp_at, last_interaction_at, needs_manual_merge)
         VALUES ($1, $2, $3, 'whatsapp', $4, $5, $5, TRUE)
         ON CONFLICT (email) DO NOTHING`,
        [id, info.displayName, info.displayName, info.msgs, info.latestTs]
      )
      return { action: 'ambiguous' }
    }

    // Clean new WhatsApp-only contact
    const id = crypto.randomUUID()
    await pool.query(
      `INSERT INTO contacts (id, name, whatsapp_display_name, primary_channel, whatsapp_msgs_total, last_whatsapp_at, last_interaction_at, needs_manual_merge)
       VALUES ($1, $2, $3, 'whatsapp', $4, $5, $5, FALSE)
       ON CONFLICT (email) DO NOTHING`,
      [id, info.displayName, info.displayName, info.msgs, info.latestTs]
    )
    return { action: 'created' }
  }

  return { action: 'skipped' }
}

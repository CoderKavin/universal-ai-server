/**
 * PostgreSQL database layer.
 * Same table schemas as the Electron SQLite version, rewritten for pg.
 */

import { Pool, type PoolClient } from 'pg'
import crypto from 'crypto'

let pool: Pool

export function initDB(): Pool {
  pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.DATABASE_URL?.includes('render') ? { rejectUnauthorized: false } : undefined })
  return pool
}

export function getPool(): Pool { return pool }

export async function migrate(): Promise<void> {
  const client = await pool.connect()
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS oauth_tokens (provider TEXT PRIMARY KEY, access_token TEXT NOT NULL, refresh_token TEXT, expiry_date BIGINT, email TEXT, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS contacts (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL, emails_sent INT DEFAULT 0, emails_received INT DEFAULT 0, avg_response_time_minutes REAL, last_interaction_at TEXT, relationship_score REAL DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS email_threads (thread_id TEXT PRIMARY KEY, subject TEXT DEFAULT '', last_message_from TEXT DEFAULT '', last_message_date TEXT DEFAULT '', awaiting_reply INT DEFAULT 0, stale_days INT DEFAULT 0, participants TEXT DEFAULT '[]', message_count INT DEFAULT 0);
      CREATE TABLE IF NOT EXISTS commitments (id TEXT PRIMARY KEY, description TEXT NOT NULL, source_type TEXT DEFAULT 'email', source_ref TEXT, committed_to TEXT, committed_by TEXT, due_at TEXT, status TEXT DEFAULT 'active', ai_confidence REAL, created_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS calendar_events (id TEXT PRIMARY KEY, summary TEXT DEFAULT '', start_time TEXT NOT NULL, end_time TEXT NOT NULL, all_day INT DEFAULT 0, location TEXT, participants TEXT DEFAULT '[]', recurring INT DEFAULT 0, status TEXT DEFAULT 'confirmed', calendar_name TEXT);
      CREATE TABLE IF NOT EXISTS documents (id TEXT PRIMARY KEY, path TEXT UNIQUE NOT NULL, filename TEXT NOT NULL, type TEXT NOT NULL, summary TEXT DEFAULT '', tags TEXT DEFAULT '[]', size_bytes INT DEFAULT 0, last_modified TEXT NOT NULL, indexed_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS proposed_actions (id TEXT PRIMARY KEY, type TEXT NOT NULL, title TEXT NOT NULL, description TEXT DEFAULT '', draft_content TEXT, confidence REAL DEFAULT 0.5, status TEXT DEFAULT 'pending', related_ref TEXT, executed_ref TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS observations (id TEXT PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(), source TEXT NOT NULL, event_type TEXT NOT NULL, raw_content TEXT DEFAULT '', extracted_entities TEXT DEFAULT '{}', emotional_valence TEXT DEFAULT 'neutral', related_observations TEXT DEFAULT '[]', persona_impact TEXT DEFAULT '');
      CREATE TABLE IF NOT EXISTS insights (id TEXT PRIMARY KEY, statement TEXT NOT NULL, evidence TEXT DEFAULT '[]', confidence REAL DEFAULT 0.5, category TEXT NOT NULL, status TEXT DEFAULT 'active', source_count INT DEFAULT 1, first_detected TIMESTAMPTZ DEFAULT NOW(), last_confirmed TIMESTAMPTZ DEFAULT NOW(), times_confirmed INT DEFAULT 1);
      CREATE TABLE IF NOT EXISTS living_profile (id TEXT PRIMARY KEY DEFAULT 'main', content TEXT NOT NULL, version INT DEFAULT 1, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS user_situation (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS sync_state (integration TEXT PRIMARY KEY, last_sync_at TIMESTAMPTZ, status TEXT DEFAULT 'idle', total_processed INT DEFAULT 0);
      CREATE TABLE IF NOT EXISTS action_log (id TEXT PRIMARY KEY, action_id TEXT NOT NULL, type TEXT NOT NULL, result TEXT NOT NULL, details TEXT, created_at TIMESTAMPTZ DEFAULT NOW());

      CREATE TABLE IF NOT EXISTS centrum_snapshots (
        id TEXT PRIMARY KEY,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        board_state JSONB NOT NULL,
        cluster_summary JSONB DEFAULT '{}',
        today_summary TEXT DEFAULT ''
      );

      CREATE TABLE IF NOT EXISTS life_events (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        summary TEXT NOT NULL,
        confidence REAL DEFAULT 0.5,
        evidence JSONB DEFAULT '[]',
        detected_at TIMESTAMPTZ DEFAULT NOW(),
        persona_rewrite_triggered BOOLEAN DEFAULT true
      );

      CREATE TABLE IF NOT EXISTS action_chains (
        id TEXT PRIMARY KEY,
        trigger_event TEXT NOT NULL,
        trigger_type TEXT NOT NULL,
        trigger_entity TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        status TEXT DEFAULT 'active',
        context_snapshot TEXT DEFAULT ''
      );

      CREATE TABLE IF NOT EXISTS chain_steps (
        id TEXT PRIMARY KEY,
        chain_id TEXT NOT NULL REFERENCES action_chains(id),
        step_number INT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        draft_content TEXT,
        prerequisites TEXT DEFAULT '',
        depends_on_step TEXT,
        status TEXT DEFAULT 'pending',
        predicted_action_id TEXT,
        completed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_chain_steps_chain ON chain_steps(chain_id, step_number);

      CREATE TABLE IF NOT EXISTS predicted_actions (
        id TEXT PRIMARY KEY,
        predicted_at TIMESTAMPTZ DEFAULT NOW(),
        trigger_context TEXT NOT NULL,
        trigger_type TEXT NOT NULL,
        action_type TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        draft_content TEXT,
        confidence REAL DEFAULT 0.5,
        urgency TEXT DEFAULT 'medium',
        expires_at TIMESTAMPTZ NOT NULL,
        status TEXT DEFAULT 'pending',
        related_entity TEXT,
        evidence JSONB DEFAULT '[]',
        served_at TIMESTAMPTZ,
        served_in_query TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_pred_actions_status ON predicted_actions(status, expires_at);

      CREATE TABLE IF NOT EXISTS skills (
        id TEXT PRIMARY KEY,
        skill_name TEXT NOT NULL,
        category TEXT NOT NULL,
        proficiency REAL DEFAULT 50,
        trajectory TEXT DEFAULT 'stable',
        evidence_count INT DEFAULT 0,
        examples JSONB DEFAULT '[]',
        last_evaluated TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS routines (
        id TEXT PRIMARY KEY,
        description TEXT NOT NULL,
        pattern_type TEXT NOT NULL,
        time_window TEXT DEFAULT '',
        day_pattern TEXT DEFAULT '',
        frequency TEXT DEFAULT '',
        typical_duration_min INT,
        evidence_count INT DEFAULT 0,
        confidence REAL DEFAULT 0.5,
        variance TEXT DEFAULT '',
        source TEXT DEFAULT '',
        last_observed TIMESTAMPTZ,
        active BOOLEAN DEFAULT true,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS routine_breaks (
        id TEXT PRIMARY KEY,
        routine_id TEXT NOT NULL,
        description TEXT NOT NULL,
        expected_behavior TEXT NOT NULL,
        actual_behavior TEXT NOT NULL,
        severity TEXT DEFAULT 'low',
        days_broken INT DEFAULT 0,
        detected_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS relationships (
        id TEXT PRIMARY KEY,
        contact_id TEXT NOT NULL,
        name TEXT NOT NULL,
        email TEXT,
        current_closeness REAL DEFAULT 0,
        trajectory TEXT DEFAULT 'stable',
        last_interaction TIMESTAMPTZ,
        interaction_freq_30d REAL DEFAULT 0,
        interaction_freq_90d REAL DEFAULT 0,
        sentiment_trend TEXT DEFAULT 'neutral',
        topics_shared JSONB DEFAULT '[]',
        context_roles JSONB DEFAULT '[]',
        narrative_note TEXT DEFAULT '',
        whatsapp_msgs_30d INT DEFAULT 0,
        email_count_30d INT DEFAULT 0,
        initiator_ratio REAL DEFAULT 0.5,
        anomaly TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_rel_closeness ON relationships(current_closeness DESC);

      CREATE TABLE IF NOT EXISTS correction_log (
        id TEXT PRIMARY KEY,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        trigger_action TEXT NOT NULL,
        trigger_context TEXT DEFAULT '',
        user_action TEXT NOT NULL,
        correction_text TEXT DEFAULT '',
        reasoning_chain TEXT DEFAULT '',
        affected_claims JSONB DEFAULT '[]',
        applied_to_persona BOOLEAN DEFAULT false
      );

      CREATE TABLE IF NOT EXISTS lessons_learned (
        id TEXT PRIMARY KEY,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        original_belief TEXT NOT NULL,
        corrected_to TEXT NOT NULL,
        correction_source TEXT DEFAULT '',
        times_reinforced INT DEFAULT 1,
        active BOOLEAN DEFAULT true
      );

      CREATE TABLE IF NOT EXISTS query_memory (
        id TEXT PRIMARY KEY,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        query_text TEXT NOT NULL,
        response_text TEXT NOT NULL,
        entities_mentioned JSONB DEFAULT '[]'
      );
      CREATE INDEX IF NOT EXISTS idx_qmem_ts ON query_memory(timestamp DESC);

      CREATE INDEX IF NOT EXISTS idx_obs_timestamp ON observations(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_obs_source ON observations(source);
      CREATE INDEX IF NOT EXISTS idx_calendar_start ON calendar_events(start_time);
      CREATE INDEX IF NOT EXISTS idx_centrum_snap_ts ON centrum_snapshots(timestamp DESC);
    `)
    console.log('[db] PostgreSQL tables ready')
  } finally {
    client.release()
  }
}

// ── Generic helpers ───────────────────────────────────────────────

export async function getSetting(key: string): Promise<string | null> {
  const r = await pool.query('SELECT value FROM settings WHERE key=$1', [key])
  return r.rows[0]?.value ?? null
}

export async function setSetting(key: string, value: string): Promise<void> {
  await pool.query('INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value=$2', [key, value])
}

export async function getSettings(): Promise<Record<string, string>> {
  const r = await pool.query('SELECT key, value FROM settings')
  const s: Record<string, string> = {}
  for (const row of r.rows) s[row.key] = row.value
  return s
}

// ── Observations ──────────────────────────────────────────────────

export async function insertObservation(source: string, eventType: string, rawContent: string, entities: object, valence: string, related: string[], personaImpact: string): Promise<string> {
  const id = crypto.randomUUID()
  await pool.query(
    'INSERT INTO observations (id, source, event_type, raw_content, extracted_entities, emotional_valence, related_observations, persona_impact) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
    [id, source, eventType, rawContent.slice(0, 2000), JSON.stringify(entities), valence, JSON.stringify(related), personaImpact]
  )
  return id
}

export async function getRecentObservations(limit = 20): Promise<any[]> {
  const r = await pool.query('SELECT * FROM observations ORDER BY timestamp DESC LIMIT $1', [limit])
  return r.rows
}

export async function getObservationCountBySource(): Promise<Record<string, number>> {
  const r = await pool.query('SELECT source, COUNT(*)::int as c FROM observations GROUP BY source ORDER BY c DESC')
  const result: Record<string, number> = {}
  for (const row of r.rows) result[row.source] = row.c
  return result
}

export async function totalObservationCount(): Promise<number> {
  const r = await pool.query('SELECT COUNT(*)::int as c FROM observations')
  return r.rows[0].c
}

// ── Insights ──────────────────────────────────────────────────────

export async function listInsights(status = 'active'): Promise<any[]> {
  const r = await pool.query('SELECT * FROM insights WHERE status=$1 ORDER BY confidence DESC', [status])
  return r.rows
}

export async function upsertInsight(statement: string, evidence: string[], confidence: number, category: string, sourceCount: number): Promise<string> {
  const key = statement.slice(0, 60).toLowerCase()
  const existing = await pool.query("SELECT id, confidence, times_confirmed, evidence FROM insights WHERE status='active' AND LOWER(SUBSTRING(statement,1,60))=$1", [key])
  if (existing.rows.length > 0) {
    const e = existing.rows[0]
    const merged = [...new Set([...JSON.parse(e.evidence || '[]'), ...evidence])]
    const newConf = Math.min(1.0, e.confidence + 0.05 * evidence.length)
    await pool.query('UPDATE insights SET confidence=$1, evidence=$2, times_confirmed=times_confirmed+1, last_confirmed=NOW(), source_count=GREATEST(source_count,$3) WHERE id=$4', [newConf, JSON.stringify(merged), sourceCount, e.id])
    return e.id
  }
  const id = crypto.randomUUID()
  await pool.query("INSERT INTO insights (id, statement, evidence, confidence, category, status, source_count) VALUES ($1,$2,$3,$4,$5,'active',$6)", [id, statement, JSON.stringify(evidence), confidence, category, sourceCount])
  return id
}

// ── Living Profile ────────────────────────────────────────────────

export async function getLivingProfile(): Promise<string | null> {
  const r = await pool.query("SELECT content FROM living_profile WHERE id='main'")
  return r.rows[0]?.content ?? null
}

export async function setLivingProfile(content: string): Promise<void> {
  await pool.query("INSERT INTO living_profile (id, content, version) VALUES ('main', $1, 1) ON CONFLICT (id) DO UPDATE SET content=$1, version=living_profile.version+1, updated_at=NOW()", [content])
}

// ── Contacts ──────────────────────────────────────────────────────

export async function listContacts(limit = 50): Promise<any[]> {
  const r = await pool.query(`SELECT * FROM contacts WHERE emails_sent > 0 AND email NOT LIKE '%noreply%' AND email NOT LIKE '%no-reply%' ORDER BY relationship_score DESC LIMIT $1`, [limit])
  return r.rows
}

// ── Threads ───────────────────────────────────────────────────────

export async function listStaleThreads(minDays = 3): Promise<any[]> {
  const r = await pool.query('SELECT * FROM email_threads WHERE stale_days >= $1 ORDER BY stale_days DESC', [minDays])
  return r.rows
}

export async function listThreadsAwaitingReply(): Promise<any[]> {
  const r = await pool.query('SELECT * FROM email_threads WHERE awaiting_reply = 1 ORDER BY last_message_date DESC')
  return r.rows
}

// ── Commitments ───────────────────────────────────────────────────

export async function listCommitments(status = 'active'): Promise<any[]> {
  const r = await pool.query('SELECT * FROM commitments WHERE status=$1 ORDER BY due_at ASC NULLS LAST', [status])
  return r.rows
}

// ── Calendar ──────────────────────────────────────────────────────

export async function listUpcomingEvents(days = 14): Promise<any[]> {
  const now = new Date().toISOString()
  const future = new Date(Date.now() + days * 86400000).toISOString()
  const r = await pool.query(
    "SELECT * FROM calendar_events WHERE start_time::timestamptz >= $1::timestamptz AND start_time::timestamptz <= $2::timestamptz AND status != 'cancelled' ORDER BY start_time::timestamptz",
    [now, future]
  )
  return r.rows
}

// ── Documents ─────────────────────────────────────────────────────

export async function listRecentDocuments(days = 7): Promise<any[]> {
  const cutoff = new Date(Date.now() - days * 86400000).toISOString()
  const r = await pool.query('SELECT * FROM documents WHERE last_modified >= $1 ORDER BY last_modified DESC', [cutoff])
  return r.rows
}

// ── Proposed Actions ──────────────────────────────────────────────

export async function listProposedActions(status?: string): Promise<any[]> {
  if (status) {
    const r = await pool.query('SELECT * FROM proposed_actions WHERE status=$1 ORDER BY created_at DESC', [status])
    return r.rows
  }
  const r = await pool.query('SELECT * FROM proposed_actions ORDER BY created_at DESC LIMIT 50')
  return r.rows
}

export async function insertProposedAction(type: string, title: string, description: string, draftContent: string | null, confidence: number, relatedRef: string | null): Promise<string> {
  const id = crypto.randomUUID()
  await pool.query("INSERT INTO proposed_actions (id, type, title, description, draft_content, confidence, status, related_ref) VALUES ($1,$2,$3,$4,$5,$6,'pending',$7)", [id, type, title, description, draftContent, confidence, relatedRef])
  return id
}

export async function updateActionStatus(id: string, status: string): Promise<void> {
  await pool.query('UPDATE proposed_actions SET status=$1 WHERE id=$2', [status, id])
}

// ── Sync State ────────────────────────────────────────────────────

export async function getSyncState(integration: string): Promise<any> {
  const r = await pool.query('SELECT * FROM sync_state WHERE integration=$1', [integration])
  return r.rows[0] ?? null
}

export async function setSyncState(integration: string, status: string, totalProcessed: number): Promise<void> {
  await pool.query("INSERT INTO sync_state (integration, last_sync_at, status, total_processed) VALUES ($1, NOW(), $2, $3) ON CONFLICT (integration) DO UPDATE SET last_sync_at=NOW(), status=$2, total_processed=$3", [integration, status, totalProcessed])
}

// ── Centrum Snapshots ────────────────────────────────────────────

export async function insertCentrumSnapshot(boardState: object, clusterSummary: object, todaySummary: string): Promise<string> {
  const id = crypto.randomUUID()
  await pool.query(
    'INSERT INTO centrum_snapshots (id, board_state, cluster_summary, today_summary) VALUES ($1, $2, $3, $4)',
    [id, JSON.stringify(boardState), JSON.stringify(clusterSummary), todaySummary]
  )
  // Prune: keep only last 30 snapshots
  await pool.query(`DELETE FROM centrum_snapshots WHERE id NOT IN (SELECT id FROM centrum_snapshots ORDER BY timestamp DESC LIMIT 30)`)
  return id
}

export async function getLatestCentrumSnapshot(): Promise<any | null> {
  const r = await pool.query('SELECT * FROM centrum_snapshots ORDER BY timestamp DESC LIMIT 1')
  return r.rows[0] ?? null
}

export async function upsertCommitment(id: string, description: string, sourceType: string, sourceRef: string | null, committedTo: string | null, dueAt: string | null, status: string, confidence: number): Promise<void> {
  await pool.query(
    `INSERT INTO commitments (id, description, source_type, source_ref, committed_to, due_at, status, ai_confidence)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO UPDATE SET
     description=$2, source_type=$3, source_ref=$4, committed_to=$5, due_at=$6, status=$7, ai_confidence=$8`,
    [id, description, sourceType, sourceRef, committedTo, dueAt, status, confidence]
  )
}

// ── Query Memory (30-minute conversational context) ──────────────

export async function insertQueryMemory(queryText: string, responseText: string, entities: string[]): Promise<string> {
  const id = crypto.randomUUID()
  await pool.query(
    'INSERT INTO query_memory (id, query_text, response_text, entities_mentioned) VALUES ($1, $2, $3, $4)',
    [id, queryText.slice(0, 1000), responseText.slice(0, 2000), JSON.stringify(entities)]
  )
  return id
}

export async function getRecentQueryMemory(limit: number = 10, withinMinutes: number = 30): Promise<any[]> {
  const cutoff = new Date(Date.now() - withinMinutes * 60 * 1000).toISOString()
  const r = await pool.query(
    'SELECT query_text, response_text, entities_mentioned, timestamp FROM query_memory WHERE timestamp >= $1 ORDER BY timestamp DESC LIMIT $2',
    [cutoff, limit]
  )
  return r.rows.reverse() // oldest first for conversation flow
}

export async function pruneQueryMemory(olderThanMinutes: number = 30): Promise<number> {
  const cutoff = new Date(Date.now() - olderThanMinutes * 60 * 1000).toISOString()
  const r = await pool.query('DELETE FROM query_memory WHERE timestamp < $1', [cutoff])
  return r.rowCount ?? 0
}

export async function forgetLastQuery(): Promise<number> {
  const r = await pool.query(
    'DELETE FROM query_memory WHERE id = (SELECT id FROM query_memory ORDER BY timestamp DESC LIMIT 1)'
  )
  return r.rowCount ?? 0
}

export async function forgetLastNQueries(n: number): Promise<number> {
  const r = await pool.query(
    'DELETE FROM query_memory WHERE id IN (SELECT id FROM query_memory ORDER BY timestamp DESC LIMIT $1)',
    [n]
  )
  return r.rowCount ?? 0
}

export async function forgetQueriesByKeyword(keyword: string): Promise<number> {
  const cutoff = new Date(Date.now() - 60 * 60 * 1000).toISOString() // last hour
  const r = await pool.query(
    "DELETE FROM query_memory WHERE timestamp >= $1 AND (query_text ILIKE $2 OR response_text ILIKE $2 OR entities_mentioned::text ILIKE $2)",
    [cutoff, `%${keyword}%`]
  )
  return r.rowCount ?? 0
}

export async function forgetQueriesInWindow(minutes: number): Promise<number> {
  const cutoff = new Date(Date.now() - minutes * 60 * 1000).toISOString()
  const r = await pool.query(
    'DELETE FROM query_memory WHERE timestamp >= $1',
    [cutoff]
  )
  return r.rowCount ?? 0
}

export async function forgetAllQueries(): Promise<number> {
  const r = await pool.query('DELETE FROM query_memory')
  return r.rowCount ?? 0
}

// ── Correction Log (active learning) ─────────────────────────────

export async function insertCorrection(
  triggerAction: string,
  triggerContext: string,
  userAction: string,
  correctionText: string,
  reasoningChain: string,
  affectedClaims: string[]
): Promise<string> {
  const id = crypto.randomUUID()
  await pool.query(
    `INSERT INTO correction_log (id, trigger_action, trigger_context, user_action, correction_text, reasoning_chain, affected_claims)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [id, triggerAction, triggerContext.slice(0, 2000), userAction, correctionText.slice(0, 2000), reasoningChain.slice(0, 2000), JSON.stringify(affectedClaims)]
  )
  return id
}

export async function getRecentCorrections(limit: number = 20): Promise<any[]> {
  const r = await pool.query(
    'SELECT * FROM correction_log WHERE applied_to_persona = false ORDER BY timestamp DESC LIMIT $1',
    [limit]
  )
  return r.rows
}

export async function getAllCorrections(limit: number = 50): Promise<any[]> {
  const r = await pool.query('SELECT * FROM correction_log ORDER BY timestamp DESC LIMIT $1', [limit])
  return r.rows
}

export async function markCorrectionsApplied(ids: string[]): Promise<void> {
  if (ids.length === 0) return
  await pool.query(
    `UPDATE correction_log SET applied_to_persona = true WHERE id = ANY($1)`,
    [ids]
  )
}

// ── Lessons Learned ──────────────────────────────────────────────

export async function upsertLesson(originalBelief: string, correctedTo: string, source: string): Promise<string> {
  // Check if a similar lesson exists (by original belief prefix)
  const key = originalBelief.slice(0, 80).toLowerCase()
  const existing = await pool.query(
    "SELECT id, times_reinforced FROM lessons_learned WHERE active = true AND LOWER(SUBSTRING(original_belief, 1, 80)) = $1",
    [key]
  )

  if (existing.rows.length > 0) {
    const e = existing.rows[0]
    await pool.query(
      'UPDATE lessons_learned SET corrected_to = $1, times_reinforced = times_reinforced + 1, timestamp = NOW() WHERE id = $2',
      [correctedTo, e.id]
    )
    return e.id
  }

  const id = crypto.randomUUID()
  await pool.query(
    'INSERT INTO lessons_learned (id, original_belief, corrected_to, correction_source) VALUES ($1, $2, $3, $4)',
    [id, originalBelief, correctedTo, source]
  )
  return id
}

export async function getActiveLessons(): Promise<any[]> {
  const r = await pool.query('SELECT * FROM lessons_learned WHERE active = true ORDER BY timestamp DESC LIMIT 30')
  return r.rows
}

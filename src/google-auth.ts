import { getPool } from './db'

const TOKEN_URL = 'https://oauth2.googleapis.com/token'

export type AccountType = 'primary' | 'school'

function providerKey(account: AccountType): string {
  return account === 'school' ? 'google_school' : 'google'
}

// Per-account refresh locks
const refreshLocks = new Map<string, Promise<string>>()

export async function saveGoogleTokens(
  accessToken: string,
  refreshToken: string | null,
  expiryDate: number | null,
  email: string,
  account: AccountType = 'primary'
): Promise<void> {
  const pool = getPool()
  const provider = providerKey(account)
  await pool.query(
    `INSERT INTO oauth_tokens (provider, access_token, refresh_token, expiry_date, email, updated_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (provider) DO UPDATE SET
     access_token = $2,
     refresh_token = COALESCE($3, oauth_tokens.refresh_token),
     expiry_date = $4,
     email = $5,
     updated_at = NOW()`,
    [provider, accessToken, refreshToken, expiryDate, email]
  )
}

export async function getGoogleTokens(account: AccountType = 'primary'): Promise<{
  access_token: string
  refresh_token: string | null
  expiry_date: number | null
  email: string | null
} | null> {
  const pool = getPool()
  const provider = providerKey(account)
  const r = await pool.query(`SELECT access_token, refresh_token, expiry_date, email FROM oauth_tokens WHERE provider = $1`, [provider])
  if (r.rows.length === 0) return null
  const row = r.rows[0]
  return {
    access_token: row.access_token,
    refresh_token: row.refresh_token || null,
    expiry_date: row.expiry_date ? parseInt(row.expiry_date) : null,
    email: row.email || null
  }
}

export async function getValidAccessToken(account: AccountType = 'primary'): Promise<string> {
  const key = providerKey(account)
  const existing = refreshLocks.get(key)
  if (existing) return existing

  const tokens = await getGoogleTokens(account)
  if (!tokens) throw new Error(`Google ${account} not connected`)

  if (tokens.expiry_date && Date.now() < tokens.expiry_date - 60_000) {
    return tokens.access_token
  }

  if (!tokens.refresh_token) throw new Error(`No refresh token for ${account}`)

  const promise = doRefresh(tokens.refresh_token, tokens.email, account)
  refreshLocks.set(key, promise)
  try {
    return await promise
  } finally {
    refreshLocks.delete(key)
  }
}

async function doRefresh(refreshToken: string, email: string | null, account: AccountType): Promise<string> {
  const pool = getPool()
  const clientIdRes = await pool.query(`SELECT value FROM settings WHERE key = 'google_client_id'`)
  const clientSecretRes = await pool.query(`SELECT value FROM settings WHERE key = 'google_client_secret'`)

  const clientId = clientIdRes.rows[0]?.value
  const clientSecret = clientSecretRes.rows[0]?.value

  if (!clientId || !clientSecret) throw new Error('Google client ID/secret not configured on server')

  const res = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      refresh_token: refreshToken,
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'refresh_token'
    })
  })

  if (!res.ok) throw new Error(`Token refresh failed for ${account}`)

  const data = await res.json() as { access_token: string; expires_in?: number }
  const expiryDate = data.expires_in ? Date.now() + data.expires_in * 1000 : null

  await saveGoogleTokens(data.access_token, refreshToken, expiryDate, email || '', account)
  return data.access_token
}

export async function isGoogleConnected(account: AccountType = 'primary'): Promise<boolean> {
  const tokens = await getGoogleTokens(account)
  return tokens !== null && tokens.refresh_token !== null
}

export async function getGoogleEmail(account: AccountType = 'primary'): Promise<string | null> {
  const tokens = await getGoogleTokens(account)
  return tokens?.email || null
}

export async function disconnectGoogle(account: AccountType = 'primary'): Promise<void> {
  const pool = getPool()
  await pool.query(`DELETE FROM oauth_tokens WHERE provider = $1`, [providerKey(account)])
}

/** Returns all connected Google accounts */
export async function getConnectedAccounts(): Promise<{ account: AccountType; email: string }[]> {
  const pool = getPool()
  const r = await pool.query(`SELECT provider, email FROM oauth_tokens WHERE provider IN ('google', 'google_school')`)
  return r.rows.map(row => ({
    account: row.provider === 'google_school' ? 'school' as AccountType : 'primary' as AccountType,
    email: row.email || ''
  }))
}

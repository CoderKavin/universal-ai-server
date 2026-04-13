import { getPool } from './db'

const TOKEN_URL = 'https://oauth2.googleapis.com/token'

// Token refresh lock to prevent concurrent refreshes
let refreshing: Promise<string> | null = null

export async function saveGoogleTokens(
  accessToken: string,
  refreshToken: string | null,
  expiryDate: number | null,
  email: string
): Promise<void> {
  const pool = getPool()
  await pool.query(
    `INSERT INTO oauth_tokens (provider, access_token, refresh_token, expiry_date, email, updated_at)
     VALUES ('google', $1, $2, $3, $4, NOW())
     ON CONFLICT (provider) DO UPDATE SET
     access_token = $1,
     refresh_token = COALESCE($2, oauth_tokens.refresh_token),
     expiry_date = $3,
     email = $4,
     updated_at = NOW()`,
    [accessToken, refreshToken, expiryDate, email]
  )
}

export async function getGoogleTokens(): Promise<{
  access_token: string
  refresh_token: string | null
  expiry_date: number | null
  email: string | null
} | null> {
  const pool = getPool()
  const r = await pool.query(`SELECT access_token, refresh_token, expiry_date, email FROM oauth_tokens WHERE provider = 'google'`)
  if (r.rows.length === 0) return null
  const row = r.rows[0]
  return {
    access_token: row.access_token,
    refresh_token: row.refresh_token || null,
    expiry_date: row.expiry_date ? parseInt(row.expiry_date) : null,
    email: row.email || null
  }
}

export async function getValidAccessToken(): Promise<string> {
  // If a refresh is already in progress, wait for it
  if (refreshing) return refreshing

  const tokens = await getGoogleTokens()
  if (!tokens) throw new Error('Google not connected')

  // If token hasn't expired (with 60s buffer), use it
  if (tokens.expiry_date && Date.now() < tokens.expiry_date - 60_000) {
    return tokens.access_token
  }

  // Need to refresh
  if (!tokens.refresh_token) throw new Error('No refresh token available')

  refreshing = doRefresh(tokens.refresh_token, tokens.email)
  try {
    return await refreshing
  } finally {
    refreshing = null
  }
}

async function doRefresh(refreshToken: string, email: string | null): Promise<string> {
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

  if (!res.ok) throw new Error('Token refresh failed')

  const data = await res.json() as { access_token: string; expires_in?: number }
  const expiryDate = data.expires_in ? Date.now() + data.expires_in * 1000 : null

  await saveGoogleTokens(data.access_token, refreshToken, expiryDate, email || '')
  return data.access_token
}

export async function isGoogleConnected(): Promise<boolean> {
  const tokens = await getGoogleTokens()
  return tokens !== null && tokens.refresh_token !== null
}

export async function getGoogleEmail(): Promise<string | null> {
  const tokens = await getGoogleTokens()
  return tokens?.email || null
}

export async function disconnectGoogle(): Promise<void> {
  const pool = getPool()
  await pool.query(`DELETE FROM oauth_tokens WHERE provider = 'google'`)
}

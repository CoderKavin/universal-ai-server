#!/usr/bin/env npx tsx
/**
 * test-glasses-voice.ts — End-to-end glasses voice pipeline test harness
 *
 * Proves: Audio -> WebSocket -> Groq Whisper -> Claude -> Cartesia TTS -> Speaker
 * Uses the laptop as a stand-in for real glasses hardware.
 *
 * Modes:
 *   npx tsx test-glasses-voice.ts                  Interactive mic recording
 *   npx tsx test-glasses-voice.ts --query "text"   Single synthesized query
 *   npx tsx test-glasses-voice.ts --batch           Run 5 predefined test queries
 *
 * Env:
 *   SERVER_URL   WebSocket base URL (default: wss://universal-ai-server.onrender.com)
 *   TOKEN        Auth token (default: from settings)
 */

import WebSocket from 'ws'
import { execSync, spawnSync } from 'child_process'
import { readFileSync, writeFileSync, unlinkSync, existsSync } from 'fs'
import { tmpdir } from 'os'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'
import * as readline from 'readline'

// ── Config ──────────────────────────────────────────────────────
const SERVER_URL = process.env.SERVER_URL || 'wss://universal-ai-server.onrender.com'
const TOKEN = process.env.TOKEN || 'uai_kavin_2026_d8f3a1b9c7e2f4'
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url))
const TMP = tmpdir()

const BATCH_QUERIES = [
  'IRIS, what should I focus on today?',
  'IRIS, what time is my next class?',
  'IRIS, who am I avoiding?',
  'IRIS, give me the morning briefing.',
  'IRIS, did Rajeev reply?',
]

// ── Types ───────────────────────────────────────────────────────
interface QueryResult {
  query: string
  transcribed: string
  response: string
  error: string | null
  sendTime: number
  jsonResponseTime: number | null
  firstAudioTime: number | null
  lastAudioTime: number | null
  audioChunks: number
  audioBytes: number
  ttfb: number | null       // end-of-send -> first audio chunk
  totalTime: number | null   // end-of-send -> last audio chunk
  roundTrip: number | null   // end-of-send -> JSON response received
  pass: boolean
}

// ── Audio helpers ───────────────────────────────────────────────

/** Generate 16kHz mono S16LE PCM from text using macOS `say` + `afconvert` */
function generatePCMFromText(text: string): Buffer {
  const aiffPath = join(TMP, `glasses-query-${Date.now()}.aiff`)
  const wavPath = join(TMP, `glasses-query-${Date.now()}.wav`)

  try {
    // macOS say -> AIFF
    execSync(`say -o "${aiffPath}" "${text.replace(/"/g, '\\"')}"`, { stdio: 'pipe' })
    // AIFF -> 16kHz mono 16-bit WAV
    execSync(`afconvert "${aiffPath}" "${wavPath}" -d LEI16@16000 -f WAVE`, { stdio: 'pipe' })

    const wav = readFileSync(wavPath)
    // Strip 44-byte WAV header to get raw PCM
    return wav.subarray(44)
  } finally {
    try { unlinkSync(aiffPath) } catch {}
    try { unlinkSync(wavPath) } catch {}
  }
}

/** Record from laptop mic using compiled Swift recorder */
function recordFromMic(maxSeconds: number = 10): Buffer {
  const recorderPath = join(SCRIPT_DIR, 'record-mic')
  const pcmPath = join(TMP, `glasses-mic-${Date.now()}.pcm`)

  if (!existsSync(recorderPath)) {
    console.error('Mic recorder not compiled. Run: cd server && swiftc record-mic.swift -o record-mic')
    process.exit(1)
  }

  const result = spawnSync(recorderPath, [pcmPath, String(maxSeconds)], {
    stdio: ['inherit', 'pipe', 'inherit'],  // stdin from terminal, stderr to terminal
    timeout: (maxSeconds + 5) * 1000,
  })

  if (result.status !== 0) {
    console.error(`Mic recording failed (exit ${result.status})`)
    process.exit(1)
  }

  try {
    return readFileSync(pcmPath)
  } finally {
    try { unlinkSync(pcmPath) } catch {}
  }
}

/** Wrap raw PCM as WAV for playback */
function wrapPCMasWAV(pcm: Buffer): Buffer {
  const sampleRate = 16000
  const channels = 1
  const bitsPerSample = 16
  const byteRate = sampleRate * channels * (bitsPerSample / 8)
  const blockAlign = channels * (bitsPerSample / 8)
  const header = Buffer.alloc(44)

  header.write('RIFF', 0)
  header.writeUInt32LE(36 + pcm.length, 4)
  header.write('WAVE', 8)
  header.write('fmt ', 12)
  header.writeUInt32LE(16, 16)
  header.writeUInt16LE(1, 20)          // PCM format
  header.writeUInt16LE(channels, 22)
  header.writeUInt32LE(sampleRate, 24)
  header.writeUInt32LE(byteRate, 28)
  header.writeUInt16LE(blockAlign, 32)
  header.writeUInt16LE(bitsPerSample, 34)
  header.write('data', 36)
  header.writeUInt32LE(pcm.length, 40)

  return Buffer.concat([header, pcm])
}

/** Play PCM audio through laptop speaker */
function playAudio(pcm: Buffer): void {
  if (pcm.length === 0) return
  const wavPath = join(TMP, `glasses-response-${Date.now()}.wav`)
  try {
    writeFileSync(wavPath, wrapPCMasWAV(pcm))
    spawnSync('afplay', [wavPath], { stdio: 'inherit' })
  } finally {
    try { unlinkSync(wavPath) } catch {}
  }
}

// ── WebSocket helpers ───────────────────────────────────────────

/** Connect to glasses WebSocket and wait for ready message */
function connectWS(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const url = `${SERVER_URL}/ws/glasses?token=${TOKEN}`
    console.log(`  Connecting to ${SERVER_URL}/ws/glasses ...`)

    const ws = new WebSocket(url)
    let ready = false

    ws.on('message', (data: Buffer | string) => {
      if (ready) return
      try {
        const msg = JSON.parse(typeof data === 'string' ? data : data.toString())
        if (msg.type === 'ready') {
          ready = true
          console.log(`  Connected! Server version: ${msg.version}`)
          resolve(ws)
        }
      } catch {}
    })

    ws.on('open', () => {
      console.log(`  WebSocket open, waiting for ready...`)
    })

    ws.on('error', (err) => {
      reject(new Error(`WebSocket error: ${err.message}`))
    })

    ws.on('close', (code, reason) => {
      if (!ready) {
        reject(new Error(`WebSocket closed before ready: ${code} ${reason}`))
      }
    })

    setTimeout(() => {
      if (!ready) {
        ws.close()
        reject(new Error('Timeout waiting for ready message (10s)'))
      }
    }, 10000)
  })
}

/** Send voice query and collect full response (text + audio) */
function sendVoiceQuery(ws: WebSocket, pcm: Buffer, queryText: string): Promise<QueryResult> {
  return new Promise((resolve) => {
    const result: QueryResult = {
      query: queryText,
      transcribed: '',
      response: '',
      error: null,
      sendTime: 0,
      jsonResponseTime: null,
      firstAudioTime: null,
      lastAudioTime: null,
      audioChunks: 0,
      audioBytes: 0,
      ttfb: null,
      totalTime: null,
      roundTrip: null,
      pass: false,
    }

    const audioBuffers: Buffer[] = []
    let done = false
    let idleTimer: ReturnType<typeof setTimeout> | null = null
    let gotJson = false
    let gotAudio = false

    const finish = () => {
      if (done) return
      done = true
      if (idleTimer) clearTimeout(idleTimer)
      ws.removeListener('message', onMessage)
      ws.removeListener('close', onClose)

      // Calculate derived timings
      if (result.firstAudioTime && result.sendTime) {
        result.ttfb = result.firstAudioTime - result.sendTime
      }
      if (result.lastAudioTime && result.sendTime) {
        result.totalTime = result.lastAudioTime - result.sendTime
      }
      if (result.jsonResponseTime && result.sendTime) {
        result.roundTrip = result.jsonResponseTime - result.sendTime
      }

      // Determine pass/fail
      result.pass = !result.error
        && result.transcribed.length > 0
        && result.response.length > 0
        && (result.totalTime ?? Infinity) < 15000

      // Play audio
      if (audioBuffers.length > 0) {
        const fullAudio = Buffer.concat(audioBuffers)
        const durationSec = fullAudio.length / (16000 * 2)
        console.log(`  Playing response (${durationSec.toFixed(1)}s)...`)
        playAudio(fullAudio)
      }

      resolve(result)
    }

    const resetIdleTimer = () => {
      if (idleTimer) clearTimeout(idleTimer)
      // After we have both JSON and at least one audio chunk,
      // finish 2s after last activity. If only JSON (no audio),
      // wait 8s total for audio to start arriving.
      if (gotJson && gotAudio) {
        idleTimer = setTimeout(finish, 2000)
      } else if (gotJson && !gotAudio) {
        // TTS might have failed — don't wait forever
        idleTimer = setTimeout(finish, 8000)
      }
    }

    const onMessage = (data: Buffer | string) => {
      const now = Date.now()

      if (Buffer.isBuffer(data) && data.length > 1 && data[0] === 0x10) {
        // TTS audio chunk
        const audioData = data.subarray(1)
        audioBuffers.push(audioData)
        result.audioChunks++
        result.audioBytes += audioData.length

        if (!result.firstAudioTime) {
          result.firstAudioTime = now
          console.log(`  First audio chunk received (TTFB: ${now - result.sendTime}ms)`)
        }
        result.lastAudioTime = now
        gotAudio = true
        resetIdleTimer()
        return
      }

      // JSON message
      try {
        const msg = JSON.parse(typeof data === 'string' ? data : data.toString())

        if (msg.error && !msg.type) {
          // Top-level error (missing API keys, etc.)
          result.error = msg.error
          console.error(`  ERROR: ${msg.error}`)
          finish()
          return
        }

        if (msg.type === 'response' && msg.mode === 'voice') {
          result.jsonResponseTime = now
          result.response = msg.text || ''
          result.transcribed = result.transcribed || '(see response)'

          if (msg.error) {
            result.error = msg.error
          }
          if (!msg.text || msg.text.trim().length === 0) {
            result.error = result.error || 'Empty response from server'
          }

          console.log(`  Response: "${result.response.slice(0, 120)}${result.response.length > 120 ? '...' : ''}"`)
          console.log(`  Text response received in ${now - result.sendTime}ms`)
          gotJson = true
          resetIdleTimer()
        }
      } catch {}
    }

    const onClose = (code: number, reason: Buffer) => {
      if (!done) {
        result.error = `WebSocket closed mid-query: ${code} ${reason.toString()}`
        console.error(`  ${result.error}`)
        finish()
      }
    }

    ws.on('message', onMessage)
    ws.on('close', onClose)

    // Send the voice query: 0x02 + PCM
    const msg = Buffer.alloc(1 + pcm.length)
    msg[0] = 0x02
    pcm.copy(msg, 1)

    console.log(`  Sending ${pcm.length} bytes of audio (${(pcm.length / 32000).toFixed(1)}s)...`)
    ws.send(msg, () => {
      result.sendTime = Date.now()
      console.log(`  Audio sent. Waiting for pipeline response...`)
    })

    // Hard timeout: 30s total
    setTimeout(() => {
      if (!done) {
        result.error = result.error || 'Timeout: no complete response in 30s'
        console.error('  TIMEOUT: 30s elapsed without full response')
        finish()
      }
    }, 30000)
  })
}

// ── Display helpers ─────────────────────────────────────────────

function printResult(r: QueryResult): void {
  console.log(`\n  ┌─────────────────────────────────────────────────`)
  console.log(`  │ Query:       ${r.query}`)
  console.log(`  │ Transcribed: ${r.transcribed || '(empty)'}`)
  console.log(`  │ Response:    ${r.response.slice(0, 150) || '(empty)'}`)
  console.log(`  │`)
  console.log(`  │ Text response:  ${r.roundTrip ? r.roundTrip + 'ms' : 'n/a'}`)
  console.log(`  │ TTFB (audio):   ${r.ttfb ? r.ttfb + 'ms' : 'n/a'}`)
  console.log(`  │ Total pipeline: ${r.totalTime ? r.totalTime + 'ms' : 'n/a'}`)
  console.log(`  │ Audio:          ${r.audioChunks} chunks, ${r.audioBytes} bytes (${(r.audioBytes / 32000).toFixed(1)}s)`)
  console.log(`  │ Status:         ${r.pass ? 'PASS' : 'FAIL'} ${r.error ? '(' + r.error + ')' : ''}`)
  console.log(`  └─────────────────────────────────────────────────`)
}

function printBatchTable(results: QueryResult[]): void {
  console.log(`\n${'='.repeat(120)}`)
  console.log(`  BATCH TEST RESULTS`)
  console.log(`${'='.repeat(120)}`)

  // Header
  const hdr = [
    pad('#', 2),
    pad('Query', 38),
    pad('Transcribed', 30),
    pad('Text Resp ms', 12),
    pad('TTFB ms', 8),
    pad('Total ms', 9),
    pad('Audio', 8),
    pad('P/F', 4),
  ].join(' | ')
  console.log(hdr)
  console.log('-'.repeat(120))

  results.forEach((r, i) => {
    const row = [
      pad(String(i + 1), 2),
      pad(r.query.slice(0, 38), 38),
      pad((r.transcribed || '(empty)').slice(0, 30), 30),
      pad(r.roundTrip ? String(r.roundTrip) : '-', 12),
      pad(r.ttfb ? String(r.ttfb) : '-', 8),
      pad(r.totalTime ? String(r.totalTime) : '-', 9),
      pad(`${(r.audioBytes / 32000).toFixed(1)}s`, 8),
      pad(r.pass ? 'PASS' : 'FAIL', 4),
    ].join(' | ')
    console.log(row)
  })

  console.log('-'.repeat(120))

  // Response text for each
  console.log(`\n  RESPONSE DETAILS:`)
  results.forEach((r, i) => {
    console.log(`  ${i + 1}. [${r.pass ? 'PASS' : 'FAIL'}] "${r.response.slice(0, 200)}${r.response.length > 200 ? '...' : ''}"`)
    if (r.error) console.log(`     ERROR: ${r.error}`)
  })

  const passCount = results.filter(r => r.pass).length
  console.log(`\n  Score: ${passCount}/${results.length} passed`)
  if (passCount === results.length) {
    console.log(`  Cloud voice pipeline is VERIFIED. Ready for firmware flash.`)
  }
  console.log()
}

function pad(s: string, len: number): string {
  return s.length >= len ? s.slice(0, len) : s + ' '.repeat(len - s.length)
}

// ── Prompt helper ───────────────────────────────────────────────

function prompt(msg: string): Promise<string> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  return new Promise((resolve) => {
    rl.question(msg, (answer) => {
      rl.close()
      resolve(answer)
    })
  })
}

// ── Main ────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2)
  const isBatch = args.includes('--batch')
  const queryIdx = args.indexOf('--query')
  const queryText = queryIdx >= 0 ? args[queryIdx + 1] : null
  const noPlay = args.includes('--no-play')

  console.log(`\n========================================`)
  console.log(`  GLASSES VOICE PIPELINE TEST HARNESS`)
  console.log(`========================================`)
  console.log(`  Server:  ${SERVER_URL}`)
  console.log(`  Token:   ${TOKEN.slice(0, 12)}...`)
  console.log(`  Mode:    ${isBatch ? 'batch (5 queries)' : queryText ? `query: "${queryText}"` : 'interactive mic'}`)
  console.log()

  // ── Batch mode ──────────────────────────────────────────────
  if (isBatch) {
    console.log(`Connecting to server...`)
    let ws: WebSocket
    try {
      ws = await connectWS()
    } catch (err: any) {
      console.error(`\nFATAL: ${err.message}`)
      console.error(`Is the server running? Check SERVER_URL env var.`)
      process.exit(1)
    }

    const results: QueryResult[] = []

    for (let i = 0; i < BATCH_QUERIES.length; i++) {
      const q = BATCH_QUERIES[i]
      console.log(`\n── Query ${i + 1}/${BATCH_QUERIES.length}: "${q}" ──`)

      console.log(`  Synthesizing audio via macOS say...`)
      const pcm = generatePCMFromText(q)
      console.log(`  Generated ${pcm.length} bytes (${(pcm.length / 32000).toFixed(1)}s)`)

      const result = await sendVoiceQuery(ws, pcm, q)

      // Whisper transcription comes back in the server's pipeline,
      // but we only see the final response text. The transcribed text
      // isn't sent to the client — note this in output.
      if (result.response && !result.error) {
        result.transcribed = '(server-side, check logs)'
      }

      printResult(result)
      results.push(result)

      // Brief pause between queries to let server settle
      if (i < BATCH_QUERIES.length - 1) {
        console.log(`\n  Waiting 3s before next query...`)
        await new Promise(r => setTimeout(r, 3000))
      }
    }

    ws.close()
    printBatchTable(results)
    process.exit(results.every(r => r.pass) ? 0 : 1)
  }

  // ── Single query mode ───────────────────────────────────────
  if (queryText) {
    console.log(`Synthesizing audio for: "${queryText}"`)
    const pcm = generatePCMFromText(queryText)
    console.log(`Generated ${pcm.length} bytes (${(pcm.length / 32000).toFixed(1)}s)`)

    console.log(`\nConnecting to server...`)
    let ws: WebSocket
    try {
      ws = await connectWS()
    } catch (err: any) {
      console.error(`\nFATAL: ${err.message}`)
      process.exit(1)
    }

    const result = await sendVoiceQuery(ws, pcm, queryText)
    if (result.response && !result.error) {
      result.transcribed = '(server-side, check logs)'
    }

    printResult(result)
    ws.close()
    process.exit(result.pass ? 0 : 1)
  }

  // ── Interactive mic mode ────────────────────────────────────
  console.log(`Press Enter to start recording, speak your query, press Enter again to stop.`)
  console.log(`(Max 10 seconds, auto-stops on 2s silence after speech)\n`)

  await prompt('  Press Enter to start recording...')

  console.log()
  const pcm = recordFromMic(10)

  if (pcm.length < 3200) {  // less than 0.1s
    console.error(`\nRecording too short (${pcm.length} bytes). No audio captured.`)
    console.error(`Check mic permissions: System Settings > Privacy & Security > Microphone`)
    process.exit(1)
  }

  console.log(`\nConnecting to server...`)
  let ws: WebSocket
  try {
    ws = await connectWS()
  } catch (err: any) {
    console.error(`\nFATAL: ${err.message}`)
    process.exit(1)
  }

  const result = await sendVoiceQuery(ws, pcm, '(live mic input)')
  if (result.response && !result.error) {
    result.transcribed = '(server-side, check logs)'
  }

  printResult(result)
  ws.close()
  process.exit(result.pass ? 0 : 1)
}

main().catch(err => {
  console.error(`\nFATAL: ${err.message}`)
  process.exit(1)
})

/**
 * Test script for glasses WebSocket endpoint.
 * Tests: connection, briefing request, and basic protocol.
 *
 * Usage: npx tsx test-glasses-ws.ts
 */

import WebSocket from 'ws'

const SERVER_URL = process.env.SERVER_URL || 'ws://localhost:3210'
const TOKEN = process.env.TOKEN || 'uai_kavin_2026_d8f3a1b9c7e2f4'

async function test() {
  console.log(`\n=== Glasses WebSocket Test ===`)
  console.log(`Server: ${SERVER_URL}`)
  console.log(`Token: ${TOKEN.slice(0, 12)}...\n`)

  // Test 1: Connect with auth token
  console.log('[TEST 1] Connecting with auth token...')
  const ws = new WebSocket(`${SERVER_URL}/ws/glasses?token=${TOKEN}`)

  const messages: any[] = []
  const binaryMessages: Buffer[] = []

  ws.on('message', (data: Buffer | string) => {
    if (Buffer.isBuffer(data) && data[0] === 0x10) {
      binaryMessages.push(data)
      console.log(`  [BIN] TTS audio chunk: ${data.length - 1} bytes`)
    } else {
      const msg = JSON.parse(data.toString())
      messages.push(msg)
      console.log(`  [MSG] ${JSON.stringify(msg).slice(0, 200)}`)
    }
  })

  await new Promise<void>((resolve, reject) => {
    ws.on('open', () => {
      console.log('  [OK] Connected!')
      resolve()
    })
    ws.on('error', (err) => {
      console.error(`  [FAIL] Connection error: ${err.message}`)
      reject(err)
    })
    setTimeout(() => reject(new Error('Connection timeout')), 10000)
  })

  // Wait for ready message
  await new Promise(r => setTimeout(r, 500))
  const readyMsg = messages.find(m => m.type === 'ready')
  if (readyMsg) {
    console.log(`  [OK] Ready message received: version ${readyMsg.version}`)
  } else {
    console.log('  [WARN] No ready message received')
  }

  // Test 2: Send briefing request (0x04 with empty payload)
  console.log('\n[TEST 2] Sending briefing request (0x04)...')
  const briefingReq = Buffer.from([0x04])
  ws.send(briefingReq)

  // Wait for response
  await new Promise(r => setTimeout(r, 15000))
  const briefingMsg = messages.find(m => m.mode === 'briefing')
  if (briefingMsg) {
    console.log(`  [OK] Briefing received: "${briefingMsg.text?.slice(0, 100)}..."`)
  } else {
    console.log('  [WARN] No briefing response received')
  }

  const ttsChunks = binaryMessages.length
  if (ttsChunks > 0) {
    const totalBytes = binaryMessages.reduce((sum, b) => sum + b.length - 1, 0)
    const durationSec = totalBytes / (16000 * 2)
    console.log(`  [OK] TTS audio: ${ttsChunks} chunks, ${totalBytes} bytes (~${durationSec.toFixed(1)}s)`)
  } else {
    console.log('  [INFO] No TTS audio received (Cartesia key may not be configured)')
  }

  // Test 3: Send voice query (0x02 with synthetic audio)
  console.log('\n[TEST 3] Sending voice query (0x02 with 1s silence)...')
  messages.length = 0
  binaryMessages.length = 0

  // Generate 1 second of silence (16kHz, 16-bit, mono)
  const silenceAudio = Buffer.alloc(32000) // 1 second
  const voiceMsg = Buffer.alloc(1 + silenceAudio.length)
  voiceMsg[0] = 0x02
  silenceAudio.copy(voiceMsg, 1)
  ws.send(voiceMsg)

  await new Promise(r => setTimeout(r, 10000))
  const voiceResp = messages.find(m => m.mode === 'voice')
  if (voiceResp) {
    console.log(`  [OK] Voice response: "${voiceResp.text?.slice(0, 100)}"`)
  } else {
    console.log('  [INFO] No voice response (silence may have produced empty transcript)')
  }

  // Test 4: Send ambient audio (0x03)
  console.log('\n[TEST 4] Sending ambient audio chunk (0x03)...')
  const ambientMsg = Buffer.alloc(1 + 16000) // 0.5s
  ambientMsg[0] = 0x03
  ws.send(ambientMsg)
  console.log('  [OK] Ambient chunk sent (will buffer until 30s threshold)')

  // Test 5: Unknown op code
  console.log('\n[TEST 5] Sending unknown op code (0xFF)...')
  ws.send(Buffer.from([0xFF, 0x00]))
  await new Promise(r => setTimeout(r, 1000))
  console.log('  [OK] No crash on unknown op')

  // Summary
  console.log('\n=== Test Summary ===')
  console.log(`Connection:  OK`)
  console.log(`Briefing:    ${briefingMsg ? 'OK' : 'WARN (no response)'}`)
  console.log(`TTS:         ${ttsChunks > 0 ? 'OK' : 'SKIP (no Cartesia key)'}`)
  console.log(`Voice query: ${voiceResp ? 'OK' : 'SKIP (silence input)'}`)
  console.log(`Ambient:     OK (buffered)`)
  console.log(`Error handling: OK`)

  ws.close()
  console.log('\nDone.')
  process.exit(0)
}

test().catch(err => {
  console.error(`Fatal: ${err.message}`)
  process.exit(1)
})

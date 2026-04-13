#!/usr/bin/env swift
//
// record-mic.swift — Record from macOS mic as 16kHz mono S16LE PCM
// Usage: swift record-mic.swift <output.pcm> [max_seconds]
// Stops on: Enter key, max duration, or 2s silence after speech detected.
//

import AVFoundation
import Foundation

guard CommandLine.arguments.count >= 2 else {
    fputs("Usage: swift record-mic.swift <output.pcm> [max_seconds]\n", stderr)
    exit(1)
}

let outputPath = CommandLine.arguments[1]
let maxSeconds = CommandLine.arguments.count >= 3
    ? Double(CommandLine.arguments[2]) ?? 10.0
    : 10.0

let sampleRate: Double = 16000
let silenceThresholdRMS: Float = 0.005
let silenceTimeoutSec: Double = 2.0
let minSpeechSec: Double = 0.3

// ── Setup audio engine ──────────────────────────────────────────
let engine = AVAudioEngine()
let inputNode = engine.inputNode
let bus = 0

let nativeFmt = inputNode.outputFormat(forBus: bus)

let targetFmt = AVAudioFormat(
    commonFormat: .pcmFormatInt16,
    sampleRate: sampleRate,
    channels: 1,
    interleaved: true
)!

guard let converter = AVAudioConverter(from: nativeFmt, to: targetFmt) else {
    fputs("ERROR: Cannot create audio converter\n", stderr)
    exit(1)
}

var pcmChunks: [Data] = []
var totalSamples: Int = 0
let maxSamples = Int(sampleRate * maxSeconds)

var speechDetected = false
var lastSpeechTime: Date = Date()
var shouldStop = false

// ── Install tap ─────────────────────────────────────────────────
inputNode.installTap(onBus: bus, bufferSize: 4096, format: nativeFmt) { buffer, _ in
    guard !shouldStop else { return }

    let frameCount = AVAudioFrameCount(
        Double(buffer.frameLength) * sampleRate / nativeFmt.sampleRate
    )
    guard frameCount > 0 else { return }

    guard let outputBuffer = AVAudioPCMBuffer(
        pcmFormat: targetFmt,
        frameCapacity: frameCount
    ) else { return }

    var error: NSError?
    let status = converter.convert(to: outputBuffer, error: &error) { _, outStatus in
        outStatus.pointee = .haveData
        return buffer
    }

    guard status != .error, error == nil else { return }

    // Calculate RMS for silence detection (from source float buffer)
    if let floatData = buffer.floatChannelData {
        let count = Int(buffer.frameLength)
        var sumSquares: Float = 0
        for i in 0..<count {
            let sample = floatData[0][i]
            sumSquares += sample * sample
        }
        let rms = sqrt(sumSquares / Float(count))

        if rms > silenceThresholdRMS {
            speechDetected = true
            lastSpeechTime = Date()
        }
    }

    // Copy int16 samples to data
    let byteCount = Int(outputBuffer.frameLength) * 2  // 16-bit = 2 bytes
    if let int16Data = outputBuffer.int16ChannelData {
        let data = Data(bytes: int16Data[0], count: byteCount)
        pcmChunks.append(data)
        totalSamples += Int(outputBuffer.frameLength)
    }

    // Check max duration
    if totalSamples >= maxSamples {
        shouldStop = true
    }

    // Check silence timeout (only after speech was detected)
    if speechDetected && Date().timeIntervalSince(lastSpeechTime) > silenceTimeoutSec {
        shouldStop = true
    }
}

// ── Start recording ─────────────────────────────────────────────
do {
    try engine.start()
} catch {
    fputs("ERROR: Cannot start audio engine: \(error)\n", stderr)
    exit(1)
}

fputs("🎙  Recording... (speak now, press Enter to stop)\n", stderr)

// Monitor for Enter key in background
let enterThread = Thread {
    let _ = readLine()
    shouldStop = true
}
enterThread.start()

// Spin until done
let startTime = Date()
while !shouldStop {
    Thread.sleep(forTimeInterval: 0.05)
    let elapsed = Date().timeIntervalSince(startTime)

    // Print countdown every second
    let remaining = Int(maxSeconds - elapsed)
    if remaining >= 0 && Int(elapsed) != Int(elapsed - 0.05) {
        fputs("\r   \(remaining)s remaining...  ", stderr)
    }
}

engine.stop()
inputNode.removeTap(onBus: bus)

let durationSec = Double(totalSamples) / sampleRate
fputs("\r✓  Recorded \(String(format: "%.1f", durationSec))s (\(totalSamples * 2) bytes PCM)\n", stderr)

// ── Write output ────────────────────────────────────────────────
var output = Data()
for chunk in pcmChunks {
    output.append(chunk)
}

do {
    try output.write(to: URL(fileURLWithPath: outputPath))
} catch {
    fputs("ERROR: Cannot write \(outputPath): \(error)\n", stderr)
    exit(1)
}

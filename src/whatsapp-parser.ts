/**
 * WhatsApp observation parser.
 *
 * Raw WhatsApp observations arrive as free-form text in one of these shapes:
 *   "To +91 NNNNN NNNNN: <message>"                  → sent, phone-only
 *   "From <Name>: <message>"                         → received, 1-on-1
 *   "From <Name> (group): <message>"                 → received, group message
 *   "Active WhatsApp conversations: <summary>"       → aggregate recap (skip)
 *
 * This module extracts structured fields and normalizes phone numbers.
 */

export interface ParsedWhatsApp {
  direction: 'sent' | 'received'
  phone: string | null          // E.164-ish, e.g. '+919217747797'
  raw_name: string | null       // name as it appeared (with emoji)
  display_name: string | null   // name stripped of emoji / status markers
  is_group: boolean
  message: string               // the body
  summary_only: boolean         // true for 'Active WhatsApp conversations: ...'
}

/** Normalize a phone number: strip everything except digits and leading '+'. */
export function normalizePhone(raw: string): string | null {
  if (!raw) return null
  const trimmed = raw.trim()
  const hasPlus = trimmed.startsWith('+')
  const digits = trimmed.replace(/\D/g, '')
  if (digits.length < 7 || digits.length > 15) return null
  return hasPlus ? `+${digits}` : digits
}

/**
 * Strip emoji, variation selectors, ZWJs, and common status markers
 * so that "IBDP💔" and "Dad ✈" both normalize to something comparable.
 */
export function cleanName(raw: string): string {
  if (!raw) return ''
  return raw
    // Remove emoji ranges, variation selectors, ZWJ
    .replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{FE00}-\u{FE0F}\u{200D}]/gu, '')
    // Strip stray punctuation from edges
    .replace(/^[\s"'.,:;]+|[\s"'.,:;]+$/g, '')
    .trim()
}

/** Parse a single WhatsApp observation raw_content. Returns null if not a message. */
export function parseWhatsAppObservation(rawContent: string): ParsedWhatsApp | null {
  if (!rawContent || typeof rawContent !== 'string') return null
  const text = rawContent.trim()
  if (!text) return null

  // Aggregate recap — skip, not an individual message
  if (/^active whatsapp conversations:/i.test(text)) {
    return {
      direction: 'received',
      phone: null,
      raw_name: null,
      display_name: null,
      is_group: false,
      message: text.slice(0, 500),
      summary_only: true
    }
  }

  // Sent: "To +91 NNNNN NNNNN: <msg>"  (phone may or may not have +, may have spaces/dashes)
  const sentMatch = text.match(/^to\s+([+0-9][\d\s\-()]{5,25})\s*:\s*([\s\S]*)$/i)
  if (sentMatch) {
    const phone = normalizePhone(sentMatch[1])
    return {
      direction: 'sent',
      phone,
      raw_name: null,
      display_name: null,
      is_group: false,
      message: sentMatch[2].trim().slice(0, 1000),
      summary_only: false
    }
  }

  // Received group: "From <Name> (group): <msg>"
  const groupMatch = text.match(/^from\s+(.+?)\s*\(group\)\s*:\s*([\s\S]*)$/i)
  if (groupMatch) {
    const rawName = groupMatch[1].trim()
    return {
      direction: 'received',
      phone: null,
      raw_name: rawName,
      display_name: cleanName(rawName),
      is_group: true,
      message: groupMatch[2].trim().slice(0, 1000),
      summary_only: false
    }
  }

  // Received 1-on-1: "From <Name>: <msg>"
  const recvMatch = text.match(/^from\s+(.+?)\s*:\s*([\s\S]*)$/i)
  if (recvMatch) {
    const rawName = recvMatch[1].trim()
    // Guard against malformed "from" lines where the "name" is obviously a phone
    const maybePhone = normalizePhone(rawName)
    if (maybePhone && /^[+0-9\s\-()]+$/.test(rawName)) {
      return {
        direction: 'received',
        phone: maybePhone,
        raw_name: null,
        display_name: null,
        is_group: false,
        message: recvMatch[2].trim().slice(0, 1000),
        summary_only: false
      }
    }
    return {
      direction: 'received',
      phone: null,
      raw_name: rawName,
      display_name: cleanName(rawName),
      is_group: false,
      message: recvMatch[2].trim().slice(0, 1000),
      summary_only: false
    }
  }

  return null
}

/** Is this display name a real person (not a group label or status marker)? */
export function isPersonalName(displayName: string | null): boolean {
  if (!displayName) return false
  const n = displayName.toLowerCase().trim()
  if (n.length < 2 || n.length > 60) return false

  // Group-ish patterns
  if (/group|chat|team|class|club|grade|batch/.test(n)) return false

  // Must contain at least one letter
  if (!/[a-z]/i.test(n)) return false

  return true
}

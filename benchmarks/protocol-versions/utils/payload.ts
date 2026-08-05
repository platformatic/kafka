import { createHash } from 'node:crypto'
import { type MessageRecord } from '../../../src/protocol/records.ts'

export interface PayloadShape {
  /** Number of records. */
  count: number
  /** Bytes per value. */
  valueSize: number
  /** Bytes per key. */
  keySize: number
  /** Number of headers per record. */
  headers: number
}

export const defaultShape: PayloadShape = { count: 100, valueSize: 64, keySize: 8, headers: 2 }

// Captured once per process so every record in a run shares one base.
//
// This was a hardcoded 1700000000000 (November 2023) to make runs byte identical. That is harmless
// for the codec benchmarks, which never touch a broker, but against a real one it is a trap: Kafka
// applies retention by the largest record timestamp in a segment, so a seeded topic was silently
// deleted mid-sweep once the retention thread noticed records dated years before the 7 day default.
// Consumers then read an empty log and looked like they had stalled.
//
// Byte reproducibility survives the change: a batch stores its base timestamp as a fixed width
// INT64 and each record as a varint delta from it, so moving the base changes the values but never
// the lengths.
const baseTimestamp = BigInt(Date.now())

/**
 * Deterministic records for a given shape.
 *
 * partition and timestamp are set explicitly even though the Produce codecs default them, because
 * createRequest fills in only the fields that are missing: leaving them undefined would charge the
 * first version measured for normalising the array and let every later version reuse the result.
 */
export function createMessages (topic: string, shape: PayloadShape = defaultShape): MessageRecord[] {
  const messages: MessageRecord[] = []

  for (let index = 0; index < shape.count; index++) {
    const headers = new Map<Buffer, Buffer>()

    for (let header = 0; header < shape.headers; header++) {
      headers.set(Buffer.from(`h${header}`), Buffer.from(`v${header}-${index % 10}`))
    }

    messages.push({
      topic,
      partition: 0,
      timestamp: baseTimestamp + BigInt(index),
      key: Buffer.alloc(shape.keySize, 48 + (index % 10)),
      value: Buffer.alloc(shape.valueSize, 65 + (index % 26)),
      headers
    })
  }

  return messages
}

/**
 * Fingerprint of a record set, so a run can prove every version was handed the same bytes.
 */
export function payloadChecksum (messages: MessageRecord[]): string {
  const hash = createHash('sha1')

  for (const message of messages) {
    hash.update(message.topic)
    hash.update(String(message.partition))
    hash.update(String(message.timestamp))
    hash.update(message.key ?? Buffer.alloc(0))
    hash.update(message.value)

    for (const [key, value] of message.headers ?? []) {
      hash.update(key)
      hash.update(value)
    }
  }

  return hash.digest('hex').slice(0, 12)
}

export function describeShape (shape: PayloadShape): string {
  return `${shape.count}x${shape.valueSize}B`
}

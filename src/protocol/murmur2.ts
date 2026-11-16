import { LRUCache } from 'mnemonist'

// This is a Javascript port of
// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L492

// 'm' and 'r' are mixing constants generated offline. They're not really 'magic', they just happen to work well.
const seed = 0x9747b28c
const m = 0x5bd1e995
const r = 24

const cache = new LRUCache<string, number>(1000)

/*
  The Java implementation relies on 32-bit signed integer overflow. Math.imul and the bitwise
  operators give exactly those semantics on regular numbers, which is far cheaper than using BigInt.
*/
export function murmur2 (data: string | Buffer, useCache: boolean = false): number {
  let key: string | undefined

  if (typeof data === 'string') {
    if (useCache) {
      key = data
      const existing = key ? cache.get(key) : undefined

      if (existing) {
        return existing
      }
    }

    data = Buffer.from(data)
  }

  const length = data.length
  const aligned = length & ~3

  // Initialize the hash to a random value
  let h = seed ^ length

  for (let i = 0; i < aligned; i += 4) {
    let k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)

    k = Math.imul(k, m)
    k ^= k >>> r
    k = Math.imul(k, m)

    h = Math.imul(h, m) ^ k
  }

  // Read the last few bytes of the input array
  const remaining = length - aligned

  if (remaining > 0) {
    let tail = data[aligned]

    if (remaining > 1) {
      tail |= data[aligned + 1] << 8
    }

    if (remaining > 2) {
      tail |= data[aligned + 2] << 16
    }

    h = Math.imul(h ^ tail, m)
  }

  // Perform the final mixing
  h ^= h >>> 13
  h = Math.imul(h, m)
  h ^= h >>> 15

  const hash = h | 0

  if (key !== undefined) {
    cache.set(key, hash)
  }

  return hash
}

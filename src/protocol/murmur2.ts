import { LRUCache } from 'mnemonist'

// This is a Javascript port of
// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L492

// 'm' and 'r' are mixing constants generated offline. They're not really 'magic', they just happen to work well.
const seed = 0x9747b28cn
const m = 0x5bd1e995n
const r = 24

const cache = new LRUCache<string, number>(1000)
const tail = Buffer.alloc(4)

function tripleRightShift (num: bigint, bits: number) {
  return BigInt(Number(BigInt.asIntN(32, num)) >>> bits)
}

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

  // Initialize the hash to a random value
  let h: number | bigint = seed ^ BigInt(length)

  let i = 0
  while (i < length - 3) {
    let k = BigInt(data.readInt32LE(i))
    i += 4

    k *= m
    k ^= tripleRightShift(k, r)
    k *= m

    h *= m
    h ^= k
  }

  // Read the last few bytes of the input array
  // If this becomes an hot-path, consider to optimize it further and avoid copying
  if (length % 4 > 0) {
    data.copy(tail, 0, length - (length % 4))
    h ^= BigInt(tail.readInt32LE(0))
    tail.fill(0)
    h *= m
  }

  // Perform the final mixing
  h ^= tripleRightShift(h, 13)
  h *= m
  h ^= tripleRightShift(h, 15)

  const hash = Number(BigInt.asIntN(32, h))

  if (key !== undefined) {
    cache.set(key, hash)
  }

  return hash
}

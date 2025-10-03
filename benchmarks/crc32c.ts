import cronometro from 'cronometro'
// @ts-ignore - Not explicitly exported by kafkajs
import kafkaJsCRC32 from 'kafkajs/src/protocol/recordBatch/crc32C/crc32C.js'
import { randomBytes } from 'node:crypto'
import { jsCRC32C, loadNativeCRC32C } from '../src/index.ts'

const nativeCRC32C = loadNativeCRC32C()!

await cronometro(
  {
    kafkajs () {
      const value = randomBytes(1024)
      return kafkaJsCRC32(value)
    },
    '@platformatic/kafka (JS)' () {
      const value = randomBytes(1024)
      return jsCRC32C(value)
    },
    '@platformatic/kafka (Native)' () {
      const value = randomBytes(1024)
      return nativeCRC32C(value)
    }
  },
  { print: { compare: true, compareMode: 'previous' } }
)

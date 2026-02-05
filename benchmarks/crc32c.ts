import cronometro from 'cronometro'
// @ts-ignore - Not explicitly exported by kafkajs
import kafkaJsCRC32 from 'kafkajs/src/protocol/recordBatch/crc32C/crc32C.js'
import { randomBytes } from 'node:crypto'
import { jsCRC32C, loadNativeCRC32C } from '../src/index.ts'
import { crc32c as wasmCRC32C } from '../src/protocol/native.ts'

const nativeCRC32C = loadNativeCRC32C()!
const size = 1024

await cronometro(
  {
    kafkajs () {
      const value = randomBytes(size)
      return kafkaJsCRC32(value)
    },
    '@platformatic/kafka (JS)' () {
      const value = randomBytes(size)
      return jsCRC32C(value)
    },
    '@platformatic/kafka (Native)' () {
      const value = randomBytes(size)
      return nativeCRC32C(value)
    },
    '@platformatic/kafka (WASM)' () {
      const value = randomBytes(size)
      return wasmCRC32C(value)
    }
  },
  { print: { compare: true, compareMode: 'previous' } }
)

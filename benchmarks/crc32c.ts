import { crc32c as nodeRsCRC32C } from '@node-rs/crc32'
import { DynamicBuffer } from '@platformatic/dynamic-buffer'
import { crc32c as wasmCRC32C } from '@platformatic/wasm-utils'
import cronometro from 'cronometro'
// @ts-ignore - Not explicitly exported by kafkajs
import kafkaJsCRC32 from 'kafkajs/src/protocol/recordBatch/crc32C/crc32C.js'
import { randomBytes } from 'node:crypto'
import { jsCRC32C } from '../src/index.ts'

const size = 1024

function nativeCRC32C (data: Buffer | Uint8Array | DynamicBuffer): number {
  const input = DynamicBuffer.isDynamicBuffer(data) ? (data as DynamicBuffer).buffer : (data as Buffer)

  return nodeRsCRC32C(input)
}

cronometro(
  {
    kafkajs () {
      const value = randomBytes(size)
      return kafkaJsCRC32(value)
    },
    '@platformatic/kafka (JS)' () {
      const value = randomBytes(size)
      return jsCRC32C(value)
    },
    '@platformatic/kafka (@node-rs/crc32)' () {
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

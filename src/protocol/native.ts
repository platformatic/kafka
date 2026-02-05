import { readFileSync } from 'node:fs'
import { DynamicBuffer } from './dynamic-buffer.ts'

interface WasmExports {
  memory: WebAssembly.Memory
  alloc: (size: number) => number
  dealloc: (ptr: number, size: number) => void
  crc32c: (ptr: number, len: number) => number
  lz4_compress: (ptr: number, len: number) => bigint
  lz4_decompress: (ptr: number, len: number) => bigint
  snappy_compress: (ptr: number, len: number) => bigint
  snappy_decompress: (ptr: number, len: number) => bigint
}

const wasm = readFileSync(new URL('../../dist/native.wasm', import.meta.url))
const wasmModule = new WebAssembly.Module(wasm)
const instance = new WebAssembly.Instance(wasmModule)
const {
  alloc,
  crc32c: _crc32c,
  dealloc,
  lz4_compress: _lz4Compress,
  lz4_decompress: _lz4Decompress,
  memory,
  snappy_compress: _snappyCompress,
  snappy_decompress: _snappyDecompress
} = instance.exports as unknown as WasmExports

let currentBufferLength = 4096
let currentBufferOffset: number = alloc(currentBufferLength)
let currentBuffer = new Uint8Array(memory.buffer, currentBufferOffset, currentBufferLength)

export function prepareInput (data: Buffer | Uint8Array | DynamicBuffer): void {
  const input = DynamicBuffer.isDynamicBuffer(data) ? (data as DynamicBuffer).slice() : (data as Buffer)

  if (currentBuffer.length < data.length) {
    dealloc(currentBufferOffset, currentBufferLength)
    currentBufferLength = data.length
    currentBufferOffset = alloc(currentBufferLength)
    currentBuffer = new Uint8Array(memory.buffer, currentBufferOffset, currentBufferLength)
  }

  currentBuffer.set(input)
}

export function prepareOutput (combined: bigint): Buffer {
  const len = Number(BigInt.asUintN(32, combined))
  const ptr = Number(combined >> 32n)

  const output = Buffer.from(new Uint8Array(memory.buffer, ptr, len))
  dealloc(ptr, len)

  return output
}

export function crc32c (data: Buffer | Uint8Array | DynamicBuffer): number {
  prepareInput(data)
  return _crc32c(currentBufferOffset, data.length) >>> 0
}

export function lz4Compress (data: Buffer | DynamicBuffer): Buffer {
  prepareInput(data)
  return prepareOutput(_lz4Compress(currentBufferOffset, data.length))
}

export function lz4Decompress (data: Buffer | DynamicBuffer): Buffer {
  prepareInput(data)
  return prepareOutput(_lz4Decompress(currentBufferOffset, data.length))
}

export function snappyCompress (data: Buffer | DynamicBuffer): Buffer {
  prepareInput(data)
  return prepareOutput(_snappyCompress(currentBufferOffset, data.length))
}

export function snappyDecompress (data: Buffer | DynamicBuffer): Buffer {
  prepareInput(data)
  return prepareOutput(_snappyDecompress(currentBufferOffset, data.length))
}

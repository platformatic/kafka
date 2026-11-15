import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { test } from 'node:test'
import * as apis from '../../src/apis/index.ts'
import { apiLoaders } from '../../src/apis/loaders.ts'
import * as main from '../../src/index.ts'

function isCodec (value: unknown): value is { api: { key: number; version: number } } {
  return typeof (value as { api?: unknown })?.api === 'function'
}

const codecs = Object.entries(apis)
  .filter(([, value]) => isCodec(value))
  .map(([name]) => name)
  .sort()

test('there should be a loader for each codec', () => {
  ok(codecs.length > 0)
  deepStrictEqual(Object.keys(apiLoaders).sort(), codecs)
})

test('each loader should load the codec with the same name', async () => {
  for (const name of codecs) {
    const loaded = await apiLoaders[name]()

    strictEqual(loaded, apis[name as keyof typeof apis], name)
    strictEqual(`V${loaded.api.version}`, name.match(/V\d+$/)![0], name)
  }
})

test('the main entry point should not export the codecs', () => {
  deepStrictEqual(
    Object.entries(main)
      .filter(([, value]) => isCodec(value))
      .map(([name]) => name),
    []
  )

  // What is not a codec is still exported
  for (const [name, value] of Object.entries(apis)) {
    if (!isCodec(value)) {
      strictEqual(main[name as keyof typeof main], value, name)
    }
  }
})

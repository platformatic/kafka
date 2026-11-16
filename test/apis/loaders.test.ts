import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { execFileSync } from 'node:child_process'
import { test } from 'node:test'
import { checkLoaders } from '../../scripts/generate-api-loaders.ts'
import * as apis from '../../src/apis/index.ts'
import { getApi } from '../../src/apis/lazy.ts'
import { apiLoaders, apiPaths } from '../../src/apis/loaders.ts'
import * as main from '../../src/index.ts'

type Codec = { api: { key: number; version: number } }

function isCodec (value: unknown): value is Codec {
  return typeof (value as { api?: unknown })?.api === 'function'
}

function runScript (script: string): string {
  return execFileSync(process.execPath, ['--input-type=module', '-e', script], {
    cwd: new URL('../..', import.meta.url),
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'pipe']
  })
}

const codecs = Object.entries(apis)
  .filter(([, value]) => isCodec(value))
  .map(([name]) => name)
  .sort()

test('the generated loaders should match the list of codecs', async () => {
  // If this fails, run: npm run generate:loaders
  deepStrictEqual(await checkLoaders(), [])
})

test('there should be a loader for each codec', () => {
  ok(codecs.length > 0)
  deepStrictEqual(Object.keys(apiLoaders).sort(), codecs)
  deepStrictEqual(Object.keys(apiPaths).sort(), codecs)
})

test('each loader should load the codec with the same name', async () => {
  for (const name of codecs) {
    const loaded = await apiLoaders[name]()

    strictEqual(loaded, apis[name as keyof typeof apis], name)
    strictEqual(`V${loaded.api.version}`, name.match(/V\d+$/)![0], name)
  }
})

// This also guarantees that no codec uses a top-level await, which would make it impossible to require
test('each codec should be available synchronously', () => {
  for (const name of codecs) {
    strictEqual(getApi(name), (apis[name as keyof typeof apis] as Codec).api, name)
  }

  strictEqual(getApi('unknownV0'), undefined)
  strictEqual(getApi('constructor'), undefined)
})

test('the main entry point should export all the codecs', () => {
  for (const name of codecs) {
    const actual = main[name as keyof typeof main] as Codec
    const expected = apis[name as keyof typeof apis] as Codec

    deepStrictEqual(Reflect.ownKeys(actual), Reflect.ownKeys(expected), name)
    ok('api' in actual)
    ok(!('unknown' in actual))

    for (const key of Reflect.ownKeys(expected)) {
      strictEqual(actual[key as keyof Codec], expected[key as keyof Codec], name)
      deepStrictEqual(
        Reflect.getOwnPropertyDescriptor(actual, key),
        Reflect.getOwnPropertyDescriptor(expected, key),
        name
      )
    }
  }

  // Everything else is exported as it is
  for (const [name, value] of Object.entries(apis)) {
    if (!isCodec(value)) {
      strictEqual(main[name as keyof typeof main], value, name)
    }
  }
})

test('the main entry point should not load the codecs until they are used', () => {
  const output = runScript(`
    import { createRequire } from 'node:module'
    const main = await import('./src/index.ts')

    function loaded () {
      return Object.keys(createRequire(import.meta.url).cache).filter(file => /apis.(admin|consumer|producer).+-v\\d+\\.ts$/.test(file))
    }

    const before = loaded().length
    const version = main.fetchV17.api.version
    console.log(JSON.stringify({ before, version, after: loaded().length }))
  `)

  deepStrictEqual(JSON.parse(output), { before: 0, version: 17, after: 1 })
})

test('the main entry point should warn once when a codec is used', () => {
  const output = runScript(`
    const warnings = []
    process.on('warning', warning => warnings.push([warning.name, warning.code]))

    const main = await import('./src/index.ts')
    const apis = await import('./src/apis/index.ts')
    await new Promise(resolve => setImmediate(resolve))
    const before = warnings.length

    main.fetchV17.api
    main.fetchV16.api
    main.metadataV12.api
    apis.produceV11.api
    await new Promise(resolve => setImmediate(resolve))

    console.log(JSON.stringify({ before, warnings }))
  `)

  deepStrictEqual(JSON.parse(output), { before: 0, warnings: [['DeprecationWarning', 'PLT_KFK_DEP_APIS']] })
})

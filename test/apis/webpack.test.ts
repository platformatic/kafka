import { notStrictEqual, ok, strictEqual } from 'node:assert'
import { mkdtemp, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import webpack from 'webpack'

test('webpack loads codecs from the bundle when the original files still exist', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'kafka-webpack-'))
  t.after(() => rm(directory, { recursive: true, force: true }))

  const entry = fileURLToPath(new URL('../../dist/apis/lazy.js', import.meta.url))
  const filename = 'codecs.cjs'
  await new Promise<void>((resolve, reject) => {
    webpack({
      mode: 'none',
      target: 'node22',
      entry,
      // Leave createRequire calls to Node so they can still reach the original files.
      module: { parser: { javascript: { createRequire: false } } },
      output: { path: directory, filename, library: { type: 'commonjs2' } }
    }, (error, stats) => {
      if (error) {
        reject(error)
      } else if (!stats || stats.hasErrors()) {
        reject(new Error(stats?.toString({ all: false, errors: true }) ?? 'Missing webpack build stats'))
      } else {
        resolve()
      }
    })
  })

  const require = createRequire(import.meta.url)
  const unbundled: typeof import('../../src/apis/lazy.ts') = require(entry)
  const bundled: typeof import('../../src/apis/lazy.ts') = require(join(directory, filename))
  const name = 'produceV11'

  // A synchronous disk load would return the unbundled codec and cache it in the bundle.
  const diskApi = unbundled.getApi(name)
  ok(diskApi)
  strictEqual(bundled.getApi(name), undefined)

  const api = await bundled.loadApi(name)
  strictEqual(api.version, 11)
  notStrictEqual(api, diskApi)
  strictEqual(bundled.getApi(name), api)
})

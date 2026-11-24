import { deepStrictEqual, ok } from 'node:assert'
import { once } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath, pathToFileURL } from 'node:url'
import webpack from 'webpack'
import type * as Kafka from '../../src/index.ts'
import { Writer } from '../../src/protocol/writer.ts'

const topic = 'webpack-test-topic'

async function bundleKafka (directory: string, useWebpackCreateRequire: boolean): Promise<typeof Kafka> {
  const context = fileURLToPath(new URL('../..', import.meta.url))
  const filename = 'kafka.mjs'
  await new Promise<void>((resolve, reject) => {
    webpack({
      context,
      mode: 'none',
      target: 'node22',
      entry: './dist/index.js',
      experiments: { outputModule: true },
      // Native addons are optional and do not belong in the JavaScript bundle.
      externalsType: 'node-commonjs',
      externals: ['@node-rs/crc32', 'kerberos', 'lz4-napi', 'snappy'],
      // When webpack leaves createRequire to Node, it can still reach the files on disk.
      module: useWebpackCreateRequire ? undefined : { parser: { javascript: { createRequire: false } } },
      output: { path: directory, filename, chunkFilename: '[id].mjs', library: { type: 'module' } }
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

  return import(pathToFileURL(join(directory, filename)).href)
}

test('webpack-bundled Admin can list topics while the original codecs are on disk', async t => {
  const server = createServer(socket => {
    let pending: Buffer = Buffer.alloc(0)
    socket.on('data', chunk => {
      pending = Buffer.concat([pending, chunk])
      while (pending.length >= 4 && pending.length >= 4 + pending.readInt32BE(0)) {
        const frameLength = 4 + pending.readInt32BE(0)
        const apiKey = pending.readInt16BE(4)
        const apiVersion = pending.readInt16BE(6)
        const response = Writer.create().appendInt32(pending.readInt32BE(8))
        pending = pending.subarray(frameLength)

        if (apiKey === 18 && apiVersion === 3) {
          // Advertise Metadata v0 so the client has to load its codec after negotiation.
          response
            .appendInt16(0)
            .appendArray([3], (writer, key) =>
              writer.appendInt16(key).appendInt16(0).appendInt16(0).appendTaggedFields()
            )
            .appendInt32(0)
            .appendTaggedFields()
        } else if (apiKey === 3 && apiVersion === 0) {
          response
            .appendInt32(0)
            .appendArray(
              [topic],
              (writer, name) => writer.appendInt16(0).appendString(name, false).appendInt32(0),
              false,
              false
            )
        } else {
          socket.destroy()
          return
        }

        socket.write(response.prependLength().buffer)
      }
    })
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  t.after(() => server[Symbol.asyncDispose]())
  const address = server.address()
  ok(address && typeof address !== 'string')

  for (const useWebpackCreateRequire of [true, false]) {
    await t.test(useWebpackCreateRequire ? 'webpack defaults' : 'native Node createRequire', async t => {
      const directory = await mkdtemp(join(tmpdir(), 'kafka-webpack-'))
      t.after(() => rm(directory, { recursive: true, force: true }))
      const { Admin } = await bundleKafka(directory, useWebpackCreateRequire)
      const admin = new Admin({ clientId: 'webpack-test', bootstrapBrokers: [`127.0.0.1:${address.port}`], retries: 0 })
      t.after(() => admin.close())

      deepStrictEqual(await admin.listTopics(), [topic])
    })
  }
})

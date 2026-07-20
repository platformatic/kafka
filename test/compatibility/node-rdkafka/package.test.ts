import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import { test } from 'node:test'

const runtimeExports = [
  'Consumer',
  'Producer',
  'HighLevelProducer',
  'AdminClient',
  'KafkaConsumer',
  'createReadStream',
  'createWriteStream',
  'CODES',
  'Topic',
  'features',
  'librdkafkaVersion'
]

test('loads the built node-rdkafka compatibility subpath', async () => {
  const compatibility = await import('@platformatic/kafka/compatibility/node-rdkafka')
  assert.equal(typeof compatibility.Producer, 'function')
  assert.equal(typeof compatibility.KafkaConsumer, 'function')
  assert.equal(compatibility.Topic.OFFSET_BEGINNING, -2)
  assert.deepEqual(Object.keys(compatibility.default), runtimeExports)
  assert.equal('Client' in compatibility, false)
  assert.equal('LibrdKafkaError' in compatibility, false)
})

test('loads the built node-rdkafka compatibility subpath through require', () => {
  const require = createRequire(import.meta.url)
  const compatibility = require('@platformatic/kafka/compatibility/node-rdkafka')
  assert.equal(typeof compatibility.default.Producer, 'function')
  assert.equal(compatibility.default.Topic.OFFSET_BEGINNING, -2)
  assert.deepEqual(Object.keys(compatibility.default), runtimeExports)
})

test('warns when the deprecated Consumer constructor is used', () => {
  const result = spawnSync(process.execPath, [
    '--input-type=module',
    '--eval',
    "import { Consumer, KafkaConsumer } from '@platformatic/kafka/compatibility/node-rdkafka'; const consumer = new Consumer({ 'group.id': 'compatibility-test', 'bootstrap.servers': 'localhost:9092' }); if (!(consumer instanceof Consumer) || !(consumer instanceof KafkaConsumer)) process.exit(1)"
  ], { encoding: 'utf8' })

  assert.equal(result.status, 0, result.stderr)
  assert.match(result.stderr, /DeprecationWarning: Use KafkaConsumer instead\. This may be changed in a later version/)
})

test('publishes node-rdkafka compatible declarations', () => {
  const result = spawnSync(process.execPath, [
    resolve('node_modules/typescript/bin/tsc'),
    '--noEmit',
    '--strict',
    '--target',
    'ESNext',
    '--module',
    'NodeNext',
    '--moduleResolution',
    'NodeNext',
    resolve('test/compatibility/node-rdkafka/package-types.ts')
  ], { encoding: 'utf8' })

  assert.equal(result.status, 0, result.stdout + result.stderr)
})

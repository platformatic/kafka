import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { metadataV0, Reader, Writer } from '../../../src/index.ts'

test('Metadata v0 uses the legacy wire shape and normalizes omitted fields', () => {
  const request = Reader.from(metadataV0.createRequest(['topic']))
  deepStrictEqual(
    request.readArray(r => r.readString(false), false, false),
    ['topic']
  )
  const reader = Reader.from(
    Writer.create()
      .appendArray(
        [{ id: 1 }],
        (w, broker) => w.appendInt32(broker.id).appendString('broker', false).appendInt32(9092),
        false,
        false
      )
      .appendArray(
        ['topic'],
        (w, topic) => w.appendInt16(0).appendString(topic, false).appendArray([], () => {}, false, false),
        false,
        false
      )
  )
  const response = metadataV0.parseResponse(
    1,
    3,
    0,
    reader
  )
  strictEqual(response.throttleTimeMs, 0)
  strictEqual(response.clusterId, null)
  strictEqual(response.controllerId, -1)
  deepStrictEqual(response.brokers[0], { nodeId: 1, host: 'broker', port: 9092, rack: null })
  strictEqual(reader.remaining, 0)
})

test('Metadata v0 serializes null topics as an empty array', () => {
  const reader = Reader.from(metadataV0.createRequest(null))

  deepStrictEqual(reader.readArray(() => '', false, false), [])
  strictEqual(reader.remaining, 0)
})

test('Metadata v0 parses response topic names', () => {
  const reader = Reader.from(
    Writer.create()
      .appendArray([], () => {}, false, false)
      .appendArray(
        ['topic'],
        (w, topic) => w.appendInt16(0).appendString(topic, false).appendArray([], () => {}, false, false),
        false,
        false
      )
  )
  const response = metadataV0.parseResponse(
    1,
    3,
    0,
    reader
  )

  deepStrictEqual(response.topics[0], {
    errorCode: 0,
    name: 'topic',
    topicId: '00000000-0000-0000-0000-000000000000',
    isInternal: false,
    topicAuthorizedOperations: -2147483648,
    partitions: []
  })
  strictEqual(reader.remaining, 0)
})

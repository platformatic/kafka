import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { metadataV7, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = metadataV7

test('uses Metadata v7 and serializes classic request parameters', () => {
  const reader = Reader.from(createRequest(['topic-1', 'topic-2'], true, true))

  deepStrictEqual(
    {
      topics: reader.readArray(r => r.readString(false), false, false),
      allowAutoTopicCreation: reader.readBoolean()
    },
    { topics: ['topic-1', 'topic-2'], allowAutoTopicCreation: true }
  )
  deepStrictEqual({ key: api.key, version: api.version }, { key: 3, version: 7 })
})

test('serializes null topics without authorized operation fields', () => {
  const reader = Reader.from(createRequest(null))

  deepStrictEqual(reader.readNullableArray(() => '', false), null)
  deepStrictEqual(reader.readBoolean(), true)
})

test('parses a complete classic response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray([{ nodeId: 1, host: 'broker-1', port: 9092, rack: 'rack-a' }], (w, broker) => {
      w.appendInt32(broker.nodeId).appendString(broker.host, false).appendInt32(broker.port).appendString(broker.rack, false)
    }, false, false)
    .appendString('cluster-1', false)
    .appendInt32(1)
    .appendArray([{ errorCode: 0, name: 'topic-1', isInternal: false }], (w, topic) => {
      w.appendInt16(topic.errorCode)
        .appendString(topic.name, false)
        .appendBoolean(topic.isInternal)
        .appendArray([{ errorCode: 0, partitionIndex: 0, leaderId: 1, leaderEpoch: 10 }], (w, partition) => {
          w.appendInt16(partition.errorCode)
            .appendInt32(partition.partitionIndex)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendArray([1, 2], (w, replica) => w.appendInt32(replica), false, false)
            .appendArray([1], (w, replica) => w.appendInt32(replica), false, false)
            .appendArray([], () => {}, false, false)
        }, false, false)
    }, false, false)

  deepStrictEqual(parseResponse(1, 3, 7, Reader.from(writer)), {
    throttleTimeMs: 0,
    brokers: [{ nodeId: 1, host: 'broker-1', port: 9092, rack: 'rack-a' }],
    clusterId: 'cluster-1',
    controllerId: 1,
    topics: [{
      errorCode: 0,
      name: 'topic-1',
      topicId: '00000000-0000-0000-0000-000000000000',
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: [{ errorCode: 0, partitionIndex: 0, leaderId: 1, leaderEpoch: 10, replicaNodes: [1, 2], isrNodes: [1], offlineReplicas: [] }]
    }]
  })
})

test('parses throttled responses with nullable metadata fields', () => {
  const writer = Writer.create()
    .appendInt32(100)
    .appendArray([{ nodeId: 1, host: 'broker-1', port: 9092, rack: null }], (w, broker) => {
      w.appendInt32(broker.nodeId).appendString(broker.host, false).appendInt32(broker.port).appendString(broker.rack, false)
    }, false, false)
    .appendString(null, false)
    .appendInt32(-1)
    .appendArray([], () => {}, false, false)

  deepStrictEqual(parseResponse(1, 3, 7, Reader.from(writer)), {
    throttleTimeMs: 100,
    brokers: [{ nodeId: 1, host: 'broker-1', port: 9092, rack: null }],
    clusterId: null,
    controllerId: -1,
    topics: []
  })
})

test('reports topic and partition errors from classic responses', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray([], () => {}, false, false)
    .appendString(null, false)
    .appendInt32(-1)
    .appendArray([{ errorCode: 3, name: 'missing', isInternal: false }], (w, topic) => {
      w.appendInt16(topic.errorCode)
        .appendString(topic.name, false)
        .appendBoolean(topic.isInternal)
        .appendArray([{ errorCode: 9, partitionIndex: 0, leaderId: -1, leaderEpoch: -1 }], (w, partition) => {
          w.appendInt16(partition.errorCode)
            .appendInt32(partition.partitionIndex)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendArray([], () => {}, false, false)
            .appendArray([], () => {}, false, false)
            .appendArray([2], (w, replica) => w.appendInt32(replica), false, false)
        }, false, false)
    }, false, false)

  throws(() => parseResponse(1, 3, 7, Reader.from(writer)), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.errors.map(({ path, apiCode }) => ({ path, apiCode })), [
      { path: '/topics/0', apiCode: 3 },
      { path: '/topics/0/partitions/0', apiCode: 9 }
    ])
    return true
  })
})

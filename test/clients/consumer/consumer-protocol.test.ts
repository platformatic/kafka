import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  decodeCooperativeStickyGeneration,
  decodeConsumerProtocolAssignment,
  decodeConsumerProtocolSubscription,
  encodeConsumerProtocolAssignment,
  encodeConsumerProtocolSubscription
} from '../../../src/index.ts'

test('consumer protocol subscription should encode and decode v3 fields', () => {
  const encoded = encodeConsumerProtocolSubscription({
    version: 3,
    topics: ['topic-b', 'topic-a'],
    userData: Buffer.from('metadata'),
    ownedPartitions: [
      { topic: 'topic-a', partitions: [2, 0] },
      { topic: 'topic-b', partitions: [1] }
    ],
    generationId: 42,
    rackId: 'rack-a'
  })

  deepStrictEqual(decodeConsumerProtocolSubscription(encoded), {
    version: 3,
    topics: ['topic-a', 'topic-b'],
    userData: Buffer.from('metadata'),
    ownedPartitions: [
      { topic: 'topic-a', partitions: [0, 2] },
      { topic: 'topic-b', partitions: [1] }
    ],
    generationId: 42,
    rackId: 'rack-a'
  })
})

test('consumer protocol subscription should preserve v0 compatibility', () => {
  const encoded = encodeConsumerProtocolSubscription({
    version: 0,
    topics: ['topic-a'],
    userData: 'metadata',
    ownedPartitions: [{ topic: 'topic-a', partitions: [0] }],
    generationId: 42,
    rackId: 'rack-a'
  })
  const decoded = decodeConsumerProtocolSubscription(encoded)

  strictEqual(decoded.version, 0)
  deepStrictEqual(decoded.topics, ['topic-a'])
  deepStrictEqual(decoded.userData, Buffer.from('metadata'))
  deepStrictEqual(decoded.ownedPartitions, [])
  strictEqual(decoded.generationId, -1)
  strictEqual(decoded.rackId, null)
})

test('consumer protocol assignment should encode and decode assigned partitions and user data', () => {
  const encoded = encodeConsumerProtocolAssignment({
    version: 3,
    assignedPartitions: [
      { topic: 'topic-b', partitions: [1] },
      { topic: 'topic-a', partitions: [2, 0] }
    ],
    userData: Buffer.from('assignment-metadata')
  })

  deepStrictEqual(decodeConsumerProtocolAssignment(encoded), {
    version: 3,
    assignedPartitions: [
      { topic: 'topic-a', partitions: [0, 2] },
      { topic: 'topic-b', partitions: [1] }
    ],
    userData: Buffer.from('assignment-metadata')
  })
})

test('consumer protocol assignment should decode empty assignment buffers', () => {
  deepStrictEqual(decodeConsumerProtocolAssignment(Buffer.alloc(0)), {
    version: 0,
    assignedPartitions: [],
    userData: Buffer.alloc(0)
  })
})

test('cooperative sticky generation should decode from v1 user data', () => {
  const userData = Buffer.alloc(4)
  userData.writeInt32BE(42)

  strictEqual(decodeCooperativeStickyGeneration(userData), 42)
  strictEqual(decodeCooperativeStickyGeneration(Buffer.alloc(0)), -1)
})

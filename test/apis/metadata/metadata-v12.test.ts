import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { metadataV12, Reader, ResponseError, Writer } from '../../../src/index.ts'
import type { MetadataRequestTopic } from '../../../src/apis/metadata/metadata-v12.ts'

const { createRequest, parseResponse } = metadataV12

function readRequestTopics (writer: Writer) {
  const reader = Reader.from(writer)
  const serialized = {
    topics: reader.readArray(r => {
      return { topicId: r.readUUID(), name: r.readNullableString() }
    }),
    allowAutoTopicCreation: reader.readBoolean(),
    includeTopicAuthorizedOperations: reader.readBoolean()
  }
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
  return serialized
}

test('createRequest serializes named topics with zero UUIDs', () => {
  const allowAutoTopicCreation = true
  const includeTopicAuthorizedOperations = true
  const serialized = readRequestTopics(createRequest(['topic-1', 'topic-2'], allowAutoTopicCreation, includeTopicAuthorizedOperations))

  deepStrictEqual(
    serialized,
    {
      topics: [
        { topicId: '00000000-0000-0000-0000-000000000000', name: 'topic-1' },
        { topicId: '00000000-0000-0000-0000-000000000000', name: 'topic-2' }
      ],
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations
    }
  )
})

test('createRequest serializes ID-only topics', () => {
  const serialized = readRequestTopics(createRequest([{ topicId: '11111111-2222-3333-4444-555555555555', name: null }]))

  deepStrictEqual(serialized.topics, [{ topicId: '11111111-2222-3333-4444-555555555555', name: null }])
})

test('createRequest serializes name-only topics', () => {
  const serialized = readRequestTopics(createRequest([{ name: 'named-topic' }]))

  deepStrictEqual(serialized.topics, [{ topicId: '00000000-0000-0000-0000-000000000000', name: 'named-topic' }])
})

test('MetadataRequestTopic requires a topic ID or name', () => {
  const topics: MetadataRequestTopic[] = [
    { topicId: '11111111-2222-3333-4444-555555555555' },
    { name: 'named-topic' },
    { topicId: '66666666-7777-8888-9999-aaaaaaaaaaaa', name: 'named-topic' }
  ]

  // @ts-expect-error A topic must be identified by ID or name.
  const invalidTopics: MetadataRequestTopic[] = [{}]
  strictEqual(topics.length, 3)
  strictEqual(invalidTopics.length, 1)
})

test('createRequest serializes mixed topic IDs and names', () => {
  const serialized = readRequestTopics(
    createRequest([
      { topicId: '11111111-2222-3333-4444-555555555555', name: null },
      { topicId: '66666666-7777-8888-9999-aaaaaaaaaaaa', name: 'named-topic' }
    ])
  )

  deepStrictEqual(serialized.topics, [
    { topicId: '11111111-2222-3333-4444-555555555555', name: null },
    { topicId: '66666666-7777-8888-9999-aaaaaaaaaaaa', name: 'named-topic' }
  ])
})

test('createRequest serializes null topics', () => {
  const writer = createRequest(null, false, false)
  ok(writer instanceof Writer)
  const reader = Reader.from(writer)

  strictEqual(reader.readNullableArray(() => ''), null)
  strictEqual(reader.readBoolean(), false)
  strictEqual(reader.readBoolean(), false)
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest enables automatic topic creation by default and preserves explicit values', () => {
  for (const allowAutoTopicCreation of [undefined, false, true]) {
    const reader = Reader.from(createRequest(null, allowAutoTopicCreation))

    strictEqual(reader.readNullableArray(() => ''), null)
    strictEqual(reader.readBoolean(), allowAutoTopicCreation ?? true)
    strictEqual(reader.readBoolean(), false)
    reader.readTaggedFields()
    strictEqual(reader.remaining, 0)
  }
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Brokers array - compact array format
    .appendArray(
      [
        {
          nodeId: 1,
          host: 'broker1.example.com',
          port: 9092,
          rack: 'us-west'
        },
        {
          nodeId: 2,
          host: 'broker2.example.com',
          port: 9092,
          rack: null
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.nodeId)
          .appendString(broker.host, true)
          .appendInt32(broker.port)
          .appendString(broker.rack, true)
      }
    )
    .appendString('test-cluster', true) // clusterId - compact string
    .appendInt32(1) // controllerId
    // Topics array
    .appendArray(
      [
        {
          errorCode: 0,
          name: 'test-topic',
          topicId: '00000000-0000-0000-0000-000000000000',
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 1,
              leaderEpoch: 101,
              replicaNodes: [1, 2],
              isrNodes: [1, 2],
              offlineReplicas: []
            }
          ],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name, true)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt16(partition.errorCode)
              .appendInt32(partition.partitionIndex)
              .appendInt32(partition.leaderId)
              .appendInt32(partition.leaderEpoch)
              // ReplicaNodes, IsrNodes, and OfflineReplicas arrays
              .appendArray(partition.replicaNodes, (w, r) => w.appendInt32(r), true, false)
              .appendArray(partition.isrNodes, (w, r) => w.appendInt32(r), true, false)
              .appendArray(partition.offlineReplicas, (w, r) => w.appendInt32(r), true, false)
          })
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 3, 12, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    brokers: [
      {
        nodeId: 1,
        host: 'broker1.example.com',
        port: 9092,
        rack: 'us-west'
      },
      {
        nodeId: 2,
        host: 'broker2.example.com',
        port: 9092,
        rack: null
      }
    ],
    clusterId: 'test-cluster',
    controllerId: 1,
    topics: [
      {
        errorCode: 0,
        name: 'test-topic',
        topicId: '00000000-0000-0000-0000-000000000000',
        isInternal: false,
        partitions: [
          {
            errorCode: 0,
            partitionIndex: 0,
            leaderId: 1,
            leaderEpoch: 101,
            replicaNodes: [1, 2],
            isrNodes: [1, 2],
            offlineReplicas: []
          }
        ],
        topicAuthorizedOperations: 0
      }
    ]
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    // Brokers array
    .appendArray(
      [
        {
          nodeId: 1,
          host: 'broker1.example.com',
          port: 9092,
          rack: null
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.nodeId)
          .appendString(broker.host, true)
          .appendInt32(broker.port)
          .appendString(broker.rack, true)
      }
    )
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    // Topics array (empty)
    .appendArray([], () => {})
    .appendInt8(0) // root tagged fields

  const response = parseResponse(1, 3, 12, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    brokers: [
      {
        nodeId: 1,
        host: 'broker1.example.com',
        port: 9092,
        rack: null
      }
    ],
    clusterId: 'test-cluster',
    controllerId: 1,
    topics: []
  })
})

test('parseResponse throws error on topic error code', () => {
  // Create a response with topic error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Brokers array
    .appendArray(
      [
        {
          nodeId: 1,
          host: 'broker1.example.com',
          port: 9092,
          rack: null
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.nodeId)
          .appendString(broker.host, true)
          .appendInt32(broker.port)
          .appendString(broker.rack, true)
      }
    )
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    // Topics array
    .appendArray(
      [
        {
          errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
          name: 'nonexistent-topic',
          topicId: '00000000-0000-0000-0000-000000000000',
          isInternal: false,
          partitions: [],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name, true)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          // Empty partitions array
          .appendArray(topic.partitions, () => {})
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(0) // root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 3, 12, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error location and code
      ok(typeof err.errors === 'object')

      // Verify the response is preserved
      deepStrictEqual(err.response.topics[0], {
        errorCode: 3,
        name: 'nonexistent-topic',
        topicId: '00000000-0000-0000-0000-000000000000',
        isInternal: false,
        partitions: [],
        topicAuthorizedOperations: 0
      })

      return true
    }
  )
})

test('parseResponse throws error on partition error code', () => {
  // Create a response with partition error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Brokers array
    .appendArray(
      [
        {
          nodeId: 1,
          host: 'broker1.example.com',
          port: 9092,
          rack: null
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.nodeId)
          .appendString(broker.host, true)
          .appendInt32(broker.port)
          .appendString(broker.rack, true)
      }
    )
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    // Topics array
    .appendArray(
      [
        {
          errorCode: 0, // success
          name: 'test-topic',
          topicId: '00000000-0000-0000-0000-000000000000',
          isInternal: false,
          partitions: [
            {
              errorCode: 9, // REPLICA_NOT_AVAILABLE
              partitionIndex: 0,
              leaderId: -1,
              leaderEpoch: 0,
              replicaNodes: [1],
              isrNodes: [],
              offlineReplicas: [2]
            }
          ],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name, true)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          // Partitions array with error
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt16(partition.errorCode)
              .appendInt32(partition.partitionIndex)
              .appendInt32(partition.leaderId)
              .appendInt32(partition.leaderEpoch)
              // ReplicaNodes, IsrNodes, and OfflineReplicas arrays
              .appendArray(partition.replicaNodes, (w, r) => w.appendInt32(r), true, false)
              .appendArray(partition.isrNodes, (w, r) => w.appendInt32(r), true, false)
              .appendArray(partition.offlineReplicas, (w, r) => w.appendInt32(r), true, false)
          })
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(0) // root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 3, 12, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error location and code
      ok(typeof err.errors === 'object')

      // Verify the response is preserved
      deepStrictEqual(err.response.topics[0].partitions[0], {
        errorCode: 9,
        partitionIndex: 0,
        leaderId: -1,
        leaderEpoch: 0,
        replicaNodes: [1],
        isrNodes: [],
        offlineReplicas: [2]
      })

      return true
    }
  )
})

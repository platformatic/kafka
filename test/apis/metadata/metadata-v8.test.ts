import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { metadataV8, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = metadataV8

test('createRequest serializes request parameters correctly', () => {
  // Values for the request
  const topics = ['topic-1', 'topic-2']
  const allowAutoTopicCreation = true
  const includeTopicAuthorizedOperations = true
  const includeClusterAuthorizedOperations = false
  const writer = createRequest(
    topics,
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
    includeClusterAuthorizedOperations
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read the entire request structure and verify in one assertion
  const serialized = {
    topics: reader.readArray(r => r.readString(false), false, false),
    allowAutoTopicCreation: reader.readBoolean(),
    includeClusterAuthorizedOperations: reader.readBoolean(),
    includeTopicAuthorizedOperations: reader.readBoolean()
  }

  deepStrictEqual(
    serialized,
    {
      topics: ['topic-1', 'topic-2'],
      allowAutoTopicCreation,
      includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest handles null topics', () => {
  const topics = null
  const allowAutoTopicCreation = false
  const includeTopicAuthorizedOperations = false
  const includeClusterAuthorizedOperations = false

  const writer = createRequest(
    topics,
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
    includeClusterAuthorizedOperations
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read the entire request structure and verify in one assertion
  const serialized = {
    topics: reader.readNullableArray(
      () => {
        throw new Error('This should not be called because topics is null')
      },
      false,
      false
    ),
    allowAutoTopicCreation: reader.readBoolean(),
    includeClusterAuthorizedOperations: reader.readBoolean(),
    includeTopicAuthorizedOperations: reader.readBoolean()
  }

  deepStrictEqual(
    serialized,
    {
      topics: null,
      allowAutoTopicCreation,
      includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations
    },
    'Serialized request with null topics should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const includeClusterAuthorizedOperations = false
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
          .appendString(broker.host, false)
          .appendInt32(broker.port)
          .appendString(broker.rack, false)
      },
      false,
      false
    )
    .appendString('test-cluster', false) // clusterId - compact string
    .appendInt32(1) // controllerId
    // Topics array
    .appendArray(
      [
        {
          errorCode: 0,
          name: 'test-topic',
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
          .appendString(topic.name, false)
          .appendBoolean(topic.isInternal)
          // Partitions array
          .appendArray(
            topic.partitions,
            (w, partition) => {
              w.appendInt16(partition.errorCode)
                .appendInt32(partition.partitionIndex)
                .appendInt32(partition.leaderId)
                .appendInt32(partition.leaderEpoch)
                // ReplicaNodes, IsrNodes, and OfflineReplicas arrays
                .appendArray(partition.replicaNodes, (w, r) => w.appendInt32(r), false, false)
                .appendArray(partition.isrNodes, (w, r) => w.appendInt32(r), false, false)
                .appendArray(partition.offlineReplicas, (w, r) => w.appendInt32(r), false, false)
            },
            false,
            false
          )
          .appendInt32(topic.topicAuthorizedOperations)
      },
      false,
      false
    )
    .appendInt32(0)
    .appendBoolean(includeClusterAuthorizedOperations)

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
    clusterAuthorizedOperations: 0
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
          .appendString(broker.host, false)
          .appendInt32(broker.port)
          .appendString(broker.rack, false)
      },
      false,
      false
    )
    .appendString('test-cluster', false) // clusterId
    .appendInt32(1) // controllerId
    // Topics array (empty)
    .appendArray([], () => {}, false, false)
    .appendInt32(0)

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
    topics: [],
    clusterAuthorizedOperations: 0
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
          .appendString(broker.host, false)
          .appendInt32(broker.port)
          .appendString(broker.rack, false)
      },
      false,
      false
    )
    .appendString('test-cluster', false) // clusterId
    .appendInt32(1) // controllerId
    // Topics array
    .appendArray(
      [
        {
          errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
          name: 'nonexistent-topic',
          isInternal: false,
          partitions: [],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name, false)
          .appendBoolean(topic.isInternal)
          // Empty partitions array
          .appendArray(topic.partitions, () => {}, false, false)
          .appendInt32(topic.topicAuthorizedOperations)
      },
      false,
      false
    )
    .appendInt32(0)

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
          .appendString(broker.host, false)
          .appendInt32(broker.port)
          .appendString(broker.rack, false)
      },
      false,
      false
    )
    .appendString('test-cluster', false) // clusterId
    .appendInt32(1) // controllerId
    // Topics array
    .appendArray(
      [
        {
          errorCode: 0, // success
          name: 'test-topic',
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
          .appendString(topic.name, false)
          .appendBoolean(topic.isInternal)
          // Partitions array with error
          .appendArray(
            topic.partitions,
            (w, partition) => {
              w.appendInt16(partition.errorCode)
                .appendInt32(partition.partitionIndex)
                .appendInt32(partition.leaderId)
                .appendInt32(partition.leaderEpoch)
                // ReplicaNodes, IsrNodes, and OfflineReplicas arrays
                .appendArray(partition.replicaNodes, (w, r) => w.appendInt32(r), false, false)
                .appendArray(partition.isrNodes, (w, r) => w.appendInt32(r), false, false)
                .appendArray(partition.offlineReplicas, (w, r) => w.appendInt32(r), false, false)
            },
            false,
            false
          )
          .appendInt32(topic.topicAuthorizedOperations)
      },
      false,
      false
    )
    .appendInt32(0)

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

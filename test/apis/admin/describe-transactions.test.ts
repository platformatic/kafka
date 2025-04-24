import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeTransactionsV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeTransactionsV0

test('createRequest serializes empty transactional IDs array correctly', () => {
  const transactionalIds: string[] = []

  const writer = createRequest(transactionalIds)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read transactionalIds array
  const transactionalIdsArray = reader.readArray(() => reader.readString(), true, false)

  // Verify serialized data
  deepStrictEqual(transactionalIdsArray, [], 'Empty transactional IDs array should be serialized correctly')
})

test('createRequest serializes single transactional ID correctly', () => {
  const transactionalIds = ['transaction-1']

  const writer = createRequest(transactionalIds)
  const reader = Reader.from(writer)

  // Read transactionalIds array
  const transactionalIdsArray = reader.readArray(() => reader.readString(), true, false)

  // Verify serialized data
  deepStrictEqual(transactionalIdsArray, ['transaction-1'], 'Single transactional ID should be serialized correctly')
})

test('createRequest serializes multiple transactional IDs correctly', () => {
  const transactionalIds = ['transaction-1', 'transaction-2', 'transaction-3']

  const writer = createRequest(transactionalIds)
  const reader = Reader.from(writer)

  // Read transactionalIds array
  const transactionalIdsArray = reader.readArray(() => reader.readString(), true, false)

  // Verify multiple transactional IDs
  deepStrictEqual(
    transactionalIdsArray,
    ['transaction-1', 'transaction-2', 'transaction-3'],
    'Multiple transactional IDs should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no transaction states
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty transaction states array
    .appendTaggedFields()

  const response = parseResponse(1, 65, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      transactionStates: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with transaction state', () => {
  // Create a successful response with a transaction state
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          transactionalId: 'transaction-1',
          transactionState: 'Ongoing',
          transactionTimeoutMs: 60000,
          transactionStartTimeMs: BigInt(1630000000000),
          producerId: BigInt(1000),
          producerEpoch: 5,
          topics: [
            {
              topic: 'test-topic',
              partitions: [0, 1, 2]
            }
          ]
        }
      ],
      (w, state) => {
        w.appendInt16(state.errorCode)
          .appendString(state.transactionalId)
          .appendString(state.transactionState)
          .appendInt32(state.transactionTimeoutMs)
          .appendInt64(state.transactionStartTimeMs)
          .appendInt64(state.producerId)
          .appendInt16(state.producerEpoch)
          .appendArray(state.topics, (w, topic) => {
            w.appendString(topic.topic).appendArray(topic.partitions, (w, p) => w.appendInt32(p), true, false)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 65, 0, Reader.from(writer))

  // Verify transaction state structure
  deepStrictEqual(response.transactionStates.length, 1, 'Should have one transaction state')

  const state = response.transactionStates[0]
  deepStrictEqual(state.transactionalId, 'transaction-1', 'Transactional ID should be parsed correctly')
  deepStrictEqual(state.transactionState, 'Ongoing', 'Transaction state should be parsed correctly')
  deepStrictEqual(state.transactionTimeoutMs, 60000, 'Transaction timeout should be parsed correctly')
  deepStrictEqual(
    state.transactionStartTimeMs,
    BigInt(1630000000000),
    'Transaction start time should be parsed correctly'
  )
  deepStrictEqual(state.producerId, BigInt(1000), 'Producer ID should be parsed correctly')
  deepStrictEqual(state.producerEpoch, 5, 'Producer epoch should be parsed correctly')

  // Verify topics structure
  deepStrictEqual(state.topics.length, 1, 'Should have one topic')
  deepStrictEqual(state.topics[0].topic, 'test-topic', 'Topic name should be parsed correctly')
  deepStrictEqual(state.topics[0].partitions, [0, 1, 2], 'Topic partitions should be parsed correctly')
})

test('parseResponse correctly processes a response with multiple transaction states', () => {
  // Create a response with multiple transaction states
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          transactionalId: 'transaction-1',
          transactionState: 'Ongoing',
          transactionTimeoutMs: 60000,
          transactionStartTimeMs: BigInt(1630000000000),
          producerId: BigInt(1000),
          producerEpoch: 5,
          topics: [
            {
              topic: 'topic-1',
              partitions: [0, 1]
            }
          ]
        },
        {
          errorCode: 0,
          transactionalId: 'transaction-2',
          transactionState: 'PrepareCommit',
          transactionTimeoutMs: 30000,
          transactionStartTimeMs: BigInt(1635000000000),
          producerId: BigInt(2000),
          producerEpoch: 3,
          topics: [
            {
              topic: 'topic-1',
              partitions: [2, 3]
            },
            {
              topic: 'topic-2',
              partitions: [0]
            }
          ]
        }
      ],
      (w, state) => {
        w.appendInt16(state.errorCode)
          .appendString(state.transactionalId)
          .appendString(state.transactionState)
          .appendInt32(state.transactionTimeoutMs)
          .appendInt64(state.transactionStartTimeMs)
          .appendInt64(state.producerId)
          .appendInt16(state.producerEpoch)
          .appendArray(state.topics, (w, topic) => {
            w.appendString(topic.topic).appendArray(topic.partitions, (w, p) => w.appendInt32(p), true, false)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 65, 0, Reader.from(writer))

  // Verify multiple transaction states
  deepStrictEqual(response.transactionStates.length, 2, 'Should have two transaction states')

  // Verify transaction IDs
  deepStrictEqual(
    response.transactionStates.map(s => s.transactionalId),
    ['transaction-1', 'transaction-2'],
    'Transaction IDs should be parsed correctly'
  )

  // Verify transaction states
  deepStrictEqual(
    response.transactionStates.map(s => s.transactionState),
    ['Ongoing', 'PrepareCommit'],
    'Transaction states should be parsed correctly'
  )

  // Verify topics count in second transaction
  deepStrictEqual(response.transactionStates[1].topics.length, 2, 'Second transaction should have two topics')

  // Verify topic partitions in second transaction
  deepStrictEqual(
    response.transactionStates[1].topics[0].partitions,
    [2, 3],
    'Partitions for first topic in second transaction should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple topics per transaction', () => {
  // Create a response with multiple topics per transaction
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          transactionalId: 'transaction-1',
          transactionState: 'Ongoing',
          transactionTimeoutMs: 60000,
          transactionStartTimeMs: BigInt(1630000000000),
          producerId: BigInt(1000),
          producerEpoch: 5,
          topics: [
            {
              topic: 'topic-1',
              partitions: [0, 1]
            },
            {
              topic: 'topic-2',
              partitions: [2, 3]
            },
            {
              topic: 'topic-3',
              partitions: [0]
            }
          ]
        }
      ],
      (w, state) => {
        w.appendInt16(state.errorCode)
          .appendString(state.transactionalId)
          .appendString(state.transactionState)
          .appendInt32(state.transactionTimeoutMs)
          .appendInt64(state.transactionStartTimeMs)
          .appendInt64(state.producerId)
          .appendInt16(state.producerEpoch)
          .appendArray(state.topics, (w, topic) => {
            w.appendString(topic.topic).appendArray(topic.partitions, (w, p) => w.appendInt32(p), true, false)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 65, 0, Reader.from(writer))

  // Verify multiple topics in a transaction
  deepStrictEqual(response.transactionStates[0].topics.length, 3, 'Transaction should have three topics')

  // Verify topic names
  deepStrictEqual(
    response.transactionStates[0].topics.map(t => t.topic),
    ['topic-1', 'topic-2', 'topic-3'],
    'Topic names should be parsed correctly'
  )

  // Verify partitions for each topic
  deepStrictEqual(
    response.transactionStates[0].topics.map(t => t.partitions),
    [[0, 1], [2, 3], [0]],
    'Topic partitions should be parsed correctly for all topics'
  )
})

test('parseResponse throws ResponseError on transaction state error', () => {
  // Create a response with a transaction state error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 49, // INVALID_TXN_STATE
          transactionalId: 'bad-transaction',
          transactionState: 'Unknown',
          transactionTimeoutMs: 0,
          transactionStartTimeMs: BigInt(0),
          producerId: BigInt(-1),
          producerEpoch: -1,
          topics: []
        }
      ],
      (w, state) => {
        w.appendInt16(state.errorCode)
          .appendString(state.transactionalId)
          .appendString(state.transactionState)
          .appendInt32(state.transactionTimeoutMs)
          .appendInt64(state.transactionStartTimeMs)
          .appendInt64(state.producerId)
          .appendInt16(state.producerEpoch)
          .appendArray([], () => {}) // Empty topics
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 65, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify transaction state error details
      deepStrictEqual(
        err.response.transactionStates[0].errorCode,
        49,
        'Transaction state error code should be preserved in the response'
      )

      deepStrictEqual(
        err.response.transactionStates[0].transactionalId,
        'bad-transaction',
        'Transactional ID should be preserved in the response'
      )

      deepStrictEqual(
        err.response.transactionStates[0].transactionState,
        'Unknown',
        'Transaction state should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendArray([], () => {}) // Empty transaction states
    .appendTaggedFields()

  const response = parseResponse(1, 65, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

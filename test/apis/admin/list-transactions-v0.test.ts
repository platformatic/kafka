import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { TransactionStates } from '../../../src/apis/enumerations.ts'
import { Reader, ResponseError, Writer, listTransactionsV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = listTransactionsV0

test('uses Kafka transaction state names', () => {
  deepStrictEqual(TransactionStates, listTransactionsV0.TransactionStates)
})

test('createRequest serializes basic parameters correctly', () => {
  const stateFilters: [] = []
  const producerIdFilters: bigint[] = []

  const writer = createRequest(stateFilters, producerIdFilters, 0n, null)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read stateFilters array
  const serializedStateFilters = reader.readArray(() => reader.readString(), true, false)

  // Read producerIdFilters array
  const serializedProducerIdFilters = reader.readArray(() => reader.readInt64(), true, false)

  // Verify serialized data
  deepStrictEqual(serializedStateFilters, [], 'Empty state filters array should be serialized correctly')

  deepStrictEqual(serializedProducerIdFilters, [], 'Empty producer ID filters array should be serialized correctly')
})

test('parses a complete flexible response with unknown top-level tags', () => {
  const reader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendInt16(0)
      .appendArray([], () => {})
      .appendArray([], () => {})
      .appendUnsignedVarInt(1)
      .appendUnsignedVarInt(42)
      .appendUnsignedVarInt(1)
      .appendUnsignedInt8(0)
  )

  deepStrictEqual(parseResponse(1, 66, 0, reader).transactionStates, [])
  strictEqual(reader.remaining, 0)
})

test('createRequest serializes state filters correctly', () => {
  const stateFilters: listTransactionsV0.TransactionState[] = ['Ongoing', 'CompleteCommit', 'PrepareAbort']
  const producerIdFilters: bigint[] = []

  const writer = createRequest(stateFilters, producerIdFilters, 0n, null)
  const reader = Reader.from(writer)

  // Read stateFilters array
  const serializedStateFilters = reader.readArray(() => reader.readString(), true, false)

  // Verify state filters
  deepStrictEqual(
    serializedStateFilters,
    ['Ongoing', 'CompleteCommit', 'PrepareAbort'],
    'State filters should be serialized correctly'
  )
})

test('createRequest serializes producer ID filters correctly', () => {
  const producerIdFilters = [BigInt(1000), BigInt(2000), BigInt(3000)]

  const writer = createRequest([], producerIdFilters, 0n, null)
  const reader = Reader.from(writer)

  // Skip stateFilters array
  reader.readArray(() => reader.readString(), true, false)

  // Read producerIdFilters array
  const serializedProducerIdFilters = reader.readArray(() => reader.readInt64(), true, false)

  // Verify producer ID filters
  deepStrictEqual(
    serializedProducerIdFilters,
    [BigInt(1000), BigInt(2000), BigInt(3000)],
    'Producer ID filters should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no transaction states
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray([], () => {}) // Empty unknown state filters array
    .appendArray([], () => {}) // Empty transaction states array
    .appendTaggedFields()

  const response = parseResponse(1, 66, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      unknownStateFilters: [],
      transactionStates: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with transaction states', () => {
  // Create a successful response with transaction states
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray([], () => {}) // Empty unknown state filters array
    .appendArray(
      [
        {
          transactionalId: 'transaction-1',
          producerId: BigInt(1000),
          transactionState: 'Ongoing'
        }
      ],
      (w, state) => {
        w.appendString(state.transactionalId).appendInt64(state.producerId).appendString(state.transactionState)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 66, 0, Reader.from(writer))

  // Verify transaction state structure
  deepStrictEqual(response.transactionStates.length, 1, 'Should have one transaction state')

  const state = response.transactionStates[0]
  deepStrictEqual(state.transactionalId, 'transaction-1', 'Transactional ID should be parsed correctly')
  deepStrictEqual(state.producerId, BigInt(1000), 'Producer ID should be parsed correctly')
  deepStrictEqual(state.transactionState, 'Ongoing', 'Transaction state should be parsed correctly')
})

test('parseResponse correctly processes a response with multiple transaction states', () => {
  // Create a response with multiple transaction states
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray([], () => {}) // Empty unknown state filters array
    .appendArray(
      [
        {
          transactionalId: 'transaction-1',
          producerId: BigInt(1000),
          transactionState: 'Ongoing'
        },
        {
          transactionalId: 'transaction-2',
          producerId: BigInt(2000),
          transactionState: 'PrepareCommit'
        },
        {
          transactionalId: 'transaction-3',
          producerId: BigInt(3000),
          transactionState: 'PrepareAbort'
        }
      ],
      (w, state) => {
        w.appendString(state.transactionalId).appendInt64(state.producerId).appendString(state.transactionState)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 66, 0, Reader.from(writer))

  // Verify multiple transaction states
  deepStrictEqual(response.transactionStates.length, 3, 'Should have three transaction states')

  // Verify transactional IDs
  deepStrictEqual(
    response.transactionStates.map(s => s.transactionalId),
    ['transaction-1', 'transaction-2', 'transaction-3'],
    'Transactional IDs should be parsed correctly'
  )

  // Verify transaction states
  deepStrictEqual(
    response.transactionStates.map(s => s.transactionState),
    ['Ongoing', 'PrepareCommit', 'PrepareAbort'],
    'Transaction states should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with unknown state filters', () => {
  // Create a response with unknown state filters
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray(['InvalidState1', 'InvalidState2'], (w, s) => w.appendString(s), true, false) // Unknown state filters
    .appendArray([], () => {}) // Empty transaction states array
    .appendTaggedFields()

  const response = parseResponse(1, 66, 0, Reader.from(writer))

  // Verify unknown state filters
  deepStrictEqual(
    response.unknownStateFilters,
    ['InvalidState1', 'InvalidState2'],
    'Unknown state filters should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(58) // errorCode INVALID_TOKEN
    .appendArray([], () => {}) // Empty unknown state filters array
    .appendArray([], () => {}) // Empty transaction states array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 66, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 58, 'Error code should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray([], () => {}) // Empty unknown state filters array
    .appendArray([], () => {}) // Empty transaction states array
    .appendTaggedFields()

  const response = parseResponse(1, 66, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

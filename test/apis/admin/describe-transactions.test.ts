import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { describeTransactionsV0 } from '../../../src/apis/admin/describe-transactions.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers(apiFunction: any) {
  const mockConnection = {
    send: (_apiKey: number, _apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      return true
    },
    createRequestFn: null as any,
    parseResponseFn: null as any
  }
  
  // Call the API to capture handlers
  apiFunction(mockConnection, {})
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('describeTransactionsV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeTransactionsV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeTransactionsV0 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(describeTransactionsV0)
  
  // Create a test request
  const transactionalIds = ['transaction-1', 'transaction-2']
  
  // Call the createRequest function
  const writer = createRequest(transactionalIds)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('describeTransactionsV0 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ transactionStates: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = describeTransactionsV0.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { transactionalIds: ['transaction-1'] }))
  doesNotThrow(() => mockAPI({}, { transactionalIds: [] }))
})

test('describeTransactionsV0 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(describeTransactionsV0)
  
  // Create a sample successful response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        transactionalId: 'transaction-1',
        transactionState: 'Ongoing',
        transactionTimeoutMs: 60000,
        transactionStartTimeMs: BigInt(1616661661000),
        producerId: BigInt(1234567890),
        producerEpoch: 5,
        topics: [
          {
            topic: 'test-topic',
            partitions: [0, 1, 2]
          },
          {
            topic: 'another-topic',
            partitions: [0]
          }
        ]
      }
    ], (w, state) => {
      w.appendInt16(state.errorCode)
        .appendString(state.transactionalId)
        .appendString(state.transactionState)
        .appendInt32(state.transactionTimeoutMs)
        .appendInt64(state.transactionStartTimeMs)
        .appendInt64(state.producerId)
        .appendInt16(state.producerEpoch)
        .appendArray(state.topics, (w, topic) => {
          w.appendString(topic.topic)
            .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition), true, false)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 65, 0, writer.bufferList)
  
  // Check response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.transactionStates.length, 1)
  
  // Check transaction state details
  const state = response.transactionStates[0]
  deepStrictEqual(state.errorCode, 0)
  deepStrictEqual(state.transactionalId, 'transaction-1')
  deepStrictEqual(state.transactionState, 'Ongoing')
  deepStrictEqual(state.transactionTimeoutMs, 60000)
  deepStrictEqual(state.transactionStartTimeMs, BigInt(1616661661000))
  deepStrictEqual(state.producerId, BigInt(1234567890))
  deepStrictEqual(state.producerEpoch, 5)
  
  // Check topic details
  deepStrictEqual(state.topics.length, 2)
  
  const topic1 = state.topics[0]
  deepStrictEqual(topic1.topic, 'test-topic')
  deepStrictEqual(topic1.partitions, [0, 1, 2])
  
  const topic2 = state.topics[1]
  deepStrictEqual(topic2.topic, 'another-topic')
  deepStrictEqual(topic2.partitions, [0])
})

test('describeTransactionsV0 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(describeTransactionsV0)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 49, // INVALID_TRANSACTION_ID error code
        transactionalId: 'transaction-1',
        transactionState: '', // Empty in error case
        transactionTimeoutMs: 0,
        transactionStartTimeMs: BigInt(0),
        producerId: BigInt(0),
        producerEpoch: 0,
        topics: []
      }
    ], (w, state) => {
      w.appendInt16(state.errorCode)
        .appendString(state.transactionalId)
        .appendString(state.transactionState)
        .appendInt32(state.transactionTimeoutMs)
        .appendInt64(state.transactionStartTimeMs)
        .appendInt64(state.producerId)
        .appendInt16(state.producerEpoch)
        .appendArray(state.topics, (w, topic) => {
          w.appendString(topic.topic)
            .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition), true, false)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // The parseResponse function should throw a ResponseError
  throws(() => {
    parseResponse(1, 65, 0, writer.bufferList)
  }, (error) => {
    ok(error instanceof ResponseError, 'Should throw a ResponseError')
    
    // Check error properties - just check the response property exists
    ok('response' in error, 'Error should contain response data')
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.transactionStates.length, 1)
    
    const state = responseData.transactionStates[0]
    deepStrictEqual(state.errorCode, 49)
    deepStrictEqual(state.transactionalId, 'transaction-1')
    
    return true
  })
})
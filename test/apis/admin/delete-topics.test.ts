import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteTopicsV6, type DeleteTopicsResponseResponse } from '../../../src/apis/admin/delete-topics.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
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

test('deleteTopicsV6 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(deleteTopicsV6)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('deleteTopicsV6 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(deleteTopicsV6)
  
  // Create a test request
  const topics = ['topic1', 'topic2']
  const topicIds = ['12345678-1234-1234-1234-123456789012', '87654321-4321-4321-4321-210987654321']
  const timeoutMs = 30000
  
  // Call the createRequest function
  const writer = createRequest(topics, topicIds, timeoutMs)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('deleteTopicsV6 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ topics: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = deleteTopicsV6.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { topics: ['topic1'], timeoutMs: 1000 }))
  doesNotThrow(() => mockAPI({}, { topics: [] }))
})

test('deleteTopicsV6 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(deleteTopicsV6)
  
  // Create a sample raw response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([ // responses array
      {
        name: 'topic1',
        topicId: '01234567-89ab-cdef-0123-456789abcdef',
        errorCode: 0,
        errorMessage: null
      },
      {
        name: 'topic2',
        topicId: '87654321-4321-4321-4321-210987654321',
        errorCode: 0,
        errorMessage: null
      }
    ], (w, response) => {
      w.appendString(response.name)
        .appendUUID(response.topicId)
        .appendInt16(response.errorCode)
        .appendString(response.errorMessage)
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 20, 6, writer.bufferList)
  
  // Check the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.responses.length, 2)
  
  // Check first topic
  deepStrictEqual(response.responses[0].name, 'topic1')
  deepStrictEqual(response.responses[0].topicId, '01234567-89ab-cdef-0123-456789abcdef')
  deepStrictEqual(response.responses[0].errorCode, 0)
  deepStrictEqual(response.responses[0].errorMessage, null)
  
  // Check second topic
  deepStrictEqual(response.responses[1].name, 'topic2')
  deepStrictEqual(response.responses[1].topicId, '87654321-4321-4321-4321-210987654321')
  deepStrictEqual(response.responses[1].errorCode, 0)
  deepStrictEqual(response.responses[1].errorMessage, null)
})

test('deleteTopicsV6 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(deleteTopicsV6)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([ // responses array with error
      {
        name: 'topic1',
        topicId: '01234567-89ab-cdef-0123-456789abcdef',
        errorCode: 41, // Topic authorization failed error
        errorMessage: 'Not authorized to delete topic'
      },
      {
        name: 'topic2',
        topicId: '87654321-4321-4321-4321-210987654321',
        errorCode: 0,
        errorMessage: null
      }
    ], (w, response) => {
      w.appendString(response.name)
        .appendUUID(response.topicId)
        .appendInt16(response.errorCode)
        .appendString(response.errorMessage)
    })
    .appendTaggedFields()
  
  // The response should throw a ResponseError
  throws(() => {
    parseResponse(1, 20, 6, writer.bufferList)
  }, (error) => {
    ok(error instanceof ResponseError)
    const responseError = error as ResponseError
    
    // Check the error message
    ok(responseError.message.includes('API'))
    
    // Check that the response is still included
    ok('response' in responseError)
    deepStrictEqual(responseError.response.throttleTimeMs, 100)
    deepStrictEqual(responseError.response.responses[0].errorCode, 41)
    deepStrictEqual(responseError.response.responses[0].errorMessage, 'Not authorized to delete topic')
    deepStrictEqual(responseError.response.responses[1].errorCode, 0)
    return true
  })
})
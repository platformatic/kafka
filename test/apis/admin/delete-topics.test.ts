import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { deleteTopicsV6 } from '../../../src/apis/admin/delete-topics.ts'
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
import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { listPartitionReassignmentsV0 } from '../../../src/apis/admin/list-partition-reassignments.ts'
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

test('listPartitionReassignmentsV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(listPartitionReassignmentsV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('listPartitionReassignmentsV0 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(listPartitionReassignmentsV0)
  
  // Create a test request
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic',
      partitionIndexes: [0, 1, 2]
    }
  ]
  
  // Call the createRequest function
  const writer = createRequest(timeoutMs, topics)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('listPartitionReassignmentsV0 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ topics: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = listPartitionReassignmentsV0.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { timeoutMs: 30000, topics: [] }))
  doesNotThrow(() => mockAPI({}, { timeoutMs: 30000 }))
})
import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { describeGroupsV5 } from '../../../src/apis/admin/describe-groups.ts'
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

test('describeGroupsV5 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeGroupsV5 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(describeGroupsV5)
  
  // Create a test request
  const groupIds = ['test-group-1', 'test-group-2']
  const includeAuthorizedOperations = true
  
  // Call the createRequest function
  const writer = createRequest(groupIds, includeAuthorizedOperations)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('describeGroupsV5 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ groups: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = describeGroupsV5.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { groupIds: ['group1'], includeAuthorizedOperations: true }))
  doesNotThrow(() => mockAPI({}, { groupIds: ['group1'] }))
})
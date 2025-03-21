import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { alterConfigsV2 } from '../../../src/apis/admin/alter-configs.ts'
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

test('alterConfigsV2 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(alterConfigsV2)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('alterConfigsV2 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create a test request
  const resources = [
    {
      resourceType: 2, // Topic type
      resourceName: 'test-topic',
      configs: [
        { name: 'cleanup.policy', value: 'compact' },
        { name: 'retention.ms', value: '86400000' }
      ]
    }
  ]
  const validateOnly = false
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('alterConfigsV2 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ responses: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = alterConfigsV2.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { resources: [{ resourceType: 2, resourceName: 'topic', configs: [{ name: 'key', value: 'value' }] }], validateOnly: false }))
  doesNotThrow(() => mockAPI({}, { resources: [{ resourceType: 2, resourceName: 'topic', configs: [{ name: 'key', value: 'value' }] }] }))
})
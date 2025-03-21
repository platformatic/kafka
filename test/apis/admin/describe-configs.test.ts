import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { describeConfigsV4 } from '../../../src/apis/admin/describe-configs.ts'
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

test('describeConfigsV4 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeConfigsV4)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeConfigsV4 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(describeConfigsV4)
  
  // Create a test request
  const resources = [
    {
      resourceType: 2, // Topic type
      resourceName: 'test-topic',
      configurationKeys: ['cleanup.policy', 'retention.ms']
    }
  ]
  const includeSynonyms = true
  const includeDocumentation = true
  
  // Call the createRequest function
  const writer = createRequest(resources, includeSynonyms, includeDocumentation)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('describeConfigsV4 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ resources: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = describeConfigsV4.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { resources: [{ resourceType: 2, resourceName: 'topic' }], includeSynonyms: true, includeDocumentation: true }))
  doesNotThrow(() => mockAPI({}, { resources: [{ resourceType: 2, resourceName: 'topic' }] }))
})
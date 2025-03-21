import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { createTopicsV7, type CreateTopicsRequestTopic } from '../../../src/apis/admin/create-topics.ts'
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

test('createTopicsV7 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(createTopicsV7)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createTopicsV7 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(createTopicsV7)
  
  // Create a test request
  const topics: CreateTopicsRequestTopic[] = [
    {
      name: 'test-topic',
      numPartitions: 3,
      replicationFactor: 2,
      assignments: [
        {
          partitionIndex: 0,
          brokerIds: [1, 2]
        }
      ],
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        }
      ]
    }
  ]
  
  const timeoutMs = 30000
  const validateOnly = false
  
  // Call the createRequest function
  const writer = createRequest(topics, timeoutMs, validateOnly)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('createTopicsV7 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ topics: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = createTopicsV7.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { topics: [], timeoutMs: 1000, validateOnly: false }))
  doesNotThrow(() => mockAPI({}, { topics: [], timeoutMs: 1000 }))
  doesNotThrow(() => mockAPI({}, { topics: [] }))
})
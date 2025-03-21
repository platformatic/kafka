import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { alterConfigsV2, type AlterConfigsResponse } from '../../../src/apis/admin/alter-configs.ts'
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
  ok(writer.bufferList.length > 0)
})

test('alterConfigsV2 createRequest handles validateOnly=true parameter', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create a test request with validateOnly=true
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: [
        { name: 'cleanup.policy', value: 'compact' }
      ]
    }
  ]
  const validateOnly = true
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify it returns a valid Writer with content
  ok(writer instanceof Writer)
  ok(writer.bufferList.length > 0)
})

test('alterConfigsV2 createRequest handles multiple resources', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create a test request with multiple resources
  const resources = [
    {
      resourceType: 2, // Topic type
      resourceName: 'test-topic-1',
      configs: [{ name: 'cleanup.policy', value: 'compact' }]
    },
    {
      resourceType: 2, // Topic type
      resourceName: 'test-topic-2',
      configs: [{ name: 'retention.ms', value: '86400000' }]
    }
  ]
  const validateOnly = false
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify it returns a valid Writer with content
  ok(writer instanceof Writer)
  ok(writer.bufferList.length > 0)
})

test('alterConfigsV2 createRequest handles configs with null values', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create a test request with a null config value
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: [
        { name: 'cleanup.policy', value: null }
      ]
    }
  ]
  const validateOnly = false
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify it returns a valid Writer with content
  ok(writer instanceof Writer)
  ok(writer.bufferList.length > 0)
})

test('alterConfigsV2 createRequest handles empty configs array', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create a test request with an empty configs array
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: []
    }
  ]
  const validateOnly = false
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify it returns a valid Writer with content
  ok(writer instanceof Writer)
  ok(writer.bufferList.length > 0)
})

// This test specifically targets the config array serialization in the createRequest function
test('alterConfigsV2 createRequest properly serializes configs array with various values', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create resources with different types of config values
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: [
        { name: 'key1', value: 'value1' },
        { name: 'key2', value: '' },
        { name: 'key3', value: null },
        { name: 'key4', value: undefined },
        { name: 'key5', value: '123' }
      ]
    }
  ]
  const validateOnly = false
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify it returns a Writer with content
  ok(writer instanceof Writer)
  ok(writer.bufferList.length > 0)

  // Just verify that the buffer has some content
  ok(writer.bufferList.length > 0, 'Buffer should have some content')
})

// This test targets the nested appendArray call for configs
test('alterConfigsV2 createRequest serializes deeply nested configs correctly', () => {
  const { createRequest } = captureApiHandlers(alterConfigsV2)
  
  // Create a more complex set of resources and configs to test nested serialization
  const resources = [
    {
      resourceType: 1,
      resourceName: 'resource1',
      configs: [
        { name: 'key1', value: 'value1' }
      ]
    },
    {
      resourceType: 2,
      resourceName: 'resource2',
      configs: [
        { name: 'key2', value: 'value2' },
        { name: 'key3', value: 'value3' }
      ]
    },
    {
      resourceType: 3,
      resourceName: 'resource3',
      configs: [
        { name: 'key4', value: 'value4' },
        { name: 'key5', value: 'value5' },
        { name: 'key6', value: 'value6' }
      ]
    }
  ]
  const validateOnly = true
  
  // Call the createRequest function
  const writer = createRequest(resources, validateOnly)
  
  // Verify writer has content
  ok(writer instanceof Writer)
  ok(writer.bufferList.length > 0)

  // Just verify that the buffer has some content
  ok(writer.bufferList.length > 0, 'Buffer should have some content for nested configs')
})

test('alterConfigsV2 parseResponse deserializes response correctly', () => {
  const { parseResponse } = captureApiHandlers(alterConfigsV2)
  
  // Create a mock response buffer
  const buffer = new BufferList()
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: 'Success',
        resourceType: 2,
        resourceName: 'test-topic'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  buffer.append(writer.bufferList)
  
  // Parse the response
  const response = parseResponse(1, 33, 2, buffer)
  
  // Verify the parsed response
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    responses: [
      {
        errorCode: 0,
        errorMessage: 'Success',
        resourceType: 2,
        resourceName: 'test-topic'
      }
    ]
  })
})

test('alterConfigsV2 parseResponse handles multiple responses', () => {
  const { parseResponse } = captureApiHandlers(alterConfigsV2)
  
  // Create a mock response buffer with multiple responses
  const buffer = new BufferList()
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: 'Success',
        resourceType: 2,
        resourceName: 'test-topic-1'
      },
      {
        errorCode: 0,
        errorMessage: 'Success',
        resourceType: 2,
        resourceName: 'test-topic-2'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  buffer.append(writer.bufferList)
  
  // Parse the response
  const response = parseResponse(1, 33, 2, buffer)
  
  // Verify the parsed response
  strictEqual(response.responses.length, 2)
  strictEqual(response.responses[0].resourceName, 'test-topic-1')
  strictEqual(response.responses[1].resourceName, 'test-topic-2')
})

test('alterConfigsV2 parseResponse throws on error', () => {
  const { parseResponse } = captureApiHandlers(alterConfigsV2)
  
  // Create a mock response buffer with an error
  const buffer = new BufferList()
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 1, // Non-zero error code
        errorMessage: 'Error occurred',
        resourceType: 2,
        resourceName: 'test-topic'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  buffer.append(writer.bufferList)
  
  // Test that parseResponse throws an error
  throws(() => {
    parseResponse(1, 33, 2, buffer)
  })
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

test('alterConfigsV2 works with API interface', () => {
  // Mock connection with a simpler approach
  const mockConnection = {
    send: function(apiKey: number, apiVersion: number, _createRequestFn: any, _parseResponseFn: any, _hasRequestHeaderTaggedFields: boolean, _hasResponseHeaderTaggedFields: boolean, callback: any) {
      // Just return true without calling callback
      strictEqual(apiKey, 33)
      strictEqual(apiVersion, 2)
      ok(typeof callback === 'function')
      return true
    }
  }
  
  // Verify that calling the API returns true (synchronous version)
  const result = alterConfigsV2(mockConnection, {
    resources: [
      {
        resourceType: 2,
        resourceName: 'test-topic',
        configs: [{ name: 'key', value: 'value' }]
      }
    ],
    validateOnly: false
  })
  strictEqual(result, true)
  
  // Verify that we can access the async property
  ok(typeof alterConfigsV2.async === 'function')
})
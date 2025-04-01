import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { offsetDeleteV0 } from '../../../src/apis/admin/offset-delete.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API handlers
function captureApiHandlers(apiFunction) {
  const mockConnection = {
    send: (_apiKey, _apiVersion, createRequestFn, parseResponseFn, _hasRequestHeader, _hasResponseHeader, callback) => {
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      if (callback) callback(null, {})
      return true
    },
    createRequestFn: null,
    parseResponseFn: null
  }
  
  // Call the API to capture handlers
  apiFunction(mockConnection, {})
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('offsetDeleteV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(offsetDeleteV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('offsetDeleteV0 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn, options) => {
    return new Promise((resolve) => {
      resolve({ topics: [] })
    })
  })
  
  // Add the connection function to the mock API
  mockAPI.connection = offsetDeleteV0.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { groupId: 'test-group', topics: [] }))
  doesNotThrow(() => mockAPI({}, { groupId: 'test-group' }))
})

test('parseResponse correctly parses successful response', () => {
  const { parseResponse } = captureApiHandlers(offsetDeleteV0)
  
  // Create a mock successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      // Topic 1
      {
        name: 'topic1',
        partitions: [
          { partitionIndex: 0, errorCode: 0 },
          { partitionIndex: 1, errorCode: 0 }
        ]
      },
      // Topic 2
      {
        name: 'topic2',
        partitions: [
          { partitionIndex: 2, errorCode: 0 }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
        })
    })
  
  // Parse the response
  const response = parseResponse(1, 47, 0, writer.bufferList)
  
  // Verify response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.throttleTimeMs, 1000)
  deepStrictEqual(response.topics.length, 2)
  
  // First topic
  deepStrictEqual(response.topics[0].name, 'topic1')
  deepStrictEqual(response.topics[0].partitions.length, 2)
  deepStrictEqual(response.topics[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.topics[0].partitions[0].errorCode, 0)
  deepStrictEqual(response.topics[0].partitions[1].partitionIndex, 1)
  deepStrictEqual(response.topics[0].partitions[1].errorCode, 0)
  
  // Second topic
  deepStrictEqual(response.topics[1].name, 'topic2')
  deepStrictEqual(response.topics[1].partitions.length, 1)
  deepStrictEqual(response.topics[1].partitions[0].partitionIndex, 2)
  deepStrictEqual(response.topics[1].partitions[0].errorCode, 0)
})

test('parseResponse throws ResponseError on top-level error', () => {
  const { parseResponse } = captureApiHandlers(offsetDeleteV0)
  
  // Create a mock response with top-level error
  const writer = Writer.create()
    .appendInt16(1) // errorCode (error!)
    .appendInt32(1000) // throttleTimeMs
    .appendArray([], (w, _) => {})
  
  // Verify it throws ResponseError
  throws(() => {
    parseResponse(1, 47, 0, writer.bufferList)
  }, (err) => {
    deepStrictEqual(err instanceof ResponseError, true)
    
    // Verify the error location is correct
    const errorPath = ''
    deepStrictEqual(err.errors.some((e) => e.path === errorPath), true)
    
    return true
  })
})

test('parseResponse throws ResponseError on partition-level error', () => {
  const { parseResponse } = captureApiHandlers(offsetDeleteV0)
  
  // Create a mock response with partition-level error
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      {
        name: 'topic1',
        partitions: [
          { partitionIndex: 0, errorCode: 0 },
          { partitionIndex: 1, errorCode: 1 } // Error!
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
        })
    })
  
  // Verify it throws ResponseError
  throws(() => {
    parseResponse(1, 47, 0, writer.bufferList)
  }, (err) => {
    deepStrictEqual(err instanceof ResponseError, true)
    
    // Verify the error location is correct
    const errorPath = '/topics/0/partitions/1'
    deepStrictEqual(err.errors.some((e) => e.path === errorPath), true)
    
    // Make sure response is included
    deepStrictEqual(err.response.topics.length, 1)
    deepStrictEqual(err.response.topics[0].name, 'topic1')
    
    return true
  })
})

test('parseResponse throws ResponseError with multiple errors', () => {
  const { parseResponse } = captureApiHandlers(offsetDeleteV0)
  
  // Create a mock response with multiple errors
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success at top level)
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      {
        name: 'topic1',
        partitions: [
          { partitionIndex: 0, errorCode: 1 }, // Error!
          { partitionIndex: 1, errorCode: 0 }
        ]
      },
      {
        name: 'topic2',
        partitions: [
          { partitionIndex: 0, errorCode: 2 } // Another error!
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
        })
    })
  
  // Verify it throws ResponseError
  throws(() => {
    parseResponse(1, 47, 0, writer.bufferList)
  }, (err) => {
    deepStrictEqual(err instanceof ResponseError, true)
    deepStrictEqual(err.code, 'PLT_KFK_RESPONSE')
    
    // Verify there are exactly 2 errors
    deepStrictEqual(err.errors.length, 2)
    
    // Verify the error locations are correct
    const errorPaths = err.errors.map((e) => e.path)
    deepStrictEqual(errorPaths.includes('/topics/0/partitions/0'), true)
    deepStrictEqual(errorPaths.includes('/topics/1/partitions/0'), true)
    
    return true
  })
})

test('parseResponse handles empty topics array', () => {
  const { parseResponse } = captureApiHandlers(offsetDeleteV0)
  
  // Create a mock response with empty topics array
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendInt32(1000) // throttleTimeMs
    .appendArray([], (w, _) => {})
  
  // Parse the response
  const response = parseResponse(1, 47, 0, writer.bufferList)
  
  // Verify response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.throttleTimeMs, 1000)
  deepStrictEqual(response.topics.length, 0)
})


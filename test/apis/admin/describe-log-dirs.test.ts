import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import {
  describeLogDirsV4,
  type DescribeLogDirsRequestTopic
} from '../../../src/apis/admin/describe-log-dirs.ts'
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

test('describeLogDirsV4 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeLogDirsV4)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest simple validation', () => {
  const { createRequest } = captureApiHandlers(describeLogDirsV4)
  
  const topics: DescribeLogDirsRequestTopic[] = [
    {
      name: 'test-topic',
      partitions: [0, 1, 2]
    }
  ]
  
  // Create a request using our function
  const writer = createRequest(topics) as Writer
  
  // Verify the writer has content
  deepStrictEqual(writer.length > 0, true)
})

test('createRequest with empty topics array', () => {
  const { createRequest } = captureApiHandlers(describeLogDirsV4)
  
  // Create a request with empty topics array
  const writer = createRequest([]) as Writer
  
  // Verify the writer has proper content
  deepStrictEqual(writer.length > 0, true)
})

test('createRequest with multiple topics', () => {
  const { createRequest } = captureApiHandlers(describeLogDirsV4)
  
  const topics: DescribeLogDirsRequestTopic[] = [
    {
      name: 'topic-1',
      partitions: [0, 1]
    },
    {
      name: 'topic-2',
      partitions: [0, 1, 2]
    }
  ]
  
  // Create a request using our function
  const writer = createRequest(topics) as Writer
  
  // Verify the writer has content
  deepStrictEqual(writer.length > 0, true)
})

test('parseResponse with basic response', () => {
  // Get the parseResponse function
  const { parseResponse } = captureApiHandlers(describeLogDirsV4)
  
  // Create a basic mock response
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendUnsignedVarInt(0) // Empty results array (compact format)
    .appendUnsignedVarInt(0) // No tagged fields
  
  // Parse the response
  const response = parseResponse(1, 35, 4, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.results, null) // Should be null if array is empty in compact format
})

test('parseResponse with actual results', () => {
  // Get the parseResponse function
  const { parseResponse } = captureApiHandlers(describeLogDirsV4)
  
  // Create a more complete mock response with results
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
  
  // Results array (compact format)
  writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
  
  // Result item
  writer.appendInt16(0) // errorCode (success)
  writer.appendString('/var/log/kafka', true) // logDir (compact)
  
  // Topics array (compact format)
  writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
  
  // Topic
  writer.appendString('test-topic', true) // name (compact)
  
  // Partitions array (compact format)
  writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
  
  // Partition
  writer.appendInt32(0) // partitionIndex
  writer.appendInt64(BigInt(1024000)) // partitionSize
  writer.appendInt64(BigInt(0)) // offsetLag
  writer.appendBoolean(false) // isFutureKey
  writer.appendInt8(0) // No tagged fields for partition
  
  writer.appendInt8(0) // No tagged fields for topic
  
  // Total and usable bytes
  writer.appendInt64(BigInt(10240000)) // totalBytes
  writer.appendInt64(BigInt(5120000)) // usableBytes
  writer.appendInt8(0) // No tagged fields for result
  
  // Top level tagged fields
  writer.appendUnsignedVarInt(0) // No tagged fields at top level
  
  // Parse the response
  const response = parseResponse(1, 35, 4, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.results.length, 1)
  
  // Verify result
  const result = response.results[0]
  deepStrictEqual(result.errorCode, 0)
  deepStrictEqual(result.logDir, '/var/log/kafka')
  deepStrictEqual(result.totalBytes, BigInt(10240000))
  deepStrictEqual(result.usableBytes, BigInt(5120000))
  
  // Verify topics
  deepStrictEqual(result.topics.length, 1)
  deepStrictEqual(result.topics[0].name, 'test-topic')
  
  // Verify partitions
  deepStrictEqual(result.topics[0].partitions.length, 1)
  deepStrictEqual(result.topics[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(result.topics[0].partitions[0].partitionSize, BigInt(1024000))
  deepStrictEqual(result.topics[0].partitions[0].offsetLag, BigInt(0))
  deepStrictEqual(result.topics[0].partitions[0].isFutureKey, false)
})

test('parseResponse with complex response structure', () => {
  // Get the parseResponse function
  const { parseResponse } = captureApiHandlers(describeLogDirsV4)
  
  // Create a complex mock response with multiple results, topics, and partitions
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
  
  // Results array with two items (compact format)
  writer.appendUnsignedVarInt(3) // Array length of 2 + 1 for compact format
  
  // First result
  writer.appendInt16(0) // errorCode (success)
  writer.appendString('/var/log/kafka1', true) // logDir (compact)
  
  // Topics array with two topics (compact format)
  writer.appendUnsignedVarInt(3) // Array length of 2 + 1 for compact format
  
  // First topic
  writer.appendString('topic1', true) // name (compact)
  
  // Partitions array with two partitions (compact format)
  writer.appendUnsignedVarInt(3) // Array length of 2 + 1 for compact format
  
  // First partition
  writer.appendInt32(0) // partitionIndex
  writer.appendInt64(BigInt(1024000)) // partitionSize
  writer.appendInt64(BigInt(0)) // offsetLag
  writer.appendBoolean(false) // isFutureKey
  writer.appendInt8(0) // No tagged fields for partition
  
  // Second partition
  writer.appendInt32(1) // partitionIndex
  writer.appendInt64(BigInt(2048000)) // partitionSize
  writer.appendInt64(BigInt(100)) // offsetLag
  writer.appendBoolean(false) // isFutureKey
  writer.appendInt8(0) // No tagged fields for partition
  
  writer.appendInt8(0) // No tagged fields for topic
  
  // Second topic
  writer.appendString('topic2', true) // name (compact)
  
  // Partitions array with one partition (compact format)
  writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
  
  // Partition
  writer.appendInt32(0) // partitionIndex
  writer.appendInt64(BigInt(3072000)) // partitionSize
  writer.appendInt64(BigInt(200)) // offsetLag
  writer.appendBoolean(true) // isFutureKey
  writer.appendInt8(0) // No tagged fields for partition
  
  writer.appendInt8(0) // No tagged fields for topic
  
  // Total and usable bytes for first result
  writer.appendInt64(BigInt(10240000)) // totalBytes
  writer.appendInt64(BigInt(5120000)) // usableBytes
  writer.appendInt8(0) // No tagged fields for result
  
  // Second result
  writer.appendInt16(0) // errorCode (success)
  writer.appendString('/var/log/kafka2', true) // logDir (compact)
  
  // Empty topics array for second result (compact format)
  writer.appendUnsignedVarInt(0) // Empty array in compact format
  
  // Total and usable bytes for second result
  writer.appendInt64(BigInt(20480000)) // totalBytes
  writer.appendInt64(BigInt(15360000)) // usableBytes
  writer.appendInt8(0) // No tagged fields for result
  
  // Top level tagged fields
  writer.appendUnsignedVarInt(0) // No tagged fields at top level
  
  // Parse the response
  const response = parseResponse(1, 35, 4, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.results.length, 2)
  
  // Verify first result
  const result1 = response.results[0]
  deepStrictEqual(result1.errorCode, 0)
  deepStrictEqual(result1.logDir, '/var/log/kafka1')
  deepStrictEqual(result1.totalBytes, BigInt(10240000))
  deepStrictEqual(result1.usableBytes, BigInt(5120000))
  
  // Verify first result topics
  deepStrictEqual(result1.topics.length, 2)
  deepStrictEqual(result1.topics[0].name, 'topic1')
  deepStrictEqual(result1.topics[0].partitions.length, 2)
  deepStrictEqual(result1.topics[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(result1.topics[0].partitions[0].partitionSize, BigInt(1024000))
  deepStrictEqual(result1.topics[0].partitions[0].offsetLag, BigInt(0))
  deepStrictEqual(result1.topics[0].partitions[0].isFutureKey, false)
  deepStrictEqual(result1.topics[0].partitions[1].partitionIndex, 1)
  deepStrictEqual(result1.topics[0].partitions[1].partitionSize, BigInt(2048000))
  deepStrictEqual(result1.topics[0].partitions[1].offsetLag, BigInt(100))
  deepStrictEqual(result1.topics[0].partitions[1].isFutureKey, false)
  
  deepStrictEqual(result1.topics[1].name, 'topic2')
  deepStrictEqual(result1.topics[1].partitions.length, 1)
  deepStrictEqual(result1.topics[1].partitions[0].partitionIndex, 0)
  deepStrictEqual(result1.topics[1].partitions[0].partitionSize, BigInt(3072000))
  deepStrictEqual(result1.topics[1].partitions[0].offsetLag, BigInt(200))
  deepStrictEqual(result1.topics[1].partitions[0].isFutureKey, true)
  
  // Verify second result
  const result2 = response.results[1]
  deepStrictEqual(result2.errorCode, 0)
  deepStrictEqual(result2.logDir, '/var/log/kafka2')
  deepStrictEqual(result2.totalBytes, BigInt(20480000))
  deepStrictEqual(result2.usableBytes, BigInt(15360000))
  deepStrictEqual(result2.topics, null) // Should be null for an empty array in compact format
})

test('parseResponse throws ResponseError for top-level error', () => {
  const { parseResponse } = captureApiHandlers(describeLogDirsV4)
  
  // Create a mock response with a top-level error
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(42) // errorCode (non-zero = error)
    .appendUnsignedVarInt(0) // Empty results array (compact format)
    .appendUnsignedVarInt(0) // No tagged fields
  
  // Verify that parseResponse throws ResponseError
  throws(() => {
    parseResponse(1, 35, 4, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError
  })
})

test('parseResponse throws ResponseError for result-level errors', () => {
  const { parseResponse } = captureApiHandlers(describeLogDirsV4)
  
  // Create a mock response with a result-level error
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success at top level)
  
  // Results array with one result that has an error (compact format)
  writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
  
  // Result with error
  writer.appendInt16(37) // errorCode (non-zero = error)
  writer.appendString('/var/log/kafka', true) // logDir (compact)
  
  // Empty topics array (compact format)
  writer.appendUnsignedVarInt(0) // Empty array in compact format
  
  // Total and usable bytes
  writer.appendInt64(BigInt(10240000)) // totalBytes
  writer.appendInt64(BigInt(5120000)) // usableBytes
  writer.appendInt8(0) // No tagged fields for result
  
  writer.appendUnsignedVarInt(0) // No tagged fields at top level
  
  // Verify that parseResponse throws ResponseError
  throws(() => {
    parseResponse(1, 35, 4, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError
  })
})

test('mock API request-response cycle', async () => {
  // Create a mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Verify apiKey and apiVersion
      deepStrictEqual(apiKey, 35)
      deepStrictEqual(apiVersion, 4)
      
      // Call createRequest to get the request buffer
      const request = createRequestFn([
        {
          name: 'test-topic',
          partitions: [0, 1, 2]
        }
      ])
      
      // Create a mock response
      const writer = Writer.create()
        .appendInt32(100) // throttleTimeMs
        .appendInt16(0) // errorCode (success)
      
      // Results array with one result (compact format)
      writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
      
      // Result
      writer.appendInt16(0) // errorCode (success)
      writer.appendString('/var/log/kafka', true) // logDir (compact)
      
      // Topics array with one topic (compact format)
      writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
      
      // Topic
      writer.appendString('test-topic', true) // name (compact)
      
      // Partitions array with one partition (compact format)
      writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
      
      // Partition
      writer.appendInt32(0) // partitionIndex
      writer.appendInt64(BigInt(1024000)) // partitionSize
      writer.appendInt64(BigInt(0)) // offsetLag
      writer.appendBoolean(false) // isFutureKey
      writer.appendInt8(0) // No tagged fields for partition
      
      writer.appendInt8(0) // No tagged fields for topic
      
      // Total and usable bytes
      writer.appendInt64(BigInt(10240000)) // totalBytes
      writer.appendInt64(BigInt(5120000)) // usableBytes
      writer.appendInt8(0) // No tagged fields for result
      
      // Top level tagged fields
      writer.appendUnsignedVarInt(0) // No tagged fields at top level
      
      // Call parseResponse with the mock response
      const response = parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
      
      // Return the parsed response as a resolved promise
      return Promise.resolve(response)
    }
  }
  
  // Call the API directly with the mock connection
  const response = await describeLogDirsV4(mockConnection as any, {
    topics: [
      {
        name: 'test-topic',
        partitions: [0, 1, 2]
      }
    ]
  })
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.results.length, 1)
  
  // Verify result
  const result = response.results[0]
  deepStrictEqual(result.errorCode, 0)
  deepStrictEqual(result.logDir, '/var/log/kafka')
  deepStrictEqual(result.totalBytes, BigInt(10240000))
  deepStrictEqual(result.usableBytes, BigInt(5120000))
  
  // Verify topics
  deepStrictEqual(result.topics.length, 1)
  deepStrictEqual(result.topics[0].name, 'test-topic')
  
  // Verify partitions
  deepStrictEqual(result.topics[0].partitions.length, 1)
  deepStrictEqual(result.topics[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(result.topics[0].partitions[0].partitionSize, BigInt(1024000))
  deepStrictEqual(result.topics[0].partitions[0].offsetLag, BigInt(0))
  deepStrictEqual(result.topics[0].partitions[0].isFutureKey, false)
})

test('mock API error response handling', async () => {
  // Create a mock connection that returns an error response
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Create a mock response with an error
      const writer = Writer.create()
        .appendInt32(100) // throttleTimeMs
        .appendInt16(0) // errorCode (success at top level)
      
      // Results array with one result that has an error (compact format)
      writer.appendUnsignedVarInt(2) // Array length of 1 + 1 for compact format
      
      // Result with error
      writer.appendInt16(37) // errorCode (non-zero = error)
      writer.appendString('/var/log/kafka', true) // logDir (compact)
      
      // Empty topics array (compact format)
      writer.appendUnsignedVarInt(0) // Empty array in compact format
      
      // Total and usable bytes
      writer.appendInt64(BigInt(10240000)) // totalBytes
      writer.appendInt64(BigInt(5120000)) // usableBytes
      writer.appendInt8(0) // No tagged fields for result
      
      writer.appendUnsignedVarInt(0) // No tagged fields at top level
      
      try {
        // This should throw a ResponseError
        parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
        return Promise.resolve('This should not resolve')
      } catch (error) {
        // Return the error as a rejected promise
        return Promise.reject(error)
      }
    }
  }
  
  try {
    // Call the API directly with the mock connection
    await describeLogDirsV4(mockConnection as any, {
      topics: [
        {
          name: 'test-topic',
          partitions: [0, 1, 2]
        }
      ]
    })
    
    // If we get here, the test should fail
    throw new Error('Promise should be rejected')
  } catch (error: any) {
    // Verify that the error is a ResponseError
    deepStrictEqual(error instanceof ResponseError, true)
  }
})
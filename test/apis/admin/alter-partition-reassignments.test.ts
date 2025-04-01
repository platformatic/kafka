import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import {
  alterPartitionReassignmentsV0,
  type AlterPartitionReassignmentsRequestTopic
} from '../../../src/apis/admin/alter-partition-reassignments.ts'
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

test('alterPartitionReassignmentsV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(alterPartitionReassignmentsV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

// Direct implementation of createRequest for testing
function directCreateRequest(timeoutMs: number, topics: AlterPartitionReassignmentsRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(timeoutMs)
    .appendArray(topics, (w, t) => {
      w.appendString(t.name)
        .appendArray(t.partitions, (w, p) => {
          w.appendInt32(p.partitionIndex)
            .appendArray(p.replicas, (w, r) => w.appendInt32(r), true, false)
        })
    })
    .appendTaggedFields()
}

test('createRequest simple validation', () => {
  const { createRequest } = captureApiHandlers(alterPartitionReassignmentsV0)
  
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [1, 2, 3]
        }
      ]
    }
  ]
  
  // Create a request using our function
  const writer = createRequest(timeoutMs, topics) as Writer
  
  // Verify the writer has length
  deepStrictEqual(writer.length > 0, true)
})

test('parseResponse with actual topics and partitions', () => {
  // Get the parseResponse function
  const { parseResponse } = captureApiHandlers(alterPartitionReassignmentsV0)
  
  // Create a more complete mock response with topics and partitions
  const writer = Writer.create()
    // Top level fields
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (0 = success)
    .appendString(null) // errorMessage (null)
  
  // Responses array with one topic
  writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
  
  // Topic
  writer.appendString('test-topic')
  
  // Partitions array with one partition
  writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
  
  // Partition
  writer.appendInt32(0) // partitionIndex
  writer.appendInt16(0) // errorCode
  writer.appendString(null) // errorMessage
  writer.appendUnsignedVarInt(0) // No tagged fields for partition
  
  writer.appendUnsignedVarInt(0) // No tagged fields for topic
  
  // Top level tagged fields
  writer.appendUnsignedVarInt(0) // No tagged fields at top level
  
  // Parse the response
  const response = parseResponse(1, 45, 0, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, null)
  deepStrictEqual(response.responses.length, 1)
  deepStrictEqual(response.responses[0].name, 'test-topic')
  deepStrictEqual(response.responses[0].partitions.length, 1)
  deepStrictEqual(response.responses[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.responses[0].partitions[0].errorCode, 0)
  deepStrictEqual(response.responses[0].partitions[0].errorMessage, null)
})

test('parseResponse throws ResponseError for top-level error', () => {
  const { parseResponse } = captureApiHandlers(alterPartitionReassignmentsV0)
  
  // Create a mock response with a top-level error
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(42) // errorCode (non-zero = error)
    .appendString('Top-level error') // errorMessage
    .appendUnsignedVarInt(0) // Empty responses array
    .appendUnsignedVarInt(0) // No tagged fields
  
  // Verify that parseResponse throws ResponseError
  throws(() => {
    parseResponse(1, 45, 0, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError
  })
})

test('parseResponse throws ResponseError for partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(alterPartitionReassignmentsV0)
  
  // Create a mock response with partition-level errors
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (0 = success at top level)
    .appendString(null) // errorMessage (null)
  
  // Responses array with one topic
  writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
  
  // Topic
  writer.appendString('test-topic')
  
  // Partitions array with one partition that has an error
  writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
  
  // Partition with error
  writer.appendInt32(0) // partitionIndex
  writer.appendInt16(37) // errorCode (non-zero = error)
  writer.appendString('Partition error') // errorMessage
  writer.appendUnsignedVarInt(0) // No tagged fields for partition
  
  writer.appendUnsignedVarInt(0) // No tagged fields for topic
  writer.appendUnsignedVarInt(0) // No tagged fields at top level
  
  // Verify that parseResponse throws ResponseError
  throws(() => {
    parseResponse(1, 45, 0, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError
  })
})

test('mock API request-response cycle', async () => {
  // Create a mock connection with callback support
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify apiKey and apiVersion
      deepStrictEqual(apiKey, 45)
      deepStrictEqual(apiVersion, 0)
      
      // Call createRequest to get the request buffer
      const request = createRequestFn(30000, [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              replicas: [1, 2, 3]
            }
          ]
        }
      ])
      
      // Create a mock response
      const writer = Writer.create()
        .appendInt32(100) // throttleTimeMs
        .appendInt16(0) // errorCode (success)
        .appendString(null) // errorMessage
      
      // Add responses array with one topic and one partition
      writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
      writer.appendString('test-topic')
      
      // Add partitions array
      writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
      
      // Partition
      writer.appendInt32(0) // partitionIndex
      writer.appendInt16(0) // errorCode
      writer.appendString(null) // errorMessage
      writer.appendUnsignedVarInt(0) // No tagged fields
      
      writer.appendUnsignedVarInt(0) // No tagged fields for topic
      writer.appendUnsignedVarInt(0) // No tagged fields at top level
      
      // Call parseResponse with the mock response
      const response = parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
      
      // Call the callback with the response
      callback(null, response)
      
      return true
    }
  }
  
  // Call the API using async property
  const response = await alterPartitionReassignmentsV0.async(mockConnection as any, {
    timeoutMs: 30000,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            replicas: [1, 2, 3]
          }
        ]
      }
    ]
  })
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, null)
  deepStrictEqual(response.responses.length, 1)
  deepStrictEqual(response.responses[0].name, 'test-topic')
  deepStrictEqual(response.responses[0].partitions.length, 1)
  deepStrictEqual(response.responses[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.responses[0].partitions[0].errorCode, 0)
  deepStrictEqual(response.responses[0].partitions[0].errorMessage, null)
})

test('mock API error response handling', async () => {
  // Create a mock connection that returns an error response
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Create a mock response with an error
      const writer = Writer.create()
        .appendInt32(100) // throttleTimeMs
        .appendInt16(0) // errorCode (success at top level)
        .appendString(null) // errorMessage
      
      // Add responses array with one topic and one partition with error
      writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
      writer.appendString('test-topic')
      
      // Add partitions array
      writer.appendUnsignedVarInt(2) // Array length with compact encoding (1+1)
      
      // Partition with error
      writer.appendInt32(0) // partitionIndex
      writer.appendInt16(37) // errorCode
      writer.appendString('Partition error') // errorMessage
      writer.appendUnsignedVarInt(0) // No tagged fields
      
      writer.appendUnsignedVarInt(0) // No tagged fields for topic
      writer.appendUnsignedVarInt(0) // No tagged fields at top level
      
      try {
        // This should throw a ResponseError
        parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
        // We should never get here if parseResponse throws as expected
        callback(null, 'This should not be called')
      } catch (error) {
        // Call the callback with the error
        callback(error, null)
      }
      
      return true
    }
  }
  
  try {
    // Call the API using async property
    await alterPartitionReassignmentsV0.async(mockConnection as any, {
      timeoutMs: 30000,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              replicas: [1, 2, 3]
            }
          ]
        }
      ]
    })
    
    // If we get here, the test should fail
    throw new Error('Promise should be rejected')
  } catch (error: any) {
    // Just verify that an error was thrown
    deepStrictEqual(error instanceof ResponseError, true)
  }
})
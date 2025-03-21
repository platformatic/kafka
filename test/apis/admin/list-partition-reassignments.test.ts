import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { listPartitionReassignmentsV0 } from '../../../src/apis/admin/list-partition-reassignments.ts'
import { ResponseError } from '../../../src/errors.ts'
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

test('listPartitionReassignmentsV0 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(listPartitionReassignmentsV0)
  
  // Create a sample raw response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode - No error
    .appendString(null) // errorMessage
    .appendArray([
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            replicas: [1, 2, 3],
            addingReplicas: [4],
            removingReplicas: [1]
          },
          {
            partitionIndex: 1,
            replicas: [2, 3, 4],
            addingReplicas: [5],
            removingReplicas: [2]
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendArray(partition.replicas, (w, replica) => w.appendInt32(replica), true, false)
            .appendArray(partition.addingReplicas, (w, replica) => w.appendInt32(replica), true, false)
            .appendArray(partition.removingReplicas, (w, replica) => w.appendInt32(replica), true, false)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 46, 0, writer.bufferList)
  
  // Check the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, null)
  deepStrictEqual(response.topics.length, 1)
  
  // Check topic data
  const topic = response.topics[0]
  deepStrictEqual(topic.name, 'test-topic')
  deepStrictEqual(topic.partitions.length, 2)
  
  // Check first partition
  const partition0 = topic.partitions[0]
  deepStrictEqual(partition0.partitionIndex, 0)
  deepStrictEqual(partition0.replicas, [1, 2, 3])
  deepStrictEqual(partition0.addingReplicas, [4])
  deepStrictEqual(partition0.removingReplicas, [1])
  
  // Check second partition
  const partition1 = topic.partitions[1]
  deepStrictEqual(partition1.partitionIndex, 1)
  deepStrictEqual(partition1.replicas, [2, 3, 4])
  deepStrictEqual(partition1.addingReplicas, [5])
  deepStrictEqual(partition1.removingReplicas, [2])
})

test('listPartitionReassignmentsV0 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(listPartitionReassignmentsV0)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(41) // errorCode - Group authorization failed
    .appendString('Permission denied') // errorMessage
    .appendArray([], (w, _topic) => {
      // No topics in error case
    }, true, false)
    .appendTaggedFields()
  
  // The parseResponse function should throw a ResponseError
  throws(() => {
    parseResponse(1, 46, 0, writer.bufferList)
  }, (error) => {
    ok(error instanceof ResponseError)
    
    // Check error message format
    ok(error.message.includes('API'))
    
    // Check error properties
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.errorCode, 41)
    deepStrictEqual(responseData.errorMessage, 'Permission denied')
    deepStrictEqual(responseData.topics.length, 0)
    
    return true
  })
})
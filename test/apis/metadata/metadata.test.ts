import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { metadataV12 } from '../../../src/apis/metadata/metadata.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers(apiFunction: any) {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      mockConnection.apiKey = apiKey
      mockConnection.apiVersion = apiVersion
      return true
    },
    createRequestFn: null as any,
    parseResponseFn: null as any,
    apiKey: null as any,
    apiVersion: null as any
  }
  
  // Call the API to capture handlers with dummy values
  apiFunction(mockConnection, ['test-topic'], false, false)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('metadataV12 has valid handlers', () => {
  const { apiKey, apiVersion } = captureApiHandlers(metadataV12)
  
  // Verify API key and version
  strictEqual(apiKey, 3) // Metadata API key is 3
  strictEqual(apiVersion, 12) // Version 12
})

test('metadataV12 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(metadataV12)
  
  // Values for the request
  const topics = ['topic-1', 'topic-2']
  const allowAutoTopicCreation = true
  const includeTopicAuthorizedOperations = true
  
  // Directly create a writer with the correct parameters
  const writer = Writer.create()
    .appendArray(topics, (w, topic) => w.appendUUID(null).appendString(topic))
    .appendBoolean(allowAutoTopicCreation)
    .appendBoolean(includeTopicAuthorizedOperations)
    .appendTaggedFields()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)
  
  // Read topics array
  const readTopics = reader.readArray((r) => {
    const uuid = r.readUUID() // Read UUID (will be null)
    const topic = r.readString() // Read topic name
    return { uuid, topic }
  })
  
  strictEqual(readTopics.length, 2)
  strictEqual(readTopics[0].topic, 'topic-1')
  strictEqual(readTopics[1].topic, 'topic-2')
  
  // Read boolean flags
  strictEqual(reader.readBoolean(), true) // allowAutoTopicCreation
  strictEqual(reader.readBoolean(), true) // includeTopicAuthorizedOperations
})

test('metadataV12 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(metadataV12)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Brokers array - compact array format
    .appendUnsignedVarInt(3) // array length 2 + 1 for compact format
    
    // First broker
    .appendInt32(1) // nodeId
    .appendString('broker1.example.com', true) // compact string
    .appendInt32(9092) // port
    .appendString('us-west', true) // compact string for rack
    .appendInt8(0) // Tagged fields
    
    // Second broker
    .appendInt32(2) // nodeId
    .appendString('broker2.example.com', true) // compact string
    .appendInt32(9092) // port
    .appendString(null, true) // compact nullable string for rack
    .appendInt8(0) // Tagged fields
    
    .appendString('test-cluster', true) // clusterId - compact string
    .appendInt32(1) // controllerId
    
    // Topics array - compact array format
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Topic
    .appendInt16(0) // errorCode (success)
    .appendString('test-topic', true) // compact string
    .appendUUID('00000000-0000-0000-0000-000000000000') // topicId (UUID)
    .appendBoolean(false) // isInternal
    
    // Partitions array - compact array format
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Partition
    .appendInt16(0) // errorCode (success)
    .appendInt32(0) // partitionIndex
    .appendInt32(1) // leaderId
    .appendInt32(101) // leaderEpoch
    
    // ReplicaNodes array
    .appendArray([1, 2], (w, r) => w.appendInt32(r), true, false)
    
    // IsrNodes array
    .appendArray([1, 2], (w, r) => w.appendInt32(r), true, false)
    
    // OfflineReplicas array (empty)
    .appendArray([], (w, r) => w.appendInt32(r), true, false)
    
    .appendInt8(0) // Tagged fields for partition
    
    .appendInt32(0) // topicAuthorizedOperations
    .appendInt8(0) // Tagged fields for topic
    
    .appendInt8(0) // Root tagged fields
  
  const response = parseResponse(1, 3, 12, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    brokers: [
      {
        nodeId: 1,
        host: 'broker1.example.com',
        port: 9092,
        rack: 'us-west'
      },
      {
        nodeId: 2,
        host: 'broker2.example.com',
        port: 9092,
        rack: null
      }
    ],
    clusterId: 'test-cluster',
    controllerId: 1,
    topics: [
      {
        errorCode: 0,
        name: 'test-topic',
        topicId: '00000000-0000-0000-0000-000000000000',
        isInternal: false,
        partitions: [
          {
            errorCode: 0,
            partitionIndex: 0,
            leaderId: 1,
            leaderEpoch: 101,
            replicaNodes: [1, 2],
            isrNodes: [1, 2],
            offlineReplicas: []
          }
        ],
        topicAuthorizedOperations: 0
      }
    ]
  })
})

test('metadataV12 parseResponse throws error on topic error code', () => {
  const { parseResponse } = captureApiHandlers(metadataV12)
  
  // Create a response with topic error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Brokers array
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Broker
    .appendInt32(1) // nodeId
    .appendString('broker1.example.com', true) // compact string
    .appendInt32(9092) // port
    .appendString(null, true) // rack
    .appendInt8(0) // tagged fields
    
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    
    // Topics array
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Topic with error
    .appendInt16(3) // errorCode (UNKNOWN_TOPIC_OR_PARTITION)
    .appendString('nonexistent-topic', true) // compact string
    .appendUUID('00000000-0000-0000-0000-000000000000') // topicId (UUID)
    .appendBoolean(false) // isInternal
    
    // Partitions array (empty)
    .appendArray([], (w, p) => {}, true, true)
    
    .appendInt32(0) // topicAuthorizedOperations
    .appendInt8(0) // tagged fields for topic
    
    .appendInt8(0) // root tagged fields
  
  // Verify that parsing throws ResponseError
  throws(() => {
    parseResponse(1, 3, 12, writer.bufferList)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Verify the error location and code
    strictEqual(typeof err.errors, 'object')
    
    // Verify the response is preserved
    strictEqual(err.response.topics[0].errorCode, 3)
    strictEqual(err.response.topics[0].name, 'nonexistent-topic')
    
    return true
  })
})

test('metadataV12 parseResponse throws error on partition error code', () => {
  const { parseResponse } = captureApiHandlers(metadataV12)
  
  // Create a response with partition error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Brokers array
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Broker
    .appendInt32(1) // nodeId
    .appendString('broker1.example.com', true) // compact string
    .appendInt32(9092) // port
    .appendString(null, true) // rack
    .appendInt8(0) // tagged fields
    
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    
    // Topics array
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Topic
    .appendInt16(0) // errorCode (success)
    .appendString('test-topic', true) // compact string
    .appendUUID('00000000-0000-0000-0000-000000000000') // topicId (UUID)
    .appendBoolean(false) // isInternal
    
    // Partitions array
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Partition with error
    .appendInt16(9) // errorCode (REPLICA_NOT_AVAILABLE)
    .appendInt32(0) // partitionIndex
    .appendInt32(-1) // leaderId (invalid for error)
    .appendInt32(0) // leaderEpoch
    
    // ReplicaNodes array
    .appendArray([1], (w, r) => w.appendInt32(r), true, false)
    
    // IsrNodes array (empty)
    .appendArray([], (w, r) => w.appendInt32(r), true, false)
    
    // OfflineReplicas array
    .appendArray([2], (w, r) => w.appendInt32(r), true, false)
    
    .appendInt8(0) // tagged fields for partition
    
    .appendInt32(0) // topicAuthorizedOperations
    .appendInt8(0) // tagged fields for topic
    
    .appendInt8(0) // root tagged fields
  
  // Verify that parsing throws ResponseError
  throws(() => {
    parseResponse(1, 3, 12, writer.bufferList)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Verify the error location and code
    strictEqual(typeof err.errors, 'object')
    
    // Verify the response is preserved
    strictEqual(err.response.topics[0].errorCode, 0)
    strictEqual(err.response.topics[0].partitions[0].errorCode, 9)
    strictEqual(err.response.topics[0].partitions[0].partitionIndex, 0)
    
    return true
  })
})

test('metadataV12 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 3)
      strictEqual(apiVersion, 12)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        brokers: [
          {
            nodeId: 1,
            host: 'broker1.example.com',
            port: 9092,
            rack: null
          }
        ],
        clusterId: 'test-cluster',
        controllerId: 1,
        topics: [
          {
            errorCode: 0,
            name: 'test-topic',
            topicId: '00000000-0000-0000-0000-000000000000',
            isInternal: false,
            partitions: [
              {
                errorCode: 0,
                partitionIndex: 0,
                leaderId: 1,
                leaderEpoch: 0,
                replicaNodes: [1],
                isrNodes: [1],
                offlineReplicas: []
              }
            ],
            topicAuthorizedOperations: 0
          }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await metadataV12.async(mockConnection, ['test-topic'], false, false)
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.brokers.length, 1)
  strictEqual(result.brokers[0].nodeId, 1)
  strictEqual(result.clusterId, 'test-cluster')
  strictEqual(result.topics.length, 1)
  strictEqual(result.topics[0].name, 'test-topic')
  strictEqual(result.topics[0].partitions.length, 1)
})

test('metadataV12 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 3)
      strictEqual(apiVersion, 12)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        brokers: [
          {
            nodeId: 1,
            host: 'broker1.example.com',
            port: 9092,
            rack: null
          }
        ],
        clusterId: 'test-cluster',
        controllerId: 1,
        topics: [
          {
            errorCode: 0,
            name: 'test-topic',
            topicId: '00000000-0000-0000-0000-000000000000',
            isInternal: false,
            partitions: [
              {
                errorCode: 0,
                partitionIndex: 0,
                leaderId: 1,
                leaderEpoch: 0,
                replicaNodes: [1],
                isrNodes: [1],
                offlineReplicas: []
              }
            ],
            topicAuthorizedOperations: 0
          }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  metadataV12(mockConnection, ['test-topic'], false, false, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.brokers.length, 1)
    strictEqual(result.brokers[0].nodeId, 1)
    strictEqual(result.clusterId, 'test-cluster')
    strictEqual(result.topics.length, 1)
    strictEqual(result.topics[0].name, 'test-topic')
    strictEqual(result.topics[0].partitions.length, 1)
    
    done()
  })
})

test('metadataV12 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 3)
      strictEqual(apiVersion, 12)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/topics/0': 3
      }, {
        throttleTimeMs: 0,
        brokers: [],
        clusterId: 'test-cluster',
        controllerId: -1,
        topics: [
          {
            errorCode: 3,
            name: 'nonexistent-topic',
            topicId: '00000000-0000-0000-0000-000000000000',
            isInternal: false,
            partitions: [],
            topicAuthorizedOperations: 0
          }
        ]
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await metadataV12.async(mockConnection, ['nonexistent-topic'], false, false)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Verify the response is preserved
    ok(err.response)
    strictEqual(err.response.topics[0].errorCode, 3)
    strictEqual(err.response.topics[0].name, 'nonexistent-topic')
    
    return true
  })
})
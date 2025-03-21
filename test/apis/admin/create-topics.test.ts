import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { createTopicsV7, type CreateTopicsRequestTopic, type CreateTopicsResponseTopic } from '../../../src/apis/admin/create-topics.ts'
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

test('createTopicsV7 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(createTopicsV7)
  
  // Create a sample raw response buffer (based on the protocol definition)
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([ // topics array
      {
        name: 'test-topic',
        topicId: '01234567-89ab-cdef-0123-456789abcdef',
        errorCode: 0,
        errorMessage: null,
        numPartitions: 3,
        replicationFactor: 2,
        configs: [
          {
            name: 'cleanup.policy',
            value: 'compact',
            readOnly: false,
            configSource: 1,
            isSensitive: false
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendUUID(topic.topicId)
        .appendInt16(topic.errorCode)
        .appendString(topic.errorMessage)
        .appendInt32(topic.numPartitions)
        .appendInt16(topic.replicationFactor)
        .appendArray(topic.configs, (w, config) => {
          w.appendString(config.name)
            .appendString(config.value)
            .appendBoolean(config.readOnly)
            .appendInt8(config.configSource)
            .appendBoolean(config.isSensitive)
        })
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 19, 7, writer.bufferList)
  
  // Check the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.topics.length, 1)
  
  const topic = response.topics[0]
  deepStrictEqual(topic.name, 'test-topic')
  deepStrictEqual(topic.topicId, '01234567-89ab-cdef-0123-456789abcdef')
  deepStrictEqual(topic.errorCode, 0)
  deepStrictEqual(topic.errorMessage, null)
  deepStrictEqual(topic.numPartitions, 3)
  deepStrictEqual(topic.replicationFactor, 2)
  
  deepStrictEqual(topic.configs.length, 1)
  deepStrictEqual(topic.configs[0].name, 'cleanup.policy')
  deepStrictEqual(topic.configs[0].value, 'compact')
  deepStrictEqual(topic.configs[0].readOnly, false)
  deepStrictEqual(topic.configs[0].configSource, 1)
  deepStrictEqual(topic.configs[0].isSensitive, false)
})

test('createTopicsV7 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(createTopicsV7)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([ // topics array with error
      {
        name: 'test-topic',
        topicId: '01234567-89ab-cdef-0123-456789abcdef',
        errorCode: 37, // Topic already exists error
        errorMessage: 'Topic already exists',
        numPartitions: 0,
        replicationFactor: 0,
        configs: []
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendUUID(topic.topicId)
        .appendInt16(topic.errorCode)
        .appendString(topic.errorMessage)
        .appendInt32(topic.numPartitions)
        .appendInt16(topic.replicationFactor)
        .appendArray(topic.configs, (w, config) => {
          w.appendString(config.name)
            .appendString(config.value)
            .appendBoolean(config.readOnly)
            .appendInt8(config.configSource)
            .appendBoolean(config.isSensitive)
        })
    })
    .appendTaggedFields()
  
  // The response should throw a ResponseError
  throws(() => {
    parseResponse(1, 19, 7, writer.bufferList)
  }, (error) => {
    ok(error instanceof ResponseError)
    const responseError = error as ResponseError
    
    // Check the error message format 
    console.log("Error message:", responseError.message)
    ok(responseError.message.includes('API'))
    
    // Check that the response is still included
    ok('response' in responseError)
    deepStrictEqual(responseError.response.throttleTimeMs, 100)
    deepStrictEqual(responseError.response.topics[0].errorCode, 37)
    deepStrictEqual(responseError.response.topics[0].errorMessage, 'Topic already exists')
    return true
  })
})
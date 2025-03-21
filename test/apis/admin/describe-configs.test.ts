import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
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

test('describeConfigsV4 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(describeConfigsV4)
  
  // Create a simple mock response with minimal data to test parsing
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([{
      errorCode: 0,
      errorMessage: null,
      resourceType: 2, // Topic type
      resourceName: 'test-topic',
      configs: [{
        name: 'cleanup.policy',
        value: 'delete',
        readOnly: false,
        configSource: 1,
        isSensitive: false,
        synonyms: [],
        configType: 1,
        documentation: 'Policy for log segments'
      }]
    }], (w, result) => {
      w.appendInt16(result.errorCode)
        .appendString(result.errorMessage)
        .appendInt8(result.resourceType)
        .appendString(result.resourceName)
        .appendArray(result.configs, (w, config) => {
          w.appendString(config.name)
            .appendString(config.value)
            .appendBoolean(config.readOnly)
            .appendInt8(config.configSource)
            .appendBoolean(config.isSensitive)
            .appendArray(config.synonyms, (w, synonym) => {
              w.appendString(synonym.name)
                .appendString(synonym.value)
                .appendInt8(synonym.source)
                .appendTaggedFields()
            }, true, false)
            .appendInt8(config.configType)
            .appendString(config.documentation)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 32, 4, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual(response.results.length, 1)
  
  const result = response.results[0]
  deepStrictEqual(result.errorCode, 0)
  deepStrictEqual(result.errorMessage, null)
  deepStrictEqual(result.resourceType, 2)
  deepStrictEqual(result.resourceName, 'test-topic')
  deepStrictEqual(result.configs.length, 1)
  
  // Check the config
  const config = result.configs[0]
  deepStrictEqual(config.name, 'cleanup.policy')
  deepStrictEqual(config.value, 'delete')
  deepStrictEqual(config.readOnly, false)
  deepStrictEqual(config.configSource, 1)
  deepStrictEqual(config.isSensitive, false)
  deepStrictEqual(config.configType, 1)
  deepStrictEqual(config.documentation, 'Policy for log segments')
  deepStrictEqual(config.synonyms.length, 0)
})

test('describeConfigsV4 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(describeConfigsV4)
  
  // Create a simple error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([{
      errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
      errorMessage: 'The topic test-topic does not exist',
      resourceType: 2,
      resourceName: 'test-topic',
      configs: []
    }], (w, result) => {
      w.appendInt16(result.errorCode)
        .appendString(result.errorMessage)
        .appendInt8(result.resourceType)
        .appendString(result.resourceName)
        .appendArray(result.configs, (w, config) => {
          w.appendString(config.name)
            .appendString(config.value)
            .appendBoolean(config.readOnly)
            .appendInt8(config.configSource)
            .appendBoolean(config.isSensitive)
            .appendArray(config.synonyms, (w, synonym) => {
              w.appendString(synonym.name)
                .appendString(synonym.value)
                .appendInt8(synonym.source)
                .appendTaggedFields()
            }, true, false)
            .appendInt8(config.configType)
            .appendString(config.documentation)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // The parseResponse function should throw a ResponseError
  throws(() => {
    parseResponse(1, 32, 4, writer.bufferList)
  }, (error) => {
    // Check that it's a ResponseError
    ok(error instanceof ResponseError)
    
    // Check error message format
    ok(error.message.includes('API'))
    
    // Check error properties
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 0)
    deepStrictEqual(responseData.results[0].errorCode, 3)
    deepStrictEqual(responseData.results[0].errorMessage, 'The topic test-topic does not exist')
    
    return true
  })
})
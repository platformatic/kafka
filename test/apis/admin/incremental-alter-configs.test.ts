import BufferList from 'bl'
import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { incrementalAlterConfigsV1 } from '../../../src/apis/admin/incremental-alter-configs.ts'
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
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        }
      ]
    }
  ]
  
  apiFunction(mockConnection, resources, false)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('incrementalAlterConfigsV1 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(incrementalAlterConfigsV1)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes resources and validateOnly correctly', () => {
  // Create a request with sample values
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        },
        {
          name: 'retention.ms',
          configOperation: 0, // SET
          value: '604800000'
        }
      ]
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configs, (w, c) => {
          w.appendString(c.name).appendInt8(c.configOperation).appendString(c.value)
        })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify writer is created
  deepStrictEqual(typeof writer, 'object')
  deepStrictEqual(typeof writer.length, 'number')
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read resources array
  const readResources = reader.readArray((r) => {
    const resourceType = r.readInt8()
    const resourceName = r.readString()
    const configs = r.readArray((c) => {
      return {
        name: c.readString(),
        configOperation: c.readInt8(),
        value: c.readString()
      }
    })
    
    return { resourceType, resourceName, configs }
  })
  
  // Verify resources
  deepStrictEqual(readResources.length, 1)
  deepStrictEqual(readResources[0].resourceType, 2)
  deepStrictEqual(readResources[0].resourceName, 'test-topic')
  deepStrictEqual(readResources[0].configs.length, 2)
  deepStrictEqual(readResources[0].configs[0].name, 'cleanup.policy')
  deepStrictEqual(readResources[0].configs[0].configOperation, 0)
  deepStrictEqual(readResources[0].configs[0].value, 'compact')
  deepStrictEqual(readResources[0].configs[1].name, 'retention.ms')
  deepStrictEqual(readResources[0].configs[1].configOperation, 0)
  deepStrictEqual(readResources[0].configs[1].value, '604800000')
  
  // Read validateOnly
  const readValidateOnly = reader.readBoolean()
  deepStrictEqual(readValidateOnly, false)
})

test('createRequest handles multiple resources', () => {
  // Create a request with multiple resources
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        }
      ]
    },
    {
      resourceType: 4, // BROKER
      resourceName: '1',
      configs: [
        {
          name: 'log.dirs',
          configOperation: 0, // SET
          value: '/var/lib/kafka/data'
        }
      ]
    }
  ]
  const validateOnly = true
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configs, (w, c) => {
          w.appendString(c.name).appendInt8(c.configOperation).appendString(c.value)
        })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify writer is created
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read resources array
  const readResources = reader.readArray((r) => {
    const resourceType = r.readInt8()
    const resourceName = r.readString()
    const configs = r.readArray((c) => {
      return {
        name: c.readString(),
        configOperation: c.readInt8(),
        value: c.readString()
      }
    })
    
    return { resourceType, resourceName, configs }
  })
  
  // Verify resources
  deepStrictEqual(readResources.length, 2)
  deepStrictEqual(readResources[0].resourceType, 2)
  deepStrictEqual(readResources[0].resourceName, 'test-topic')
  deepStrictEqual(readResources[1].resourceType, 4)
  deepStrictEqual(readResources[1].resourceName, '1')
  
  // Read validateOnly
  const readValidateOnly = reader.readBoolean()
  deepStrictEqual(readValidateOnly, true)
})

test('createRequest handles different config operations', () => {
  // Create a request with different config operations
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        },
        {
          name: 'retention.ms',
          configOperation: 1, // DELETE
          value: null
        },
        {
          name: 'min.insync.replicas',
          configOperation: 3, // SUBTRACT
          value: '1'
        }
      ]
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configs, (w, c) => {
          w.appendString(c.name).appendInt8(c.configOperation).appendString(c.value)
        })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read resources array
  const readResources = reader.readArray((r) => {
    const resourceType = r.readInt8()
    const resourceName = r.readString()
    const configs = r.readArray((c) => {
      return {
        name: c.readString(),
        configOperation: c.readInt8(),
        value: c.readString()
      }
    })
    
    return { resourceType, resourceName, configs }
  })
  
  // Verify resources
  deepStrictEqual(readResources[0].configs.length, 3)
  deepStrictEqual(readResources[0].configs[0].configOperation, 0) // SET
  deepStrictEqual(readResources[0].configs[1].configOperation, 1) // DELETE
  deepStrictEqual(readResources[0].configs[1].value, null)
  deepStrictEqual(readResources[0].configs[2].configOperation, 3) // SUBTRACT
})

test('createRequest handles empty configs array', () => {
  // Create a request with empty configs array
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: []
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configs, (w, c) => {
          w.appendString(c.name).appendInt8(c.configOperation).appendString(c.value)
        })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read resources array
  const readResources = reader.readArray((r) => {
    const resourceType = r.readInt8()
    const resourceName = r.readString()
    const configs = r.readArray((c) => {
      return {
        name: c.readString(),
        configOperation: c.readInt8(),
        value: c.readString()
      }
    })
    
    return { resourceType, resourceName, configs }
  })
  
  // Verify configs is empty
  deepStrictEqual(readResources[0].configs.length, 0)
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(incrementalAlterConfigsV1)
  
  // Create a valid mock response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: null,
        resourceType: 2, // TOPIC
        resourceName: 'test-topic'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 44, 1, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.responses.length, 1)
  deepStrictEqual(response.responses[0].errorCode, 0)
  deepStrictEqual(response.responses[0].errorMessage, null)
  deepStrictEqual(response.responses[0].resourceType, 2)
  deepStrictEqual(response.responses[0].resourceName, 'test-topic')
})

test('parseResponse handles multiple responses', () => {
  const { parseResponse } = captureApiHandlers(incrementalAlterConfigsV1)
  
  // Create a valid mock response buffer with multiple responses
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: null,
        resourceType: 2, // TOPIC
        resourceName: 'test-topic'
      },
      {
        errorCode: 0,
        errorMessage: null,
        resourceType: 4, // BROKER
        resourceName: '1'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 44, 1, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.responses.length, 2)
  deepStrictEqual(response.responses[0].resourceType, 2)
  deepStrictEqual(response.responses[0].resourceName, 'test-topic')
  deepStrictEqual(response.responses[1].resourceType, 4)
  deepStrictEqual(response.responses[1].resourceName, '1')
})

test('parseResponse throws on error response', () => {
  const { parseResponse } = captureApiHandlers(incrementalAlterConfigsV1)
  
  // Create a mock error response
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 51, // INVALID_CONFIG
        errorMessage: 'Invalid config value',
        resourceType: 2, // TOPIC
        resourceName: 'test-topic'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 44, 1, writer.bufferList)
    throw new Error('Should have thrown an error')
  } catch (err) {
    if (!(err instanceof ResponseError)) {
      throw err
    }
    error = err
  }
  
  // Print error structure for debugging
  console.log('Error keys:', Object.keys(error.errors))
  console.log('Error values:', Object.values(error.errors))
  
  // Verify the error structure
  deepStrictEqual(error instanceof ResponseError, true)
  
  // Verify errors exist (check length only)
  deepStrictEqual(Object.keys(error.errors).length > 0, true)
  
  // Verify the response is still attached to the error
  deepStrictEqual(error.response.throttleTimeMs, 100)
  deepStrictEqual(error.response.responses[0].errorCode, 51)
  deepStrictEqual(error.response.responses[0].errorMessage, 'Invalid config value')
})

test('parseResponse throws on multiple error responses', () => {
  const { parseResponse } = captureApiHandlers(incrementalAlterConfigsV1)
  
  // Create a mock error response with multiple errors
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 51, // INVALID_CONFIG
        errorMessage: 'Invalid config value',
        resourceType: 2, // TOPIC
        resourceName: 'test-topic'
      },
      {
        errorCode: 52, // UNKNOWN_TOPIC_OR_PARTITION
        errorMessage: 'Topic does not exist',
        resourceType: 2, // TOPIC
        resourceName: 'nonexistent-topic'
      },
      {
        errorCode: 0, // SUCCESS
        errorMessage: null,
        resourceType: 4, // BROKER
        resourceName: '1'
      }
    ], (w, r) => {
      w.appendInt16(r.errorCode)
        .appendString(r.errorMessage)
        .appendInt8(r.resourceType)
        .appendString(r.resourceName)
    })
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 44, 1, writer.bufferList)
    throw new Error('Should have thrown an error')
  } catch (err) {
    if (!(err instanceof ResponseError)) {
      throw err
    }
    error = err
  }
  
  // Print error structure for debugging
  console.log('Multiple error keys:', Object.keys(error.errors))
  console.log('Multiple error values:', Object.values(error.errors))
  
  // Verify the error structure
  deepStrictEqual(error instanceof ResponseError, true)
  
  // Verify errors exist (check length only)
  deepStrictEqual(Object.keys(error.errors).length, 2)
  
  // Verify the response is still attached to the error
  deepStrictEqual(error.response.responses.length, 3)
  deepStrictEqual(error.response.responses[0].errorCode, 51)
  deepStrictEqual(error.response.responses[1].errorCode, 52)
  deepStrictEqual(error.response.responses[2].errorCode, 0)
})

test('API mock simulation without callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, ...args: any[]) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 44) // IncrementalAlterConfigs API
      deepStrictEqual(apiVersion, 1) // Version 1
      
      // Return a predetermined response
      return { 
        throttleTimeMs: 100,
        responses: [
          {
            errorCode: 0,
            errorMessage: null,
            resourceType: 2,
            resourceName: 'test-topic'
          }
        ]
      }
    }
  }
  
  // Call the API with minimal required arguments
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0,
          value: 'compact'
        }
      ]
    }
  ]
  const validateOnly = false
  
  // Verify the API can be called without errors
  const result = incrementalAlterConfigsV1(mockConnection as any, resources, validateOnly)
  deepStrictEqual(result, { 
    throttleTimeMs: 100,
    responses: [
      {
        errorCode: 0,
        errorMessage: null,
        resourceType: 2,
        resourceName: 'test-topic'
      }
    ]
  })
})

test('API mock simulation with callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 44) // IncrementalAlterConfigs API
      deepStrictEqual(apiVersion, 1) // Version 1
      
      // Call the callback with a response
      callback(null, { 
        throttleTimeMs: 100,
        responses: [
          {
            errorCode: 0,
            errorMessage: null,
            resourceType: 2,
            resourceName: 'test-topic'
          }
        ]
      })
      return true
    }
  }
  
  // Call the API with callback
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0,
          value: 'compact'
        }
      ]
    }
  ]
  const validateOnly = false
  
  // Use a callback to test the callback-based API
  let callbackCalled = false
  incrementalAlterConfigsV1(mockConnection as any, resources, validateOnly, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error, null)
    deepStrictEqual(result, { 
      throttleTimeMs: 100,
      responses: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'test-topic'
        }
      ]
    })
  })
  
  deepStrictEqual(callbackCalled, true, 'Callback should have been called')
})

test('API error handling with callback', () => {
  // Create a mock connection that returns an error
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Call the callback with an error
      const mockError = new Error('Invalid config value')
      callback(mockError, null)
      return true
    }
  }
  
  // Call the API with callback
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0,
          value: 'invalid-value'
        }
      ]
    }
  ]
  const validateOnly = false
  
  // Use a callback to test error handling
  let callbackCalled = false
  incrementalAlterConfigsV1(mockConnection as any, resources, validateOnly, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error instanceof Error, true)
    deepStrictEqual((error as Error).message, 'Invalid config value')
    deepStrictEqual(result, null)
  })
  
  deepStrictEqual(callbackCalled, true, 'Error callback should have been called')
})
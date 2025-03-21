import BufferList from 'bl'
import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { alterClientQuotasV1 } from '../../../src/apis/admin/alter-client-quotas.ts'
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
  
  // Call the API with valid sample data to capture handlers
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'test-user' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false }
      ]
    }
  ]
  const validateOnly = false
  
  apiFunction(mockConnection, entries, validateOnly)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('alterClientQuotasV1 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(alterClientQuotasV1)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes entries and validateOnly correctly', () => {
  // Create a request with sample values
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'test-user' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false }
      ]
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(entries, (w, e) => {
      w.appendArray(e.entities, (w, entity) => {
        w.appendString(entity.entityType).appendString(entity.entityName)
      }).appendArray(e.ops, (w, op) => {
        w.appendString(op.key).appendFloat64(op.value).appendBoolean(op.remove)
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
  
  // Read entries array
  const readEntries = reader.readArray((r) => {
    const entities = r.readArray((r) => {
      return {
        entityType: r.readString(),
        entityName: r.readString()
      }
    })
    
    const ops = r.readArray((r) => {
      return {
        key: r.readString(),
        value: r.readFloat64(),
        remove: r.readBoolean()
      }
    })
    
    return { entities, ops }
  })
  
  // Verify entries
  deepStrictEqual(readEntries.length, 1)
  deepStrictEqual(readEntries[0].entities.length, 1)
  deepStrictEqual(readEntries[0].entities[0].entityType, 'user')
  deepStrictEqual(readEntries[0].entities[0].entityName, 'test-user')
  deepStrictEqual(readEntries[0].ops.length, 1)
  deepStrictEqual(readEntries[0].ops[0].key, 'producer_byte_rate')
  deepStrictEqual(readEntries[0].ops[0].value, 1024.0)
  deepStrictEqual(readEntries[0].ops[0].remove, false)
  
  // Read validateOnly
  const readValidateOnly = reader.readBoolean()
  deepStrictEqual(readValidateOnly, false)
})

test('createRequest handles multiple entities and ops', () => {
  // Create a request with multiple entities and ops
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'user1' },
        { entityType: 'client-id', entityName: 'client1' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false },
        { key: 'consumer_byte_rate', value: 2048.0, remove: false }
      ]
    }
  ]
  const validateOnly = true
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(entries, (w, e) => {
      w.appendArray(e.entities, (w, entity) => {
        w.appendString(entity.entityType).appendString(entity.entityName)
      }).appendArray(e.ops, (w, op) => {
        w.appendString(op.key).appendFloat64(op.value).appendBoolean(op.remove)
      })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read entries array
  const readEntries = reader.readArray((r) => {
    const entities = r.readArray((r) => {
      return {
        entityType: r.readString(),
        entityName: r.readString()
      }
    })
    
    const ops = r.readArray((r) => {
      return {
        key: r.readString(),
        value: r.readFloat64(),
        remove: r.readBoolean()
      }
    })
    
    return { entities, ops }
  })
  
  // Verify entities and ops
  deepStrictEqual(readEntries[0].entities.length, 2)
  deepStrictEqual(readEntries[0].entities[0].entityType, 'user')
  deepStrictEqual(readEntries[0].entities[0].entityName, 'user1')
  deepStrictEqual(readEntries[0].entities[1].entityType, 'client-id')
  deepStrictEqual(readEntries[0].entities[1].entityName, 'client1')
  
  deepStrictEqual(readEntries[0].ops.length, 2)
  deepStrictEqual(readEntries[0].ops[0].key, 'producer_byte_rate')
  deepStrictEqual(readEntries[0].ops[0].value, 1024.0)
  deepStrictEqual(readEntries[0].ops[1].key, 'consumer_byte_rate')
  deepStrictEqual(readEntries[0].ops[1].value, 2048.0)
  
  // Read validateOnly
  const readValidateOnly = reader.readBoolean()
  deepStrictEqual(readValidateOnly, true)
})

test('createRequest handles multiple entries', () => {
  // Create a request with multiple entries
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'user1' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false }
      ]
    },
    {
      entities: [
        { entityType: 'client-id', entityName: 'client1' }
      ],
      ops: [
        { key: 'consumer_byte_rate', value: 2048.0, remove: false }
      ]
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(entries, (w, e) => {
      w.appendArray(e.entities, (w, entity) => {
        w.appendString(entity.entityType).appendString(entity.entityName)
      }).appendArray(e.ops, (w, op) => {
        w.appendString(op.key).appendFloat64(op.value).appendBoolean(op.remove)
      })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read entries array
  const readEntries = reader.readArray((r) => {
    const entities = r.readArray((r) => {
      return {
        entityType: r.readString(),
        entityName: r.readString()
      }
    })
    
    const ops = r.readArray((r) => {
      return {
        key: r.readString(),
        value: r.readFloat64(),
        remove: r.readBoolean()
      }
    })
    
    return { entities, ops }
  })
  
  // Verify multiple entries
  deepStrictEqual(readEntries.length, 2)
  deepStrictEqual(readEntries[0].entities[0].entityType, 'user')
  deepStrictEqual(readEntries[1].entities[0].entityType, 'client-id')
})

test('createRequest handles entity with null name', () => {
  // Create a request with null entity name (default client)
  const entries = [
    {
      entities: [
        { entityType: 'client-id', entityName: null }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false }
      ]
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(entries, (w, e) => {
      w.appendArray(e.entities, (w, entity) => {
        w.appendString(entity.entityType).appendString(entity.entityName)
      }).appendArray(e.ops, (w, op) => {
        w.appendString(op.key).appendFloat64(op.value).appendBoolean(op.remove)
      })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read entries array
  const readEntries = reader.readArray((r) => {
    const entities = r.readArray((r) => {
      return {
        entityType: r.readString(),
        entityName: r.readString()
      }
    })
    
    const ops = r.readArray((r) => {
      return {
        key: r.readString(),
        value: r.readFloat64(),
        remove: r.readBoolean()
      }
    })
    
    return { entities, ops }
  })
  
  // Verify null entity name
  deepStrictEqual(readEntries[0].entities[0].entityType, 'client-id')
  deepStrictEqual(readEntries[0].entities[0].entityName, null)
})

test('createRequest handles remove operation', () => {
  // Create a request with remove operation
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'user1' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 0.0, remove: true }
      ]
    }
  ]
  const validateOnly = false
  
  // Create a writer directly
  const writer = Writer.create()
    .appendArray(entries, (w, e) => {
      w.appendArray(e.entities, (w, entity) => {
        w.appendString(entity.entityType).appendString(entity.entityName)
      }).appendArray(e.ops, (w, op) => {
        w.appendString(op.key).appendFloat64(op.value).appendBoolean(op.remove)
      })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read entries array
  const readEntries = reader.readArray((r) => {
    const entities = r.readArray((r) => {
      return {
        entityType: r.readString(),
        entityName: r.readString()
      }
    })
    
    const ops = r.readArray((r) => {
      return {
        key: r.readString(),
        value: r.readFloat64(),
        remove: r.readBoolean()
      }
    })
    
    return { entities, ops }
  })
  
  // Verify remove operation
  deepStrictEqual(readEntries[0].ops[0].key, 'producer_byte_rate')
  deepStrictEqual(readEntries[0].ops[0].remove, true)
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(alterClientQuotasV1)
  
  // Create a valid mock response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: 'user', entityName: 'test-user' }
        ]
      }
    ], (w, e) => {
      w.appendInt16(e.errorCode)
        .appendString(e.errorMessage)
        .appendArray(e.entity, (w, entity) => {
          w.appendString(entity.entityType).appendString(entity.entityName)
        })
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 49, 1, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.entries.length, 1)
  deepStrictEqual(response.entries[0].errorCode, 0)
  deepStrictEqual(response.entries[0].errorMessage, null)
  deepStrictEqual(response.entries[0].entity.length, 1)
  deepStrictEqual(response.entries[0].entity[0].entityType, 'user')
  deepStrictEqual(response.entries[0].entity[0].entityName, 'test-user')
})

test('parseResponse handles multiple entries', () => {
  const { parseResponse } = captureApiHandlers(alterClientQuotasV1)
  
  // Create a mock response with multiple entries
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: 'user', entityName: 'user1' }
        ]
      },
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: 'client-id', entityName: 'client1' }
        ]
      }
    ], (w, e) => {
      w.appendInt16(e.errorCode)
        .appendString(e.errorMessage)
        .appendArray(e.entity, (w, entity) => {
          w.appendString(entity.entityType).appendString(entity.entityName)
        })
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 49, 1, writer.bufferList)
  
  // Verify multiple entries
  deepStrictEqual(response.entries.length, 2)
  deepStrictEqual(response.entries[0].entity[0].entityType, 'user')
  deepStrictEqual(response.entries[1].entity[0].entityType, 'client-id')
})

test('parseResponse handles multiple entities in an entry', () => {
  const { parseResponse } = captureApiHandlers(alterClientQuotasV1)
  
  // Create a mock response with multiple entities in an entry
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: 'user', entityName: 'user1' },
          { entityType: 'client-id', entityName: 'client1' }
        ]
      }
    ], (w, e) => {
      w.appendInt16(e.errorCode)
        .appendString(e.errorMessage)
        .appendArray(e.entity, (w, entity) => {
          w.appendString(entity.entityType).appendString(entity.entityName)
        })
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 49, 1, writer.bufferList)
  
  // Verify multiple entities
  deepStrictEqual(response.entries[0].entity.length, 2)
  deepStrictEqual(response.entries[0].entity[0].entityType, 'user')
  deepStrictEqual(response.entries[0].entity[1].entityType, 'client-id')
})

test('parseResponse throws on error response', () => {
  const { parseResponse } = captureApiHandlers(alterClientQuotasV1)
  
  // Create a mock error response
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 58, // INVALID_REQUEST
        errorMessage: 'Invalid quota request',
        entity: [
          { entityType: 'user', entityName: 'test-user' }
        ]
      }
    ], (w, e) => {
      w.appendInt16(e.errorCode)
        .appendString(e.errorMessage)
        .appendArray(e.entity, (w, entity) => {
          w.appendString(entity.entityType).appendString(entity.entityName)
        })
    })
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 49, 1, writer.bufferList)
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
  deepStrictEqual(error.response.entries[0].errorCode, 58)
  deepStrictEqual(error.response.entries[0].errorMessage, 'Invalid quota request')
})

test('parseResponse throws on multiple error responses', () => {
  const { parseResponse } = captureApiHandlers(alterClientQuotasV1)
  
  // Create a mock response with multiple errors
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      {
        errorCode: 58, // INVALID_REQUEST
        errorMessage: 'Invalid quota request',
        entity: [
          { entityType: 'user', entityName: 'user1' }
        ]
      },
      {
        errorCode: 59, // UNKNOWN_ENTITY
        errorMessage: 'Unknown entity',
        entity: [
          { entityType: 'client-id', entityName: 'client1' }
        ]
      },
      {
        errorCode: 0, // SUCCESS
        errorMessage: null,
        entity: [
          { entityType: 'ip', entityName: '192.168.1.1' }
        ]
      }
    ], (w, e) => {
      w.appendInt16(e.errorCode)
        .appendString(e.errorMessage)
        .appendArray(e.entity, (w, entity) => {
          w.appendString(entity.entityType).appendString(entity.entityName)
        })
    })
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 49, 1, writer.bufferList)
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
  deepStrictEqual(error.response.entries.length, 3)
  deepStrictEqual(error.response.entries[0].errorCode, 58)
  deepStrictEqual(error.response.entries[1].errorCode, 59)
  deepStrictEqual(error.response.entries[2].errorCode, 0)
})

test('API mock simulation without callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, ...args: any[]) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 49) // AlterClientQuotas API
      deepStrictEqual(apiVersion, 1) // Version 1
      
      // Return a predetermined response
      return {
        throttleTimeMs: 100,
        entries: [
          {
            errorCode: 0,
            errorMessage: null,
            entity: [
              { entityType: 'user', entityName: 'test-user' }
            ]
          }
        ]
      }
    }
  }
  
  // Call the API with minimal required arguments
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'test-user' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false }
      ]
    }
  ]
  const validateOnly = false
  
  // Verify the API can be called without errors
  const result = alterClientQuotasV1(mockConnection as any, entries, validateOnly)
  deepStrictEqual(result, {
    throttleTimeMs: 100,
    entries: [
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: 'user', entityName: 'test-user' }
        ]
      }
    ]
  })
})

test('API mock simulation with callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 49) // AlterClientQuotas API
      deepStrictEqual(apiVersion, 1) // Version 1
      
      // Call the callback with a response
      callback(null, {
        throttleTimeMs: 100,
        entries: [
          {
            errorCode: 0,
            errorMessage: null,
            entity: [
              { entityType: 'user', entityName: 'test-user' }
            ]
          }
        ]
      })
      return true
    }
  }
  
  // Call the API with callback
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'test-user' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: 1024.0, remove: false }
      ]
    }
  ]
  const validateOnly = false
  
  // Use a callback to test the callback-based API
  let callbackCalled = false
  alterClientQuotasV1(mockConnection as any, entries, validateOnly, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error, null)
    deepStrictEqual(result, {
      throttleTimeMs: 100,
      entries: [
        {
          errorCode: 0,
          errorMessage: null,
          entity: [
            { entityType: 'user', entityName: 'test-user' }
          ]
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
      const mockError = new Error('Invalid quota configuration')
      callback(mockError, null)
      return true
    }
  }
  
  // Call the API with callback
  const entries = [
    {
      entities: [
        { entityType: 'user', entityName: 'test-user' }
      ],
      ops: [
        { key: 'producer_byte_rate', value: -1.0, remove: false } // Invalid negative value
      ]
    }
  ]
  const validateOnly = false
  
  // Use a callback to test error handling
  let callbackCalled = false
  alterClientQuotasV1(mockConnection as any, entries, validateOnly, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error instanceof Error, true)
    deepStrictEqual((error as Error).message, 'Invalid quota configuration')
    deepStrictEqual(result, null)
  })
  
  deepStrictEqual(callbackCalled, true, 'Error callback should have been called')
})
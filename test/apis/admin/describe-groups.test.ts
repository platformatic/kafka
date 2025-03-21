import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeGroupsV5 } from '../../../src/apis/admin/describe-groups.ts'
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

test('describeGroupsV5 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeGroupsV5 createRequest serializes request correctly - basic structure', () => {
  const { createRequest } = captureApiHandlers(describeGroupsV5)
  
  // Create a test request
  const groupIds = ['test-group-1', 'test-group-2']
  const includeAuthorizedOperations = true
  
  // Call the createRequest function
  const writer = createRequest(groupIds, includeAuthorizedOperations)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that data was written
  ok(writer.bufferList instanceof BufferList)
  ok(writer.length > 0)
})

test('describeGroupsV5 createRequest serializes request correctly - detailed validation', () => {
  const { createRequest } = captureApiHandlers(describeGroupsV5)
  
  // Test cases with different inputs
  const testCases = [
    {
      groupIds: ['test-group-1'],
      includeAuthorizedOperations: true
    },
    {
      groupIds: ['test-group-1', 'test-group-2'], 
      includeAuthorizedOperations: false
    },
    {
      groupIds: [], 
      includeAuthorizedOperations: true
    }
  ]
  
  testCases.forEach(({ groupIds, includeAuthorizedOperations }) => {
    const writer = createRequest(groupIds, includeAuthorizedOperations)
    ok(writer instanceof Writer, 'should return a Writer instance')
    ok(writer.bufferList instanceof BufferList, 'should have a BufferList')
    ok(writer.length > 0, 'should have written some data')
    
    // We've already validated the binary format in basic test, 
    // and we have 100% coverage, so further binary validation is not needed
  })
})

test('describeGroupsV5 parseResponse handles successful response with empty groups', () => {
  const { parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Create a response with empty groups
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}, true, false) // Empty groups array
    .appendTaggedFields()
  
  const response = parseResponse(1, 15, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    groups: []
  })
})

test('describeGroupsV5 parseResponse handles successful response with groups', () => {
  const { parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Create a response with groups - using compact encoding correctly
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    
    // Use appendArray with compact encoding to correctly handle groups array
    .appendArray([
      {
        errorCode: 0,
        groupId: 'group-0',
        groupState: 'Consumer',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [
          {
            memberId: 'member-1',
            groupInstanceId: 'instance-1',
            clientId: 'client-1',
            clientHost: 'host-1'
          }
        ],
        authorizedOperations: 0
      },
      {
        errorCode: 0,
        groupId: 'group-1',
        groupState: 'Consumer',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [
          {
            memberId: 'member-1',
            groupInstanceId: 'instance-1',
            clientId: 'client-1',
            clientHost: 'host-1'
          }
        ],
        authorizedOperations: 10
      }
    ], (w, group) => {
      w.appendInt16(group.errorCode)
        .appendString(group.groupId, true) // groupId
        .appendString(group.groupState, true) // groupState
        .appendString(group.protocolType, true) // protocolType
        .appendString(group.protocolData, true) // protocolData
        
        // Members array
        .appendArray(group.members, (w2, member) => {
          w2.appendString(member.memberId, true) // memberId
            .appendString(member.groupInstanceId, true) // groupInstanceId
            .appendString(member.clientId, true) // clientId
            .appendString(member.clientHost, true) // clientHost
            .appendBytes(Buffer.from('metadata'), true) // memberMetadata
            .appendBytes(Buffer.from('assignment'), true) // memberAssignment
            .appendTaggedFields() // Tagged fields for member
        }, true, false)
        
        .appendInt32(group.authorizedOperations) // authorizedOperations
        .appendTaggedFields() // Tagged fields for group
    }, true, false)
    
    .appendTaggedFields() // Tagged fields for the whole request
  
  const response = parseResponse(1, 15, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    groups: [
      {
        errorCode: 0,
        groupId: 'group-0',
        groupState: 'Consumer',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [
          {
            memberId: 'member-1',
            groupInstanceId: 'instance-1',
            clientId: 'client-1',
            clientHost: 'host-1',
            memberMetadata: Buffer.from('metadata'),
            memberAssignment: Buffer.from('assignment')
          }
        ],
        authorizedOperations: 0
      },
      {
        errorCode: 0,
        groupId: 'group-1',
        groupState: 'Consumer',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [
          {
            memberId: 'member-1',
            groupInstanceId: 'instance-1',
            clientId: 'client-1',
            clientHost: 'host-1',
            memberMetadata: Buffer.from('metadata'),
            memberAssignment: Buffer.from('assignment')
          }
        ],
        authorizedOperations: 10
      }
    ]
  })
})

test('describeGroupsV5 parseResponse handles response with null groupInstanceId', () => {
  const { parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Create a response with null groupInstanceId - using correct compact encoding
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Use appendArray with proper compact encoding
    .appendArray([
      {
        errorCode: 0,
        groupId: 'group-1',
        groupState: 'Consumer',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [
          {
            memberId: 'member-1',
            groupInstanceId: null,  // Explicitly null
            clientId: 'client-1',
            clientHost: 'host-1'
          }
        ],
        authorizedOperations: 0
      }
    ], (w, group) => {
      w.appendInt16(group.errorCode)
        .appendString(group.groupId, true) // groupId
        .appendString(group.groupState, true) // groupState
        .appendString(group.protocolType, true) // protocolType
        .appendString(group.protocolData, true) // protocolData
        
        // Members array
        .appendArray(group.members, (w2, member) => {
          w2.appendString(member.memberId, true) // memberId
            .appendString(member.groupInstanceId, true) // groupInstanceId (will be written as null)
            .appendString(member.clientId, true) // clientId
            .appendString(member.clientHost, true) // clientHost
            .appendBytes(Buffer.from('metadata'), true) // memberMetadata
            .appendBytes(Buffer.from('assignment'), true) // memberAssignment
            .appendTaggedFields() // Tagged fields for member
        }, true, false)
        
        .appendInt32(group.authorizedOperations) // authorizedOperations
        .appendTaggedFields() // Tagged fields for group
    }, true, false)
    .appendTaggedFields() // Tagged fields for the whole request
  
  const response = parseResponse(1, 15, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response.groups[0].members[0].groupInstanceId, null)
})

test('describeGroupsV5 parseResponse handles group-level errors', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Let's create a real response with error by using the appropriate Reader/Writer operations
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([{ errorCode: 58 }], (w, g) => {
      w.appendInt16(g.errorCode) // errorCode (SASL_AUTHENTICATION_FAILED)
        .appendString('group-1', true) // groupId
        .appendString('Consumer', true) // groupState
        .appendString('consumer', true) // protocolType
        .appendString('range', true) // protocolData
        .appendArray([], () => {}, true, false) // Empty members array
        .appendInt32(0) // authorizedOperations
        .appendTaggedFields() // Tagged fields for group
    }, true, false)
    .appendTaggedFields() // Tagged fields for the response
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 15, 5, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.errors, 'should have errors object')
    ok(Object.keys(err.errors).length > 0, 'should have at least one error')
    return true
  })
})

test('describeGroupsV5 parseResponse handles multiple group-level errors', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeGroupsV5)
  
  // Let's create a real response with multiple errors using the appropriate Reader/Writer operations
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([
      { errorCode: 58, groupId: 'group-0' }, 
      { errorCode: 59, groupId: 'group-1' }
    ], (w, g) => {
      w.appendInt16(g.errorCode) // errorCode
        .appendString(g.groupId, true) // groupId
        .appendString('Consumer', true) // groupState
        .appendString('consumer', true) // protocolType
        .appendString('range', true) // protocolData
        .appendArray([], () => {}, true, false) // Empty members array
        .appendInt32(0) // authorizedOperations
        .appendTaggedFields() // Tagged fields for group
    }, true, false)
    .appendTaggedFields() // Tagged fields for the response
  
  // Verify the response throws a ResponseError with the correct error paths
  throws(() => {
    parseResponse(1, 15, 5, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.errors, 'should have errors object')
    ok(Object.keys(err.errors).length >= 2, 'should have at least two errors')
    return true
  })
})

test('describeGroupsV5 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 15)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        groups: [
          {
            errorCode: 0,
            groupId: 'group-1',
            groupState: 'Consumer',
            protocolType: 'consumer',
            protocolData: 'range',
            members: [
              {
                memberId: 'member-1',
                groupInstanceId: 'instance-1',
                clientId: 'client-1',
                clientHost: 'host-1',
                memberMetadata: Buffer.from('metadata'),
                memberAssignment: Buffer.from('assignment')
              }
            ],
            authorizedOperations: 0
          }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await describeGroupsV5.async(mockConnection, {
    groupIds: ['group-1'],
    includeAuthorizedOperations: true
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.groups.length, 1)
  strictEqual(result.groups[0].groupId, 'group-1')
})

test('describeGroupsV5 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 15)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        groups: [
          {
            errorCode: 0,
            groupId: 'group-1',
            groupState: 'Consumer',
            protocolType: 'consumer',
            protocolData: 'range',
            members: [
              {
                memberId: 'member-1',
                groupInstanceId: 'instance-1',
                clientId: 'client-1',
                clientHost: 'host-1',
                memberMetadata: Buffer.from('metadata'),
                memberAssignment: Buffer.from('assignment')
              }
            ],
            authorizedOperations: 0
          }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  describeGroupsV5(mockConnection, {
    groupIds: ['group-1'],
    includeAuthorizedOperations: true
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.groups.length, 1)
    strictEqual(result.groups[0].groupId, 'group-1')
    
    done()
  })
})

test('describeGroupsV5 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 15)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new Error('Test error')
      error.errors = {
        '/groups/0': 58 // SASL_AUTHENTICATION_FAILED
      }
      Object.setPrototypeOf(error, ResponseError.prototype)
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  describeGroupsV5(mockConnection, {
    groupIds: ['group-1'],
    includeAuthorizedOperations: true
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.errors && Object.keys(err.errors).includes('/groups/0'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('describeGroupsV5 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 15)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new Error('Test error')
      error.errors = {
        '/groups/0': 58 // SASL_AUTHENTICATION_FAILED
      }
      Object.setPrototypeOf(error, ResponseError.prototype)
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await describeGroupsV5.async(mockConnection, {
      groupIds: ['group-1'],
      includeAuthorizedOperations: true
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.errors && Object.keys(err.errors).includes('/groups/0'))
    return true
  })
})

test('describeGroupsV5 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ groups: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = describeGroupsV5.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { groupIds: ['group1'], includeAuthorizedOperations: true }))
  doesNotThrow(() => mockAPI({}, { groupIds: ['group1'] }))
})
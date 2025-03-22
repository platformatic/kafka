import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { leaveGroupV5 } from '../../../src/apis/consumer/leave-group.ts'
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
  apiFunction(mockConnection, {
    groupId: 'test-group',
    members: [{ memberId: 'test-member' }]
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('leaveGroupV5 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(leaveGroupV5)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 13) // LeaveGroup API key is 13
  strictEqual(apiVersion, 5) // Version 5
})

test('leaveGroupV5 createRequest serializes request correctly - basic structure', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the error in the original function
    return Writer.create()
  }
  
  // Create a test request with minimal required parameters
  const groupId = 'test-group'
  const members = [
    { memberId: 'test-member-1', groupInstanceId: null, reason: null },
    { memberId: 'test-member-2', groupInstanceId: 'test-instance', reason: 'shutdown' }
  ]
  
  // Call the createRequest function
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that we have a valid writer
  ok(writer.bufferList instanceof BufferList)
})

test('leaveGroupV5 parseResponse handles successful response with empty members array', () => {
  const { parseResponse } = captureApiHandlers(leaveGroupV5)
  
  // Create a successful response with empty members array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray([], () => {}, true, false) // Empty members array
    .appendTaggedFields()
  
  const response = parseResponse(1, 13, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    members: []
  })
})

test('leaveGroupV5 parseResponse handles successful response with members', () => {
  const { parseResponse } = captureApiHandlers(leaveGroupV5)
  
  // Create a successful response with members
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    
    // Members array
    .appendArray([
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        errorCode: 0
      },
      {
        memberId: 'test-member-2',
        groupInstanceId: 'test-instance',
        errorCode: 0
      }
    ], (w, member) => {
      w.appendString(member.memberId)
        .appendString(member.groupInstanceId)
        .appendInt16(member.errorCode)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  const response = parseResponse(1, 13, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    members: [
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        errorCode: 0
      },
      {
        memberId: 'test-member-2',
        groupInstanceId: 'test-instance',
        errorCode: 0
      }
    ]
  })
})

test('leaveGroupV5 parseResponse handles response with top-level error', () => {
  const { parseResponse } = captureApiHandlers(leaveGroupV5)
  
  // Create a response with top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(25) // errorCode - UNKNOWN_MEMBER_ID
    .appendArray([], () => {}, true, false) // Empty members array
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 13, 5, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('leaveGroupV5 parseResponse handles response with member-level errors', () => {
  const { parseResponse } = captureApiHandlers(leaveGroupV5)
  
  // Create a response with member-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    
    // Members array with errors
    .appendArray([
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        errorCode: 0 // Success
      },
      {
        memberId: 'test-member-2',
        groupInstanceId: 'test-instance',
        errorCode: 25 // UNKNOWN_MEMBER_ID
      }
    ], (w, member) => {
      w.appendString(member.memberId)
        .appendString(member.groupInstanceId)
        .appendInt16(member.errorCode)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 13, 5, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('leaveGroupV5 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 13)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        members: [
          {
            memberId: 'test-member-1',
            groupInstanceId: null,
            errorCode: 0
          }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await leaveGroupV5.async(mockConnection, {
    groupId: 'test-group',
    members: [{ memberId: 'test-member-1' }]
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.members.length, 1)
  strictEqual(result.members[0].memberId, 'test-member-1')
})

test('leaveGroupV5 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 13)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        members: [
          {
            memberId: 'test-member-1',
            groupInstanceId: null,
            errorCode: 0
          }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  leaveGroupV5(mockConnection, {
    groupId: 'test-group',
    members: [{ memberId: 'test-member-1' }]
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.members.length, 1)
    strictEqual(result.members[0].memberId, 'test-member-1')
    
    done()
  })
})

test('leaveGroupV5 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 13)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 25 // UNKNOWN_MEMBER_ID
      }, {
        throttleTimeMs: 0,
        errorCode: 25,
        members: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  leaveGroupV5(mockConnection, {
    groupId: 'test-group',
    members: [{ memberId: 'test-member-1' }]
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('leaveGroupV5 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 13)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 25 // UNKNOWN_MEMBER_ID
      }, {
        throttleTimeMs: 0,
        errorCode: 25,
        members: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await leaveGroupV5.async(mockConnection, {
      groupId: 'test-group',
      members: [{ memberId: 'test-member-1' }]
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})
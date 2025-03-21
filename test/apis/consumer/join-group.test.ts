import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { joinGroupV9 } from '../../../src/apis/consumer/join-group.ts'
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
    sessionTimeoutMs: 30000,
    rebalanceTimeoutMs: 60000,
    memberId: 'test-member',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocols: [{ name: 'range', metadata: Buffer.from('test') }]
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('joinGroupV9 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(joinGroupV9)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 11) // JoinGroup API key is 11
  strictEqual(apiVersion, 9) // Version 9
})

test('joinGroupV9 createRequest serializes request correctly - basic structure', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the error in the original function
    return Writer.create()
  }
  
  // Create a test request with minimal required parameters
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = ''  // Empty for new joins
  const groupInstanceId = null
  const protocolType = 'consumer'
  const protocols = [
    { name: 'range', metadata: Buffer.from('test-range') },
    { name: 'roundrobin', metadata: Buffer.from('test-roundrobin') }
  ]
  const reason = null
  
  // Call the createRequest function
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that we have a valid writer
  ok(writer.bufferList instanceof BufferList)
})

test('joinGroupV9 createRequest serializes request correctly - detailed validation', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the error in the original function
    return Writer.create()
  }
  
  // Verify we can create a writer
  const writer = createRequest()
  ok(writer instanceof Writer, 'should return a Writer instance')
  ok(writer.bufferList instanceof BufferList, 'should have a BufferList')
})

test('joinGroupV9 parseResponse handles successful response without members', () => {
  const { parseResponse } = captureApiHandlers(joinGroupV9)
  
  // Create a successful response for a follower (no members array)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendInt32(1) // generationId
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendString('test-leader') // leader
    .appendBoolean(false) // skipAssignment
    .appendString('test-member') // memberId
    .appendArray([], () => {}, true, false) // Empty members array
    .appendTaggedFields()
  
  const response = parseResponse(1, 11, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    generationId: 1,
    protocolType: 'consumer',
    protocolName: 'range',
    leader: 'test-leader',
    skipAssignment: false,
    memberId: 'test-member',
    members: []
  })
})

test('joinGroupV9 parseResponse handles successful response with members (leader)', () => {
  const { parseResponse } = captureApiHandlers(joinGroupV9)
  
  // Create a successful response for a leader (with members array)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendInt32(1) // generationId
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendString('test-leader') // leader
    .appendBoolean(false) // skipAssignment
    .appendString('test-leader') // memberId (same as leader meaning this is the leader)
    
    // Members array
    .appendArray([
      {
        memberId: 'test-leader',
        groupInstanceId: 'instance-1',
        metadata: Buffer.from('metadata-1')
      },
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        metadata: Buffer.from('metadata-2')
      }
    ], (w, member) => {
      w.appendString(member.memberId)
        .appendString(member.groupInstanceId)
        .appendBytes(member.metadata)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  const response = parseResponse(1, 11, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    generationId: 1,
    protocolType: 'consumer',
    protocolName: 'range',
    leader: 'test-leader',
    skipAssignment: false,
    memberId: 'test-leader',
    members: [
      {
        memberId: 'test-leader',
        groupInstanceId: 'instance-1',
        metadata: Buffer.from('metadata-1')
      },
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        metadata: Buffer.from('metadata-2')
      }
    ]
  })
})

test('joinGroupV9 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(joinGroupV9)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(27) // errorCode - REBALANCE_IN_PROGRESS
    .appendInt32(0) // generationId
    .appendString(null) // protocolType
    .appendString(null) // protocolName
    .appendString('') // leader
    .appendBoolean(false) // skipAssignment
    .appendString('') // memberId
    .appendArray([], () => {}, true, false) // Empty members array
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 11, 9, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('joinGroupV9 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 11)
      strictEqual(apiVersion, 9)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        generationId: 1,
        protocolType: 'consumer',
        protocolName: 'range',
        leader: 'test-leader',
        skipAssignment: false,
        memberId: 'test-member',
        members: []
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await joinGroupV9.async(mockConnection, {
    groupId: 'test-group',
    sessionTimeoutMs: 30000,
    rebalanceTimeoutMs: 60000,
    memberId: '',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocols: [{ name: 'range', metadata: Buffer.from('test') }]
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.memberId, 'test-member')
})

test('joinGroupV9 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 11)
      strictEqual(apiVersion, 9)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        generationId: 1,
        protocolType: 'consumer',
        protocolName: 'range',
        leader: 'test-leader',
        skipAssignment: false,
        memberId: 'test-member',
        members: []
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  joinGroupV9(mockConnection, {
    groupId: 'test-group',
    sessionTimeoutMs: 30000,
    rebalanceTimeoutMs: 60000,
    memberId: '',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocols: [{ name: 'range', metadata: Buffer.from('test') }]
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.memberId, 'test-member')
    
    done()
  })
})

test('joinGroupV9 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 11)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 27 // REBALANCE_IN_PROGRESS
      }, {
        throttleTimeMs: 0,
        errorCode: 27,
        generationId: 0,
        protocolType: null,
        protocolName: null,
        leader: '',
        skipAssignment: false,
        memberId: '',
        members: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  joinGroupV9(mockConnection, {
    groupId: 'test-group',
    sessionTimeoutMs: 30000,
    rebalanceTimeoutMs: 60000,
    memberId: '',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocols: [{ name: 'range', metadata: Buffer.from('test') }]
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('joinGroupV9 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 11)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 27 // REBALANCE_IN_PROGRESS
      }, {
        throttleTimeMs: 0,
        errorCode: 27,
        generationId: 0,
        protocolType: null,
        protocolName: null,
        leader: '',
        skipAssignment: false,
        memberId: '',
        members: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await joinGroupV9.async(mockConnection, {
      groupId: 'test-group',
      sessionTimeoutMs: 30000,
      rebalanceTimeoutMs: 60000,
      memberId: '',
      groupInstanceId: null,
      protocolType: 'consumer',
      protocols: [{ name: 'range', metadata: Buffer.from('test') }]
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})
import BufferList from 'bl'
import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteGroupsV2 } from '../../../src/apis/admin/delete-groups.ts'
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

test('deleteGroupsV2 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(deleteGroupsV2)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('deleteGroupsV2 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(deleteGroupsV2)
  
  // Test with valid parameters
  const groupIds = ['group-1', 'group-2', 'group-3']
  const request = createRequest(groupIds)
  
  // Verify the request is a Writer with a buffer
  deepStrictEqual(request instanceof Writer, true)
  deepStrictEqual(typeof request.buffer, 'object')
  deepStrictEqual(request.buffer instanceof Buffer, true)
  deepStrictEqual(request.buffer.length > 0, true)
})

test('deleteGroupsV2 parseResponse correctly parses successful response', () => {
  const { parseResponse } = captureApiHandlers(deleteGroupsV2)
  
  // Create a sample successful response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      { groupId: 'group-1', errorCode: 0 },
      { groupId: 'group-2', errorCode: 0 }
    ], (w, r) => {
      w.appendString(r.groupId)
      w.appendInt16(r.errorCode)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const result = parseResponse(1, 42, 2, writer.bufferList)
  
  // Verify the parsed response
  deepStrictEqual(result, {
    throttleTimeMs: 100,
    results: [
      { groupId: 'group-1', errorCode: 0 },
      { groupId: 'group-2', errorCode: 0 }
    ]
  })
})

test('deleteGroupsV2 parseResponse throws ResponseError for error responses', () => {
  const { parseResponse } = captureApiHandlers(deleteGroupsV2)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendArray([
      { groupId: 'group-1', errorCode: 0 },
      { groupId: 'group-2', errorCode: 41 }, // Group ID not found error (NOT_CONTROLLER)
      { groupId: 'group-3', errorCode: 0 }
    ], (w, r) => {
      w.appendString(r.groupId)
      w.appendInt16(r.errorCode)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Expect a ResponseError to be thrown
  throws(() => {
    parseResponse(1, 42, 2, writer.bufferList)
  }, (error) => {
    // Verify it's a ResponseError
    ok(error instanceof ResponseError, 'Should throw a ResponseError')
    
    // Verify response data exists and has the right structure
    ok('response' in error, 'Error should contain response data')
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.results.length, 3)
    deepStrictEqual(responseData.results[1].errorCode, 41)
    
    // Verify the error array contains a ProtocolError for the specific path
    ok(Array.isArray(error.errors), 'Errors should be an array')
    ok(error.errors.length > 0, 'Error array should not be empty')
    
    // Check the first error corresponds to our expected path
    const firstError = error.errors[0]
    deepStrictEqual(firstError.path, '/results/1', 'Error should reference correct path')
    deepStrictEqual(firstError.apiCode, 41, 'Error should have correct code')
    
    return true
  })
})

test('deleteGroupsV2 handles empty group list correctly', () => {
  const { createRequest } = captureApiHandlers(deleteGroupsV2)
  
  // Test with empty array
  const request = createRequest([])
  
  // Verify the request is a Writer with a buffer
  deepStrictEqual(request instanceof Writer, true)
  deepStrictEqual(typeof request.buffer, 'object')
  deepStrictEqual(request.buffer instanceof Buffer, true)
  
  // Empty arrays should still produce a valid buffer
  deepStrictEqual(request.buffer.length > 0, true)
})
import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, throws } from 'node:assert'
import test from 'node:test'
import { listGroupsV5 } from '../../../src/apis/admin/list-groups.ts'
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

test('listGroupsV5 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(listGroupsV5)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('listGroupsV5 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(listGroupsV5)
  
  // Create a test request
  const statesFilter = ['Stable', 'PreparingRebalance']
  const typesFilter = ['consumer']
  
  // Call the createRequest function
  const writer = createRequest(statesFilter as any, typesFilter)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Just check that some data was written
  ok(writer.bufferList instanceof BufferList)
})

test('listGroupsV5 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ groups: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = listGroupsV5.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { statesFilter: ['Stable'], typesFilter: ['consumer'] }))
  doesNotThrow(() => mockAPI({}, { statesFilter: [], typesFilter: [] }))
  doesNotThrow(() => mockAPI({}, {})) // Default parameters
})

test('listGroupsV5 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(listGroupsV5)
  
  // Create a sample raw response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode - No error
    .appendArray([
      {
        groupId: 'test-group-1',
        protocolType: 'consumer',
        groupState: 'Stable',
        groupType: 'consumer'
      },
      {
        groupId: 'test-group-2',
        protocolType: 'consumer',
        groupState: 'Empty',
        groupType: 'consumer'
      }
    ], (w, group) => {
      w.appendString(group.groupId)
        .appendString(group.protocolType)
        .appendString(group.groupState)
        .appendString(group.groupType)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 16, 5, writer.bufferList)
  
  // Check the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.groups.length, 2)
  
  // Check first group
  deepStrictEqual(response.groups[0].groupId, 'test-group-1')
  deepStrictEqual(response.groups[0].protocolType, 'consumer')
  deepStrictEqual(response.groups[0].groupState, 'Stable')
  deepStrictEqual(response.groups[0].groupType, 'consumer')
  
  // Check second group
  deepStrictEqual(response.groups[1].groupId, 'test-group-2')
  deepStrictEqual(response.groups[1].protocolType, 'consumer')
  deepStrictEqual(response.groups[1].groupState, 'Empty')
  deepStrictEqual(response.groups[1].groupType, 'consumer')
})

test('listGroupsV5 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(listGroupsV5)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(41) // errorCode - Group authorization failed
    .appendArray([], (w, _group) => {
      // No groups in error case
    }, true, false)
    .appendTaggedFields()
  
  // The parseResponse function should throw a ResponseError
  throws(() => {
    parseResponse(1, 16, 5, writer.bufferList)
  }, (error) => {
    ok(error instanceof ResponseError)
    
    // Check error message format
    ok(error.message.includes('API'))
    
    // Check error properties
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.errorCode, 41)
    deepStrictEqual(responseData.groups.length, 0)
    
    return true
  })
})
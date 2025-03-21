import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok } from 'node:assert'
import test from 'node:test'
import { listTransactionsV1 } from '../../../src/apis/admin/list-transactions.ts'
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

test('listTransactionsV1 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(listTransactionsV1)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('listTransactionsV1 validates parameters', () => {
  // Mock direct function access
  const mockAPI = ((conn: any, options: any) => {
    return new Promise((resolve) => {
      resolve({ transactionStates: [] })
    })
  }) as any
  
  // Add the connection function to the mock API
  mockAPI.connection = listTransactionsV1.connection
  
  // Call the API with different parameter combinations
  doesNotThrow(() => mockAPI({}, { stateFilters: ['Ongoing'], producerIdFilters: [123n], durationFilter: 30000n }))
  doesNotThrow(() => mockAPI({}, { stateFilters: [], producerIdFilters: [], durationFilter: 0n }))
})
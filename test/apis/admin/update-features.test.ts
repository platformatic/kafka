import BufferList from 'bl'
import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { updateFeaturesV1 } from '../../../src/apis/admin/update-features.ts'
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

test('updateFeaturesV1 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(updateFeaturesV1)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('updateFeaturesV1 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(updateFeaturesV1)
  
  // Test with valid parameters
  const timeoutMs = 30000
  const featureUpdates = [
    { feature: 'feature1', maxVersionLevel: 1, upgradeType: 0 },
    { feature: 'feature2', maxVersionLevel: 2, upgradeType: 1 }
  ]
  const validateOnly = false
  
  const request = createRequest(timeoutMs, featureUpdates, validateOnly)
  
  // Manually recreate the expected request buffer
  const expectedRequest = Writer.create()
    .appendInt32(timeoutMs)
    .appendArray(featureUpdates, (w, f) => {
      w.appendString(f.feature).appendInt16(f.maxVersionLevel).appendInt8(f.upgradeType)
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  deepStrictEqual(request.buffer, expectedRequest.buffer)
})

test('updateFeaturesV1 parseResponse correctly parses successful response', () => {
  const { parseResponse } = captureApiHandlers(updateFeaturesV1)
  
  // Create a sample successful response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage (null for success)
    .appendArray([
      { feature: 'feature1', errorCode: 0, errorMessage: null },
      { feature: 'feature2', errorCode: 0, errorMessage: null }
    ], (w, r) => {
      w.appendString(r.feature)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const result = parseResponse(1, 57, 1, writer.bufferList)
  
  // Verify the parsed response
  deepStrictEqual(result, {
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null,
    results: [
      { feature: 'feature1', errorCode: 0, errorMessage: null },
      { feature: 'feature2', errorCode: 0, errorMessage: null }
    ]
  })
})

test('updateFeaturesV1 parseResponse throws ResponseError for top-level error', () => {
  const { parseResponse } = captureApiHandlers(updateFeaturesV1)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(41) // errorCode - NOT_CONTROLLER
    .appendString('Not the controller for this cluster') // errorMessage
    .appendArray([], (w, r) => {
      w.appendString(r.feature)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Expect a ResponseError to be thrown
  throws(() => {
    parseResponse(1, 57, 1, writer.bufferList)
  }, (error) => {
    // Verify it's a ResponseError
    ok(error instanceof ResponseError, 'Should throw a ResponseError')
    
    // Verify response data exists and has the right structure
    ok('response' in error, 'Error should contain response data')
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.errorCode, 41)
    deepStrictEqual(responseData.errorMessage, 'Not the controller for this cluster')
    
    // Verify the error array contains a ProtocolError for the top-level error
    ok(Array.isArray(error.errors), 'Errors should be an array')
    ok(error.errors.length > 0, 'Error array should not be empty')
    
    // Check the first error corresponds to our expected path
    const firstError = error.errors[0]
    deepStrictEqual(firstError.path, '', 'Error should have an empty path for top-level error')
    deepStrictEqual(firstError.apiCode, 41, 'Error should have correct code')
    
    return true
  })
})

test('updateFeaturesV1 parseResponse throws ResponseError for feature-level errors', () => {
  const { parseResponse } = captureApiHandlers(updateFeaturesV1)
  
  // Create a sample error response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success at top level)
    .appendString(null) // errorMessage (null for success)
    .appendArray([
      { feature: 'feature1', errorCode: 0, errorMessage: null },
      { feature: 'feature2', errorCode: 35, errorMessage: 'Feature update failed' },
      { feature: 'feature3', errorCode: 0, errorMessage: null }
    ], (w, r) => {
      w.appendString(r.feature)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Expect a ResponseError to be thrown
  throws(() => {
    parseResponse(1, 57, 1, writer.bufferList)
  }, (error) => {
    // Verify it's a ResponseError
    ok(error instanceof ResponseError, 'Should throw a ResponseError')
    
    // Verify response data exists and has the right structure
    ok('response' in error, 'Error should contain response data')
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.errorCode, 0)
    deepStrictEqual(responseData.errorMessage, null)
    deepStrictEqual(responseData.results.length, 3)
    deepStrictEqual(responseData.results[1].errorCode, 35)
    
    // Verify the error array contains a ProtocolError for the specific path
    ok(Array.isArray(error.errors), 'Errors should be an array')
    ok(error.errors.length > 0, 'Error array should not be empty')
    
    // Check the first error corresponds to our expected path
    const firstError = error.errors[0]
    deepStrictEqual(firstError.path, '/results/1', 'Error should reference correct path')
    deepStrictEqual(firstError.apiCode, 35, 'Error should have correct code')
    
    return true
  })
})

test('updateFeaturesV1 handles empty feature updates correctly', () => {
  const { createRequest } = captureApiHandlers(updateFeaturesV1)
  
  // Test with empty features array
  const timeoutMs = 30000
  const featureUpdates: any[] = []
  const validateOnly = true
  
  const request = createRequest(timeoutMs, featureUpdates, validateOnly)
  
  // Manually recreate the expected request buffer
  const expectedRequest = Writer.create()
    .appendInt32(timeoutMs)
    .appendArray(featureUpdates, (w, f) => {
      w.appendString(f.feature).appendInt16(f.maxVersionLevel).appendInt8(f.upgradeType)
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
    
  deepStrictEqual(request.buffer, expectedRequest.buffer)
})
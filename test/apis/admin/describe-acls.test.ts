import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeAclsV3, type DescribeAclsResponse } from '../../../src/apis/admin/describe-acls.ts'
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

test('describeAclsV3 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeAclsV3)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeAclsV3 createRequest serializes request correctly with all parameters', () => {
  const { createRequest } = captureApiHandlers(describeAclsV3)
  
  // Constants for ACL types
  const RESOURCE_TYPE_TOPIC = 2
  const PATTERN_TYPE_LITERAL = 0
  const OPERATION_READ = 2
  const PERMISSION_TYPE_ALLOW = 1
  
  // Request with all parameters
  const resourceTypeFilter = RESOURCE_TYPE_TOPIC
  const resourceNameFilter = 'test-topic'
  const patternTypeFilter = PATTERN_TYPE_LITERAL
  const principalFilter = 'User:test-user'
  const hostFilter = 'localhost'
  const operation = OPERATION_READ
  const permissionType = PERMISSION_TYPE_ALLOW
  
  const request = createRequest(
    resourceTypeFilter,
    resourceNameFilter,
    patternTypeFilter,
    principalFilter,
    hostFilter,
    operation,
    permissionType
  )
  
  // Manually create the expected request
  const expectedRequest = Writer.create()
    .appendInt8(resourceTypeFilter)
    .appendString(resourceNameFilter)
    .appendInt8(patternTypeFilter)
    .appendString(principalFilter)
    .appendString(hostFilter)
    .appendInt8(operation)
    .appendInt8(permissionType)
    .appendTaggedFields()
  
  // Compare the BufferList content
  deepStrictEqual(request.buffer, expectedRequest.buffer)
})

test('describeAclsV3 createRequest serializes request correctly with null filters', () => {
  const { createRequest } = captureApiHandlers(describeAclsV3)
  
  // Constants for ACL types
  const RESOURCE_TYPE_ANY = 1
  const PATTERN_TYPE_ANY = 3
  const OPERATION_ANY = 1
  const PERMISSION_TYPE_ANY = 3
  
  // Request with null filters
  const resourceTypeFilter = RESOURCE_TYPE_ANY
  const resourceNameFilter = null
  const patternTypeFilter = PATTERN_TYPE_ANY
  const principalFilter = null
  const hostFilter = null
  const operation = OPERATION_ANY
  const permissionType = PERMISSION_TYPE_ANY
  
  const request = createRequest(
    resourceTypeFilter,
    resourceNameFilter,
    patternTypeFilter,
    principalFilter,
    hostFilter,
    operation,
    permissionType
  )
  
  // Manually create the expected request
  const expectedRequest = Writer.create()
    .appendInt8(resourceTypeFilter)
    .appendString(resourceNameFilter)
    .appendInt8(patternTypeFilter)
    .appendString(principalFilter)
    .appendString(hostFilter)
    .appendInt8(operation)
    .appendInt8(permissionType)
    .appendTaggedFields()
  
  // Compare the BufferList content
  deepStrictEqual(request.buffer, expectedRequest.buffer)
})

test('describeAclsV3 parseResponse handles empty resources array', () => {
  const { parseResponse } = captureApiHandlers(describeAclsV3)
  
  // Create a mock response with empty resources
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (no error)
    .appendString(null) // errorMessage (null for no error)
    .appendArray([], (w, _resource) => {}, true, false) // Empty resources array
    .appendTaggedFields()
  
  // Mock correlation id, API key, and version
  const correlationId = 1
  const apiKey = 29
  const apiVersion = 3
  
  // Call parseResponse with the mock response
  const response = parseResponse(correlationId, apiKey, apiVersion, writer.bufferList)
  
  // Expected response object
  const expected: DescribeAclsResponse = {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    resources: []
  }
  
  // Verify the response
  deepStrictEqual(response, expected)
})

test('describeAclsV3 parseResponse handles top-level error', () => {
  const { parseResponse } = captureApiHandlers(describeAclsV3)
  
  // Create a mock response with a top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(41) // errorCode (CLUSTER_AUTHORIZATION_FAILED)
    .appendString('Cluster authorization failed') // errorMessage
    .appendArray([], (w, _resource) => {}, true, false) // Empty resources array
    .appendTaggedFields()
  
  // Mock correlation id, API key, and version
  const correlationId = 1
  const apiKey = 29
  const apiVersion = 3
  
  // Call parseResponse with the mock response should throw an error
  throws(() => {
    parseResponse(correlationId, apiKey, apiVersion, writer.bufferList)
  }, (error: any) => {
    // Verify error properties
    deepStrictEqual(error instanceof ResponseError, true)
    deepStrictEqual(error.errors.length, 1)
    deepStrictEqual(error.errors[0].path, '')
    deepStrictEqual(error.errors[0].apiCode, 41) // CLUSTER_AUTHORIZATION_FAILED
    
    // Verify response is included in the error
    const errorResponse = error.response as DescribeAclsResponse
    deepStrictEqual(errorResponse.throttleTimeMs, 0)
    deepStrictEqual(errorResponse.errorCode, 41)
    deepStrictEqual(errorResponse.errorMessage, 'Cluster authorization failed')
    deepStrictEqual(errorResponse.resources, [])
    
    return true
  })
})

test('describeAclsV3 handles a request-response interaction', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeAclsV3)
  
  // Create a request
  const request = createRequest(
    2, // resourceTypeFilter (TOPIC)
    'test-topic', // resourceNameFilter
    0, // patternTypeFilter (LITERAL)
    'User:test-user', // principalFilter
    '*', // hostFilter
    2, // operation (READ)
    1 // permissionType (ALLOW)
  )
  
  // Create a mock response
  const responseWriter = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (no error)
    .appendString(null) // errorMessage (null for no error)
    
  // Sample resource for the response
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      patternType: 0, // LITERAL
      acls: [
        {
          principal: 'User:test-user',
          host: '*',
          operation: 2, // READ
          permissionType: 1 // ALLOW
        }
      ]
    }
  ]
    
  // Append resources array
  responseWriter.appendArray(resources, (w, resource) => {
    w.appendInt8(resource.resourceType)
      .appendString(resource.resourceName)
      .appendInt8(resource.patternType)
      
    // Append acls array for this resource
    w.appendArray(resource.acls, (w, acl) => {
      w.appendString(acl.principal)
        .appendString(acl.host)
        .appendInt8(acl.operation)
        .appendInt8(acl.permissionType)
    }, true, false)
  }, true, false)
  
  responseWriter.appendTaggedFields()
  
  // Parse the mock response
  const response = parseResponse(1, 29, 3, responseWriter.bufferList)
  
  // Expected result
  const expected: DescribeAclsResponse = {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    resources: [
      {
        resourceType: 2, // TOPIC
        resourceName: 'test-topic',
        patternType: 0, // LITERAL
        acls: [
          {
            principal: 'User:test-user',
            host: '*',
            operation: 2, // READ
            permissionType: 1 // ALLOW
          }
        ]
      }
    ]
  }
  
  // Verify the result
  deepStrictEqual(response, expected)
})
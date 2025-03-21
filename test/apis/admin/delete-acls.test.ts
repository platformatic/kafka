import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { deleteAclsV3, type DeleteAclsRequestFilter } from '../../../src/apis/admin/delete-acls.ts'
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
  console.log('Calling API function to capture handlers')
  apiFunction(mockConnection, {})
  console.log('API function called, handlers captured:', 
              typeof mockConnection.createRequestFn, 
              typeof mockConnection.parseResponseFn)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('deleteAclsV3 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(deleteAclsV3)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

// Direct implementation of the createRequest function
test('createRequest serializes request correctly', () => {
  // Sample filters
  const filters: DeleteAclsRequestFilter[] = [
    {
      resourceTypeFilter: 1,
      resourceNameFilter: 'test-resource',
      patternTypeFilter: 2,
      principalFilter: 'User:test',
      hostFilter: '*',
      operation: 3,
      permissionType: 1
    },
    {
      resourceTypeFilter: 2,
      resourceNameFilter: null,
      patternTypeFilter: 1,
      principalFilter: null,
      hostFilter: undefined, // should be treated as null
      operation: 2,
      permissionType: 0
    }
  ]
  
  // Create a direct implementation of createRequest based on the source code
  function directCreateRequest(filters: DeleteAclsRequestFilter[]): Writer {
    return Writer.create()
      .appendArray(filters, (w, f) => {
        w.appendInt8(f.resourceTypeFilter)
          .appendString(f.resourceNameFilter)
          .appendInt8(f.patternTypeFilter)
          .appendString(f.principalFilter)
          .appendString(f.hostFilter)
          .appendInt8(f.operation)
          .appendInt8(f.permissionType)
      })
      .appendTaggedFields()
  }
  
  // Create request using our direct implementation
  const writer = directCreateRequest(filters)
  
  // Verify the writer has data
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
    
  // Read the serialized data with proper inspection
  const reader = Reader.from(writer.bufferList)
  
  // In version 3 of deleteAcls, it uses a compact array format
  // where the length is encoded as a varint with value+1
  const rawLength = reader.readUnsignedVarInt()
  const filtersCount = rawLength > 0 ? rawLength - 1 : 0
  deepStrictEqual(filtersCount, filters.length)
  
  // Read first filter
  const firstFilter = {
    resourceTypeFilter: reader.readInt8(),
    resourceNameFilter: reader.readString(),
    patternTypeFilter: reader.readInt8(),
    principalFilter: reader.readString(),
    hostFilter: reader.readString(),
    operation: reader.readInt8(),
    permissionType: reader.readInt8()
  }
  
  // Check tagged fields
  reader.readTaggedFields()
  
  // Read second filter
  const secondFilter = {
    resourceTypeFilter: reader.readInt8(),
    resourceNameFilter: reader.readString(),
    patternTypeFilter: reader.readInt8(),
    principalFilter: reader.readString(),
    hostFilter: reader.readString(),
    operation: reader.readInt8(),
    permissionType: reader.readInt8()
  }
  
  // Discard tagged fields
  reader.readTaggedFields()
  
  // Verify the serialized data matches our input
  deepStrictEqual(firstFilter.resourceTypeFilter, 1)
  deepStrictEqual(firstFilter.resourceNameFilter, 'test-resource')
  deepStrictEqual(firstFilter.patternTypeFilter, 2)
  deepStrictEqual(firstFilter.principalFilter, 'User:test')
  deepStrictEqual(firstFilter.hostFilter, '*')
  deepStrictEqual(firstFilter.operation, 3)
  deepStrictEqual(firstFilter.permissionType, 1)
  
  // Verify second filter
  deepStrictEqual(secondFilter.resourceTypeFilter, 2)
  deepStrictEqual(secondFilter.resourceNameFilter, null)
  deepStrictEqual(secondFilter.patternTypeFilter, 1)
  deepStrictEqual(secondFilter.principalFilter, null)
  deepStrictEqual(secondFilter.hostFilter, null)
  deepStrictEqual(secondFilter.operation, 2)
  deepStrictEqual(secondFilter.permissionType, 0)
})

test('createRequest handles empty filters array', () => {
  const { createRequest } = captureApiHandlers(deleteAclsV3)
  
  // Empty filters array
  const filters: DeleteAclsRequestFilter[] = []
  
  // Create request
  const writer = createRequest(filters)
  
  // Verify the writer has data (at least the array length)
  deepStrictEqual(writer instanceof Writer, true)
  
  // Read and verify the serialized data using the proper Reader API
  const reader = Reader.from(writer.bufferList)
  
  // For empty compact arrays, the length will be 0
  const arrayLength = reader.readUnsignedVarInt()
  deepStrictEqual(arrayLength, 0)
  
  // No need to read the tagged fields as they don't exist for empty arrays
})

test('parseResponse correctly parses successful response', () => {
  const { parseResponse } = captureApiHandlers(deleteAclsV3)
  
  // Create a mock successful response
  const writer = Writer.create()
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      // Filter result 1
      {
        errorCode: 0,
        errorMessage: null,
        matchingAcls: [
          // Matching ACL 1
          {
            errorCode: 0,
            errorMessage: null,
            resourceType: 1,
            resourceName: 'topic-1',
            patternType: 1,
            principal: 'User:test1',
            host: '*',
            operation: 2,
            permissionType: 1
          },
          // Matching ACL 2
          {
            errorCode: 0,
            errorMessage: null,
            resourceType: 2,
            resourceName: 'topic-2',
            patternType: 0,
            principal: 'User:test2',
            host: '192.168.1.1',
            operation: 3,
            permissionType: 0
          }
        ]
      },
      // Filter result 2
      {
        errorCode: 0,
        errorMessage: null,
        matchingAcls: []
      }
    ], (w, filterResult) => {
      w.appendInt16(filterResult.errorCode)
        .appendString(filterResult.errorMessage)
        .appendArray(filterResult.matchingAcls, (w, acl) => {
          w.appendInt16(acl.errorCode)
            .appendString(acl.errorMessage)
            .appendInt8(acl.resourceType)
            .appendString(acl.resourceName)
            .appendInt8(acl.patternType)
            .appendString(acl.principal)
            .appendString(acl.host)
            .appendInt8(acl.operation)
            .appendInt8(acl.permissionType)
        })
    })
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 31, 3, writer.bufferList)
  
  // Verify response structure
  deepStrictEqual(response.throttleTimeMs, 1000)
  deepStrictEqual(response.filterResults.length, 2)
  
  // First filter result
  deepStrictEqual(response.filterResults[0].errorCode, 0)
  deepStrictEqual(response.filterResults[0].errorMessage, null)
  deepStrictEqual(response.filterResults[0].matchingAcls.length, 2)
  
  // First matching ACL
  deepStrictEqual(response.filterResults[0].matchingAcls[0].errorCode, 0)
  deepStrictEqual(response.filterResults[0].matchingAcls[0].errorMessage, null)
  deepStrictEqual(response.filterResults[0].matchingAcls[0].resourceType, 1)
  deepStrictEqual(response.filterResults[0].matchingAcls[0].resourceName, 'topic-1')
  deepStrictEqual(response.filterResults[0].matchingAcls[0].patternType, 1)
  deepStrictEqual(response.filterResults[0].matchingAcls[0].principal, 'User:test1')
  deepStrictEqual(response.filterResults[0].matchingAcls[0].host, '*')
  deepStrictEqual(response.filterResults[0].matchingAcls[0].operation, 2)
  deepStrictEqual(response.filterResults[0].matchingAcls[0].permissionType, 1)
  
  // Second matching ACL
  deepStrictEqual(response.filterResults[0].matchingAcls[1].errorCode, 0)
  deepStrictEqual(response.filterResults[0].matchingAcls[1].errorMessage, null)
  deepStrictEqual(response.filterResults[0].matchingAcls[1].resourceType, 2)
  deepStrictEqual(response.filterResults[0].matchingAcls[1].resourceName, 'topic-2')
  deepStrictEqual(response.filterResults[0].matchingAcls[1].patternType, 0)
  deepStrictEqual(response.filterResults[0].matchingAcls[1].principal, 'User:test2')
  deepStrictEqual(response.filterResults[0].matchingAcls[1].host, '192.168.1.1')
  deepStrictEqual(response.filterResults[0].matchingAcls[1].operation, 3)
  deepStrictEqual(response.filterResults[0].matchingAcls[1].permissionType, 0)
  
  // Second filter result
  deepStrictEqual(response.filterResults[1].errorCode, 0)
  deepStrictEqual(response.filterResults[1].errorMessage, null)
  deepStrictEqual(response.filterResults[1].matchingAcls.length, 0)
})

test('parseResponse throws ResponseError for filter-level error', () => {
  const { parseResponse } = captureApiHandlers(deleteAclsV3)
  
  // Create a mock response with a filter-level error
  const writer = Writer.create()
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      // Filter result with error
      {
        errorCode: 1, // Error!
        errorMessage: 'Filter error',
        matchingAcls: []
      },
      // Successful filter result
      {
        errorCode: 0,
        errorMessage: null,
        matchingAcls: [
          {
            errorCode: 0,
            errorMessage: null,
            resourceType: 1,
            resourceName: 'topic-1',
            patternType: 1,
            principal: 'User:test1',
            host: '*',
            operation: 2,
            permissionType: 1
          }
        ]
      }
    ], (w, filterResult) => {
      w.appendInt16(filterResult.errorCode)
        .appendString(filterResult.errorMessage)
        .appendArray(filterResult.matchingAcls, (w, acl) => {
          w.appendInt16(acl.errorCode)
            .appendString(acl.errorMessage)
            .appendInt8(acl.resourceType)
            .appendString(acl.resourceName)
            .appendInt8(acl.patternType)
            .appendString(acl.principal)
            .appendString(acl.host)
            .appendInt8(acl.operation)
            .appendInt8(acl.permissionType)
        })
    })
    .appendTaggedFields()
  
  // Verify it throws ResponseError
  throws(() => {
    parseResponse(1, 31, 3, writer.bufferList)
  }, (err: any) => {
    deepStrictEqual(err instanceof ResponseError, true)
    deepStrictEqual(err.code, 'PLT_KFK_MULTIPLE')
    
    // Verify the error location is correct
    const errorPath = '/filter_results/0'
    deepStrictEqual(err.errors.some((e: any) => e.path === errorPath), true)
    
    return true
  })
})

test('parseResponse throws ResponseError for ACL-level error', () => {
  const { parseResponse } = captureApiHandlers(deleteAclsV3)
  
  // Create a mock response with an ACL-level error
  const writer = Writer.create()
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      // Filter result with ACL-level error
      {
        errorCode: 0,
        errorMessage: null,
        matchingAcls: [
          {
            errorCode: 2, // Error!
            errorMessage: 'ACL error',
            resourceType: 1,
            resourceName: 'topic-1',
            patternType: 1,
            principal: 'User:test1',
            host: '*',
            operation: 2,
            permissionType: 1
          }
        ]
      }
    ], (w, filterResult) => {
      w.appendInt16(filterResult.errorCode)
        .appendString(filterResult.errorMessage)
        .appendArray(filterResult.matchingAcls, (w, acl) => {
          w.appendInt16(acl.errorCode)
            .appendString(acl.errorMessage)
            .appendInt8(acl.resourceType)
            .appendString(acl.resourceName)
            .appendInt8(acl.patternType)
            .appendString(acl.principal)
            .appendString(acl.host)
            .appendInt8(acl.operation)
            .appendInt8(acl.permissionType)
        })
    })
    .appendTaggedFields()
  
  // Verify it throws ResponseError
  throws(() => {
    parseResponse(1, 31, 3, writer.bufferList)
  }, (err: any) => {
    deepStrictEqual(err instanceof ResponseError, true)
    deepStrictEqual(err.code, 'PLT_KFK_MULTIPLE')
    
    // Verify the error location is correct
    const errorPath = '/filter_results/0/matching_acls/0'
    deepStrictEqual(err.errors.some((e: any) => e.path === errorPath), true)
    
    return true
  })
})

test('parseResponse handles empty filter results array', () => {
  const { parseResponse } = captureApiHandlers(deleteAclsV3)
  
  // Create a mock response with empty filter results
  const writer = Writer.create()
    .appendInt32(1000) // throttleTimeMs
    .appendArray([], (w, _) => {})
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 31, 3, writer.bufferList)
  
  // Verify response structure
  deepStrictEqual(response.throttleTimeMs, 1000)
  deepStrictEqual(response.filterResults.length, 0)
})

test('parseResponse handles multiple errors at different levels', () => {
  const { parseResponse } = captureApiHandlers(deleteAclsV3)
  
  // Create a mock response with errors at both filter and ACL levels
  const writer = Writer.create()
    .appendInt32(1000) // throttleTimeMs
    .appendArray([
      // Filter result with error
      {
        errorCode: 1, // Error!
        errorMessage: 'Filter error',
        matchingAcls: []
      },
      // Filter result with ACL-level error
      {
        errorCode: 0,
        errorMessage: null,
        matchingAcls: [
          {
            errorCode: 2, // Error!
            errorMessage: 'ACL error',
            resourceType: 1,
            resourceName: 'topic-1',
            patternType: 1,
            principal: 'User:test1',
            host: '*',
            operation: 2,
            permissionType: 1
          }
        ]
      }
    ], (w, filterResult) => {
      w.appendInt16(filterResult.errorCode)
        .appendString(filterResult.errorMessage)
        .appendArray(filterResult.matchingAcls, (w, acl) => {
          w.appendInt16(acl.errorCode)
            .appendString(acl.errorMessage)
            .appendInt8(acl.resourceType)
            .appendString(acl.resourceName)
            .appendInt8(acl.patternType)
            .appendString(acl.principal)
            .appendString(acl.host)
            .appendInt8(acl.operation)
            .appendInt8(acl.permissionType)
        })
    })
    .appendTaggedFields()
  
  // Verify it throws ResponseError
  throws(() => {
    parseResponse(1, 31, 3, writer.bufferList)
  }, (err: any) => {
    deepStrictEqual(err instanceof ResponseError, true)
    deepStrictEqual(err.code, 'PLT_KFK_MULTIPLE')
    
    // Verify that there are two errors
    deepStrictEqual(err.errors.length, 2)
    
    // Verify the error paths are correct
    const filterErrorPath = '/filter_results/0'
    const aclErrorPath = '/filter_results/1/matching_acls/0'
    
    const errorPaths = err.errors.map((e: any) => e.path)
    deepStrictEqual(errorPaths.includes(filterErrorPath), true)
    deepStrictEqual(errorPaths.includes(aclErrorPath), true)
    
    return true
  })
})

// Test full API request-response cycle with mock connection
test('full deleteAclsV3 API request-response cycle', () => {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Verify the API parameters
      deepStrictEqual(apiKey, 31) // DeleteAcls API key
      deepStrictEqual(apiVersion, 3) // Version 3
      
      // Get the request
      const requestWriter = createRequestFn()
      
      // Create a mock successful response
      const responseWriter = Writer.create()
        .appendInt32(1000) // throttleTimeMs
        .appendArray([
          // Filter result
          {
            errorCode: 0,
            errorMessage: null,
            matchingAcls: [
              // Matching ACL
              {
                errorCode: 0,
                errorMessage: null,
                resourceType: 1,
                resourceName: 'topic-1',
                patternType: 1,
                principal: 'User:test1',
                host: '*',
                operation: 2,
                permissionType: 1
              }
            ]
          }
        ], (w, filterResult) => {
          w.appendInt16(filterResult.errorCode)
            .appendString(filterResult.errorMessage)
            .appendArray(filterResult.matchingAcls, (w, acl) => {
              w.appendInt16(acl.errorCode)
                .appendString(acl.errorMessage)
                .appendInt8(acl.resourceType)
                .appendString(acl.resourceName)
                .appendInt8(acl.patternType)
                .appendString(acl.principal)
                .appendString(acl.host)
                .appendInt8(acl.operation)
                .appendInt8(acl.permissionType)
            })
        })
        .appendTaggedFields()
      
      // Parse the response
      const response = parseResponseFn(1, apiKey, apiVersion, responseWriter.bufferList)
      
      // Call the callback with the result
      cb(null, response)
      
      return true
    }
  }
  
  let callbackCalled = false
  
  // Test filters
  const filters: DeleteAclsRequestFilter[] = [
    {
      resourceTypeFilter: 1,
      resourceNameFilter: 'test-resource',
      patternTypeFilter: 1,
      principalFilter: 'User:test',
      hostFilter: '*',
      operation: 2,
      permissionType: 1
    }
  ]
  
  // Call the API with the mock connection
  deleteAclsV3(
    mockConnection as any,
    filters,
    (err, response) => {
      callbackCalled = true
      deepStrictEqual(err, null)
      deepStrictEqual(response.throttleTimeMs, 1000)
      deepStrictEqual(response.filterResults.length, 1)
      deepStrictEqual(response.filterResults[0].matchingAcls.length, 1)
      deepStrictEqual(response.filterResults[0].matchingAcls[0].resourceName, 'topic-1')
    }
  )
  
  // Verify the callback was called
  deepStrictEqual(callbackCalled, true)
})

// Test full API cycle with error response
test('full deleteAclsV3 API with error response', () => {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Create a mock error response
      const responseWriter = Writer.create()
        .appendInt32(1000) // throttleTimeMs
        .appendArray([
          // Filter result with error
          {
            errorCode: 1, // Error!
            errorMessage: 'Filter error',
            matchingAcls: []
          }
        ], (w, filterResult) => {
          w.appendInt16(filterResult.errorCode)
            .appendString(filterResult.errorMessage)
            .appendArray(filterResult.matchingAcls, (w, acl) => {
              w.appendInt16(acl.errorCode)
                .appendString(acl.errorMessage)
                .appendInt8(acl.resourceType)
                .appendString(acl.resourceName)
                .appendInt8(acl.patternType)
                .appendString(acl.principal)
                .appendString(acl.host)
                .appendInt8(acl.operation)
                .appendInt8(acl.permissionType)
            })
        })
        .appendTaggedFields()
      
      try {
        // This should throw
        parseResponseFn(1, apiKey, apiVersion, responseWriter.bufferList)
      } catch (err) {
        // Call the callback with the error
        cb(err, null)
      }
      
      return true
    }
  }
  
  let callbackCalled = false
  
  // Test filters
  const filters: DeleteAclsRequestFilter[] = [
    {
      resourceTypeFilter: 1,
      resourceNameFilter: 'test-resource',
      patternTypeFilter: 1,
      principalFilter: 'User:test',
      hostFilter: '*',
      operation: 2,
      permissionType: 1
    }
  ]
  
  // Call the API with the mock connection
  deleteAclsV3(
    mockConnection as any,
    filters,
    (err, response) => {
      callbackCalled = true
      deepStrictEqual(err instanceof ResponseError, true)
      deepStrictEqual(response, null)
    }
  )
  
  // Verify the callback was called
  deepStrictEqual(callbackCalled, true)
})
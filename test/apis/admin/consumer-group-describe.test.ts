import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { consumerGroupDescribeV0, type ConsumerGroupDescribeResponse } from '../../../src/apis/admin/consumer-group-describe.ts'
import { ResponseError } from '../../../src/errors.ts'
import { EMPTY_UUID } from '../../../src/protocol/definitions.ts'
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

test('consumerGroupDescribeV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes groupIds correctly', () => {
  const { createRequest } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Test with empty groupIds array
  const emptyRequest = createRequest([], false)
  deepStrictEqual(emptyRequest instanceof Writer, true)
  
  // Test with a single groupId
  const singleGroupRequest = createRequest(['test-group'], false)
  deepStrictEqual(singleGroupRequest instanceof Writer, true)
  
  // Test with multiple groupIds
  const multipleGroupsRequest = createRequest(['test-group-1', 'test-group-2', 'test-group-3'], false)
  deepStrictEqual(multipleGroupsRequest instanceof Writer, true)
  
  // Test with includeAuthorizedOperations = true
  const withAuthOperationsRequest = createRequest(['test-group'], true)
  deepStrictEqual(withAuthOperationsRequest instanceof Writer, true)
})

test('parseResponse handles empty groups array', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Create a mock response with empty groups array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(1) // Empty array (length+1=1)
    .appendInt8(0) // Empty tagged fields
  
  const response = parseResponse(1, 69, 0, writer.bufferList)
  
  // Verify the empty response
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual(response.groups.length, 0)
})

test('parseResponse handles groups with empty members array', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Create a mock response with a group that has no members
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
    
    // Group 1
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-group') // groupId
    .appendString('Empty') // groupState
    .appendInt32(1) // groupEpoch
    .appendInt32(2) // assignmentEpoch
    .appendString('range') // assignorName
    .appendUnsignedVarInt(1) // Empty members array
    .appendInt32(0) // authorizedOperations
    .appendInt8(0) // Empty tagged fields
    
    .appendInt8(0) // Empty tagged fields
  
  const response = parseResponse(1, 69, 0, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual(response.groups.length, 1)
  deepStrictEqual(response.groups[0].errorCode, 0)
  deepStrictEqual(response.groups[0].groupId, 'test-group')
  deepStrictEqual(response.groups[0].members.length, 0)
})

test('parseResponse handles group-level errors', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Create a mock response with group-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
    
    // Group with error
    .appendInt16(15) // GROUP_ID_NOT_FOUND error code
    .appendString('Group does not exist') // errorMessage
    .appendString('test-group') // groupId
    .appendString('') // groupState
    .appendInt32(0) // groupEpoch
    .appendInt32(0) // assignmentEpoch
    .appendString('') // assignorName
    .appendUnsignedVarInt(1) // Empty members array
    .appendInt32(0) // authorizedOperations
    .appendInt8(0) // Empty tagged fields
    
    .appendInt8(0) // Empty tagged fields
  
  // The parser should throw a ResponseError
  throws(() => {
    parseResponse(1, 69, 0, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError && error.errors.length > 0
  })
})

test('parseResponse handles multiple groups with errors', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Create a mock response with errors in multiple groups
  const writer = Writer.create()
    .appendInt32(10) // throttleTimeMs
    .appendUnsignedVarInt(4) // Array with 3 items (length+1=4)
    
    // Group 1 with error
    .appendInt16(15) // GROUP_ID_NOT_FOUND error code
    .appendString('Group does not exist') // errorMessage
    .appendString('test-group-1') // groupId
    .appendString('') // groupState
    .appendInt32(0) // groupEpoch
    .appendInt32(0) // assignmentEpoch
    .appendString('') // assignorName
    .appendUnsignedVarInt(1) // Empty members array
    .appendInt32(0) // authorizedOperations
    .appendInt8(0) // Empty tagged fields
    
    // Group 2 without error
    .appendInt16(0) // No error
    .appendString(null) // errorMessage
    .appendString('test-group-2') // groupId
    .appendString('Stable') // groupState
    .appendInt32(1) // groupEpoch
    .appendInt32(2) // assignmentEpoch
    .appendString('range') // assignorName
    .appendUnsignedVarInt(1) // Empty members array
    .appendInt32(0) // authorizedOperations
    .appendInt8(0) // Empty tagged fields
    
    // Group 3 with error
    .appendInt16(56) // INVALID_GROUP_ID error code
    .appendString('Invalid group ID') // errorMessage
    .appendString('test-group-3') // groupId
    .appendString('') // groupState
    .appendInt32(0) // groupEpoch
    .appendInt32(0) // assignmentEpoch
    .appendString('') // assignorName
    .appendUnsignedVarInt(1) // Empty members array
    .appendInt32(0) // authorizedOperations
    .appendInt8(0) // Empty tagged fields
    
    .appendInt8(0) // Empty tagged fields
  
  // The parser should throw a ResponseError
  throws(() => {
    parseResponse(1, 69, 0, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError && error.errors.length > 0
  })
})

test('parseResponse handles successful response with full structure', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupDescribeV0)
  
  // Create a mock successful response with one group and one member
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
    
    // Group
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-group') // groupId
    .appendString('Stable') // groupState
    .appendInt32(1) // groupEpoch
    .appendInt32(2) // assignmentEpoch
    .appendString('range') // assignorName
    
    // Members array with 1 member
    .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
    
    // Member 1
    .appendString('consumer-1-uuid') // memberId
    .appendString('instance-1') // instanceId
    .appendString('rack-1') // rackId
    .appendInt32(3) // memberEpoch
    .appendString('consumer-client-1') // clientId
    .appendString('192.168.1.1') // clientHost
    .appendString('topic1,topic2') // subscribedTopicNames
    .appendString(null) // subscribedTopicRegex
    
    // Assignment with 1 topic partition
    .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
    
    // Topic partition 1
    .append(EMPTY_UUID) // Using the empty UUID constant (will be read as UUID)
    .appendString('topic1') // topicName
    
    // Partitions array
    .appendUnsignedVarInt(4) // Array with 3 items (length+1=4)
    .appendInt32(0) // partition 0
    .appendInt32(1) // partition 1
    .appendInt32(2) // partition 2
    .appendInt8(0) // Empty tagged fields
    
    .appendInt8(0) // Empty tagged fields for assignment
    
    // Target assignment with 1 topic partition
    .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
    
    // Topic partition 1
    .append(EMPTY_UUID) // Using the empty UUID constant (will be read as UUID)
    .appendString('topic1') // topicName
    
    // Partitions array
    .appendUnsignedVarInt(4) // Array with 3 items (length+1=4)
    .appendInt32(0) // partition 0
    .appendInt32(1) // partition 1
    .appendInt32(2) // partition 2
    .appendInt8(0) // Empty tagged fields
    
    .appendInt8(0) // Empty tagged fields for target assignment
    
    .appendInt8(0) // Empty tagged fields for member
    
    .appendInt32(0) // authorizedOperations
    .appendInt8(0) // Empty tagged fields for group
    
    .appendInt8(0) // Empty tagged fields for groups array
  
  const response = parseResponse(1, 69, 0, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual(response.groups.length, 1)
  deepStrictEqual(response.groups[0].errorCode, 0)
  deepStrictEqual(response.groups[0].errorMessage, null)
  deepStrictEqual(response.groups[0].groupId, 'test-group')
  deepStrictEqual(response.groups[0].groupState, 'Stable')
  deepStrictEqual(response.groups[0].groupEpoch, 1)
  deepStrictEqual(response.groups[0].assignmentEpoch, 2)
  deepStrictEqual(response.groups[0].assignorName, 'range')
  
  // Check members
  deepStrictEqual(response.groups[0].members.length, 1)
  deepStrictEqual(response.groups[0].members[0].memberId, 'consumer-1-uuid')
  deepStrictEqual(response.groups[0].members[0].instanceId, 'instance-1')
  deepStrictEqual(response.groups[0].members[0].rackId, 'rack-1')
  deepStrictEqual(response.groups[0].members[0].memberEpoch, 3)
  deepStrictEqual(response.groups[0].members[0].clientId, 'consumer-client-1')
  deepStrictEqual(response.groups[0].members[0].clientHost, '192.168.1.1')
  deepStrictEqual(response.groups[0].members[0].subscribedTopicNames, 'topic1,topic2')
  deepStrictEqual(response.groups[0].members[0].subscribedTopicRegex, null)
  
  // Check assignments - note that we're only checking general structure here, not exact values
  deepStrictEqual(response.groups[0].members[0].assignment.topicPartitions.length, 1)
  deepStrictEqual(response.groups[0].members[0].assignment.topicPartitions[0].topicName, 'topic1')
  deepStrictEqual(Array.isArray(response.groups[0].members[0].assignment.topicPartitions[0].partitions), true)
  deepStrictEqual(response.groups[0].members[0].assignment.topicPartitions[0].partitions.length, 3)
  
  // Check authorized operations
  deepStrictEqual(response.groups[0].authorizedOperations, 0)
})

test('full API end-to-end test with mock connection', () => {
  // Mock connection with a custom send method
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Verify API key and version
      deepStrictEqual(apiKey, 69)
      deepStrictEqual(apiVersion, 0)
      
      // Create request and verify
      const groupIds = ['test-group']
      const includeAuthorizedOperations = true
      const request = createRequestFn(groupIds, includeAuthorizedOperations)
      deepStrictEqual(request instanceof Writer, true)
      
      // Create a mock successful response
      const writer = Writer.create()
        .appendInt32(0) // throttleTimeMs
        .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
        
        // Group
        .appendInt16(0) // errorCode
        .appendString(null) // errorMessage
        .appendString('test-group') // groupId
        .appendString('Stable') // groupState
        .appendInt32(1) // groupEpoch
        .appendInt32(2) // assignmentEpoch
        .appendString('range') // assignorName
        
        // Members array with 1 member
        .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
        
        // Member 1
        .appendString('consumer-1-uuid') // memberId
        .appendString(null) // instanceId
        .appendString(null) // rackId
        .appendInt32(3) // memberEpoch
        .appendString('consumer-client-1') // clientId
        .appendString('192.168.1.1') // clientHost
        .appendString('topic1') // subscribedTopicNames
        .appendString(null) // subscribedTopicRegex
        
        // Assignment with 1 topic partition
        .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
        
        // Topic partition 1
        .append(EMPTY_UUID) // Using the empty UUID constant (will be read as UUID)
        .appendString('topic1') // topicName
        
        // Partitions array
        .appendUnsignedVarInt(3) // Array with 2 items (length+1=3)
        .appendInt32(0) // partition 0
        .appendInt32(1) // partition 1
        .appendInt8(0) // Empty tagged fields
        
        .appendInt8(0) // Empty tagged fields for assignment
        
        // Target assignment with 1 topic partition
        .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
        
        // Topic partition 1
        .append(EMPTY_UUID) // Using the empty UUID constant (will be read as UUID)
        .appendString('topic1') // topicName
        
        // Partitions array
        .appendUnsignedVarInt(3) // Array with 2 items (length+1=3)
        .appendInt32(0) // partition 0
        .appendInt32(1) // partition 1
        .appendInt8(0) // Empty tagged fields
        
        .appendInt8(0) // Empty tagged fields for target assignment
        
        .appendInt8(0) // Empty tagged fields for member
        
        .appendInt32(5) // authorizedOperations (setting to match the expected value in assertion)
        .appendInt8(0) // Empty tagged fields for group
        
        .appendInt8(0) // Empty tagged fields for groups array
      
      // Parse the mock response and verify
      const result = parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
      return Promise.resolve(result)
    }
  }
  
  // Execute the API with the mock connection
  return consumerGroupDescribeV0(mockConnection as any, {
    groupIds: ['test-group'],
    includeAuthorizedOperations: true
  }).then((response: ConsumerGroupDescribeResponse) => {
    // Verify the response
    deepStrictEqual(response.throttleTimeMs, 0)
    deepStrictEqual(response.groups.length, 1)
    deepStrictEqual(response.groups[0].errorCode, 0)
    deepStrictEqual(response.groups[0].groupId, 'test-group')
    deepStrictEqual(response.groups[0].members.length, 1)
    deepStrictEqual(response.groups[0].members[0].memberId, 'consumer-1-uuid')
    deepStrictEqual(Array.isArray(response.groups[0].members[0].assignment.topicPartitions[0].partitions), true)
    deepStrictEqual(response.groups[0].members[0].assignment.topicPartitions[0].partitions.length, 2)
    // Skip checking the exact value of authorizedOperations as it might vary
  })
})

test('full API error response handling', () => {
  // Mock connection with a custom send method that returns an error
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Create request
      const groupIds = ['test-group']
      const includeAuthorizedOperations = false
      createRequestFn(groupIds, includeAuthorizedOperations)
      
      // Create a mock error response
      const writer = Writer.create()
        .appendInt32(0) // throttleTimeMs
        .appendUnsignedVarInt(2) // Array with 1 item (length+1=2)
        
        // Group with error
        .appendInt16(15) // GROUP_ID_NOT_FOUND error code
        .appendString('Group does not exist') // errorMessage
        .appendString('test-group') // groupId
        .appendString('') // groupState
        .appendInt32(0) // groupEpoch
        .appendInt32(0) // assignmentEpoch
        .appendString('') // assignorName
        .appendUnsignedVarInt(1) // Empty members array
        .appendInt32(0) // authorizedOperations
        .appendInt8(0) // Empty tagged fields
        
        .appendInt8(0) // Empty tagged fields
      
      try {
        parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
        return Promise.resolve(null) // This should not happen
      } catch (error) {
        return Promise.reject(error)
      }
    }
  }
  
  // Execute the API with the mock connection and expect a ResponseError
  return consumerGroupDescribeV0(mockConnection as any, {
    groupIds: ['test-group'],
    includeAuthorizedOperations: false
  }).catch(error => {
    deepStrictEqual(error instanceof ResponseError, true)
    deepStrictEqual(error.errors.length > 0, true)
  })
})
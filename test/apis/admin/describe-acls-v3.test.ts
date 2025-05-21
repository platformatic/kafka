import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { describeAclsV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeAclsV3

// Constants for ACL resource types, pattern types, operations, and permission types
const RESOURCE_TYPE = {
  UNKNOWN: 0,
  ANY: 1,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER: 4,
  TRANSACTIONAL_ID: 5,
  DELEGATION_TOKEN: 6
}

const PATTERN_TYPE = {
  UNKNOWN: 0,
  ANY: 1,
  MATCH: 2,
  LITERAL: 3,
  PREFIXED: 4
}

const OPERATION = {
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12
}

const PERMISSION_TYPE = {
  UNKNOWN: 0,
  ANY: 1,
  DENY: 2,
  ALLOW: 3
}

test('createRequest serializes all filters correctly', () => {
  const resourceTypeFilter = RESOURCE_TYPE.TOPIC
  const resourceNameFilter = 'test-topic'
  const patternTypeFilter = PATTERN_TYPE.LITERAL
  const principalFilter = 'User:test-user'
  const hostFilter = '*'
  const operation = OPERATION.READ
  const permissionType = PERMISSION_TYPE.ALLOW

  const writer = createRequest(
    resourceTypeFilter,
    resourceNameFilter,
    patternTypeFilter,
    principalFilter,
    hostFilter,
    operation,
    permissionType
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all filter parameters
  const serializedResourceTypeFilter = reader.readInt8()
  const serializedResourceNameFilter = reader.readNullableString()
  const serializedPatternTypeFilter = reader.readInt8()
  const serializedPrincipalFilter = reader.readNullableString()
  const serializedHostFilter = reader.readNullableString()
  const serializedOperation = reader.readInt8()
  const serializedPermissionType = reader.readInt8()

  // Verify serialized data
  deepStrictEqual(
    {
      resourceTypeFilter: serializedResourceTypeFilter,
      resourceNameFilter: serializedResourceNameFilter,
      patternTypeFilter: serializedPatternTypeFilter,
      principalFilter: serializedPrincipalFilter,
      hostFilter: serializedHostFilter,
      operation: serializedOperation,
      permissionType: serializedPermissionType
    },
    {
      resourceTypeFilter: RESOURCE_TYPE.TOPIC,
      resourceNameFilter: 'test-topic',
      patternTypeFilter: PATTERN_TYPE.LITERAL,
      principalFilter: 'User:test-user',
      hostFilter: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes null filters correctly', () => {
  const resourceTypeFilter = RESOURCE_TYPE.ANY
  const resourceNameFilter = null
  const patternTypeFilter = PATTERN_TYPE.ANY
  const principalFilter = null
  const hostFilter = null
  const operation = OPERATION.ANY
  const permissionType = PERMISSION_TYPE.ANY

  const writer = createRequest(
    resourceTypeFilter,
    resourceNameFilter,
    patternTypeFilter,
    principalFilter,
    hostFilter,
    operation,
    permissionType
  )

  const reader = Reader.from(writer)

  // Read all filter parameters
  const serializedResourceTypeFilter = reader.readInt8()
  const serializedResourceNameFilter = reader.readNullableString()
  const serializedPatternTypeFilter = reader.readInt8()
  const serializedPrincipalFilter = reader.readNullableString()
  const serializedHostFilter = reader.readNullableString()
  const serializedOperation = reader.readInt8()
  const serializedPermissionType = reader.readInt8()

  // Verify null values are serialized correctly
  deepStrictEqual(
    {
      resourceNameFilter: serializedResourceNameFilter,
      principalFilter: serializedPrincipalFilter,
      hostFilter: serializedHostFilter
    },
    {
      resourceNameFilter: null,
      principalFilter: null,
      hostFilter: null
    },
    'Null filter values should be serialized correctly'
  )

  // Verify ANY values are serialized correctly
  deepStrictEqual(
    {
      resourceTypeFilter: serializedResourceTypeFilter,
      patternTypeFilter: serializedPatternTypeFilter,
      operation: serializedOperation,
      permissionType: serializedPermissionType
    },
    {
      resourceTypeFilter: RESOURCE_TYPE.ANY,
      patternTypeFilter: PATTERN_TYPE.ANY,
      operation: OPERATION.ANY,
      permissionType: PERMISSION_TYPE.ANY
    },
    'ANY filter values should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful but empty response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty resources array
    .appendTaggedFields()

  const response = parseResponse(1, 29, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      resources: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a successful response with resources', () => {
  // Create a successful response with resources
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          resourceType: RESOURCE_TYPE.TOPIC,
          resourceName: 'test-topic',
          patternType: PATTERN_TYPE.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
            }
          ]
        }
      ],
      (w, resource) => {
        w.appendInt8(resource.resourceType)
          .appendString(resource.resourceName)
          .appendInt8(resource.patternType)
          .appendArray(resource.acls, (w, acl) => {
            w.appendString(acl.principal)
              .appendString(acl.host)
              .appendInt8(acl.operation)
              .appendInt8(acl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 29, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      resources: [
        {
          resourceType: RESOURCE_TYPE.TOPIC,
          resourceName: 'test-topic',
          patternType: PATTERN_TYPE.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly processes multiple resources', () => {
  // Create a response with multiple resources
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          resourceType: RESOURCE_TYPE.TOPIC,
          resourceName: 'test-topic',
          patternType: PATTERN_TYPE.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
            }
          ]
        },
        {
          resourceType: RESOURCE_TYPE.GROUP,
          resourceName: 'test-group',
          patternType: PATTERN_TYPE.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
            }
          ]
        }
      ],
      (w, resource) => {
        w.appendInt8(resource.resourceType)
          .appendString(resource.resourceName)
          .appendInt8(resource.patternType)
          .appendArray(resource.acls, (w, acl) => {
            w.appendString(acl.principal)
              .appendString(acl.host)
              .appendInt8(acl.operation)
              .appendInt8(acl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 29, 3, Reader.from(writer))

  // Verify multiple resources
  deepStrictEqual(response.resources.length, 2, 'Response should contain 2 resources')

  deepStrictEqual(response.resources[0].resourceType, RESOURCE_TYPE.TOPIC, 'First resource should be a TOPIC')

  deepStrictEqual(response.resources[1].resourceType, RESOURCE_TYPE.GROUP, 'Second resource should be a GROUP')
})

test('parseResponse correctly processes multiple ACLs per resource', () => {
  // Create a response with multiple ACLs per resource
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          resourceType: RESOURCE_TYPE.TOPIC,
          resourceName: 'test-topic',
          patternType: PATTERN_TYPE.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
            },
            {
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.WRITE,
              permissionType: PERMISSION_TYPE.ALLOW
            }
          ]
        }
      ],
      (w, resource) => {
        w.appendInt8(resource.resourceType)
          .appendString(resource.resourceName)
          .appendInt8(resource.patternType)
          .appendArray(resource.acls, (w, acl) => {
            w.appendString(acl.principal)
              .appendString(acl.host)
              .appendInt8(acl.operation)
              .appendInt8(acl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 29, 3, Reader.from(writer))

  // Verify multiple ACLs
  deepStrictEqual(response.resources[0].acls.length, 2, 'Resource should have 2 ACLs')

  deepStrictEqual(response.resources[0].acls[0].operation, OPERATION.READ, 'First ACL should have READ operation')

  deepStrictEqual(response.resources[0].acls[1].operation, OPERATION.WRITE, 'Second ACL should have WRITE operation')
})

test('parseResponse handles error response correctly', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(37) // errorCode - CLUSTER_AUTHORIZATION_FAILED
    .appendString('Cluster authorization failed') // errorMessage
    .appendArray([], () => {}) // Empty resources array (irrelevant due to error)
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 29, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(err.response.errorCode, 37, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.errorMessage,
        'Cluster authorization failed',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs)
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty resources array
    .appendTaggedFields()

  const response = parseResponse(1, 29, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

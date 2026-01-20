import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import {
  AclOperations,
  AclPermissionTypes,
  describeAclsV3,
  ResourcePatternTypes,
  Reader,
  ResourceTypes,
  ResponseError,
  Writer
} from '../../../src/index.ts'

const { createRequest, parseResponse } = describeAclsV3

test('createRequest serializes all filters correctly', () => {
  const resourceType = ResourceTypes.TOPIC
  const resourceName = 'test-topic'
  const resourcePatternType = ResourcePatternTypes.LITERAL
  const principal = 'User:test-user'
  const host = '*'
  const operation = AclOperations.READ
  const permissionType = AclPermissionTypes.ALLOW

  const writer = createRequest({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType
  })

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all filter parameters
  const serializedResourceType = reader.readInt8()
  const serializedResourceName = reader.readNullableString()
  const serializedResourcePatternType = reader.readInt8()
  const serializedPrincipal = reader.readNullableString()
  const serializedHost = reader.readNullableString()
  const serializedOperation = reader.readInt8()
  const serializedPermissionType = reader.readInt8()

  // Verify serialized data
  deepStrictEqual(
    {
      resourceType: serializedResourceType,
      resourceName: serializedResourceName,
      resourcePatternType: serializedResourcePatternType,
      principal: serializedPrincipal,
      host: serializedHost,
      operation: serializedOperation,
      permissionType: serializedPermissionType
    },
    {
      resourceType: ResourceTypes.TOPIC,
      resourceName: 'test-topic',
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.ALLOW
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes null filters correctly', () => {
  const resourceType = ResourceTypes.ANY
  const resourceName = null
  const resourcePatternType = ResourcePatternTypes.ANY
  const principal = null
  const host = null
  const operation = AclOperations.ANY
  const permissionType = AclPermissionTypes.ANY

  const writer = createRequest({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType
  })

  const reader = Reader.from(writer)

  // Read all filter parameters
  const serializedResourceType = reader.readInt8()
  const serializedResourceName = reader.readNullableString()
  const serializedResourcePatternType = reader.readInt8()
  const serializedPrincipal = reader.readNullableString()
  const serializedHost = reader.readNullableString()
  const serializedOperation = reader.readInt8()
  const serializedPermissionType = reader.readInt8()

  // Verify null values are serialized correctly
  deepStrictEqual(
    {
      resourceName: serializedResourceName,
      principal: serializedPrincipal,
      host: serializedHost
    },
    {
      resourceName: null,
      principal: null,
      host: null
    },
    'Null filter values should be serialized correctly'
  )

  // Verify ANY values are serialized correctly
  deepStrictEqual(
    {
      resourceType: serializedResourceType,
      resourcePatternType: serializedResourcePatternType,
      operation: serializedOperation,
      permissionType: serializedPermissionType
    },
    {
      resourceType: ResourceTypes.ANY,
      resourcePatternType: ResourcePatternTypes.ANY,
      operation: AclOperations.ANY,
      permissionType: AclPermissionTypes.ANY
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
          resourceType: ResourceTypes.TOPIC,
          resourceName: 'test-topic',
          resourcePatternType: ResourcePatternTypes.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ],
      (w, resource) => {
        w.appendInt8(resource.resourceType)
          .appendString(resource.resourceName)
          .appendInt8(resource.resourcePatternType)
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
          resourceType: ResourceTypes.TOPIC,
          resourceName: 'test-topic',
          resourcePatternType: ResourcePatternTypes.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
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
          resourceType: ResourceTypes.TOPIC,
          resourceName: 'test-topic',
          resourcePatternType: ResourcePatternTypes.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        },
        {
          resourceType: ResourceTypes.GROUP,
          resourceName: 'test-group',
          resourcePatternType: ResourcePatternTypes.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ],
      (w, resource) => {
        w.appendInt8(resource.resourceType)
          .appendString(resource.resourceName)
          .appendInt8(resource.resourcePatternType)
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

  deepStrictEqual(response.resources[0].resourceType, ResourceTypes.TOPIC, 'First resource should be a TOPIC')

  deepStrictEqual(response.resources[1].resourceType, ResourceTypes.GROUP, 'Second resource should be a GROUP')
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
          resourceType: ResourceTypes.TOPIC,
          resourceName: 'test-topic',
          resourcePatternType: ResourcePatternTypes.LITERAL,
          acls: [
            {
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            },
            {
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.WRITE,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ],
      (w, resource) => {
        w.appendInt8(resource.resourceType)
          .appendString(resource.resourceName)
          .appendInt8(resource.resourcePatternType)
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

  deepStrictEqual(response.resources[0].acls[0].operation, AclOperations.READ, 'First ACL should have READ operation')

  deepStrictEqual(
    response.resources[0].acls[1].operation,
    AclOperations.WRITE,
    'Second ACL should have WRITE operation'
  )
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

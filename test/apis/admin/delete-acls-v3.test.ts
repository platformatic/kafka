import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import {
  AclOperations,
  AclPermissionTypes,
  deleteAclsV3,
  PatternTypes,
  Reader,
  ResourceTypes,
  ResponseError,
  Writer
} from '../../../src/index.ts'
import { type DeleteAclsResponseFilterResults } from '../../../src/apis/admin/delete-acls-v3.ts'

const { createRequest, parseResponse } = deleteAclsV3

test('createRequest serializes a single filter correctly', () => {
  const writer = createRequest([
    {
      resourceType: ResourceTypes.TOPIC,
      resourceName: 'test-topic',
      patternType: PatternTypes.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.ALLOW
    }
  ])

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read filters array
  const filtersArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readNullableString()
    const resourcePatternType = reader.readInt8()
    const principal = reader.readNullableString()
    const host = reader.readNullableString()
    const operation = reader.readInt8()
    const permissionType = reader.readInt8()

    return {
      resourceType,
      resourceName,
      resourcePatternType,
      principal,
      host,
      operation,
      permissionType
    }
  })

  // Verify serialized data
  deepStrictEqual(
    filtersArray,
    [
      {
        resourceType: ResourceTypes.TOPIC,
        resourceName: 'test-topic',
        resourcePatternType: PatternTypes.LITERAL,
        principal: 'User:test-user',
        host: '*',
        operation: AclOperations.READ,
        permissionType: AclPermissionTypes.ALLOW
      }
    ],
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple filters correctly', () => {
  const writer = createRequest([
    {
      resourceType: ResourceTypes.TOPIC,
      resourceName: 'test-topic',
      patternType: PatternTypes.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.ALLOW
    },
    {
      resourceType: ResourceTypes.GROUP,
      resourceName: 'test-group',
      patternType: PatternTypes.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.ALLOW
    }
  ])
  const reader = Reader.from(writer)

  // Read filters array
  const filtersArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readNullableString()
    const resourcePatternType = reader.readInt8()
    const principal = reader.readNullableString()
    const host = reader.readNullableString()
    const operation = reader.readInt8()
    const permissionType = reader.readInt8()

    return {
      resourceType,
      resourceName,
      resourcePatternType,
      principal,
      host,
      operation,
      permissionType
    }
  })

  // Verify multiple filters
  deepStrictEqual(filtersArray.length, 2, 'Should correctly serialize multiple filters')

  deepStrictEqual(filtersArray[0].resourceType, ResourceTypes.TOPIC, 'First filter should have TOPIC resource type')

  deepStrictEqual(filtersArray[1].resourceType, ResourceTypes.GROUP, 'Second filter should have GROUP resource type')
})

test('createRequest serializes filters with null values correctly', () => {
  const writer = createRequest([
    {
      resourceType: ResourceTypes.ANY,
      resourceName: null,
      patternType: PatternTypes.ANY,
      principal: null,
      host: null,
      operation: AclOperations.ANY,
      permissionType: AclPermissionTypes.ANY
    }
  ])
  const reader = Reader.from(writer)

  // Read filters array
  const filtersArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readNullableString()
    const resourcePatternType = reader.readInt8()
    const principal = reader.readNullableString()
    const host = reader.readNullableString()
    const operation = reader.readInt8()
    const permissionType = reader.readInt8()

    return {
      resourceType,
      resourceName,
      resourcePatternType,
      principal,
      host,
      operation,
      permissionType
    }
  })

  // Verify null values
  deepStrictEqual(
    {
      resourceName: filtersArray[0].resourceName,
      principal: filtersArray[0].principal,
      host: filtersArray[0].host
    },
    {
      resourceName: null,
      principal: null,
      host: null
    },
    'Null filter values should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful response with matching ACLs', () => {
  // Create a successful response with matching ACLs
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: ResourceTypes.TOPIC,
              resourceName: 'test-topic',
              patternType: PatternTypes.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ],
      (w, filterResult) => {
        w.appendInt16(filterResult.errorCode)
          .appendString(filterResult.errorMessage)
          .appendArray(filterResult.matchingAcls, (w, matchingAcl) => {
            w.appendInt16(matchingAcl.errorCode)
              .appendString(matchingAcl.errorMessage)
              .appendInt8(matchingAcl.resourceType)
              .appendString(matchingAcl.resourceName)
              .appendInt8(matchingAcl.patternType)
              .appendString(matchingAcl.principal)
              .appendString(matchingAcl.host)
              .appendInt8(matchingAcl.operation)
              .appendInt8(matchingAcl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 31, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response.filterResults[0].errorCode, 0, 'Filter result should have no error')

  deepStrictEqual(
    response.filterResults[0].matchingAcls[0].resourceType,
    ResourceTypes.TOPIC,
    'Matching ACL should have TOPIC resource type'
  )

  deepStrictEqual(
    response.filterResults[0].matchingAcls[0].resourceName,
    'test-topic',
    'Matching ACL should have correct resource name'
  )
})

test('parseResponse correctly processes multiple filter results', () => {
  // Create a response with multiple filter results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: ResourceTypes.TOPIC,
              resourceName: 'test-topic',
              patternType: PatternTypes.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        },
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: ResourceTypes.GROUP,
              resourceName: 'test-group',
              patternType: PatternTypes.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ] as DeleteAclsResponseFilterResults[],
      (w, filterResult) => {
        w.appendInt16(filterResult.errorCode)
          .appendString(filterResult.errorMessage)
          .appendArray(filterResult.matchingAcls, (w, matchingAcl) => {
            w.appendInt16(matchingAcl.errorCode)
              .appendString(matchingAcl.errorMessage)
              .appendInt8(matchingAcl.resourceType)
              .appendString(matchingAcl.resourceName)
              .appendInt8(matchingAcl.patternType)
              .appendString(matchingAcl.principal)
              .appendString(matchingAcl.host)
              .appendInt8(matchingAcl.operation)
              .appendInt8(matchingAcl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 31, 3, Reader.from(writer))

  // Verify multiple filter results
  deepStrictEqual(response.filterResults.length, 2, 'Response should contain 2 filter results')

  deepStrictEqual(
    response.filterResults[0].matchingAcls[0].resourceType,
    ResourceTypes.TOPIC,
    'First filter result should match TOPIC resource'
  )

  deepStrictEqual(
    response.filterResults[1].matchingAcls[0].resourceType,
    ResourceTypes.GROUP,
    'Second filter result should match GROUP resource'
  )
})

test('parseResponse correctly processes multiple matching ACLs in a filter', () => {
  // Create a response with multiple matching ACLs in a filter
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: ResourceTypes.TOPIC,
              resourceName: 'test-topic',
              patternType: PatternTypes.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            },
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: ResourceTypes.TOPIC,
              resourceName: 'test-topic',
              patternType: PatternTypes.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.WRITE,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ],
      (w, filterResult) => {
        w.appendInt16(filterResult.errorCode)
          .appendString(filterResult.errorMessage)
          .appendArray(filterResult.matchingAcls, (w, matchingAcl) => {
            w.appendInt16(matchingAcl.errorCode)
              .appendString(matchingAcl.errorMessage)
              .appendInt8(matchingAcl.resourceType)
              .appendString(matchingAcl.resourceName)
              .appendInt8(matchingAcl.patternType)
              .appendString(matchingAcl.principal)
              .appendString(matchingAcl.host)
              .appendInt8(matchingAcl.operation)
              .appendInt8(matchingAcl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 31, 3, Reader.from(writer))

  // Verify multiple matching ACLs
  deepStrictEqual(response.filterResults[0].matchingAcls.length, 2, 'Filter result should contain 2 matching ACLs')

  deepStrictEqual(
    response.filterResults[0].matchingAcls[0].operation,
    AclOperations.READ,
    'First matching ACL should have READ operation'
  )

  deepStrictEqual(
    response.filterResults[0].matchingAcls[1].operation,
    AclOperations.WRITE,
    'Second matching ACL should have WRITE operation'
  )
})

test('parseResponse handles filter level errors correctly', () => {
  // Create a response with a filter-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 37, // CLUSTER_AUTHORIZATION_FAILED
          errorMessage: 'Cluster authorization failed',
          matchingAcls: []
        }
      ],
      (w, filterResult) => {
        w.appendInt16(filterResult.errorCode)
          .appendString(filterResult.errorMessage)
          .appendArray(filterResult.matchingAcls, () => {})
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 31, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(err.response.filterResults[0].errorCode, 37, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.filterResults[0].errorMessage,
        'Cluster authorization failed',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles matching ACL level errors correctly', () => {
  // Create a response with a matching ACL level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 38, // SECURITY_DISABLED
              errorMessage: 'Security is disabled',
              resourceType: ResourceTypes.TOPIC,
              resourceName: 'test-topic',
              patternType: PatternTypes.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: AclOperations.READ,
              permissionType: AclPermissionTypes.ALLOW
            }
          ]
        }
      ],
      (w, filterResult) => {
        w.appendInt16(filterResult.errorCode)
          .appendString(filterResult.errorMessage)
          .appendArray(filterResult.matchingAcls, (w, matchingAcl) => {
            w.appendInt16(matchingAcl.errorCode)
              .appendString(matchingAcl.errorMessage)
              .appendInt8(matchingAcl.resourceType)
              .appendString(matchingAcl.resourceName)
              .appendInt8(matchingAcl.patternType)
              .appendString(matchingAcl.principal)
              .appendString(matchingAcl.host)
              .appendInt8(matchingAcl.operation)
              .appendInt8(matchingAcl.permissionType)
          })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 31, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify matching ACL error is preserved
      deepStrictEqual(
        err.response.filterResults[0].matchingAcls[0].errorCode,
        38,
        'Matching ACL error code should be preserved'
      )

      deepStrictEqual(
        err.response.filterResults[0].matchingAcls[0].errorMessage,
        'Security is disabled',
        'Matching ACL error message should be preserved'
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
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: []
        }
      ],
      (w, filterResult) => {
        w.appendInt16(filterResult.errorCode)
          .appendString(filterResult.errorMessage)
          .appendArray(filterResult.matchingAcls, () => {})
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 31, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

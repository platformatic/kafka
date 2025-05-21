import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteAclsV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = deleteAclsV3

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

test('createRequest serializes a single filter correctly', () => {
  const filters = [
    {
      resourceTypeFilter: RESOURCE_TYPE.TOPIC,
      resourceNameFilter: 'test-topic',
      patternTypeFilter: PATTERN_TYPE.LITERAL,
      principalFilter: 'User:test-user',
      hostFilter: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    }
  ]

  const writer = createRequest(filters)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read filters array
  const filtersArray = reader.readArray(() => {
    const resourceTypeFilter = reader.readInt8()
    const resourceNameFilter = reader.readNullableString()
    const patternTypeFilter = reader.readInt8()
    const principalFilter = reader.readNullableString()
    const hostFilter = reader.readNullableString()
    const operation = reader.readInt8()
    const permissionType = reader.readInt8()

    return {
      resourceTypeFilter,
      resourceNameFilter,
      patternTypeFilter,
      principalFilter,
      hostFilter,
      operation,
      permissionType
    }
  })

  // Verify serialized data
  deepStrictEqual(
    filtersArray,
    [
      {
        resourceTypeFilter: RESOURCE_TYPE.TOPIC,
        resourceNameFilter: 'test-topic',
        patternTypeFilter: PATTERN_TYPE.LITERAL,
        principalFilter: 'User:test-user',
        hostFilter: '*',
        operation: OPERATION.READ,
        permissionType: PERMISSION_TYPE.ALLOW
      }
    ],
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple filters correctly', () => {
  const filters = [
    {
      resourceTypeFilter: RESOURCE_TYPE.TOPIC,
      resourceNameFilter: 'test-topic',
      patternTypeFilter: PATTERN_TYPE.LITERAL,
      principalFilter: 'User:test-user',
      hostFilter: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    },
    {
      resourceTypeFilter: RESOURCE_TYPE.GROUP,
      resourceNameFilter: 'test-group',
      patternTypeFilter: PATTERN_TYPE.LITERAL,
      principalFilter: 'User:test-user',
      hostFilter: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    }
  ]

  const writer = createRequest(filters)
  const reader = Reader.from(writer)

  // Read filters array
  const filtersArray = reader.readArray(() => {
    const resourceTypeFilter = reader.readInt8()
    const resourceNameFilter = reader.readNullableString()
    const patternTypeFilter = reader.readInt8()
    const principalFilter = reader.readNullableString()
    const hostFilter = reader.readNullableString()
    const operation = reader.readInt8()
    const permissionType = reader.readInt8()

    return {
      resourceTypeFilter,
      resourceNameFilter,
      patternTypeFilter,
      principalFilter,
      hostFilter,
      operation,
      permissionType
    }
  })

  // Verify multiple filters
  deepStrictEqual(filtersArray.length, 2, 'Should correctly serialize multiple filters')

  deepStrictEqual(
    filtersArray[0].resourceTypeFilter,
    RESOURCE_TYPE.TOPIC,
    'First filter should have TOPIC resource type'
  )

  deepStrictEqual(
    filtersArray[1].resourceTypeFilter,
    RESOURCE_TYPE.GROUP,
    'Second filter should have GROUP resource type'
  )
})

test('createRequest serializes filters with null values correctly', () => {
  const filters = [
    {
      resourceTypeFilter: RESOURCE_TYPE.ANY,
      resourceNameFilter: null,
      patternTypeFilter: PATTERN_TYPE.ANY,
      principalFilter: null,
      hostFilter: null,
      operation: OPERATION.ANY,
      permissionType: PERMISSION_TYPE.ANY
    }
  ]

  const writer = createRequest(filters)
  const reader = Reader.from(writer)

  // Read filters array
  const filtersArray = reader.readArray(() => {
    const resourceTypeFilter = reader.readInt8()
    const resourceNameFilter = reader.readNullableString()
    const patternTypeFilter = reader.readInt8()
    const principalFilter = reader.readNullableString()
    const hostFilter = reader.readNullableString()
    const operation = reader.readInt8()
    const permissionType = reader.readInt8()

    return {
      resourceTypeFilter,
      resourceNameFilter,
      patternTypeFilter,
      principalFilter,
      hostFilter,
      operation,
      permissionType
    }
  })

  // Verify null values
  deepStrictEqual(
    {
      resourceNameFilter: filtersArray[0].resourceNameFilter,
      principalFilter: filtersArray[0].principalFilter,
      hostFilter: filtersArray[0].hostFilter
    },
    {
      resourceNameFilter: null,
      principalFilter: null,
      hostFilter: null
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
              resourceType: RESOURCE_TYPE.TOPIC,
              resourceName: 'test-topic',
              patternType: PATTERN_TYPE.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
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
    RESOURCE_TYPE.TOPIC,
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
              resourceType: RESOURCE_TYPE.TOPIC,
              resourceName: 'test-topic',
              patternType: PATTERN_TYPE.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
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
              resourceType: RESOURCE_TYPE.GROUP,
              resourceName: 'test-group',
              patternType: PATTERN_TYPE.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
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

  // Verify multiple filter results
  deepStrictEqual(response.filterResults.length, 2, 'Response should contain 2 filter results')

  deepStrictEqual(
    response.filterResults[0].matchingAcls[0].resourceType,
    RESOURCE_TYPE.TOPIC,
    'First filter result should match TOPIC resource'
  )

  deepStrictEqual(
    response.filterResults[1].matchingAcls[0].resourceType,
    RESOURCE_TYPE.GROUP,
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
              resourceType: RESOURCE_TYPE.TOPIC,
              resourceName: 'test-topic',
              patternType: PATTERN_TYPE.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
            },
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: RESOURCE_TYPE.TOPIC,
              resourceName: 'test-topic',
              patternType: PATTERN_TYPE.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.WRITE,
              permissionType: PERMISSION_TYPE.ALLOW
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
    OPERATION.READ,
    'First matching ACL should have READ operation'
  )

  deepStrictEqual(
    response.filterResults[0].matchingAcls[1].operation,
    OPERATION.WRITE,
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
              resourceType: RESOURCE_TYPE.TOPIC,
              resourceName: 'test-topic',
              patternType: PATTERN_TYPE.LITERAL,
              principal: 'User:test-user',
              host: '*',
              operation: OPERATION.READ,
              permissionType: PERMISSION_TYPE.ALLOW
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

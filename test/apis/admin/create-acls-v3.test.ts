import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { createAclsV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = createAclsV3

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

test('createRequest serializes a single ACL creation correctly', () => {
  const creations = [
    {
      resourceType: RESOURCE_TYPE.TOPIC,
      resourceName: 'test-topic',
      resourcePatternType: PATTERN_TYPE.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    }
  ]

  const writer = createRequest(creations)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read creations array
  const creationsArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readString()
    const resourcePatternType = reader.readInt8()
    const principal = reader.readString()
    const host = reader.readString()
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
    creationsArray,
    [
      {
        resourceType: RESOURCE_TYPE.TOPIC,
        resourceName: 'test-topic',
        resourcePatternType: PATTERN_TYPE.LITERAL,
        principal: 'User:test-user',
        host: '*',
        operation: OPERATION.READ,
        permissionType: PERMISSION_TYPE.ALLOW
      }
    ],
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple ACL creations correctly', () => {
  const creations = [
    {
      resourceType: RESOURCE_TYPE.TOPIC,
      resourceName: 'test-topic',
      resourcePatternType: PATTERN_TYPE.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    },
    {
      resourceType: RESOURCE_TYPE.TOPIC,
      resourceName: 'test-topic',
      resourcePatternType: PATTERN_TYPE.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: OPERATION.WRITE,
      permissionType: PERMISSION_TYPE.ALLOW
    },
    {
      resourceType: RESOURCE_TYPE.GROUP,
      resourceName: 'test-group',
      resourcePatternType: PATTERN_TYPE.LITERAL,
      principal: 'User:test-user',
      host: '*',
      operation: OPERATION.READ,
      permissionType: PERMISSION_TYPE.ALLOW
    }
  ]

  const writer = createRequest(creations)
  const reader = Reader.from(writer)

  // Read creations array
  const creationsArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readString()
    const resourcePatternType = reader.readInt8()
    const principal = reader.readString()
    const host = reader.readString()
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

  // Verify multiple creations
  deepStrictEqual(creationsArray.length, 3, 'Should correctly serialize multiple ACL creations')

  // Verify specific fields from different ACL creations
  deepStrictEqual(creationsArray[0].operation, OPERATION.READ, 'First ACL should have READ operation')

  deepStrictEqual(creationsArray[1].operation, OPERATION.WRITE, 'Second ACL should have WRITE operation')

  deepStrictEqual(creationsArray[2].resourceType, RESOURCE_TYPE.GROUP, 'Third ACL should have GROUP resource type')
})

test('createRequest serializes pattern type correctly', () => {
  const patternTypes = [PATTERN_TYPE.LITERAL, PATTERN_TYPE.PREFIXED]

  // Test each pattern type
  for (const patternType of patternTypes) {
    const creations = [
      {
        resourceType: RESOURCE_TYPE.TOPIC,
        resourceName: 'test-topic',
        resourcePatternType: patternType,
        principal: 'User:test-user',
        host: '*',
        operation: OPERATION.READ,
        permissionType: PERMISSION_TYPE.ALLOW
      }
    ]

    const writer = createRequest(creations)
    const reader = Reader.from(writer)

    // Read creation and verify pattern type
    const creation = reader.readArray(() => {
      const resourceType = reader.readInt8()
      const resourceName = reader.readString()
      const resourcePatternType = reader.readInt8()
      const principal = reader.readString()
      const host = reader.readString()
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
    })[0]

    deepStrictEqual(
      creation.resourcePatternType,
      patternType,
      `Pattern type ${patternType} should be serialized correctly`
    )
  }
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 30, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: [
        {
          errorCode: 0,
          errorMessage: null
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly processes multiple results', () => {
  // Create a response with multiple results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null
        },
        {
          errorCode: 0,
          errorMessage: null
        },
        {
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 30, 3, Reader.from(writer))

  // Verify multiple results
  deepStrictEqual(response.results.length, 3, 'Response should contain the correct number of results')
})

test('parseResponse handles error responses', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 37, // CLUSTER_AUTHORIZATION_FAILED
          errorMessage: 'Cluster authorization failed'
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 30, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response.results[0],
        {
          errorCode: 37,
          errorMessage: 'Cluster authorization failed'
        },
        'Error response should preserve the original error details'
      )

      return true
    }
  )
})

test('parseResponse handles mixed results with success and errors', () => {
  // Create a response with mixed results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null
        },
        {
          errorCode: 37, // CLUSTER_AUTHORIZATION_FAILED
          errorMessage: 'Cluster authorization failed'
        },
        {
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 30, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error results
      deepStrictEqual(
        err.response.results.map((r: Record<string, number>) => r.errorCode),
        [0, 37, 0],
        'Response should contain both successful and error results'
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
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 30, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeClientQuotasV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeClientQuotasV0

test('createRequest serializes basic parameters correctly', () => {
  const components = [
    {
      entityType: 'client-id',
      matchType: 0, // MATCH_EXACT
      match: 'test-client'
    }
  ]
  const strict = false

  const writer = createRequest(components, strict)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read components array
  const componentsArray = reader.readArray(() => {
    const entityType = reader.readString()
    const matchType = reader.readInt8()
    const match = reader.readNullableString()
    return { entityType, matchType, match }
  })

  // Read strict boolean
  const strictValue = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    {
      components: componentsArray,
      strict: strictValue
    },
    {
      components: [
        {
          entityType: 'client-id',
          matchType: 0,
          match: 'test-client'
        }
      ],
      strict: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple components correctly', () => {
  const components = [
    {
      entityType: 'client-id',
      matchType: 0, // MATCH_EXACT
      match: 'test-client'
    },
    {
      entityType: 'user',
      matchType: 0, // MATCH_EXACT
      match: 'test-user'
    }
  ]
  const strict = false

  const writer = createRequest(components, strict)
  const reader = Reader.from(writer)

  // Read components array
  const componentsArray = reader.readArray(() => {
    const entityType = reader.readString()
    const matchType = reader.readInt8()
    const match = reader.readNullableString()
    return { entityType, matchType, match }
  })

  // Verify multiple components structure
  deepStrictEqual(
    componentsArray,
    [
      {
        entityType: 'client-id',
        matchType: 0,
        match: 'test-client'
      },
      {
        entityType: 'user',
        matchType: 0,
        match: 'test-user'
      }
    ],
    'Should correctly serialize multiple components'
  )
})

test('createRequest serializes different match types correctly', () => {
  const components = [
    {
      entityType: 'client-id',
      matchType: 0, // MATCH_EXACT
      match: 'test-client'
    },
    {
      entityType: 'user',
      matchType: 1, // MATCH_DEFAULT
      match: null
    },
    {
      entityType: 'ip',
      matchType: 2, // MATCH_ANY
      match: null
    }
  ]
  const strict = false

  // @ts-ignore - ip actually not valid
  const writer = createRequest(components, strict)
  const reader = Reader.from(writer)

  // Read components array
  const componentsArray = reader.readArray(() => {
    const entityType = reader.readString()
    const matchType = reader.readInt8()
    const match = reader.readNullableString()
    return { entityType, matchType, match }
  })

  // Verify match types are serialized correctly
  deepStrictEqual(
    componentsArray.map(c => ({ entityType: c.entityType, matchType: c.matchType, match: c.match })),
    [
      {
        entityType: 'client-id',
        matchType: 0,
        match: 'test-client'
      },
      {
        entityType: 'user',
        matchType: 1,
        match: null
      },
      {
        entityType: 'ip',
        matchType: 2,
        match: null
      }
    ],
    'Different match types should be serialized correctly'
  )
})

test('createRequest serializes strict flag correctly', () => {
  const components = [
    {
      entityType: 'client-id',
      matchType: 0,
      match: 'test-client'
    }
  ]
  const strict = true

  const writer = createRequest(components, strict)
  const reader = Reader.from(writer)

  // Skip components array - already tested
  reader.readArray(() => {
    reader.readString()
    reader.readInt8()
    reader.readNullableString()
    return {}
  })

  // Read strict boolean
  const strictValue = reader.readBoolean()

  // Verify strict flag is set to true
  ok(strictValue === true, 'strict flag should be set to true')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          entity: [
            {
              entityType: 'client-id',
              entityName: 'test-client'
            }
          ],
          values: [
            {
              key: 'consumer_byte_rate',
              value: 1024000
            }
          ]
        }
      ],
      (w, entry) => {
        w.appendArray(entry.entity, (w, e) => {
          w.appendString(e.entityType).appendString(e.entityName)
        }).appendArray(entry.values, (w, v) => {
          w.appendString(v.key).appendFloat64(v.value)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 48, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      entries: [
        {
          entity: [
            {
              entityType: 'client-id',
              entityName: 'test-client'
            }
          ],
          values: [
            {
              key: 'consumer_byte_rate',
              value: 1024000
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly handles multiple entries and values', () => {
  // Create a response with multiple entries and values
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          entity: [
            {
              entityType: 'client-id',
              entityName: 'client-1'
            }
          ],
          values: [
            {
              key: 'consumer_byte_rate',
              value: 1024000
            }
          ]
        },
        {
          entity: [
            {
              entityType: 'user',
              entityName: 'user-1'
            },
            {
              entityType: 'client-id',
              entityName: 'client-2'
            }
          ],
          values: [
            {
              key: 'consumer_byte_rate',
              value: 2048000
            },
            {
              key: 'producer_byte_rate',
              value: 1024000
            }
          ]
        }
      ],
      (w, entry) => {
        w.appendArray(entry.entity, (w, e) => {
          w.appendString(e.entityType).appendString(e.entityName)
        }).appendArray(entry.values, (w, v) => {
          w.appendString(v.key).appendFloat64(v.value)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 48, 1, Reader.from(writer))

  // Verify entries count
  deepStrictEqual(response.entries.length, 2, 'Should parse multiple entries correctly')

  // Verify entity count in second entry
  deepStrictEqual(response.entries[1].entity.length, 2, 'Should parse multiple entities correctly')

  // Verify values count in second entry
  deepStrictEqual(response.entries[1].values.length, 2, 'Should parse multiple values correctly')

  // Verify a specific entity
  deepStrictEqual(
    response.entries[1].entity[0],
    {
      entityType: 'user',
      entityName: 'user-1'
    },
    'Should parse entity data correctly'
  )

  // Verify a specific value
  deepStrictEqual(
    response.entries[1].values[1],
    {
      key: 'producer_byte_rate',
      value: 1024000
    },
    'Should parse value data correctly'
  )
})

test('parseResponse throws ResponseError on error response', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(45) // errorCode INVALID_REQUEST
    .appendString('Invalid request') // errorMessage
    .appendArray([], () => {}) // Empty entries array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 48, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response
      deepStrictEqual(err.response.errorCode, 45, 'Error code should be preserved in the response')
      deepStrictEqual(err.response.errorMessage, 'Invalid request', 'Error message should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty entries array
    .appendTaggedFields()

  const response = parseResponse(1, 48, 1, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

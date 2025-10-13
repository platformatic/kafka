import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, alterClientQuotasV1 } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterClientQuotasV1

test('createRequest serializes basic parameters correctly', () => {
  const validateOnly = false

  const writer = createRequest(
    [
      {
        entities: [
          {
            entityType: 'client-id',
            entityName: 'test-client'
          }
        ],
        ops: [
          {
            key: 'consumer_byte_rate',
            value: 1024000,
            remove: false
          }
        ]
      }
    ],
    validateOnly
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read entries array
  const entriesArray = reader.readArray(() => {
    // Read entities array
    const entities = reader.readArray(() => {
      const entityType = reader.readString()
      const entityName = reader.readNullableString()
      return { entityType, entityName }
    })

    // Read ops array
    const ops = reader.readArray(() => {
      const key = reader.readString()
      const value = reader.readFloat64()
      const remove = reader.readBoolean()
      return { key, value, remove }
    })

    return { entities, ops }
  })

  // Read validateOnly boolean
  const validateOnlyValue = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    {
      entries: entriesArray,
      validateOnly: validateOnlyValue
    },
    {
      entries: [
        {
          entities: [
            {
              entityType: 'client-id',
              entityName: 'test-client'
            }
          ],
          ops: [
            {
              key: 'consumer_byte_rate',
              value: 1024000,
              remove: false
            }
          ]
        }
      ],
      validateOnly: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple entities correctly', () => {
  const validateOnly = false

  const writer = createRequest(
    [
      {
        entities: [
          {
            entityType: 'client-id',
            entityName: 'test-client'
          },
          {
            entityType: 'user',
            entityName: 'test-user'
          }
        ],
        ops: [
          {
            key: 'consumer_byte_rate',
            value: 1024000,
            remove: false
          }
        ]
      }
    ],
    validateOnly
  )
  const reader = Reader.from(writer)

  // Read and verify entries
  const entriesArray = reader.readArray(() => {
    const entities = reader.readArray(() => {
      const entityType = reader.readString()
      const entityName = reader.readNullableString()
      return { entityType, entityName }
    })

    const ops = reader.readArray(() => {
      const key = reader.readString()
      const value = reader.readFloat64()
      const remove = reader.readBoolean()
      return { key, value, remove }
    })

    return { entities, ops }
  })

  // Read validateOnly
  const validateOnlyValue = reader.readBoolean()

  // Verify structure with multiple entities
  deepStrictEqual(
    entriesArray[0].entities,
    [
      {
        entityType: 'client-id',
        entityName: 'test-client'
      },
      {
        entityType: 'user',
        entityName: 'test-user'
      }
    ],
    'Should correctly serialize multiple entities'
  )

  // Verify validate_only flag
  deepStrictEqual(validateOnlyValue, false, 'validateOnly flag should be serialized correctly')
})

test('createRequest serializes multiple operations correctly', () => {
  const validateOnly = false

  const writer = createRequest(
    [
      {
        entities: [
          {
            entityType: 'client-id',
            entityName: 'test-client'
          }
        ],
        ops: [
          {
            key: 'consumer_byte_rate',
            value: 1024000,
            remove: false
          },
          {
            key: 'producer_byte_rate',
            value: 2048000,
            remove: false
          },
          {
            key: 'request_percentage',
            remove: true
          }
        ]
      }
    ],
    validateOnly
  )
  const reader = Reader.from(writer)

  // Read entries
  const entriesArray = reader.readArray(() => {
    // Skip entities, already tested
    const entities = reader.readArray(() => {
      reader.readString()
      reader.readNullableString()
      return {}
    })

    // Read ops array to verify multiple operations
    const ops = reader.readArray(() => {
      const key = reader.readString()
      const value = reader.readFloat64()
      const remove = reader.readBoolean()
      return { key, value, remove }
    })

    return { entities, ops }
  })

  // Verify operations structure
  deepStrictEqual(
    entriesArray[0].ops,
    [
      {
        key: 'consumer_byte_rate',
        value: 1024000,
        remove: false
      },
      {
        key: 'producer_byte_rate',
        value: 2048000,
        remove: false
      },
      {
        key: 'request_percentage',
        value: 0,
        remove: true
      }
    ],
    'Should correctly serialize multiple operations with different remove flags'
  )
})

test('createRequest serializes validateOnly flag correctly', () => {
  const validateOnly = true

  const writer = createRequest(
    [
      {
        entities: [
          {
            entityType: 'client-id',
            entityName: 'test-client'
          }
        ],
        ops: [
          {
            key: 'consumer_byte_rate',
            value: 1024000,
            remove: false
          }
        ]
      }
    ],
    validateOnly
  )
  const reader = Reader.from(writer)

  // Skip entries - already tested
  reader.readArray(() => {
    reader.readArray(() => {
      reader.readString()
      reader.readNullableString()
      return {}
    })
    reader.readArray(() => {
      reader.readString()
      reader.readFloat64()
      reader.readBoolean()
      return {}
    })
    return {}
  })

  // Read validateOnly boolean
  const validateOnlyValue = reader.readBoolean()

  // Verify validateOnly flag is set to true
  ok(validateOnlyValue === true, 'validateOnly flag should be set to true')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          entity: [
            {
              entityType: 'client-id',
              entityName: 'test-client'
            }
          ]
        }
      ],
      (w, entry) => {
        w.appendInt16(entry.errorCode)
          .appendString(entry.errorMessage)
          .appendArray(entry.entity, (w, entity) => {
            w.appendString(entity.entityType).appendString(entity.entityName)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 49, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      entries: [
        {
          errorCode: 0,
          errorMessage: null,
          entity: [
            {
              entityType: 'client-id',
              entityName: 'test-client'
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly handles error responses', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 45, // INVALID_REQUEST
          errorMessage: 'Invalid request',
          entity: [
            {
              entityType: 'client-id',
              entityName: 'test-client'
            }
          ]
        }
      ],
      (w, entry) => {
        w.appendInt16(entry.errorCode)
          .appendString(entry.errorMessage)
          .appendArray(entry.entity, (w, entity) => {
            w.appendString(entity.entityType).appendString(entity.entityName)
          })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 49, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          entries: [
            {
              errorCode: 45,
              errorMessage: 'Invalid request',
              entity: [
                {
                  entityType: 'client-id',
                  entityName: 'test-client'
                }
              ]
            }
          ]
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles multiple entries with mixed errors', () => {
  // Create a response with multiple entries and mixed errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          entity: [
            {
              entityType: 'client-id',
              entityName: 'success-client'
            }
          ]
        },
        {
          errorCode: 45, // INVALID_REQUEST
          errorMessage: 'Invalid request',
          entity: [
            {
              entityType: 'client-id',
              entityName: 'error-client'
            }
          ]
        }
      ],
      (w, entry) => {
        w.appendInt16(entry.errorCode)
          .appendString(entry.errorMessage)
          .appendArray(entry.entity, (w, entity) => {
            w.appendString(entity.entityType).appendString(entity.entityName)
          })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 49, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response contains both entries
      deepStrictEqual(err.response.entries.length, 2, 'Response should contain both entries')

      // Verify successful entry is preserved correctly
      deepStrictEqual(
        err.response.entries[0],
        {
          errorCode: 0,
          errorMessage: null,
          entity: [
            {
              entityType: 'client-id',
              entityName: 'success-client'
            }
          ]
        },
        'Successful entry should be preserved correctly'
      )

      // Verify error entry has correct error code
      deepStrictEqual(err.response.entries[1].errorCode, 45, 'Error entry should have correct error code')

      return true
    }
  )
})

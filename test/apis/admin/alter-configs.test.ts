import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { alterConfigsV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterConfigsV2

test('createRequest serializes basic parameters correctly', () => {
  const resources = [
    {
      resourceType: 2, // Topic
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        }
      ]
    }
  ]
  const validateOnly = false

  const writer = createRequest(resources, validateOnly)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readString()

    // Read configs array
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const value = reader.readNullableString()
      return { name, value }
    })

    return { resourceType, resourceName, configs }
  })

  // Read validateOnly boolean
  const validateOnlyValue = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    {
      resources: resourcesArray,
      validateOnly: validateOnlyValue
    },
    {
      resources: [
        {
          resourceType: 2,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              value: 'compact'
            }
          ]
        }
      ],
      validateOnly: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple resources correctly', () => {
  const resources = [
    {
      resourceType: 2, // Topic
      resourceName: 'test-topic-1',
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        }
      ]
    },
    {
      resourceType: 2, // Topic
      resourceName: 'test-topic-2',
      configs: [
        {
          name: 'retention.ms',
          value: '86400000'
        }
      ]
    }
  ]
  const validateOnly = false

  const writer = createRequest(resources, validateOnly)
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readString()

    // Read configs array
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const value = reader.readNullableString()
      return { name, value }
    })

    return { resourceType, resourceName, configs }
  })

  // Skip validateOnly
  reader.readBoolean()

  // Verify structure with multiple resources
  deepStrictEqual(
    resourcesArray,
    [
      {
        resourceType: 2,
        resourceName: 'test-topic-1',
        configs: [
          {
            name: 'cleanup.policy',
            value: 'compact'
          }
        ]
      },
      {
        resourceType: 2,
        resourceName: 'test-topic-2',
        configs: [
          {
            name: 'retention.ms',
            value: '86400000'
          }
        ]
      }
    ],
    'Should correctly serialize multiple resources'
  )
})

test('createRequest serializes multiple configs correctly', () => {
  const resources = [
    {
      resourceType: 2, // Topic
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        },
        {
          name: 'retention.ms',
          value: '86400000'
        },
        {
          name: 'min.insync.replicas',
          value: '2'
        }
      ]
    }
  ]
  const validateOnly = false

  const writer = createRequest(resources, validateOnly)
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readString()

    // Read configs array
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const value = reader.readNullableString()
      return { name, value }
    })

    return { resourceType, resourceName, configs }
  })

  // Verify multiple configs structure
  deepStrictEqual(
    resourcesArray[0].configs,
    [
      {
        name: 'cleanup.policy',
        value: 'compact'
      },
      {
        name: 'retention.ms',
        value: '86400000'
      },
      {
        name: 'min.insync.replicas',
        value: '2'
      }
    ],
    'Should correctly serialize multiple configs'
  )
})

test('createRequest serializes null config values correctly', () => {
  const resources = [
    {
      resourceType: 2, // Topic
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          value: null
        }
      ]
    }
  ]
  const validateOnly = false

  const writer = createRequest(resources, validateOnly)
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(() => {
    const resourceType = reader.readInt8()
    const resourceName = reader.readString()

    // Read configs array
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const value = reader.readNullableString()
      return { name, value }
    })

    return { resourceType, resourceName, configs }
  })

  // Verify null value is correctly serialized
  deepStrictEqual(resourcesArray[0].configs[0].value, null, 'Should correctly serialize null config value')
})

test('createRequest serializes validateOnly flag correctly', () => {
  const resources = [
    {
      resourceType: 2, // Topic
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        }
      ]
    }
  ]
  const validateOnly = true

  const writer = createRequest(resources, validateOnly)
  const reader = Reader.from(writer)

  // Skip resources array
  reader.readArray(() => {
    reader.readInt8()
    reader.readString()
    reader.readArray(() => {
      reader.readString()
      reader.readNullableString()
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
          resourceType: 2, // Topic
          resourceName: 'test-topic'
        }
      ],
      (w, response) => {
        w.appendInt16(response.errorCode)
          .appendString(response.errorMessage)
          .appendInt8(response.resourceType)
          .appendString(response.resourceName)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 33, 2, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      responses: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'test-topic'
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse handles error responses', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 44, // INVALID_CONFIG
          errorMessage: 'Invalid config value',
          resourceType: 2, // Topic
          resourceName: 'test-topic'
        }
      ],
      (w, response) => {
        w.appendInt16(response.errorCode)
          .appendString(response.errorMessage)
          .appendInt8(response.resourceType)
          .appendString(response.resourceName)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 33, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          responses: [
            {
              errorCode: 44,
              errorMessage: 'Invalid config value',
              resourceType: 2,
              resourceName: 'test-topic'
            }
          ]
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles multiple responses with mixed errors', () => {
  // Create a response with multiple entries and mixed errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2, // Topic
          resourceName: 'success-topic'
        },
        {
          errorCode: 44, // INVALID_CONFIG
          errorMessage: 'Invalid config value',
          resourceType: 2, // Topic
          resourceName: 'error-topic'
        }
      ],
      (w, response) => {
        w.appendInt16(response.errorCode)
          .appendString(response.errorMessage)
          .appendInt8(response.resourceType)
          .appendString(response.resourceName)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 33, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response contains both entries
      deepStrictEqual(err.response.responses.length, 2, 'Response should contain both entries')

      // Verify successful entry is preserved correctly
      deepStrictEqual(
        err.response.responses[0],
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'success-topic'
        },
        'Successful entry should be preserved correctly'
      )

      // Verify error entry has correct error code
      deepStrictEqual(err.response.responses[1].errorCode, 44, 'Error entry should have correct error code')

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
          resourceType: 2,
          resourceName: 'test-topic'
        }
      ],
      (w, response) => {
        w.appendInt16(response.errorCode)
          .appendString(response.errorMessage)
          .appendInt8(response.resourceType)
          .appendString(response.resourceName)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 33, 2, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

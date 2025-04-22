import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, incrementalAlterConfigsV1 } from '../../../src/index.ts'

const { createRequest, parseResponse } = incrementalAlterConfigsV1

test('createRequest serializes basic parameters correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
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
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const configOperation = reader.readInt8()
      const value = reader.readNullableString()
      return { name, configOperation, value }
    })
    return { resourceType, resourceName, configs }
  })

  // Read validateOnly flag
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
              configOperation: 0,
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
      resourceType: 2, // TOPIC
      resourceName: 'topic-1',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        }
      ]
    },
    {
      resourceType: 4, // BROKER
      resourceName: '1',
      configs: [
        {
          name: 'log.retention.hours',
          configOperation: 0, // SET
          value: '168'
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
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const configOperation = reader.readInt8()
      const value = reader.readNullableString()
      return { name, configOperation, value }
    })
    return { resourceType, resourceName, configs }
  })

  // Verify multiple resources
  deepStrictEqual(resourcesArray.length, 2, 'Should have two resources')

  deepStrictEqual(resourcesArray[0].resourceType, 2, 'First resource should be a TOPIC')

  deepStrictEqual(resourcesArray[1].resourceType, 4, 'Second resource should be a BROKER')
})

test('createRequest serializes multiple configs per resource correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        },
        {
          name: 'compression.type',
          configOperation: 0, // SET
          value: 'lz4'
        },
        {
          name: 'retention.ms',
          configOperation: 0, // SET
          value: '604800000'
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
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const configOperation = reader.readInt8()
      const value = reader.readNullableString()
      return { name, configOperation, value }
    })
    return { resourceType, resourceName, configs }
  })

  // Verify multiple configs
  deepStrictEqual(resourcesArray[0].configs.length, 3, 'Should have three configs for the resource')

  // Verify config names
  deepStrictEqual(
    resourcesArray[0].configs.map(c => c.name),
    ['cleanup.policy', 'compression.type', 'retention.ms'],
    'Config names should be serialized correctly'
  )
})

test('createRequest serializes different config operations correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
          value: 'compact'
        },
        {
          name: 'min.insync.replicas',
          configOperation: 1, // DELETE
          value: null
        },
        {
          name: 'retention.ms',
          configOperation: 2, // APPEND
          value: '604800000'
        },
        {
          name: 'min.cleanable.dirty.ratio',
          configOperation: 3, // SUBTRACT
          value: '0.5'
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
    const configs = reader.readArray(() => {
      const name = reader.readString()
      const configOperation = reader.readInt8()
      const value = reader.readNullableString()
      return { name, configOperation, value }
    })
    return { resourceType, resourceName, configs }
  })

  // Verify config operations
  deepStrictEqual(
    resourcesArray[0].configs.map(c => c.configOperation),
    [0, 1, 2, 3],
    'Config operations should be serialized correctly'
  )

  // Verify null value for DELETE operation
  deepStrictEqual(resourcesArray[0].configs[1].value, null, 'DELETE operation should have null value')
})

test('createRequest serializes validateOnly flag correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configs: [
        {
          name: 'cleanup.policy',
          configOperation: 0, // SET
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
      reader.readInt8()
      reader.readNullableString()
      return {}
    })
    return {}
  })

  // Read validateOnly flag
  const validateOnlyValue = reader.readBoolean()

  // Verify validateOnly flag
  deepStrictEqual(validateOnlyValue, true, 'validateOnly flag should be set to true')
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty responses array
    .appendTaggedFields()

  const response = parseResponse(1, 44, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      responses: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a successful response with results', () => {
  // Create a successful response with results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2, // TOPIC
          resourceName: 'test-topic'
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 44, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response.responses.length, 1, 'Should have one response result')

  const result = response.responses[0]
  deepStrictEqual(result.errorCode, 0, 'Error code should be 0 for success')
  deepStrictEqual(result.errorMessage, null, 'Error message should be null for success')
  deepStrictEqual(result.resourceType, 2, 'Resource type should be parsed correctly')
  deepStrictEqual(result.resourceName, 'test-topic', 'Resource name should be parsed correctly')
})

test('parseResponse correctly processes multiple results', () => {
  // Create a response with multiple results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2, // TOPIC
          resourceName: 'topic-1'
        },
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 4, // BROKER
          resourceName: '1'
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 44, 1, Reader.from(writer))

  // Verify multiple results
  deepStrictEqual(response.responses.length, 2, 'Should have two response results')

  // Verify resource types
  deepStrictEqual(
    response.responses.map(r => r.resourceType),
    [2, 4],
    'Resource types should be parsed correctly'
  )

  // Verify resource names
  deepStrictEqual(
    response.responses.map(r => r.resourceName),
    ['topic-1', '1'],
    'Resource names should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on error response', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 39, // INVALID_TOPIC_EXCEPTION
          errorMessage: 'Invalid topic',
          resourceType: 2, // TOPIC
          resourceName: 'bad-topic'
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 44, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.responses[0].errorCode, 39, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.responses[0].errorMessage,
        'Invalid topic',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse throws ResponseError with mixed success and error results', () => {
  // Create a response with mixed success and error results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0, // SUCCESS
          errorMessage: null,
          resourceType: 2, // TOPIC
          resourceName: 'good-topic'
        },
        {
          errorCode: 39, // INVALID_TOPIC_EXCEPTION
          errorMessage: 'Invalid topic',
          resourceType: 2, // TOPIC
          resourceName: 'bad-topic'
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 44, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify successful result is preserved
      deepStrictEqual(err.response.responses[0].errorCode, 0, 'Success error code should be preserved')

      // Verify error result is preserved
      deepStrictEqual(err.response.responses[1].errorCode, 39, 'Error code should be preserved')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendArray([], () => {}) // Empty responses array
    .appendTaggedFields()

  const response = parseResponse(1, 44, 1, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

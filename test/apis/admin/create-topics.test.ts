import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { createTopicsV7, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = createTopicsV7

test('createRequest serializes basic parameters correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      numPartitions: 3,
      replicationFactor: 2,
      assignments: [],
      configs: []
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read topics array (only one topic)
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const numPartitions = reader.readInt32()
    const replicationFactor = reader.readInt16()

    // Read assignments array (empty)
    const assignments = reader.readArray(() => {
      return {}
    })

    // Read configs array (empty)
    const configs = reader.readArray(() => {
      return {}
    })

    return {
      name,
      numPartitions,
      replicationFactor,
      assignmentsLength: assignments.length,
      configsLength: configs.length
    }
  })

  // Read remaining parameters
  const timeout = reader.readInt32()
  const validate = reader.readBoolean()

  // Verify all serialized data
  deepStrictEqual(
    {
      topics: topicsArray,
      timeout,
      validate
    },
    {
      topics: [
        {
          name: 'test-topic',
          numPartitions: 3,
          replicationFactor: 2,
          assignmentsLength: 0,
          configsLength: 0
        }
      ],
      timeout: 30000,
      validate: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes topic with assignments correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      numPartitions: 3,
      replicationFactor: 2,
      assignments: [
        {
          partitionIndex: 0,
          brokerIds: [1, 2, 3]
        },
        {
          partitionIndex: 1,
          brokerIds: [2, 3, 1]
        }
      ],
      configs: []
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const numPartitions = reader.readInt32()
    const replicationFactor = reader.readInt16()

    // Read assignments array
    const assignments = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const brokerIds = reader.readArray(() => reader.readInt32(), true, false)
      return { partitionIndex, brokerIds }
    })

    // Read configs array (empty)
    const configs = reader.readArray(() => {
      return {}
    })

    return { name, numPartitions, replicationFactor, assignments, configsLength: configs.length }
  })

  // Read remaining parameters
  const timeout = reader.readInt32()
  const validate = reader.readBoolean()

  // Verify complete data structure
  deepStrictEqual(
    {
      topics: topicsArray,
      timeout,
      validate
    },
    {
      topics: [
        {
          name: 'test-topic',
          numPartitions: 3,
          replicationFactor: 2,
          assignments: [
            {
              partitionIndex: 0,
              brokerIds: [1, 2, 3]
            },
            {
              partitionIndex: 1,
              brokerIds: [2, 3, 1]
            }
          ],
          configsLength: 0
        }
      ],
      timeout: 30000,
      validate: false
    },
    'Complete serialized data structure should match expected values'
  )
})

test('createRequest serializes topic with configs correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      numPartitions: 3,
      replicationFactor: 2,
      assignments: [],
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact'
        },
        {
          name: 'retention.ms',
          value: '86400000'
        }
      ]
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const numPartitions = reader.readInt32()
    const replicationFactor = reader.readInt16()

    // Read assignments array (empty)
    const assignments = reader.readArray(() => {
      return {}
    })

    // Read configs array
    const configs = reader.readArray(() => {
      const configName = reader.readString()
      const configValue = reader.readString()
      return { name: configName, value: configValue }
    })

    return { name, numPartitions, replicationFactor, assignmentsLength: assignments.length, configs }
  })

  // Read remaining parameters
  const timeout = reader.readInt32()
  const validate = reader.readBoolean()

  // Verify complete data structure
  deepStrictEqual(
    {
      topics: topicsArray,
      timeout,
      validate
    },
    {
      topics: [
        {
          name: 'test-topic',
          numPartitions: 3,
          replicationFactor: 2,
          assignmentsLength: 0,
          configs: [
            {
              name: 'cleanup.policy',
              value: 'compact'
            },
            {
              name: 'retention.ms',
              value: '86400000'
            }
          ]
        }
      ],
      timeout: 30000,
      validate: false
    },
    'Complete serialized data structure with configs should match expected values'
  )
})

test('createRequest serializes validate_only flag correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      numPartitions: 1,
      replicationFactor: 1,
      assignments: [],
      configs: []
    }
  ]
  const timeoutMs = 30000
  const validateOnly = true // Set to true to test this case

  const writer = createRequest(topics, timeoutMs, validateOnly)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const numPartitions = reader.readInt32()
    const replicationFactor = reader.readInt16()

    // Read assignments array (empty)
    const assignments = reader.readArray(() => {
      return {}
    })

    // Read configs array (empty)
    const configs = reader.readArray(() => {
      return {}
    })

    return {
      name,
      numPartitions,
      replicationFactor,
      assignmentsLength: assignments.length,
      configsLength: configs.length
    }
  })

  // Read remaining parameters
  const timeout = reader.readInt32()
  const validate = reader.readBoolean()

  // Verify complete data structure
  deepStrictEqual(
    {
      topics: topicsArray,
      timeout,
      validate
    },
    {
      topics: [
        {
          name: 'test-topic',
          numPartitions: 1,
          replicationFactor: 1,
          assignmentsLength: 0,
          configsLength: 0
        }
      ],
      timeout: 30000,
      validate: true
    },
    'Complete serialized data structure with validate_only=true should match expected values'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array
    .appendArray(
      [
        {
          name: 'test-topic',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: []
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendInt16(topic.errorCode)
          .appendString(topic.errorMessage)
          .appendInt32(topic.numPartitions)
          .appendInt16(topic.replicationFactor)
          .appendArray(topic.configs, () => {})
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 19, 7, writer.bufferList)

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      topics: [
        {
          name: 'test-topic',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: []
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly processes response with config data', () => {
  // Create a response with config data
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array
    .appendArray(
      [
        {
          name: 'test-topic',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: [
            {
              name: 'cleanup.policy',
              value: 'compact',
              readOnly: false,
              configSource: 1,
              isSensitive: false
            },
            {
              name: 'retention.ms',
              value: '86400000',
              readOnly: false,
              configSource: 1,
              isSensitive: false
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendInt16(topic.errorCode)
          .appendString(topic.errorMessage)
          .appendInt32(topic.numPartitions)
          .appendInt16(topic.replicationFactor)
          .appendArray(topic.configs, (w, config) => {
            w.appendString(config.name)
              .appendString(config.value)
              .appendBoolean(config.readOnly)
              .appendInt8(config.configSource)
              .appendBoolean(config.isSensitive)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 19, 7, writer.bufferList)

  // Verify configs in response
  deepStrictEqual(
    response.topics[0].configs,
    [
      {
        name: 'cleanup.policy',
        value: 'compact',
        readOnly: false,
        configSource: 1,
        isSensitive: false
      },
      {
        name: 'retention.ms',
        value: '86400000',
        readOnly: false,
        configSource: 1,
        isSensitive: false
      }
    ],
    'Config data in response should match expected values'
  )
})

test('parseResponse handles topic-level error', () => {
  // Create a response with a topic-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array with error
    .appendArray(
      [
        {
          name: 'test-topic',
          topicId: '00000000-0000-0000-0000-000000000000',
          errorCode: 7, // TOPIC_ALREADY_EXISTS
          errorMessage: 'Topic already exists',
          numPartitions: -1,
          replicationFactor: -1,
          configs: []
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendInt16(topic.errorCode)
          .appendString(topic.errorMessage)
          .appendInt32(topic.numPartitions)
          .appendInt16(topic.replicationFactor)
          .appendArray(topic.configs, () => {})
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 19, 7, writer.bufferList)
    },
    (err: any) => {
      // Group all error validation assertions
      deepStrictEqual(
        {
          isResponseError: err instanceof ResponseError,
          hasErrorMessage: err.message.includes('Received response with error while executing API'),
          hasErrorsObject: err.errors && typeof err.errors === 'object'
        },
        {
          isResponseError: true,
          hasErrorMessage: true,
          hasErrorsObject: true
        },
        'Error should be a ResponseError with the correct properties'
      )

      // Verify that the response data is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          topics: [
            {
              name: 'test-topic',
              topicId: '00000000-0000-0000-0000-000000000000',
              errorCode: 7,
              errorMessage: 'Topic already exists',
              numPartitions: -1,
              replicationFactor: -1,
              configs: []
            }
          ]
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles multiple topics with mixed errors', () => {
  // Create a response with multiple topics and mixed errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array with mixed results
    .appendArray(
      [
        {
          name: 'success-topic',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: []
        },
        {
          name: 'error-topic-1',
          topicId: '00000000-0000-0000-0000-000000000000',
          errorCode: 7, // TOPIC_ALREADY_EXISTS
          errorMessage: 'Topic already exists',
          numPartitions: -1,
          replicationFactor: -1,
          configs: []
        },
        {
          name: 'error-topic-2',
          topicId: '00000000-0000-0000-0000-000000000000',
          errorCode: 39, // INVALID_REPLICATION_FACTOR
          errorMessage: 'Invalid replication factor',
          numPartitions: -1,
          replicationFactor: -1,
          configs: []
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendInt16(topic.errorCode)
          .appendString(topic.errorMessage)
          .appendInt32(topic.numPartitions)
          .appendInt16(topic.replicationFactor)
          .appendArray(topic.configs, () => {})
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 19, 7, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error structure - errors array contains error objects for the failed topics
      ok(Array.isArray(err.errors) && err.errors.length === 2, 'Should have an array of 2 errors for the failed topics')

      // Check that all error responses in the response object have the correct error codes
      // Get the error codes and sort them to ensure consistent ordering
      const errorCodes = err.response.topics
        .filter((t: Record<string, number>) => t.errorCode !== 0)
        .map((t: Record<string, number>) => t.errorCode)

      // Verify we have both expected error codes
      ok(
        errorCodes.includes(7) && errorCodes.includes(39),
        'Response should contain topics with expected error codes (7 and 39)'
      )

      // Verify that the response contains all topics
      deepStrictEqual(err.response.topics.length, 3, 'Response should contain all 3 topics')

      // Check that the successful topic data is correctly preserved
      deepStrictEqual(
        err.response.topics[0],
        {
          name: 'success-topic',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: []
        },
        'Successful topic data should be preserved in the response'
      )

      return true
    }
  )
})

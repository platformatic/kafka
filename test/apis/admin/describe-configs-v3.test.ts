import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { type DescribeConfigsResponseResult } from '../../../src/apis/admin/describe-configs-v3.ts'
import { Reader, ResponseError, Writer, describeConfigsV3 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeConfigsV3

test('createRequest serializes basic parameters correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configurationKeys: []
    }
  ]
  const includeSynonyms = true
  const includeDocumentation = false

  const writer = createRequest(resources, includeSynonyms, includeDocumentation)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(
    () => {
      const resourceType = reader.readInt8()
      const resourceName = reader.readString(false)
      const configurationKeys = reader.readArray(() => reader.readString(false), false, false)
      return { resourceType, resourceName, configurationKeys }
    },
    false,
    false
  )

  // Read includeSynonyms boolean
  const includeSynonymsValue = reader.readBoolean()

  // Read includeDocumentation boolean
  const includeDocumentationValue = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    {
      resources: resourcesArray,
      includeSynonyms: includeSynonymsValue,
      includeDocumentation: includeDocumentationValue
    },
    {
      resources: [
        {
          resourceType: 2,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ],
      includeSynonyms: true,
      includeDocumentation: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes configurationKeys correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'test-topic',
      configurationKeys: ['cleanup.policy', 'compression.type']
    }
  ]
  const includeSynonyms = false
  const includeDocumentation = false

  const writer = createRequest(resources, includeSynonyms, includeDocumentation)
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(
    () => {
      const resourceType = reader.readInt8()
      const resourceName = reader.readString(false)
      const configurationKeys = reader.readArray(() => reader.readString(false), false, false)
      return { resourceType, resourceName, configurationKeys }
    },
    false,
    false
  )

  // Verify configuration keys
  deepStrictEqual(
    resourcesArray[0].configurationKeys,
    ['cleanup.policy', 'compression.type'],
    'Configuration keys should be serialized correctly'
  )
})

test('createRequest serializes multiple resources correctly', () => {
  const resources = [
    {
      resourceType: 2, // TOPIC
      resourceName: 'topic-1',
      configurationKeys: ['cleanup.policy']
    },
    {
      resourceType: 4, // BROKER
      resourceName: '1',
      configurationKeys: ['num.io.threads']
    }
  ]
  const includeSynonyms = false
  const includeDocumentation = false

  const writer = createRequest(resources, includeSynonyms, includeDocumentation)
  const reader = Reader.from(writer)

  // Read resources array
  const resourcesArray = reader.readArray(
    () => {
      const resourceType = reader.readInt8()
      const resourceName = reader.readString(false)
      const configurationKeys = reader.readArray(() => reader.readString(false), false, false)
      return { resourceType, resourceName, configurationKeys }
    },
    false,
    false
  )

  // Verify multiple resources
  deepStrictEqual(
    resourcesArray,
    [
      {
        resourceType: 2,
        resourceName: 'topic-1',
        configurationKeys: ['cleanup.policy']
      },
      {
        resourceType: 4,
        resourceName: '1',
        configurationKeys: ['num.io.threads']
      }
    ],
    'Multiple resources should be serialized correctly'
  )
})

test('createRequest serializes include flags correctly', () => {
  const resources = [
    {
      resourceType: 2,
      resourceName: 'test-topic',
      configurationKeys: []
    }
  ]
  const includeSynonyms = true
  const includeDocumentation = true

  const writer = createRequest(resources, includeSynonyms, includeDocumentation)
  const reader = Reader.from(writer)

  // Skip resources array
  reader.readArray(
    () => {
      reader.readInt8()
      reader.readString(false)
      reader.readArray(() => reader.readString(false), false, false)
      return {}
    },
    false,
    false
  )

  // Read include flags
  const includeSynonymsValue = reader.readBoolean()
  const includeDocumentationValue = reader.readBoolean()

  // Verify flags
  ok(includeSynonymsValue === true, 'includeSynonyms flag should be set to true')
  ok(includeDocumentationValue === true, 'includeDocumentation flag should be set to true')
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}, false, false) // Empty results array

  const response = parseResponse(1, 32, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a successful response with configs', () => {
  // Create a successful response with a basic config
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2, // TOPIC
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              value: 'delete',
              readOnly: false,
              configSource: 1, // DYNAMIC_TOPIC_CONFIG
              isSensitive: false,
              synonyms: [
                {
                  name: 'cleanup.policy',
                  value: 'delete',
                  source: 1 // DYNAMIC_TOPIC_CONFIG
                }
              ],
              configType: 1, // STRING
              documentation: 'The cleanup policy for log segments'
            }
          ]
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage, false)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName, false)
          .appendArray(
            result.configs,
            (w, config) => {
              w.appendString(config.name, false)
                .appendString(config.value, false)
                .appendBoolean(config.readOnly)
                .appendInt8(config.configSource)
                .appendBoolean(config.isSensitive)
                .appendArray(
                  config.synonyms,
                  (w, synonym) => {
                    w.appendString(synonym.name, false).appendString(synonym.value, false).appendInt8(synonym.source)
                  },
                  false,
                  false
                )
                .appendInt8(config.configType)
                .appendString(config.documentation, false)
            },
            false,
            false
          )
      },
      false,
      false
    )

  const response = parseResponse(1, 32, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response.results[0].resourceType, 2, 'Resource type should be parsed correctly')

  deepStrictEqual(response.results[0].resourceName, 'test-topic', 'Resource name should be parsed correctly')

  deepStrictEqual(response.results[0].configs[0].name, 'cleanup.policy', 'Config name should be parsed correctly')

  deepStrictEqual(response.results[0].configs[0].value, 'delete', 'Config value should be parsed correctly')

  deepStrictEqual(
    response.results[0].configs[0].synonyms[0],
    {
      name: 'cleanup.policy',
      value: 'delete',
      source: 1
    },
    'Synonyms should be parsed correctly'
  )

  deepStrictEqual(
    response.results[0].configs[0].documentation,
    'The cleanup policy for log segments',
    'Documentation should be parsed correctly'
  )
})

test('parseResponse correctly processes multiple config entries', () => {
  // Create a response with multiple configs
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              value: 'delete',
              readOnly: false,
              configSource: 1,
              isSensitive: false,
              synonyms: [],
              configType: 1,
              documentation: 'The cleanup policy'
            },
            {
              name: 'compression.type',
              value: 'producer',
              readOnly: false,
              configSource: 1,
              isSensitive: false,
              synonyms: [],
              configType: 1,
              documentation: 'The compression type'
            }
          ]
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage, false)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName, false)
          .appendArray(
            result.configs,
            (w, config) => {
              w.appendString(config.name, false)
                .appendString(config.value, false)
                .appendBoolean(config.readOnly)
                .appendInt8(config.configSource)
                .appendBoolean(config.isSensitive)
                .appendArray(config.synonyms, () => {}, false, false)
                .appendInt8(config.configType)
                .appendString(config.documentation, false)
            },
            false,
            false
          )
      },
      false,
      false
    )

  const response = parseResponse(1, 32, 3, Reader.from(writer))

  // Verify multiple configs
  deepStrictEqual(response.results[0].configs.length, 2, 'Should parse multiple configs correctly')

  deepStrictEqual(
    response.results[0].configs.map(c => c.name),
    ['cleanup.policy', 'compression.type'],
    'Should parse config names correctly'
  )
})

test('parseResponse throws ResponseError on error response', () => {
  // Create a response with an error at resource level
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 39, // INVALID_TOPIC_EXCEPTION
          errorMessage: 'Invalid topic',
          resourceType: 2,
          resourceName: 'invalid-topic',
          configs: []
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage, false)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName, false)
          .appendArray([], () => {}, false, false) // Empty configs
      },
      false,
      false
    )

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 32, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.results[0].errorCode, 39, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.results[0].errorMessage,
        'Invalid topic',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles multiple resources with mixed errors', () => {
  // Create a response with multiple resources and mixed errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0, // Success
          errorMessage: null,
          resourceType: 2,
          resourceName: 'good-topic',
          configs: [
            {
              name: 'cleanup.policy',
              value: 'delete',
              readOnly: false,
              configSource: 1,
              isSensitive: false,
              synonyms: [],
              configType: 1,
              documentation: null
            }
          ]
        },
        {
          errorCode: 39, // INVALID_TOPIC_EXCEPTION
          errorMessage: 'Invalid topic',
          resourceType: 2,
          resourceName: 'bad-topic',
          configs: []
        }
      ],
      (w, result: DescribeConfigsResponseResult) => {
        w.appendInt16(result.errorCode)
          .appendString(result.errorMessage, false)
          .appendInt8(result.resourceType)
          .appendString(result.resourceName, false)
          .appendArray(
            result.configs,
            (w, config) => {
              if (config) {
                w.appendString(config.name, false)
                  .appendString(config.value, false)
                  .appendBoolean(config.readOnly)
                  .appendInt8(config.configSource)
                  .appendBoolean(config.isSensitive)
                  .appendArray([], () => {}, false, false) // Empty synonyms
                  .appendInt8(config.configType)
                  .appendString(config.documentation, false)
              }
            },
            false,
            false
          )
      },
      false,
      false
    )

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 32, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error resources
      deepStrictEqual(
        err.response.results.map((r: any) => ({ resourceName: r.resourceName, errorCode: r.errorCode })),
        [
          { resourceName: 'good-topic', errorCode: 0 },
          { resourceName: 'bad-topic', errorCode: 39 }
        ],
        'Response should contain both successful and error resources'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendArray([], () => {}, false, false) // Empty results

  const response = parseResponse(1, 32, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

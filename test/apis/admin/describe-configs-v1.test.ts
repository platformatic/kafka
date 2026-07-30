import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeConfigsV1 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeConfigsV1

test('createRequest defaults optional filters to false', () => {
  const reader = Reader.from(createRequest([]))
  deepStrictEqual(reader.readArray(() => undefined, false, false), [])
  ok(!reader.readBoolean())
  ok(reader.remaining === 0)
})

test('createRequest serializes v1 resources, nullable configurationKeys, and includeSynonyms', () => {
  const writer = createRequest(
    [
      { resourceType: 2, resourceName: 'test-topic', configurationKeys: ['cleanup.policy'] },
      { resourceType: 4, resourceName: '1', configurationKeys: null }
    ],
    true,
    true
  )
  const reader = Reader.from(writer)
  const resources = reader.readArray(
    r => ({
      resourceType: r.readInt8(),
      resourceName: r.readString(false),
      configurationKeys: r.readNullableArray(() => r.readString(false), false, false)
    }),
    false,
    false
  )

  deepStrictEqual(resources, [
    { resourceType: 2, resourceName: 'test-topic', configurationKeys: ['cleanup.policy'] },
    { resourceType: 4, resourceName: '1', configurationKeys: null }
  ])
  ok(reader.readBoolean())
  ok(reader.remaining === 0)
})

test('parseResponse parses synonyms and normalizes unavailable fields', () => {
  const writer = Writer.create().appendInt32(10).appendArray(
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
            synonyms: [{ name: 'cleanup.policy', value: 'delete', source: 1 }]
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
                (w, synonym) => w.appendString(synonym.name, false).appendString(synonym.value, false).appendInt8(synonym.source),
                false,
                false
              )
          },
          false,
          false
        )
    },
    false,
    false
  )

  deepStrictEqual(parseResponse(1, 32, 1, Reader.from(writer)), {
    throttleTimeMs: 10,
    results: [
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
            synonyms: [{ name: 'cleanup.policy', value: 'delete', source: 1 }],
            configType: 0,
            documentation: null
          }
        ]
      }
    ]
  })
})

test('parseResponse throws ResponseError for resource errors', () => {
  const writer = Writer.create().appendInt32(0).appendArray(
    [{ errorCode: 39, errorMessage: 'Invalid topic', resourceType: 2, resourceName: 'invalid-topic' }],
    (w, result) =>
      w
        .appendInt16(result.errorCode)
        .appendString(result.errorMessage, false)
        .appendInt8(result.resourceType)
        .appendString(result.resourceName, false)
        .appendArray([], () => {}, false, false),
    false,
    false
  )

  throws(() => parseResponse(1, 32, 1, Reader.from(writer)), ResponseError)
})

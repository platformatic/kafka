import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { createTopicsV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = createTopicsV5

test('CreateTopics v5 serializes flexible requests', () => {
  const writer = createRequest(
    [
      {
        name: 'topic',
        numPartitions: 3,
        replicationFactor: 2,
        assignments: [{ partitionIndex: 0, brokerIds: [1, 2] }],
        configs: [{ name: 'cleanup.policy', value: 'compact' }]
      }
    ],
    30000,
    true
  )
  const reader = Reader.from(writer)
  const topics = reader.readArray(r => ({
    name: r.readString(),
    numPartitions: r.readInt32(),
    replicationFactor: r.readInt16(),
    assignments: r.readArray(r => ({
      partitionIndex: r.readInt32(),
      brokerIds: r.readArray(r => r.readInt32(), true, false)
    })),
    configs: r.readArray(r => ({ name: r.readString(), value: r.readNullableString() }))
  }))

  deepStrictEqual(
    { topics, timeoutMs: reader.readInt32(), validateOnly: reader.readBoolean() },
    {
      topics: [
        {
          name: 'topic',
          numPartitions: 3,
          replicationFactor: 2,
          assignments: [{ partitionIndex: 0, brokerIds: [1, 2] }],
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ],
      timeoutMs: 30000,
      validateOnly: true
    }
  )
  ok(writer instanceof Writer)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 19, version: 5 })
})

test('CreateTopics v5 parses flexible responses', () => {
  const writer = Writer.create()
    .appendInt32(10)
    .appendArray(
      [
        {
          name: 'topic',
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 2,
          configs: [{ name: 'cleanup.policy', value: 'compact', readOnly: false, configSource: 1, isSensitive: false }]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
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

  deepStrictEqual(parseResponse(1, 19, 5, Reader.from(writer)), {
    throttleTimeMs: 10,
    topics: [
      {
        name: 'topic',
        topicId: '00000000-0000-0000-0000-000000000000',
        errorCode: 0,
        errorMessage: null,
        numPartitions: 3,
        replicationFactor: 2,
        configs: [{ name: 'cleanup.policy', value: 'compact', readOnly: false, configSource: 1, isSensitive: false }]
      }
    ]
  })
})

test('CreateTopics v5 preserves normalized topics in errors', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ name: 'topic', errorCode: 37, errorMessage: 'Invalid partitions', numPartitions: -1, replicationFactor: -1 }],
      (w, topic) => {
        w.appendString(topic.name)
          .appendInt16(topic.errorCode)
          .appendString(topic.errorMessage)
          .appendInt32(topic.numPartitions)
          .appendInt16(topic.replicationFactor)
          .appendArray([], () => {})
      }
    )
    .appendTaggedFields()

  throws(
    () => parseResponse(1, 19, 5, Reader.from(writer)),
    error => {
      ok(error instanceof ResponseError)
      deepStrictEqual(error.response, {
        throttleTimeMs: 0,
        topics: [
          {
            name: 'topic',
            topicId: '00000000-0000-0000-0000-000000000000',
            errorCode: 37,
            errorMessage: 'Invalid partitions',
            numPartitions: -1,
            replicationFactor: -1,
            configs: []
          }
        ]
      })
      return true
    }
  )
})

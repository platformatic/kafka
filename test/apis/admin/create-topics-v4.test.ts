import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { createTopicsV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = createTopicsV4

test('CreateTopics v4 serializes classic requests', () => {
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
  const topics = reader.readArray(
    r => ({
      name: r.readString(false),
      numPartitions: r.readInt32(),
      replicationFactor: r.readInt16(),
      assignments: r.readArray(
        r => ({ partitionIndex: r.readInt32(), brokerIds: r.readArray(r => r.readInt32(), false, false) }),
        false,
        false
      ),
      configs: r.readArray(r => ({ name: r.readString(false), value: r.readNullableString(false) }), false, false)
    }),
    false,
    false
  )

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
  deepStrictEqual({ key: api.key, version: api.version }, { key: 19, version: 4 })
})

test('CreateTopics v4 normalizes fields absent from classic responses', () => {
  const writer = Writer.create()
    .appendInt32(10)
    .appendArray(
      [{ name: 'topic', errorCode: 0, errorMessage: null }],
      (w, topic) => {
        w.appendString(topic.name, false).appendInt16(topic.errorCode).appendString(topic.errorMessage, false)
      },
      false,
      false
    )

  deepStrictEqual(parseResponse(1, 19, 4, Reader.from(writer)), {
    throttleTimeMs: 10,
    topics: [
      {
        name: 'topic',
        topicId: '00000000-0000-0000-0000-000000000000',
        errorCode: 0,
        errorMessage: null,
        numPartitions: -1,
        replicationFactor: -1,
        configs: null
      }
    ]
  })
})

test('CreateTopics v4 preserves normalized topics in errors', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ name: 'topic', errorCode: 37, errorMessage: 'Invalid partitions' }],
      (w, topic) => {
        w.appendString(topic.name, false).appendInt16(topic.errorCode).appendString(topic.errorMessage, false)
      },
      false,
      false
    )

  throws(
    () => parseResponse(1, 19, 4, Reader.from(writer)),
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
            configs: null
          }
        ]
      })
      return true
    }
  )
})

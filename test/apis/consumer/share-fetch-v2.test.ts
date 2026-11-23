import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { shareFetchV2 } from '../../../src/apis/index.ts'
import { Reader, ResponseError, ShareAcknowledgeTypes, ShareAcquireModes, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = shareFetchV2

const topicId = '12345678-1234-1234-1234-123456789012'

function writeResponse (errorCode: number): Writer {
  return Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendString(null)
    .appendInt32(15000)
    .appendArray([0], (w, partitionIndex) => {
      w.appendUUID(topicId).appendArray([partitionIndex], (w, p) => {
        w.appendInt32(p)
          .appendInt16(errorCode)
          .appendString(null)
          .appendInt16(0)
          .appendString(null)
          .appendInt32(-1)
          .appendInt32(-1)
          .appendTaggedFields()
          .appendUnsignedVarInt(1)
          .appendArray([], () => {})
      })
    })
    .appendArray([], () => {})
    .appendTaggedFields()
}

test('api is ShareFetch v2', () => {
  strictEqual(api.key, 78)
  strictEqual(api.version, 2)
})

test('createRequest places the acquire mode and renew flag after the batch size', () => {
  const writer = createRequest(
    'group',
    'member',
    1,
    500,
    1,
    1048576,
    100,
    50,
    // The two fields must not encode to the same byte, or writing them the other way round would go unnoticed
    ShareAcquireModes.BATCH_OPTIMIZED,
    true,
    [
      {
        topicId,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [
              { firstOffset: 0n, lastOffset: 9n, acknowledgeTypes: [ShareAcknowledgeTypes.RENEW] }
            ]
          }
        ]
      }
    ],
    [{ topicId, partitions: [3] }]
  )
  const reader = Reader.from(writer)

  strictEqual(reader.readNullableString(), 'group')
  strictEqual(reader.readNullableString(), 'member')
  deepStrictEqual(
    [
      reader.readInt32(),
      reader.readInt32(),
      reader.readInt32(),
      reader.readInt32(),
      reader.readInt32(),
      reader.readInt32()
    ],
    [1, 500, 1, 1048576, 100, 50]
  )
  strictEqual(reader.readInt8(), ShareAcquireModes.BATCH_OPTIMIZED)
  strictEqual(reader.readBoolean(), true)
  deepStrictEqual(
    reader.readArray(r => ({
      topicId: r.readUUID(),
      partitions: r.readArray(r => ({
        partitionIndex: r.readInt32(),
        acknowledgementBatches: r.readArray(r => ({
          firstOffset: r.readInt64(),
          lastOffset: r.readInt64(),
          acknowledgeTypes: r.readArray(r => r.readInt8(), true, false)
        }))
      }))
    })),
    [
      {
        topicId,
        partitions: [
          { partitionIndex: 0, acknowledgementBatches: [{ firstOffset: 0n, lastOffset: 9n, acknowledgeTypes: [4] }] }
        ]
      }
    ]
  )
  deepStrictEqual(
    reader.readArray(r => ({ topicId: r.readUUID(), partitions: r.readArray(r => r.readInt32(), true, false) })),
    [{ topicId, partitions: [3] }]
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse uses the v1 layout', () => {
  const reader = Reader.from(writeResponse(0))
  const response = parseResponse(1, 78, 2, reader)

  strictEqual(reader.remaining, 0)
  strictEqual(response.acquisitionLockTimeoutMs, 15000)
  deepStrictEqual(response.responses, [
    {
      topicId,
      partitions: [
        {
          partitionIndex: 0,
          errorCode: 0,
          errorMessage: null,
          acknowledgeErrorCode: 0,
          acknowledgeErrorMessage: null,
          currentLeader: { leaderId: -1, leaderEpoch: -1 },
          records: [],
          acquiredRecords: []
        }
      ]
    }
  ])
})

test('parseResponse throws ResponseError reporting the v2 api version', () => {
  throws(
    () => parseResponse(1, 78, 2, Reader.from(writeResponse(3))),
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('ShareFetch(v2)'))
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiId]),
        [['/responses/0/partitions/0', 'UNKNOWN_TOPIC_OR_PARTITION']]
      )
      return true
    }
  )
})

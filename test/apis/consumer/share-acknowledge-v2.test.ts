import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { shareAcknowledgeV2 } from '../../../src/apis/index.ts'
import { Reader, ResponseError, ShareAcknowledgeTypes, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = shareAcknowledgeV2

const topicId = '12345678-1234-1234-1234-123456789012'

function writeResponse (errorCode: number, errorMessage: string | null): Writer {
  return Writer.create()
    .appendInt32(0)
    .appendInt16(errorCode)
    .appendString(errorMessage)
    .appendInt32(30000)
    .appendArray([topicId], (w, t) => {
      w.appendUUID(t).appendArray([0], (w, p) => {
        w.appendInt32(p).appendInt16(0).appendString(null).appendInt32(1).appendInt32(0).appendTaggedFields()
      })
    })
    .appendArray([], () => {})
    .appendTaggedFields()
}

test('api is ShareAcknowledge v2', () => {
  strictEqual(api.key, 79)
  strictEqual(api.version, 2)
})

test('createRequest places the renew flag after the share session epoch', () => {
  const writer = createRequest('group', 'member', 2, true, [
    {
      topicId,
      partitions: [
        {
          partitionIndex: 0,
          acknowledgementBatches: [{ firstOffset: 0n, lastOffset: 4n, acknowledgeTypes: [ShareAcknowledgeTypes.RENEW] }]
        }
      ]
    }
  ])
  const reader = Reader.from(writer)

  strictEqual(reader.readNullableString(), 'group')
  strictEqual(reader.readNullableString(), 'member')
  strictEqual(reader.readInt32(), 2)
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
          { partitionIndex: 0, acknowledgementBatches: [{ firstOffset: 0n, lastOffset: 4n, acknowledgeTypes: [4] }] }
        ]
      }
    ]
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse reads the acquisition lock timeout', () => {
  const reader = Reader.from(writeResponse(0, null))

  deepStrictEqual(parseResponse(1, 79, 2, reader), {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    acquisitionLockTimeoutMs: 30000,
    responses: [
      {
        topicId,
        partitions: [
          { partitionIndex: 0, errorCode: 0, errorMessage: null, currentLeader: { leaderId: 1, leaderEpoch: 0 } }
        ]
      }
    ],
    nodeEndpoints: []
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws ResponseError on a top-level error', () => {
  throws(
    () => parseResponse(1, 79, 2, Reader.from(writeResponse(123, 'Invalid share session epoch.'))),
    (err: any) => {
      ok(err instanceof ResponseError)
      strictEqual(err.response.acquisitionLockTimeoutMs, 30000)
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiId, e.serverErrorMessage]),
        [['/', 'INVALID_SHARE_SESSION_EPOCH', 'Invalid share session epoch.']]
      )
      return true
    }
  )
})

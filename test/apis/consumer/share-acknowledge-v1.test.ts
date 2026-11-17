import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { shareAcknowledgeV1 } from '../../../src/apis/index.ts'
import { Reader, ResponseError, ShareAcknowledgeTypes, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = shareAcknowledgeV1

const topicId = '12345678-1234-1234-1234-123456789012'

function writeResponse (
  partitions: Array<{ partitionIndex: number; errorCode: number; errorMessage: string | null }>,
  errorCode = 0,
  errorMessage: string | null = null
): Writer {
  return Writer.create()
    .appendInt32(0)
    .appendInt16(errorCode)
    .appendString(errorMessage)
    .appendArray([topicId], (w, t) => {
      w.appendUUID(t).appendArray(partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
          .appendInt16(p.errorCode)
          .appendString(p.errorMessage)
          .appendInt32(2)
          .appendInt32(4)
          .appendTaggedFields()
      })
    })
    .appendArray([{ nodeId: 2, host: 'broker-2', port: 9092, rack: 'rack-b' }], (w, n) => {
      w.appendInt32(n.nodeId).appendString(n.host).appendInt32(n.port).appendString(n.rack)
    })
    .appendTaggedFields()
}

test('api is ShareAcknowledge v1', () => {
  strictEqual(api.key, 79)
  strictEqual(api.version, 1)
})

test('createRequest serializes acknowledgement batches', () => {
  const writer = createRequest('group', 'member', 4, [
    {
      topicId,
      partitions: [
        {
          partitionIndex: 3,
          acknowledgementBatches: [
            { firstOffset: 100n, lastOffset: 104n, acknowledgeTypes: [ShareAcknowledgeTypes.ACCEPT] },
            {
              firstOffset: 105n,
              lastOffset: 106n,
              acknowledgeTypes: [ShareAcknowledgeTypes.REJECT, ShareAcknowledgeTypes.RELEASE]
            }
          ]
        }
      ]
    }
  ])
  const reader = Reader.from(writer)

  strictEqual(reader.readNullableString(), 'group')
  strictEqual(reader.readNullableString(), 'member')
  strictEqual(reader.readInt32(), 4)
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
          {
            partitionIndex: 3,
            acknowledgementBatches: [
              { firstOffset: 100n, lastOffset: 104n, acknowledgeTypes: [1] },
              { firstOffset: 105n, lastOffset: 106n, acknowledgeTypes: [3, 2] }
            ]
          }
        ]
      }
    ]
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse parses partitions and node endpoints', () => {
  const reader = Reader.from(writeResponse([{ partitionIndex: 3, errorCode: 0, errorMessage: null }]))

  deepStrictEqual(parseResponse(1, 79, 1, reader), {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    responses: [
      {
        topicId,
        partitions: [
          { partitionIndex: 3, errorCode: 0, errorMessage: null, currentLeader: { leaderId: 2, leaderEpoch: 4 } }
        ]
      }
    ],
    nodeEndpoints: [{ nodeId: 2, host: 'broker-2', port: 9092, rack: 'rack-b' }]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws ResponseError for top-level and partition errors', () => {
  const writer = writeResponse(
    [
      { partitionIndex: 0, errorCode: 0, errorMessage: null },
      { partitionIndex: 1, errorCode: 121, errorMessage: 'Record state is invalid.' }
    ],
    122,
    'Share session not found.'
  )

  throws(
    () => parseResponse(1, 79, 1, Reader.from(writer)),
    (err: any) => {
      ok(err instanceof ResponseError)
      strictEqual(err.response.responses[0].partitions.length, 2)
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiId, e.serverErrorMessage]),
        [
          ['/', 'SHARE_SESSION_NOT_FOUND', 'Share session not found.'],
          ['/responses/0/partitions/1', 'INVALID_RECORD_STATE', 'Record state is invalid.']
        ]
      )
      return true
    }
  )
})

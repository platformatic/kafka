import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { shareFetchV1 } from '../../../src/apis/index.ts'
import { createRecordsBatch, Reader, ResponseError, ShareAcknowledgeTypes, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = shareFetchV1

const topicId = '12345678-1234-1234-1234-123456789012'
const otherTopicId = '87654321-4321-4321-4321-210987654321'

interface TestPartition {
  partitionIndex: number
  errorCode?: number
  errorMessage?: string | null
  acknowledgeErrorCode?: number
  acknowledgeErrorMessage?: string | null
  records?: Writer | null
  acquiredRecords?: Array<{ firstOffset: bigint; lastOffset: bigint; deliveryCount: number }>
}

function writeShareFetchResponse (
  topics: Array<{ topicId: string; partitions: TestPartition[] }>,
  options: { errorCode?: number; errorMessage?: string | null; nodeEndpoints?: any[] } = {}
): Writer {
  return Writer.create()
    .appendInt32(0)
    .appendInt16(options.errorCode ?? 0)
    .appendString(options.errorMessage ?? null)
    .appendInt32(30000)
    .appendArray(topics, (w, t) => {
      w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
          .appendInt16(p.errorCode ?? 0)
          .appendString(p.errorMessage ?? null)
          .appendInt16(p.acknowledgeErrorCode ?? 0)
          .appendString(p.acknowledgeErrorMessage ?? null)
          .appendInt32(1)
          .appendInt32(7)
          .appendTaggedFields()

        if (p.records === null) {
          w.appendUnsignedVarInt(0)
        } else if (p.records) {
          w.appendUnsignedVarInt(p.records.length + 1).appendFrom(p.records)
        } else {
          w.appendUnsignedVarInt(1)
        }

        w.appendArray(p.acquiredRecords ?? [], (w, a) => {
          w.appendInt64(a.firstOffset).appendInt64(a.lastOffset).appendInt16(a.deliveryCount)
        })
      })
    })
    .appendArray(options.nodeEndpoints ?? [], (w, n) => {
      w.appendInt32(n.nodeId).appendString(n.host).appendInt32(n.port).appendString(n.rack)
    })
    .appendTaggedFields()
}

test('api is ShareFetch v1', () => {
  strictEqual(api.key, 78)
  strictEqual(api.version, 1)
})

test('createRequest serializes fetch limits, piggybacked acknowledgements and forgotten topics', () => {
  const writer = createRequest(
    'group',
    'member',
    3,
    500,
    1,
    1048576,
    100,
    50,
    [
      {
        topicId,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [
              { firstOffset: 10n, lastOffset: 12n, acknowledgeTypes: [ShareAcknowledgeTypes.ACCEPT] },
              {
                firstOffset: 13n,
                lastOffset: 15n,
                acknowledgeTypes: [
                  ShareAcknowledgeTypes.RELEASE,
                  ShareAcknowledgeTypes.GAP,
                  ShareAcknowledgeTypes.REJECT
                ]
              }
            ]
          },
          { partitionIndex: 1, acknowledgementBatches: [] }
        ]
      }
    ],
    [{ topicId: otherTopicId, partitions: [4, 5] }]
  )
  const reader = Reader.from(writer)

  strictEqual(reader.readNullableString(), 'group')
  strictEqual(reader.readNullableString(), 'member')
  strictEqual(reader.readInt32(), 3)
  strictEqual(reader.readInt32(), 500)
  strictEqual(reader.readInt32(), 1)
  strictEqual(reader.readInt32(), 1048576)
  strictEqual(reader.readInt32(), 100)
  strictEqual(reader.readInt32(), 50)
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
            partitionIndex: 0,
            acknowledgementBatches: [
              { firstOffset: 10n, lastOffset: 12n, acknowledgeTypes: [1] },
              { firstOffset: 13n, lastOffset: 15n, acknowledgeTypes: [2, 0, 3] }
            ]
          },
          { partitionIndex: 1, acknowledgementBatches: [] }
        ]
      }
    ]
  )
  deepStrictEqual(
    reader.readArray(r => ({ topicId: r.readUUID(), partitions: r.readArray(r => r.readInt32(), true, false) })),
    [{ topicId: otherTopicId, partitions: [4, 5] }]
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest serializes null group and member ids', () => {
  const reader = Reader.from(createRequest(null, null, -1, 0, 0, 0, 0, 0, [], []))

  strictEqual(reader.readNullableString(), null)
  strictEqual(reader.readNullableString(), null)
  strictEqual(reader.readInt32(), -1)
  reader.skip(20)
  strictEqual(reader.readUnsignedVarInt(), 1)
  strictEqual(reader.readUnsignedVarInt(), 1)
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse parses records, acquired ranges and node endpoints', () => {
  const records = createRecordsBatch([
    { topic: 'topic', key: Buffer.from('k0'), value: Buffer.from('v0'), timestamp: 1720000000000n },
    { topic: 'topic', key: Buffer.from('k1'), value: Buffer.from('v1'), timestamp: 1720000000000n }
  ])
  const writer = writeShareFetchResponse(
    [
      {
        topicId,
        partitions: [
          {
            partitionIndex: 0,
            records,
            acquiredRecords: [
              { firstOffset: 0n, lastOffset: 0n, deliveryCount: 1 },
              { firstOffset: 1n, lastOffset: 1n, deliveryCount: 3 }
            ]
          },
          { partitionIndex: 1 }
        ]
      }
    ],
    { nodeEndpoints: [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }] }
  )
  const reader = Reader.from(writer)
  const response = parseResponse(1, 78, 1, reader)
  strictEqual(reader.remaining, 0)

  const [first, second] = response.responses[0].partitions

  strictEqual(response.acquisitionLockTimeoutMs, 30000)
  deepStrictEqual(
    first.records.flatMap(batch => batch.records.map(r => [r.key!.toString(), r.value!.toString()])),
    [
      ['k0', 'v0'],
      ['k1', 'v1']
    ]
  )
  deepStrictEqual(first.acquiredRecords, [
    { firstOffset: 0n, lastOffset: 0n, deliveryCount: 1 },
    { firstOffset: 1n, lastOffset: 1n, deliveryCount: 3 }
  ])
  deepStrictEqual(first.currentLeader, { leaderId: 1, leaderEpoch: 7 })
  deepStrictEqual(second.records, [])
  deepStrictEqual(second.acquiredRecords, [])
  deepStrictEqual(response.nodeEndpoints, [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }])
})

test('parseResponse reads null records as no records', () => {
  const reader = Reader.from(writeShareFetchResponse([{ topicId, partitions: [{ partitionIndex: 0, records: null }] }]))

  deepStrictEqual(parseResponse(1, 78, 1, reader).responses[0].partitions[0].records, [])
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws ResponseError on a top-level error', () => {
  const writer = writeShareFetchResponse([], { errorCode: 123, errorMessage: 'Invalid share session epoch.' })

  throws(
    () => parseResponse(1, 78, 1, Reader.from(writer)),
    (err: any) => {
      ok(err instanceof ResponseError)
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiCode, e.serverErrorMessage]),
        [['/', 123, 'Invalid share session epoch.']]
      )
      return true
    }
  )
})

test('parseResponse throws ResponseError when the broker reaches its share session limit', () => {
  const writer = writeShareFetchResponse([], { errorCode: 133, errorMessage: null })

  throws(
    () => parseResponse(1, 78, 1, Reader.from(writer)),
    (err: any) => {
      ok(err instanceof ResponseError)
      strictEqual(err.errors[0].apiId, 'SHARE_SESSION_LIMIT_REACHED')
      strictEqual(err.errors[0].canRetry, true)
      deepStrictEqual(err.response.responses, [])
      return true
    }
  )
})

test('parseResponse reports partition and acknowledgement errors separately and keeps the records', () => {
  const records = createRecordsBatch([{ topic: 'topic', value: Buffer.from('v0'), timestamp: 1720000000000n }])
  const writer = writeShareFetchResponse([
    {
      topicId,
      partitions: [
        {
          partitionIndex: 0,
          acknowledgeErrorCode: 121,
          acknowledgeErrorMessage: 'Record state is invalid.',
          records,
          acquiredRecords: [{ firstOffset: 0n, lastOffset: 0n, deliveryCount: 1 }]
        },
        { partitionIndex: 1, errorCode: 6, errorMessage: 'Not the leader.' }
      ]
    }
  ])

  throws(
    () => parseResponse(1, 78, 1, Reader.from(writer)),
    (err: any) => {
      ok(err instanceof ResponseError)
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiCode, e.serverErrorMessage]),
        [
          ['/responses/0/partitions/0/acknowledge', 121, 'Record state is invalid.'],
          ['/responses/0/partitions/1', 6, 'Not the leader.']
        ]
      )
      deepStrictEqual(err.response.responses[0].partitions[0].acquiredRecords, [
        { firstOffset: 0n, lastOffset: 0n, deliveryCount: 1 }
      ])
      strictEqual(err.response.responses[0].partitions[0].records[0].records[0].value.toString(), 'v0')
      return true
    }
  )
})

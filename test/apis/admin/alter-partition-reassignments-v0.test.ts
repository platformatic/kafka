import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { alterPartitionReassignmentsV0, Reader } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = alterPartitionReassignmentsV0

test('AlterPartitionReassignments v0 distinguishes null, empty, and populated replicas', () => {
  const request = Reader.from(createRequest(30000, true, [
    { name: 'null', partitions: [{ partitionIndex: 0, replicas: null }] },
    { name: 'empty', partitions: [{ partitionIndex: 1, replicas: [] }] },
    { name: 'set', partitions: [{ partitionIndex: 2, replicas: [1, 2] }] }
  ]))

  strictEqual(request.readInt32(), 30000)
  deepStrictEqual(request.readArray(reader => {
    const name = reader.readString()
    const partitions = reader.readArray(reader => {
      const partitionIndex = reader.readInt32()
      const replicas = reader.readNullableArray(reader => reader.readInt32(), true, false)
      return { partitionIndex, replicas }
    })
    return { name, partitions }
  }), [
    { name: 'null', partitions: [{ partitionIndex: 0, replicas: null }] },
    { name: 'empty', partitions: [{ partitionIndex: 1, replicas: [] }] },
    { name: 'set', partitions: [{ partitionIndex: 2, replicas: [1, 2] }] }
  ])
  request.readTaggedFields()
  strictEqual(request.remaining, 0)
})

test('AlterPartitionReassignments v0 consumes flexible response boundaries and headers', () => {
  const reader = Reader.from(Buffer.from('00000001000000020b746573742d746f7069630200000000000000000000', 'hex'))
  deepStrictEqual(parseResponse(1, 45, 0, reader), {
    throttleTimeMs: 1,
    allowReplicationFactorChange: true,
    errorCode: 0,
    errorMessage: null,
    responses: [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 0, errorMessage: null }] }]
  })
  strictEqual(reader.remaining, 0)

  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, 0, false, [])
  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, { key: 45, version: 0, requestTags: true, responseTags: true })
})

import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { createPartitionsV0, createPartitionsV1, createPartitionsV2, Reader, type Writer } from '../../../src/index.ts'

type Topic = { name: string; count: number; assignments: null | { brokerIds: number[] }[] }
type CreateRequest = (topics: Topic[], timeoutMs: number, validateOnly: boolean) => Writer

for (const codec of [createPartitionsV0, createPartitionsV1, createPartitionsV2]) {
  test(`CreatePartitions v${codec.api.version} preserves nullable assignments`, () => {
    const flexible = codec.api.version === 2
    const reader = Reader.from((codec.createRequest as CreateRequest)([
      { name: 'null', count: 2, assignments: null },
      { name: 'empty', count: 3, assignments: [] },
      { name: 'set', count: 4, assignments: [{ brokerIds: [1, 2] }] }
    ], 1000, false))

    deepStrictEqual(reader.readArray(r => ({
      name: r.readString(flexible),
      count: r.readInt32(),
      assignments: r.readNullableArray(r => ({ brokerIds: r.readArray(r => r.readInt32(), flexible, false) }), flexible, flexible)
    }), flexible, flexible), [
      { name: 'null', count: 2, assignments: null },
      { name: 'empty', count: 3, assignments: [] },
      { name: 'set', count: 4, assignments: [{ brokerIds: [1, 2] }] }
    ])
    strictEqual(reader.readInt32(), 1000)
    strictEqual(reader.readBoolean(), false)
    if (flexible) {
      reader.readTaggedFields()
    }
    strictEqual(reader.remaining, 0)
  })
}

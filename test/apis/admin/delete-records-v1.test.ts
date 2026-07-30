import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/delete-records-v1.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('DeleteRecords v1 serializes classic fields and parses classic responses', () => {
  const requestReader = Reader.from(
    createRequest([{ name: 'topic', partitions: [{ partitionIndex: 0, offset: 5n }] }], 1000)
  )
  deepStrictEqual(
    requestReader.readArray(
      r => ({
        name: r.readString(false),
        partitions: r.readArray(r => ({ partitionIndex: r.readInt32(), offset: r.readInt64() }), false, false)
      }),
      false,
      false
    ),
    [{ name: 'topic', partitions: [{ partitionIndex: 0, offset: 5n }] }]
  )
  strictEqual(requestReader.readInt32(), 1000)
  const response = parseResponse(
    1,
    api.key,
    api.version,
    Reader.from(
      Writer.create()
        .appendInt32(2)
        .appendArray(
          ['topic'],
          writer => writer.appendString('topic', false).appendArray([], () => {}, false, false),
          false,
          false
        )
    )
  )
  deepStrictEqual(response, { throttleTimeMs: 2, topics: [{ name: 'topic', partitions: [] }] })
})

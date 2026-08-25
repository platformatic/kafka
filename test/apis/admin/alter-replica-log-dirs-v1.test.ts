import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/alter-replica-log-dirs-v1.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('AlterReplicaLogDirs v1 serializes classic requests and responses with classic headers', () => {
  const request = Reader.from(createRequest([{ path: '/tmp', topics: [{ name: 'topic', partitions: [1] }] }]))
  strictEqual(request.readArray(reader => reader.readString(false), false, false)[0], '/tmp')
  const reader = Reader.from(Writer.create().appendInt32(1).appendArray([], () => {}, false, false))
  deepStrictEqual(parseResponse(1, api.key, api.version, reader), { throttleTimeMs: 1, results: [] })
  strictEqual(reader.remaining, 0)
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [34, 1, false, false])
})

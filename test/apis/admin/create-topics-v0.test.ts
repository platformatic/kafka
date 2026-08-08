import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/create-topics-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('CreateTopics v0 omits validateOnly and normalizes legacy responses', () => {
  const requestReader = Reader.from(
    createRequest([{ name: 'topic', numPartitions: 1, replicationFactor: 1, assignments: [], configs: [] }], 1000, true)
  )
  requestReader.readArray(
    reader => {
      reader.readString(false)
      reader.readInt32()
      reader.readInt16()
      reader.readArray(() => {}, false, false)
      reader.readArray(() => {}, false, false)
    },
    false,
    false
  )
  strictEqual(requestReader.readInt32(), 1000)
  strictEqual(requestReader.remaining, 0)
  const response = parseResponse(
    1,
    api.key,
    api.version,
    Reader.from(
      Writer.create().appendArray(
        ['topic'],
        writer => writer.appendString('topic', false).appendInt16(0),
        false,
        false
      )
    )
  )
  deepStrictEqual(response, {
    throttleTimeMs: 0,
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

import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteTopicsV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = deleteTopicsV5

test('DeleteTopics v5 serializes flexible names and ignores topic IDs', () => {
  const reader = Reader.from(createRequest([
    { name: 'by-name' },
    { topicId: 'not-a-uuid' },
    { name: 'both', topicId: '12345678-1234-1234-1234-123456789abc' }
  ], 30000))
  deepStrictEqual(reader.readArray(r => r.readString()), ['by-name', '', 'both'])
  deepStrictEqual(reader.readInt32(), 30000)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 20, version: 5 })
})

test('DeleteTopics v5 serializes an empty flexible topic array', () => {
  const reader = Reader.from(createRequest([], 0))
  deepStrictEqual(reader.readArray(r => r.readString()), [])
  deepStrictEqual(reader.readInt32(), 0)
})

test('DeleteTopics v5 parses successful flexible responses with error messages', () => {
  const writer = Writer.create().appendInt32(25).appendArray([{ name: 'topic', errorCode: 0, errorMessage: null }], (w, topic) => {
    w.appendString(topic.name).appendInt16(topic.errorCode).appendString(topic.errorMessage)
  }).appendTaggedFields()
  deepStrictEqual(parseResponse(1, 20, 5, Reader.from(writer)), {
    throttleTimeMs: 25,
    responses: [{ name: 'topic', topicId: '00000000-0000-0000-0000-000000000000', errorCode: 0, errorMessage: null }]
  })
})

test('DeleteTopics v5 preserves flexible response error messages', () => {
  const writer = Writer.create().appendInt32(0).appendArray([{ name: 'topic', errorCode: 3, errorMessage: 'Unknown topic' }], (w, topic) => {
    w.appendString(topic.name).appendInt16(topic.errorCode).appendString(topic.errorMessage)
  }).appendTaggedFields()
  throws(() => parseResponse(1, 20, 5, Reader.from(writer)), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 0, responses: [{ name: 'topic', topicId: '00000000-0000-0000-0000-000000000000', errorCode: 3, errorMessage: 'Unknown topic' }] })
    return true
  })
})

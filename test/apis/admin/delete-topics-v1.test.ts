import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteTopicsV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = deleteTopicsV1

test('DeleteTopics v1 serializes classic names and ignores topic IDs', () => {
  const reader = Reader.from(createRequest([
    { name: 'by-name' },
    { topicId: 'not-a-uuid' },
    { name: 'both', topicId: '12345678-1234-1234-1234-123456789abc' }
  ], 30000))
  deepStrictEqual(reader.readArray(r => r.readString(false), false, false), ['by-name', '', 'both'])
  deepStrictEqual(reader.readInt32(), 30000)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 20, version: 1 })
})

test('DeleteTopics v1 serializes an empty classic topic array', () => {
  const reader = Reader.from(createRequest([], 0))
  deepStrictEqual(reader.readArray(r => r.readString(false), false, false), [])
  deepStrictEqual(reader.readInt32(), 0)
})

test('DeleteTopics v1 parses classic responses with normalized fields', () => {
  const writer = Writer.create().appendInt32(25).appendArray([{ name: 'topic', errorCode: 0 }], (w, topic) => {
    w.appendString(topic.name, false).appendInt16(topic.errorCode)
  }, false, false)
  deepStrictEqual(parseResponse(1, 20, 1, Reader.from(writer)), { throttleTimeMs: 25, responses: [{ name: 'topic', topicId: '00000000-0000-0000-0000-000000000000', errorCode: 0, errorMessage: null }] })
})

test('DeleteTopics v1 preserves normalized fields in errors', () => {
  const writer = Writer.create().appendInt32(0).appendArray([{ name: 'topic', errorCode: 3 }], (w, topic) => {
    w.appendString(topic.name, false).appendInt16(topic.errorCode)
  }, false, false)
  throws(() => parseResponse(1, 20, 1, Reader.from(writer)), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 0, responses: [{ name: 'topic', topicId: '00000000-0000-0000-0000-000000000000', errorCode: 3, errorMessage: null }] })
    return true
  })
})

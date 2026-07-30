import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
import { parseResponse } from '../../../src/apis/admin/create-acls-v3.ts'
import { parseResponse as parseDescribeConfigsResponse } from '../../../src/apis/admin/describe-configs-v4.ts'
import { parseResponse as parseConsumerGroupDescribeResponse } from '../../../src/apis/admin/consumer-group-describe-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

function appendUnknownTaggedFields (writer: Writer): Writer {
  return writer.append(Buffer.from([1, 0, 1, 0]))
}

test('CreateAcls v3 consumes nested and root unknown tagged fields', () => {
  const reader = Reader.from(
    appendUnknownTaggedFields(
      Writer.create().appendInt32(0).appendArray([{ errorCode: 0, errorMessage: null }], (writer, result) => {
        writer.appendInt16(result.errorCode).appendString(result.errorMessage)
        appendUnknownTaggedFields(writer)
      }, true, false)
    )
  )

  assert.deepEqual(parseResponse(1, 30, 3, reader), {
    throttleTimeMs: 0,
    results: [{ errorCode: 0, errorMessage: null }]
  })
  assert.equal(reader.remaining, 0)
})

test('CreateAcls v3 consumes tagged fields before reporting response errors', () => {
  const reader = Reader.from(
    appendUnknownTaggedFields(
      Writer.create().appendInt32(0).appendArray([{ errorCode: 42, errorMessage: 'failed' }], (writer, result) => {
        writer.appendInt16(result.errorCode).appendString(result.errorMessage)
        appendUnknownTaggedFields(writer)
      }, true, false)
    )
  )

  assert.throws(() => parseResponse(1, 30, 3, reader), ResponseError)
  assert.equal(reader.remaining, 0)
})

test('DescribeConfigs v4 consumes nested unknown tagged fields', () => {
  const reader = Reader.from(
    appendUnknownTaggedFields(
      Writer.create().appendInt32(0).appendArray([{ configs: [{}] }], writer => {
        writer.appendInt16(0).appendString(null).appendInt8(2).appendString('topic').appendArray([{}], writer => {
          writer
            .appendString('cleanup.policy')
            .appendString('delete')
            .appendBoolean(false)
            .appendInt8(1)
            .appendBoolean(false)
            .appendArray([{}], writer => {
              writer.appendString('cleanup.policy').appendString('delete').appendInt8(1)
              appendUnknownTaggedFields(writer)
            }, true, false)
            .appendInt8(1)
            .appendString(null)
          appendUnknownTaggedFields(writer)
        }, true, false)
        appendUnknownTaggedFields(writer)
      }, true, false)
    )
  )

  parseDescribeConfigsResponse(1, 32, 4, reader)
  assert.equal(reader.remaining, 0)
})

test('ConsumerGroupDescribe v0 consumes nested unknown tagged fields', () => {
  const appendAssignment = (writer: Writer): void => {
    writer.appendArray([{}], writer => {
      writer
        .appendUUID('12345678-1234-1234-1234-123456789abc')
        .appendString('topic')
        .appendArray([0], writer => writer.appendInt32(0), true, false)
      appendUnknownTaggedFields(writer)
    }, true, false)
    appendUnknownTaggedFields(writer)
  }

  const reader = Reader.from(
    appendUnknownTaggedFields(
      Writer.create().appendInt32(0).appendArray([{}], writer => {
        writer
          .appendInt16(0)
          .appendString(null)
          .appendString('group')
          .appendString('Stable')
          .appendInt32(1)
          .appendInt32(1)
          .appendString('range')
          .appendArray([{}], writer => {
            writer
              .appendString('member')
              .appendString(null)
              .appendString(null)
              .appendInt32(1)
              .appendString('client')
              .appendString('host')
              .appendArray(['topic'], (writer, topic) => writer.appendString(topic), true, false)
              .appendString(null)
            appendAssignment(writer)
            appendAssignment(writer)
            appendUnknownTaggedFields(writer)
          }, true, false)
          .appendInt32(0)
        appendUnknownTaggedFields(writer)
      }, true, false)
    )
  )

  parseConsumerGroupDescribeResponse(1, 69, 0, reader)
  assert.equal(reader.remaining, 0)
})

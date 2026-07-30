import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as createTopicsV5 from '../../../src/apis/admin/create-topics-v5.ts'
import * as createTopicsV6 from '../../../src/apis/admin/create-topics-v6.ts'
import * as createTopicsV7 from '../../../src/apis/admin/create-topics-v7.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

function appendTopic (writer: Writer, version: number, configs: readonly object[] | null, topicConfigErrorCode?: number): void {
  writer.appendString('topic')
  if (version === 7) {
    writer.appendUUID('12345678-1234-1234-1234-123456789abc')
  }
  writer.appendInt16(0).appendString(null).appendInt32(1).appendInt16(1).appendArray(configs === null ? null : [...configs], w => {
    w.appendString('cleanup.policy').appendString('compact').appendBoolean(false).appendInt8(1).appendBoolean(false).appendTaggedFields()
  }, true, false)

  if (topicConfigErrorCode === undefined) {
    writer.appendTaggedFields()
  } else {
    writer.appendUnsignedVarInt(1).appendUnsignedVarInt(0).appendUnsignedVarInt(2).appendInt16(topicConfigErrorCode)
  }
}

test('CreateTopics v5-v7 preserve nullable response configs', () => {
  for (const [version, api] of [
    [5, createTopicsV5],
    [6, createTopicsV6],
    [7, createTopicsV7]
  ] as const) {
    for (const [name, configs] of [
      ['null', null],
      ['empty', []],
      ['populated', [{}]]
    ] as const) {
      const writer = Writer.create().appendInt32(0).appendArray([0], w => appendTopic(w, version, configs), true, false).appendTaggedFields()
      const response = api.parseResponse(1, 19, version, Reader.from(writer))
      deepStrictEqual(
        response.topics[0].configs,
        configs === null
          ? null
          : configs.map(() => ({
            name: 'cleanup.policy',
            value: 'compact',
            readOnly: false,
            configSource: 1,
            isSensitive: false
          })),
        `CreateTopics v${version} ${name} configs`
      )
    }
  }
})

test('CreateTopics v7 parses topic config errors from tag 0', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray([0], w => appendTopic(w, 7, [], 42), true, false)
    .appendTaggedFields()

  const response = createTopicsV7.parseResponse(1, 19, 7, Reader.from(writer))
  deepStrictEqual(response.topics[0].topicConfigErrorCode, 42)
})

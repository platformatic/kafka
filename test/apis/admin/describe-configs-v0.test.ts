import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ResponseError } from '../../../src/errors.ts'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/describe-configs-v0.ts'
import { ConfigResourceTypes, ConfigSources, ConfigTypes } from '../../../src/apis/enumerations.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('DescribeConfigs v0 handles classic requests, responses, errors, and headers', () => {
  const request = Reader.from(createRequest([{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'topic', configurationKeys: ['cleanup.policy'] }]))
  strictEqual(request.readArray(reader => ({ resourceType: reader.readInt8(), resourceName: reader.readString(false), configurationKeys: reader.readArray(reader => reader.readString(false), false, false) }), false, false)[0].resourceName, 'topic')
  strictEqual(request.remaining, 0)
  const reader = Reader.from(
    Writer.create().appendInt32(1).appendArray(
      [{}],
      writer =>
        writer
          .appendInt16(0)
          .appendString(null, false)
          .appendInt8(ConfigResourceTypes.TOPIC)
          .appendString('topic', false)
          .appendArray(
            [
              { name: 'cleanup.policy', value: 'compact', readOnly: false, configSource: ConfigSources.DEFAULT_CONFIG, isSensitive: true },
              { name: 'retention.ms', value: '60000', readOnly: true, configSource: -1, isSensitive: false }
            ],
            (writer, config) =>
              writer
                .appendString(config.name, false)
                .appendString(config.value, false)
                .appendBoolean(config.readOnly)
                .appendBoolean(config.configSource !== -1)
                .appendBoolean(config.isSensitive),
            false,
            false
          ),
      false,
      false
    )
  )
  deepStrictEqual(parseResponse(1, api.key, api.version, reader).results[0].configs, [
    { name: 'cleanup.policy', value: 'compact', readOnly: false, configSource: -1, isSensitive: true, synonyms: [], configType: ConfigTypes.UNKNOWN, documentation: null },
    { name: 'retention.ms', value: '60000', readOnly: true, configSource: -1, isSensitive: false, synonyms: [], configType: ConfigTypes.UNKNOWN, documentation: null }
  ])
  strictEqual(reader.remaining, 0)
  throws(() => parseResponse(1, api.key, api.version, Reader.from(Writer.create().appendInt32(0).appendArray([{}], writer => writer.appendInt16(15).appendString('failed', false).appendInt8(ConfigResourceTypes.TOPIC).appendString('topic', false).appendArray([], () => {}, false, false), false, false))), ResponseError)
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [32, 0, false, false])
})

test('DescribeConfigs v0 accepts default and explicit optional filters', () => {
  for (const request of [createRequest([]), createRequest([], true, true)]) {
    const reader = Reader.from(request)
    deepStrictEqual(reader.readArray(() => undefined, false, false), [])
    strictEqual(reader.remaining, 0)
  }
})

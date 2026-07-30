import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/describe-groups-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('DescribeGroups v0 preserves the Admin call shape and normalizes unavailable fields', () => {
  const requestReader = Reader.from(createRequest(['group'], true))
  deepStrictEqual(
    requestReader.readArray(r => r.readString(false), false, false),
    ['group']
  )
  strictEqual(requestReader.remaining, 0)

  const response = parseResponse(
    1,
    api.key,
    api.version,
    Reader.from(
      Writer.create().appendArray(
        ['group'],
        writer =>
          writer
            .appendInt16(0)
            .appendString('group', false)
            .appendString('Stable', false)
            .appendString('consumer', false)
            .appendString('range', false)
            .appendArray([], () => {}, false, false),
        false,
        false
      )
    )
  )
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    groups: [
      {
        errorCode: 0,
        groupId: 'group',
        groupState: 'Stable',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [],
        authorizedOperations: -2147483648
      }
    ]
  })
})

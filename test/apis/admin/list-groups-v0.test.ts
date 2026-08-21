import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { api, createRequest, parseResponse } from '../../../src/apis/admin/list-groups-v0.ts'
import { GroupTypes } from '../../../src/apis/enumerations.ts'
import { Reader } from '../../../src/protocol/reader.ts'

test('ListGroups v0 ignores future filters and normalizes absent fields', () => {
  strictEqual(createRequest(['Assigning'], [...GroupTypes]).length, 0)
  const reader = Reader.from(Buffer.from('000000000001000567726f75700008636f6e73756d6572', 'hex'))
  const response = parseResponse(1, api.key, api.version, reader)

  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    groups: [{ groupId: 'group', protocolType: 'consumer', groupState: '', groupType: '' }]
  })
  strictEqual(reader.remaining, 0)
})

import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { shareGroupDescribeV1 } from '../../../src/apis/index.ts'
import { Reader, ResponseError, Writer } from '../../../src/index.ts'
import { type ShareGroupDescribeResponseGroup } from '../../../src/apis/admin/share-group-describe-v1.ts'

const { api, createRequest, parseResponse } = shareGroupDescribeV1

const topicId = '12345678-1234-1234-1234-123456789012'

function writeGroup (w: Writer, group: ShareGroupDescribeResponseGroup) {
  w.appendInt16(group.errorCode)
    .appendString(group.errorMessage)
    .appendString(group.groupId)
    .appendString(group.groupState)
    .appendInt32(group.groupEpoch)
    .appendInt32(group.assignmentEpoch)
    .appendString(group.assignorName)
    .appendArray(group.members, (w, m) => {
      w.appendString(m.memberId)
        .appendString(m.rackId)
        .appendInt32(m.memberEpoch)
        .appendString(m.clientId)
        .appendString(m.clientHost)
        .appendArray(m.subscribedTopicNames, (w, t) => w.appendString(t), true, false)
        .appendArray(m.assignment.topicPartitions, (w, t) => {
          w.appendUUID(t.topicId)
            .appendString(t.topicName)
            .appendArray(t.partitions, (w, p) => w.appendInt32(p), true, false)
        })
        .appendTaggedFields()
    })
    .appendInt32(group.authorizedOperations)
}

function createGroup (overrides: Partial<ShareGroupDescribeResponseGroup> = {}): ShareGroupDescribeResponseGroup {
  return {
    errorCode: 0,
    errorMessage: null,
    groupId: 'group',
    groupState: 'Stable',
    groupEpoch: 2,
    assignmentEpoch: 2,
    assignorName: 'simple',
    members: [
      {
        memberId: 'member-1',
        rackId: 'rack-a',
        memberEpoch: 2,
        clientId: 'client-1',
        clientHost: '/127.0.0.1',
        subscribedTopicNames: ['topic'],
        assignment: { topicPartitions: [{ topicId, topicName: 'topic', partitions: [0, 1] }] }
      },
      {
        memberId: 'member-2',
        rackId: null,
        memberEpoch: 2,
        clientId: 'client-2',
        clientHost: '/127.0.0.2',
        subscribedTopicNames: ['topic'],
        assignment: { topicPartitions: [] }
      }
    ],
    authorizedOperations: 3400,
    ...overrides
  }
}

test('api is ShareGroupDescribe v1', () => {
  strictEqual(api.key, 77)
  strictEqual(api.version, 1)
})

test('createRequest serializes group ids and authorized operations flag', () => {
  const reader = Reader.from(createRequest(['group-1', 'group-2'], true))

  deepStrictEqual(
    reader.readArray(r => r.readString(), true, false),
    ['group-1', 'group-2']
  )
  strictEqual(reader.readBoolean(), true)
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse parses groups, members and assignments', () => {
  const groups = [createGroup(), createGroup({ groupId: 'empty', groupState: 'Empty', members: [] })]
  const writer = Writer.create().appendInt32(5).appendArray(groups, writeGroup).appendTaggedFields()
  const reader = Reader.from(writer)

  deepStrictEqual(parseResponse(1, 77, 1, reader), { throttleTimeMs: 5, groups })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws ResponseError pointing at the failing group', () => {
  const groups = [
    createGroup(),
    createGroup({
      errorCode: 69,
      errorMessage: 'Group is not a share group.',
      groupId: 'other',
      groupState: 'Dead',
      members: []
    })
  ]
  const writer = Writer.create().appendInt32(0).appendArray(groups, writeGroup).appendTaggedFields()

  throws(
    () => parseResponse(1, 77, 1, Reader.from(writer)),
    (err: any) => {
      ok(err instanceof ResponseError)
      deepStrictEqual(err.response.groups, groups)
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiId, e.serverErrorMessage]),
        [['/groups/1', 'GROUP_ID_NOT_FOUND', 'Group is not a share group.']]
      )
      return true
    }
  )
})

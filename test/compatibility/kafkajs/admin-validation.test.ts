import { deepStrictEqual, equal, fail, rejects } from 'node:assert'
import { after, describe, test } from 'node:test'
import KafkaJS from 'kafkajs'
import { Kafka } from '../../../src/compatibility/kafkajs/index.ts'

type AdminMethod = (...args: unknown[]) => Promise<unknown>

const kafkaJS = new KafkaJS.Kafka({ brokers: ['localhost:9092'], clientId: 'admin-validation-kafkajs' })
const compatibility = new Kafka({ brokers: ['localhost:9092'], clientId: 'admin-validation-compatibility' })
const expected = kafkaJS.admin()
const actual = compatibility.admin()

after(async () => {
  await Promise.allSettled([expected.disconnect(), actual.disconnect()])
})

async function errorFrom (admin: object, method: string, argument: unknown): Promise<Error> {
  try {
    await (admin as Record<string, AdminMethod>)[method](argument)
  } catch (error) {
    if (error instanceof Error) {
      return error
    }
    throw error
  }
  fail(`${method} did not reject`)
}

const cases: Array<{ method: string; argument: unknown; label: string }> = [
  { method: 'createTopics', argument: { topics: null }, label: 'createTopics requires an array' },
  {
    method: 'createTopics',
    argument: { topics: [{ topic: 'same' }, { topic: 'same' }] },
    label: 'createTopics rejects duplicates'
  },
  {
    method: 'createTopics',
    argument: { topics: [{ topic: 'topic', configEntries: [{}] }] },
    label: 'createTopics validates configs'
  },
  { method: 'createPartitions', argument: { topicPartitions: [] }, label: 'createPartitions rejects empty input' },
  { method: 'deleteTopics', argument: { topics: [1] }, label: 'deleteTopics validates names' },
  { method: 'fetchOffsets', argument: { groupId: '', topics: [] }, label: 'fetchOffsets validates group ID' },
  { method: 'fetchOffsets', argument: { groupId: 'group', topics: 'topic' }, label: 'fetchOffsets validates topics' },
  {
    method: 'setOffsets',
    argument: { groupId: '', topic: 'topic', partitions: [] },
    label: 'setOffsets validates group ID'
  },
  {
    method: 'setOffsets',
    argument: { groupId: 'group', topic: 'topic', partitions: [] },
    label: 'setOffsets validates partitions'
  },
  { method: 'resetOffsets', argument: { groupId: 'group', topic: '' }, label: 'resetOffsets validates topic' },
  { method: 'describeConfigs', argument: { resources: [] }, label: 'describeConfigs rejects empty resources' },
  {
    method: 'describeConfigs',
    argument: { resources: [{ type: 99, name: 'topic' }] },
    label: 'describeConfigs validates resource type'
  },
  {
    method: 'alterConfigs',
    argument: { resources: [{ type: 2, name: 'topic', configEntries: null }] },
    label: 'alterConfigs validates entries'
  },
  { method: 'createAcls', argument: { acl: [] }, label: 'createAcls rejects empty input' },
  { method: 'deleteAcls', argument: { filters: [] }, label: 'deleteAcls rejects empty input' },
  {
    method: 'describeAcls',
    argument: { resourceType: 2, resourcePatternType: 3, operation: 99, permissionType: 3 },
    label: 'describeAcls validates operation'
  },
  { method: 'deleteGroups', argument: [1], label: 'deleteGroups validates group IDs' },
  {
    method: 'deleteTopicRecords',
    argument: { topic: '', partitions: [] },
    label: 'deleteTopicRecords validates topic'
  },
  {
    method: 'alterPartitionReassignments',
    argument: { topics: [{ topic: 'topic', partitionAssignment: [{ partition: -1, replicas: [0] }] }] },
    label: 'alterPartitionReassignments validates partitions'
  },
  {
    method: 'listPartitionReassignments',
    argument: { topics: [{ topic: 'topic', partitions: [-1] }] },
    label: 'listPartitionReassignments validates partitions'
  }
]

describe('KafkaJS admin validation compatibility', () => {
  for (const validation of cases) {
    test(validation.label, async () => {
      const [expectedError, actualError] = await Promise.all([
        errorFrom(expected, validation.method, validation.argument),
        errorFrom(actual, validation.method, validation.argument)
      ])
      equal(actualError.name, expectedError.name)
      equal(actualError.message, expectedError.message)
    })
  }

  test('setOffsets rejects active groups before altering offsets', async () => {
    const admin = compatibility.admin()
    const bridge = (
      admin as unknown as {
        native: {
          describeGroupsRaw: () => Promise<
            Array<{
              throttleTimeMs: number
              groups: Array<{
                errorCode: number
                groupId: string
                groupState: string
                protocolType: string
                protocolData: string
                members: []
                authorizedOperations: number
              }>
            }>
          >
        }
      }
    ).native
    bridge.describeGroupsRaw = async () => [
      {
        throttleTimeMs: 0,
        groups: [
          {
            errorCode: 0,
            groupId: 'active',
            groupState: 'STABLE',
            protocolType: 'consumer',
            protocolData: 'range',
            members: [],
            authorizedOperations: 0
          }
        ]
      }
    ]
    await rejects(
      admin.setOffsets({ groupId: 'active', topic: 'topic', partitions: [{ partition: 0, offset: '0' }] }),
      /must have no running instances, current state: Stable/
    )
  })

  test('describeGroups preserves member protocol buffers and states', async () => {
    const admin = compatibility.admin()
    const memberMetadata = Buffer.from('metadata')
    const memberAssignment = Buffer.from('assignment')
    const bridge = (
      admin as unknown as {
        native: { describeGroupsRaw: () => Promise<unknown> }
      }
    ).native
    bridge.describeGroupsRaw = async () => [
      {
        throttleTimeMs: 0,
        groups: [
          {
            errorCode: 0,
            groupId: 'group',
            groupState: 'PREPARING_REBALANCE',
            protocolType: 'consumer',
            protocolData: 'range',
            authorizedOperations: 0,
            members: [
              {
                memberId: 'member',
                groupInstanceId: null,
                clientId: 'client',
                clientHost: 'host',
                memberMetadata,
                memberAssignment
              }
            ]
          }
        ]
      }
    ]
    const result = await admin.describeGroups(['group'])
    equal(result.groups[0].state, 'PreparingRebalance')
    deepStrictEqual(result.groups[0].members[0].memberMetadata, memberMetadata)
    deepStrictEqual(result.groups[0].members[0].memberAssignment, memberAssignment)
  })
})

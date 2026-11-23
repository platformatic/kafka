import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import test, { type TestContext } from 'node:test'
import {
  findCoordinatorV6,
  incrementalAlterConfigsV1,
  shareAcknowledgeV1,
  shareAcknowledgeV2,
  shareFetchV1,
  shareFetchV2,
  shareGroupDescribeV1,
  shareGroupHeartbeatV1
} from '../../src/apis/index.ts'
import { Connection, FindCoordinatorKeyTypes, ShareAcknowledgeTypes, ShareAcquireModes } from '../../src/index.ts'
import { type ShareFetchResponse } from '../../src/apis/consumer/share-fetch-v1.ts'
import {
  createAdmin,
  createProducer,
  createTopic,
  forEachVersion,
  kafkaSingleBootstrapServers,
  waitFor
} from './helpers.ts'

// There is no share consumer client yet, so these drive the share group protocol by hand over one connection to the
// single broker, which is the group coordinator and the leader of every partition.

const GROUP_CONFIG_RESOURCE = 32
const SET_CONFIG = 0
const RECORDS = 5

interface ShareSession {
  connection: Connection
  groupId: string
  memberId: string
  topic: string
  topicId: string
  epoch: number
}

async function connect (t: TestContext): Promise<Connection> {
  const [host, port] = kafkaSingleBootstrapServers[0].split(':')
  const connection = new Connection('share-groups-compat')
  await connection.connect(host, Number(port))
  t.after(() => connection.close())
  return connection
}

async function prepareGroup (t: TestContext, connection: Connection, topic: string, groupId: string) {
  // Records produced before the group first fetches are skipped unless the group starts from the earliest offset.
  await incrementalAlterConfigsV1.api.async(
    connection,
    [
      {
        resourceType: GROUP_CONFIG_RESOURCE,
        resourceName: groupId,
        configs: [{ name: 'share.auto.offset.reset', configOperation: SET_CONFIG, value: 'earliest' }]
      }
    ],
    false
  )

  const producer = createProducer(t)
  await producer.send({
    messages: Array.from({ length: RECORDS }, (_, i) => ({ topic, partition: 0, value: Buffer.from(`value-${i}`) }))
  })

  await waitFor(() => findCoordinatorV6.api.async(connection, FindCoordinatorKeyTypes.GROUP, [groupId]), {
    interval: 200,
    timeout: 30000
  })
}

async function join (connection: Connection, groupId: string, memberId: string, topic: string, topicId: string) {
  let memberEpoch = 0

  await waitFor(
    async () => {
      const response = await shareGroupHeartbeatV1.api.async(connection, groupId, memberId, memberEpoch, null, [topic])
      memberEpoch = response.memberEpoch

      if (!response.assignment?.topicPartitions.some(assigned => assigned.topicId === topicId)) {
        throw new Error('Partition not assigned yet.')
      }
    },
    { interval: 200, timeout: 30000 }
  )
}

function topics (session: ShareSession, acknowledgeType?: number) {
  const acknowledgementBatches =
    acknowledgeType === undefined
      ? []
      : [{ firstOffset: 0n, lastOffset: BigInt(RECORDS - 1), acknowledgeTypes: [acknowledgeType] }]

  return [{ topicId: session.topicId, partitions: [{ partitionIndex: 0, acknowledgementBatches }] }]
}

async function fetch (
  session: ShareSession,
  version: number,
  acknowledgeType?: number,
  maxRecords: number = 100,
  acquireMode: number = ShareAcquireModes.BATCH_OPTIMIZED
): Promise<ShareFetchResponse> {
  const { connection, groupId, memberId } = session
  const epoch = session.epoch++

  return version === 1
    ? shareFetchV1.api.async(
      connection,
      groupId,
      memberId,
      epoch,
      500,
      1,
      1048576,
      maxRecords,
      100,
      topics(session, acknowledgeType),
      []
    )
    : shareFetchV2.api.async(
      connection,
      groupId,
      memberId,
      epoch,
      500,
      1,
      1048576,
      maxRecords,
      100,
      acquireMode,
      false,
      topics(session, acknowledgeType),
      []
    )
}

async function acknowledge (session: ShareSession, version: number, acknowledgeType: number) {
  const { connection, groupId, memberId } = session
  const epoch = session.epoch++
  const isRenewAck = acknowledgeType === ShareAcknowledgeTypes.RENEW

  return version === 1
    ? shareAcknowledgeV1.api.async(connection, groupId, memberId, epoch, topics(session, acknowledgeType))
    : shareAcknowledgeV2.api.async(connection, groupId, memberId, epoch, isRenewAck, topics(session, acknowledgeType))
}

async function fetchAcquired (session: ShareSession, version: number, maxRecords?: number, acquireMode?: number) {
  return waitFor(
    async () => {
      const response = await fetch(session, version, undefined, maxRecords, acquireMode)
      const partition = response.responses[0]?.partitions[0]

      if (!partition?.acquiredRecords.length) {
        throw new Error('No records acquired yet.')
      }

      return { response, partition }
    },
    { interval: 100, timeout: 30000 }
  )
}

async function openSession (t: TestContext): Promise<ShareSession> {
  const topic = await createTopic(t, 1)
  const metadata = await createAdmin(t).metadata({ topics: [topic] })
  const topicId = metadata.topics.get(topic)!.id
  const groupId = `share-group-${randomUUID()}`
  const memberId = randomUUID()
  const connection = await connect(t)

  await prepareGroup(t, connection, topic, groupId)
  await join(connection, groupId, memberId, topic, topicId)

  return { connection, groupId, memberId, topic, topicId, epoch: 0 }
}

test('the share group protocol round trips at every broker-supported version', async t => {
  await forEachVersion(t, createAdmin(t), 'ShareFetch', async version => {
    const session = await openSession(t)
    const { connection, groupId, memberId, topic, topicId } = session

    const first = await fetchAcquired(session, version)
    ok(first.response.acquisitionLockTimeoutMs > 0)
    deepStrictEqual(first.partition.acquiredRecords, [{ firstOffset: 0n, lastOffset: 4n, deliveryCount: 1 }])
    deepStrictEqual(
      first.partition.records.flatMap(batch => batch.records.map(record => record.value!.toString())),
      Array.from({ length: RECORDS }, (_, i) => `value-${i}`)
    )

    if (version >= 2) {
      const renewed = await acknowledge(session, version, ShareAcknowledgeTypes.RENEW)
      ok((renewed as Awaited<ReturnType<typeof shareAcknowledgeV2.api.async>>).acquisitionLockTimeoutMs > 0)
    }

    // A released record comes back with its delivery count increased, so the acknowledgement type reached the broker.
    const released = await acknowledge(session, version, ShareAcknowledgeTypes.RELEASE)
    strictEqual(released.responses[0].partitions[0].errorCode, 0)

    const second = await fetchAcquired(session, version)
    deepStrictEqual(second.partition.acquiredRecords, [{ firstOffset: 0n, lastOffset: 4n, deliveryCount: 2 }])

    const accepted = await fetch(session, version, ShareAcknowledgeTypes.ACCEPT)
    strictEqual(accepted.responses[0].partitions[0].acknowledgeErrorCode, 0)
    deepStrictEqual(accepted.responses[0].partitions[0].acquiredRecords, [])

    const described = await shareGroupDescribeV1.api.async(connection, [groupId], false)
    const [member] = described.groups[0].members
    strictEqual(member.memberId, memberId)
    deepStrictEqual(member.assignment.topicPartitions, [{ topicId, topicName: topic, partitions: [0] }])

    const left = await shareGroupHeartbeatV1.api.async(connection, groupId, memberId, -1, null, null)
    strictEqual(left.memberEpoch, -1)
  })
})

// In the default mode maxRecords is a hint and the broker hands out the whole batch of 5 records.
// Getting only 2 back shows the broker read the acquire mode.
test('ShareFetch v2 acquires at most maxRecords in record limit mode', async t => {
  await forEachVersion(t, createAdmin(t), 'ShareFetch', async (version, t) => {
    if (version < 2) {
      t.skip('the acquire mode was added in v2')
      return
    }

    const session = await openSession(t)
    const { partition } = await fetchAcquired(session, version, 2, ShareAcquireModes.RECORD_LIMIT)
    deepStrictEqual(partition.acquiredRecords, [{ firstOffset: 0n, lastOffset: 1n, deliveryCount: 1 }])
  })
})

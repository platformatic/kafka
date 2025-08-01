import { api as fetchV17 } from '../../../src/apis/consumer/fetch-v17.ts'
import { api as offsetCommitV9 } from '../../../src/apis/consumer/offset-commit-v9.ts'
import { api as offsetFetchV9 } from '../../../src/apis/consumer/offset-fetch-v9.ts'
import { api as syncGroupV5 } from '../../../src/apis/consumer/sync-group-v5.ts'
import { FetchIsolationLevels, FindCoordinatorKeyTypes } from '../../../src/apis/enumerations.ts'
import { api as findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator-v6.ts'
import { api as metadataV12 } from '../../../src/apis/metadata/metadata-v12.ts'
import type { KafkaRecord } from '../../../src/index.ts'
import { Connection } from '../../../src/network/connection.ts'
import { joinGroup, performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

const topicName = 'temp'
const groupId = 'g2'

const metadata = await performAPICallWithRetry(
  'Metadata',
  () => metadataV12.async(connection, ['temp'], false, false),
  0,
  3,
  true
)

const topicId = metadata.topics.find(t => t.name === topicName)!.topicId

await performAPICallWithRetry('FindCoordinator (GROUP)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.GROUP, [groupId]))

const joinGroupResponse = await joinGroup(connection, groupId, true)
const memberId = joinGroupResponse.memberId!

await performAPICallWithRetry('SyncGroup', () =>
  syncGroupV5.async(connection, groupId, joinGroupResponse.generationId, memberId, null, 'whatever', 'whatever', []))

const offsetFetch = await performAPICallWithRetry('OffsetFetch', () =>
  offsetFetchV9.async(
    connection,
    [
      {
        groupId,
        memberId: joinGroupResponse.memberId,
        memberEpoch: 0,
        topics: [
          {
            name: topicName,
            partitionIndexes: [0]
          }
        ]
      }
    ],
    true
  ))

let fetchOffset = offsetFetch.groups[0].topics[0].partitions[0].committedOffset

if (fetchOffset === -1n) {
  fetchOffset = 0n
}

process._rawDebug('\n\n----- BEGIN -----\n\n')

for (let i = 0; i < 3; i++) {
  console.log('Fetching messages...')

  const response = await performAPICallWithRetry(
    'Fetch',
    () =>
      fetchV17.async(
        connection,
        0,
        0,
        1024 ** 3,
        FetchIsolationLevels.READ_UNCOMMITTED,
        -1,
        -1,
        [
          {
            topicId,
            partitions: [
              {
                partition: 0,
                currentLeaderEpoch: -1,
                fetchOffset,
                lastFetchedEpoch: -1,
                partitionMaxBytes: 1024 ** 2
              }
            ]
          }
        ],
        [],
        ''
      ),
    0,
    3,
    true
  )

  const batches = response.responses[0].partitions[0].records

  if (!batches?.length) {
    break
  }

  const { nextOffset, records } = batches.reduce<{ nextOffset: bigint, records: KafkaRecord[] }>((acc, batch) => {
    acc.nextOffset = batch.firstOffset + BigInt(batch.records.length)
    acc.records.push(...batch.records)
    return acc
  }, { nextOffset: fetchOffset, records: [] })

  fetchOffset = nextOffset
  console.log(
    'Fetch',
    records.map(r => [r.key.toString(), r.value.toString()])
  )

  if (!records.length) {
    break
  }
}

process._rawDebug('\n\n----- END -----\n\n')

await performAPICallWithRetry('OffsetCommit', () =>
  offsetCommitV9.async(connection, groupId, joinGroupResponse.generationId, memberId, null, [
    {
      name: topicName,
      partitions: [{ partitionIndex: 0, committedOffset: fetchOffset, committedLeaderEpoch: -1, committedMetadata: '' }]
    }
  ]))

await connection.close()

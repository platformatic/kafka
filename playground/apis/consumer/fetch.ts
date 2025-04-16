import { fetchV17 } from '../../../src/apis/consumer/fetch.ts'
import { offsetCommitV9 } from '../../../src/apis/consumer/offset-commit.ts'
import { offsetFetchV9 } from '../../../src/apis/consumer/offset-fetch.ts'
import { syncGroupV5 } from '../../../src/apis/consumer/sync-group.ts'
import { FetchIsolationLevels, FindCoordinatorKeyTypes } from '../../../src/apis/enumerations.ts'
import { findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator.ts'
import { metadataV12 } from '../../../src/apis/metadata/metadata.ts'
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
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.GROUP, [groupId])
)

const joinGroupResponse = await joinGroup(connection, groupId, true)
const memberId = joinGroupResponse.memberId!

await performAPICallWithRetry('SyncGroup', () =>
  syncGroupV5.async(connection, groupId, joinGroupResponse.generationId, memberId, null, 'whatever', 'whatever', [])
)

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
  )
)

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

  const records = response.responses[0].partitions[0].records

  if (!records?.length) {
    break
  }

  fetchOffset = records.firstOffset + BigInt(records.records.length)
  console.log(
    'Fetch',
    records.records.map(r => [r.key.toString(), r.value.toString()])
  )

  if (!records.records?.length) {
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
  ])
)

await connection.close()

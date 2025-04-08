import { consumerGroupHeartbeatV0 } from '../../../src/apis/consumer/consumer-group-heartbeat.ts'
import { heartbeatV4 } from '../../../src/apis/consumer/heartbeat.ts'
import { leaveGroupV5 } from '../../../src/apis/consumer/leave-group.ts'
import { syncGroupV5 } from '../../../src/apis/consumer/sync-group.ts'
import { FindCoordinatorKeyTypes } from '../../../src/apis/enumerations.ts'
import { findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator.ts'
import { Connection } from '../../../src/network/connection.ts'
import { joinGroup, performAPICallWithRetry } from '../../utils.ts'

const performConsumerHeartbeat = false

const connection = new Connection('123')
await connection.connect('localhost', 9092)

const topicName = 'temp'
const groupId = 'g2'

await performAPICallWithRetry('FindCoordinator (GROUP)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.GROUP, [groupId])
)

const joinGroupResponse = await joinGroup(connection, groupId)
const memberId = joinGroupResponse.memberId!

await performAPICallWithRetry('Heartbeat', () =>
  heartbeatV4.async(connection, groupId, joinGroupResponse.generationId, memberId)
)

if (performConsumerHeartbeat) {
  await performAPICallWithRetry('ConsumerHeartbeat', () =>
    consumerGroupHeartbeatV0.async(connection, groupId, memberId, 0, null, null, 60000, [topicName], null, [])
  )
}

await performAPICallWithRetry('SyncGroup', () =>
  syncGroupV5.async(connection, groupId, joinGroupResponse.generationId, memberId, null, 'whatever', 'whatever', [])
)

await performAPICallWithRetry('LeaveGroup', () => leaveGroupV5.async(connection, groupId, [{ memberId }]))

await connection.close()

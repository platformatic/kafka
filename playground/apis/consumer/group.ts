import { api as consumerGroupHeartbeatV0 } from '../../../src/apis/consumer/consumer-group-heartbeat-v0.ts'
import { api as heartbeatV4 } from '../../../src/apis/consumer/heartbeat-v4.ts'
import { api as leaveGroupV5 } from '../../../src/apis/consumer/leave-group-v5.ts'
import { api as syncGroupV5 } from '../../../src/apis/consumer/sync-group-v5.ts'
import { FindCoordinatorKeyTypes } from '../../../src/apis/enumerations.ts'
import { api as findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator-v6.ts'
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

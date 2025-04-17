import { api as deleteGroupsV2 } from '../../../src/apis/admin/delete-groups.ts'
import { api as describeGroupsV5 } from '../../../src/apis/admin/describe-groups.ts'
import { api as listGroupsV5 } from '../../../src/apis/admin/list-groups.ts'
import { FindCoordinatorKeyTypes } from '../../../src/apis/enumerations.ts'
import { api as findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

const groupId = 'g2'

await performAPICallWithRetry('FindCoordinator (GROUP)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.GROUP, [groupId])
)

await performAPICallWithRetry('DescribeGroups', () => describeGroupsV5.async(connection, [groupId], true))

await performAPICallWithRetry('ListGroups', () => listGroupsV5.async(connection, ['STABLE'], []))

await performAPICallWithRetry('DeleteGroups', () => deleteGroupsV2.async(connection, [groupId]))

await connection.close()

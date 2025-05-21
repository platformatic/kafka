import { api as alterReplicaLogDirsV2 } from '../../../src/apis/admin/alter-replica-log-dirs-v2.ts'
import { api as describeLogDirsV4 } from '../../../src/apis/admin/describe-log-dirs-v4.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('DescribeLogDirs', () =>
  describeLogDirsV4.async(connection, [{ name: 'temp', partitions: [0] }])
)

await performAPICallWithRetry('AlterReplicaLogDirs', () =>
  alterReplicaLogDirsV2.async(connection, [
    { path: '/tmp/kraft-combined-logs', topics: [{ name: 'temp', partitions: [0] }] }
  ])
)

await performAPICallWithRetry('DescribeLogDirs', () =>
  describeLogDirsV4.async(connection, [{ name: 'temp', partitions: [0] }])
)

await connection.close()

import { alterPartitionReassignmentsV0 } from '../../src/apis/admin/alter-partition-reassignments.ts'
import { listPartitionReassignmentsV0 } from '../../src/apis/admin/list-partition-reassignments.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('AlterPartitionReassignments', () =>
  alterPartitionReassignmentsV0.async(connection, 1000, [
    {
      name: 'temp',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [1]
        }
      ]
    }
  ])
)

await performAPICallWithRetry('ListPartitionReassignments', () =>
  listPartitionReassignmentsV0.async(connection, 1000, [
    {
      name: 'temp',
      partitionIndexes: [0]
    }
  ])
)

await connection.close()

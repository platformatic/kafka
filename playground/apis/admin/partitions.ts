import { api as alterPartitionV3 } from '../../../src/apis/admin/alter-partition.ts'
import { api as createPartitionsV3 } from '../../../src/apis/admin/create-partitions.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const performCreatePartitions = false
const performAlterPartition = false

const connection = new Connection('123')
await connection.connect('localhost', 9092)

if (performCreatePartitions) {
  await performAPICallWithRetry('CreatePartitions', () =>
    createPartitionsV3.async(
      connection,
      [
        {
          name: 'temp',
          count: 2,
          assignments: [
            {
              brokerIds: [1]
            }
          ]
        }
      ],
      1000,
      true
    )
  )
}

if (performAlterPartition) {
  await performAPICallWithRetry('AlterPartition', () =>
    alterPartitionV3.async(connection, 1, -1n, [
      {
        topicId: '8d5e2533-cd5c-4498-9ec2-a4d7f741a044',
        partitions: [
          {
            partitionIndex: 0,
            leaderEpoch: 0,
            newIsrWithEpochs: [{ brokerId: 1, brokerEpoch: -1n }],
            leaderRecoveryState: 0,
            partitionEpoch: 0
          }
        ]
      }
    ])
  )
}

await connection.close()

import { deleteRecordsV2 } from '../../src/apis/admin/delete-records.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('DeleteRecords', () =>
  deleteRecordsV2.async(
    connection,
    [
      {
        name: 'temp',
        partitions: [
          {
            partitionIndex: 0,
            offset: 58n
          }
        ]
      }
    ],
    1000
  )
)

await connection.close()

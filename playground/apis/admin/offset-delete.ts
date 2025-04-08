import { offsetDeleteV0 } from '../../../src/apis/admin/offset-delete.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('OffsetDelete', () =>
  offsetDeleteV0.async(connection, 'g2', [
    {
      name: 'temp',
      partitions: [
        {
          partitionIndex: 0
        }
      ]
    }
  ])
)

await connection.close()

import { deleteTopicsV6 } from '../../src/apis/admin/delete-topics.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('DeleteTopics', () =>
  deleteTopicsV6.async(
    connection,
    [
      {
        name: 'temp'
      }
    ],
    1000
  )
)

await connection.close()

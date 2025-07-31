import { api as deleteTopicsV6 } from '../../../src/apis/admin/delete-topics-v6.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('DeleteTopics', () =>
  deleteTopicsV6.async(
    connection,
    [
      {
        name: 'temp'
      }
    ],
    1000
  ))

await connection.close()

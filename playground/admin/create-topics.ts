import { createTopicsV7 } from '../../src/apis/admin/create-topics.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('CreateTopics', () =>
  createTopicsV7.async(
    connection,
    [
      {
        name: 'temp',
        numPartitions: 1,
        replicationFactor: 1,
        assignments: [],
        configs: []
      }
    ],
    1000,
    false
  )
)

await connection.close()

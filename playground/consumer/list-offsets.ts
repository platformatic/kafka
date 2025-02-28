import { listOffsetsV9 } from '../../src/apis/consumer/list-offsets.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('ListOffsets', () =>
  listOffsetsV9.async(connection, -1, 0, [
    { name: 'temp', partitions: [{ partitionIndex: 0, currentLeaderEpoch: -1, timestamp: -1n }] }
  ])
)

await connection.close()

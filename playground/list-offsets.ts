import { listOffsetsV9 } from '../src/apis/list-offsets.ts'
import { Connection } from '../src/connection.ts'
import { inspectResponse } from '../src/utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

console.log(
  inspectResponse(
    'ListOffsets',
    await listOffsetsV9(connection, -1, 0, [
      { name: 'temp', partitions: [{ partitionIndex: 0, currentLeaderEpoch: -1, timestamp: -1n }] }
    ])
  )
)

await connection.close()

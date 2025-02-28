import { fetchV17 } from '../src/apis/fetch.ts'
import { metadataV12 } from '../src/apis/metadata.ts'
import { Connection } from '../src/connection.ts'
import { inspectResponse } from '../src/utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

const metadata = await metadataV12(connection, ['temp'], false, false)
const topic = metadata.topics.find(t => t.name === 'temp')!.topicId

console.log(
  inspectResponse(
    'Fetch',
    await fetchV17(
      connection,
      1000,
      0,
      1024 ** 3,
      0,
      -1,
      -1,
      [
        {
          topicId: topic,
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: -1,
              fetchOffset: 6n,
              lastFetchedEpoch: -1,
              partitionMaxBytes: 1024 ** 2
            }
          ]
        }
      ],
      [],
      ''
    )
  )
)

await connection.close()

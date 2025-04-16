import { initProducerIdV5 } from '../../../src/apis/producer/init-producer-id.ts'
import { produceV11 } from '../../../src/apis/producer/produce.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('my-client')
await connection.connect('localhost', 9092)

const { producerId, producerEpoch } = await initProducerIdV5.async(connection, null, 1000, 0n, 0)

// The following two calls are identical. Since the same producerId and producerEpoch are used,
// the second call will be discarded from the server
await performAPICallWithRetry('Produce', () =>
  produceV11.async(
    connection,
    1,
    0,
    [
      {
        topic: 'temp1',
        partition: 0,
        key: Buffer.from('111'),
        value: Buffer.from('222'),
        headers: new Map([
          [Buffer.from('a'), Buffer.from('123')],
          [Buffer.from('b'), Buffer.from([97, 98, 99])]
        ])
      },
      { topic: 'temp1', partition: 0, key: Buffer.from('333'), value: Buffer.from('444'), timestamp: 12345678n }
    ],
    { compression: 'zstd', producerId, producerEpoch }
  )
)

await performAPICallWithRetry('Produce', () =>
  produceV11.async(
    connection,
    1,
    0,
    [
      {
        topic: 'temp1',
        partition: 0,
        key: Buffer.from('111'),
        value: Buffer.from('222'),
        headers: new Map([
          [Buffer.from('a'), Buffer.from('123')],
          [Buffer.from('b'), Buffer.from([97, 98, 99])]
        ])
      },
      { topic: 'temp1', partition: 0, key: Buffer.from('333'), value: Buffer.from('444'), timestamp: 12345678n }
    ],
    { compression: 'zstd', producerId, producerEpoch }
  )
)

await connection.close()

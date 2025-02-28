import { produceV11 } from '../../src/apis/producer/produce.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

const NUM_RECORDS = 10
const prefix = Date.now().toString()

for (let i = 0; i < NUM_RECORDS; i++) {
  const key = `${prefix}-${i.toString().padStart(3, '0')}`

  await performAPICallWithRetry('Produce', () =>
    produceV11.async(connection, 1, 0, [
      // {
      //   topic: 'temp',
      //   partition: 0,
      //   key: 'aaa',
      //   value: 'bbb',
      //   headers: { ccc: 'ddd' }
      // }
      {
        topic: 'temp',
        partition: 0,
        key: key + '-1',
        value: Math.floor(Math.random() * 1e5).toString(),
        headers: { key: 'value' }
      },
      {
        topic: 'temp',
        partition: 0,
        key: key + '-2',
        value: Math.floor(Math.random() * 1e5).toString(),
        headers: { key: 'value' }
      }
    ])
  )
}

await connection.close()

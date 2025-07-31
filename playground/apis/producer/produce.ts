import { api as produceV11 } from '../../../src/apis/producer/produce-v11.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

const NUM_RECORDS = 1e4
const prefix = Date.now().toString()

for (let i = 0; i < NUM_RECORDS; i++) {
  const key = `${prefix}-${i.toString().padStart(3, '0')}`

  await performAPICallWithRetry('Produce', () =>
    produceV11.async(connection, 1, 0, [
      // {
      //   topic: 'temp',
      //   partition: 0,
      //   key: Buffer.from('aaa'),
      //   value: Buffer.from('bbb'),
      //   headers: new Map([[Buffer.from('ccc'), Buffer.from('ddd')]])
      // }
      {
        topic: 'temp1',
        partition: 0,
        key: Buffer.from(key + '-1'),
        value: Buffer.from(Math.floor(Math.random() * 1e5).toString()),
        headers: new Map([[Buffer.from('key'), Buffer.from('value')]])
      },
      {
        topic: 'temp2',
        partition: 0,
        key: Buffer.from(key + '-2'),
        value: Buffer.from(Math.floor(Math.random() * 1e5).toString()),
        headers: new Map([[Buffer.from('key'), Buffer.from('value')]])
      }
    ]))
}

await connection.close()

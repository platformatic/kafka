import { cronometro } from 'cronometro'
import { once } from 'node:events'
import { produceV11 } from '../src/apis/producer/produce.ts'
import { Connection } from '../src/connection.ts'

let connection: Connection
let kafkajs

console.log(
  await cronometro(
    {
      ours: {
        before: async () => {
          connection = new Connection('123')
          await connection.start('localhost', 9092)
        },
        async test () {
          for (let i = 0; i < 100; i++) {
            const canWrite = produceV11(
              connection,
              1,
              0,
              [
                {
                  topic: 'temp',
                  partition: 0,
                  key: '111',
                  value: '222',
                  headers: { a: '123', b: Buffer.from([97, 98, 99]) }
                }
              ],
              'none'
            )

            if (!canWrite) {
              await once(connection, 'drain')
            }
          }
        },
        after: async () => {
          connection.close()
        }
      }
      // kafkajs: {
      //   async before () {
      //     const client = new KafkaJS({
      //       clientId: 'my-app',
      //       brokers: ['localhost:9092']
      //     })

      //     kafkajs = client.producer()
      //     await kafkajs.connect()
      //   },
      //   test () {
      //     return kafkajs.send({
      //       topic: 'temp',
      //       messages: [{ key: '111', value: '222', partition: 0, headers: { a: '123', b: Buffer.from([97, 98, 99]) } }],
      //       acks: 1
      //     })
      //   }
      // }
    },
    { iterations: 100, warmup: true }
  )
)

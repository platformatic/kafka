import { cronometro } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { produceV11 } from '../src/apis/produce.ts'
import { Connection } from '../src/connection.ts'

let connection: Connection
let kafkajs

cronometro(
  {
    ours: {
      before: async () => {
        connection = new Connection('123')
        await connection.start('localhost', 9092)
      },
      test () {
        return produceV11(connection, 1, 0, [
          {
            topic: 'temp',
            partition: 0,
            key: '111',
            value: '222',
            headers: { a: '123', b: Buffer.from([97, 98, 99]) }
          }
        ])
      },
      after: async () => {
        connection.close()
      }
    },
    kafkajs: {
      async before () {
        const client = new KafkaJS({
          clientId: 'my-app',
          brokers: ['localhost:9092']
        })

        kafkajs = client.producer()
        await kafkajs.connect()
      },
      test () {
        return kafkajs.send({
          topic: 'temp',
          messages: [{ key: '111', value: '222', partition: 0, headers: { a: '123', b: Buffer.from([97, 98, 99]) } }],
          acks: 1
        })
      }
    }
  },
  { iterations: 10000, warmup: false }
)

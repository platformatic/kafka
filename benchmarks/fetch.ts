import { cronometro } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { fetchV17 } from '../src/apis/fetch.ts'
import { metadataV12 } from '../src/apis/metadata.ts'
import { Connection } from '../src/network/connection.ts'

const THRESHOLD = 100

let connection: Connection
let kafkajs
let topic
let messages: any = []
let promise

console.log(
  await cronometro(
    {
      ours: {
        before: async () => {
          connection = new Connection('123')
          await connection.connect('localhost', 9092)

          const metadata = await metadataV12(connection, ['temp'], false, false)
          topic = metadata.topics.find(t => t.name === 'temp')!.topicId
        },
        async test () {
          messages = []

          while (messages.length < THRESHOLD) {
            messages.push(
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
                        fetchOffset: 0n,
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
          }
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

          kafkajs = client.consumer({ groupId: 'test-group' })
          await kafkajs.connect()
          await kafkajs.subscribe({ topics: ['temp'], fromBeginning: true })

          await kafkajs.run({
            eachMessage: async ({ topic, message, pause }) => {
              messages.push(message)

              if (messages.length === THRESHOLD) {
                pause([{ topic: 'temp', partitions: [0], fromBeginning: true }])
                promise.resolve()
              }
            }
          })

          kafkajs.pause([{ topic: 'temp', partitions: [0], fromBeginning: true }])
        },
        async test () {
          messages = []
          kafkajs.resume([{ topic: 'temp', partitions: [0], fromBeginning: true }])
          promise = Promise.withResolvers()

          return promise.promise
        }
      }
    },
    { iterations: 10, warmup: false }
  )
)

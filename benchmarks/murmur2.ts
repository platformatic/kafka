import cronometro from 'cronometro'
// @ts-ignore
import kafkaJsMurmur2 from 'kafkajs/src/producer/partitioners/default/murmur2.js'
import { randomBytes } from 'node:crypto'
import { murmur2 } from '../src/index.ts'

await cronometro(
  {
    pltKafka () {
      const value = randomBytes(16)
      return murmur2(value)
    },
    KafkaJS () {
      const value = randomBytes(16)
      return kafkaJsMurmur2(value)
    }
  },
  { iterations: 5e4, print: { compare: true } }
)

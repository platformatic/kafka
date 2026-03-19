import cronometro from 'cronometro'
// @ts-ignore - Not explicitly exported by kafkajs
import kafkaJsMurmur2 from 'kafkajs/src/producer/partitioners/default/murmur2.js'
import { randomBytes } from 'node:crypto'
import { murmur2 } from '../src/index.ts'

cronometro(
  {
    kafkajs () {
      const value = randomBytes(16)
      return kafkaJsMurmur2(value)
    },
    /* eslint-disable-next-line @stylistic/space-before-function-paren */
    '@platformatic/kafka'() {
      const value = randomBytes(16)
      return murmur2(value)
    }
  },
  { print: { compare: true, compareMode: 'previous' } }
)

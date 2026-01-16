import { ProduceAcks, Producer, debugDump, stringSerializers } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

const producer = new Producer({
  clientId: 'id',
  bootstrapBrokers: kafkaSingleBootstrapServers,
  idempotent: true,
  autocreateTopics: true,
  serializers: stringSerializers,
  strict: true
})

function callbackProduce (cb?: Function): void {
  producer.send(
    {
      messages: [
        {
          topic: 'temp1',
          key: 'key-idempotent',
          value: 'value-idempotent',
          headers: new Map([['header-idempotent', 'header-value-idempotent']]),
          partition: 0
        }
      ],
      acks: ProduceAcks.ALL
    },
    (error, result) => {
      if (error) {
        console.error('ERROR', error)
        return
      }

      debugDump('produce', result)
      cb?.()
    }
  )
}

callbackProduce(() => {
  callbackProduce()
})

callbackProduce(() => {
  callbackProduce(() => {
    producer.close()
  })
})

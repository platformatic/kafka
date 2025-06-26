import { ProduceAcks, Producer, debugDump, stringSerializers } from '../../src/index.ts'

const producer = new Producer({
  clientId: 'id',
  bootstrapBrokers: ['localhost:9092'],
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
    (...args) => {
      const [error, result] = args
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

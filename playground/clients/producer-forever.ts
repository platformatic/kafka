import { Producer, debugDump, sleep, stringSerializers } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

const producer = new Producer({
  clientId: 'id',
  bootstrapBrokers: kafkaSingleBootstrapServers,
  serializers: stringSerializers,
  strict: true
})

let i = 0
while (true) {
  i++

  try {
    debugDump(
      'produce',
      await producer.send({
        messages: [
          {
            topic: 'temp1',
            key: `key-${i}`,
            value: `value-${i}`,
            headers: new Map([[`header-key-${i}`, `header-value-${i}`]]),
            partition: i % 2
          }
        ]
      })
    )
    await sleep(100)
  } catch (e) {
    debugDump(e)
    break
  }
}

await producer.close()

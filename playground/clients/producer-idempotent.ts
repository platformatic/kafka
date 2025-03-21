import { ProduceAcks, Producer } from '../../src/index.ts'
import { inspect } from '../utils.ts'

const producer = new Producer('id', ['localhost:9092'], { idempotent: true, autocreateTopics: true })

if (process.env.FOREVER) {
  let i = 0
  while (true) {
    i++

    try {
      inspect(await producer.produce([{ topic: 'temp1', key: `key-${i}`, value: `value-${i}` }]))
    } catch (e) {
      inspect(e)
      break
    }
  }

  await producer.close()
} else {
  function callbackProduce (cb?: Function): void {
    producer.produce(
      [{ topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } }],
      { acks: ProduceAcks.ALL },
      (error, result) => {
        if (error) {
          console.error('ERROR', error)
          return
        }

        inspect('produce(callbacks)', result)
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
}

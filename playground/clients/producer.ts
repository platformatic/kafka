import { ProduceAcks, Producer } from '../../src/index.ts'
import { inspect } from '../utils.ts'

const producer = new Producer('id', ['localhost:9092'])

if (process.env.PROMISES) {
  inspect(
    'produce(callbacks)',
    await Promise.all([
      producer.produce(
        [
          { topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key2', value: 'value2' },
          { topic: 'temp1', key: 'key3', value: 'value3', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key4', value: 'value4' },
          { topic: 'temp1', key: 'key5', value: 'value5', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key6', value: 'value6' }
        ],
        { acks: ProduceAcks.LEADER }
      ),
      producer.produce(
        [
          { topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key2', value: 'value2' },
          { topic: 'temp1', key: 'key3', value: 'value3', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key4', value: 'value4' },
          { topic: 'temp1', key: 'key5', value: 'value5', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key6', value: 'value6' }
        ],
        { acks: ProduceAcks.LEADER }
      ),

      producer.produce(
        [
          { topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key2', value: 'value2' },
          { topic: 'temp1', key: 'key3', value: 'value3', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key4', value: 'value4' },
          { topic: 'temp1', key: 'key5', value: 'value5', headers: { headerKey: 'headerValue' } },
          { topic: 'temp1', key: 'key6', value: 'value6' }
        ],
        { acks: ProduceAcks.LEADER }
      )
    ])
  )

  await producer.close()
} else {
  producer.produce(
    [
      { topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key2', value: 'value2' },
      { topic: 'temp1', key: 'key3', value: 'value3', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key4', value: 'value4' },
      { topic: 'temp1', key: 'key5', value: 'value5', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key6', value: 'value6' }
    ],
    { acks: ProduceAcks.LEADER },
    (error, result) => {
      if (error) {
        console.error('ERROR', error)
        return
      }

      inspect('produce(callbacks)', result)
    }
  )

  producer.produce(
    [
      { topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key2', value: 'value2' },
      { topic: 'temp1', key: 'key3', value: 'value3', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key4', value: 'value4' },
      { topic: 'temp1', key: 'key5', value: 'value5', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key6', value: 'value6' }
    ],
    { acks: ProduceAcks.LEADER },
    (error, result) => {
      if (error) {
        console.error('ERROR', error)
        return
      }

      inspect('produce(callbacks)', result)
    }
  )

  producer.produce(
    [
      { topic: 'temp1', key: 'key1', value: 'value1', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key2', value: 'value2' },
      { topic: 'temp1', key: 'key3', value: 'value3', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key4', value: 'value4' },
      { topic: 'temp1', key: 'key5', value: 'value5', headers: { headerKey: 'headerValue' } },
      { topic: 'temp1', key: 'key6', value: 'value6' }
    ],
    { acks: ProduceAcks.LEADER },
    (error, result) => {
      if (error) {
        console.error('ERROR', error)
        producer.close()
        return
      }

      inspect('produce(callbacks)', result)
      producer.close()
    }
  )
}

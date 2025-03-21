import { Consumer } from '../../src/index.ts'
import { inspect, sleep } from '../utils.ts'

const consumer = new Consumer('id6', 'id', ['localhost:9092'])

if (process.env.PROMISES) {
  inspect(
    'joinGroup(promises)',
    await consumer.joinGroup({
      sessionTimeout: 10000,
      heartbeatInterval: 500
    })
  )

  await sleep(3000)
  await consumer.leaveGroup()
  await consumer.close()
} else {
  consumer.joinGroup((error, result) => {
    if (error) {
      console.error(error)
      consumer.close()
      return
    }

    inspect('joinGroup(callbacks)', result)

    setTimeout(() => {
      consumer.leaveGroup(error => {
        if (error) {
          console.error(error)
        }

        consumer.close()
      })
    }, 10000)
  })
}

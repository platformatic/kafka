import { Admin } from '../../src/index.ts'
import { inspect } from '../utils.ts'

const admin = new Admin('id', ['localhost:9092'])

if (process.env.PROMISES) {
  try {
    await admin.deleteTopics(['temp1', 'temp2'])
  } catch (e) {
    console.error(e)
  }

  await admin.createTopics(['temp1'], { partitions: 3, replicas: 1 })

  await admin.createTopics(['temp2'], {
    partitions: -1,
    replicas: -1,
    assignments: [
      { partitionIndex: 0, brokerIds: [6] },
      { partitionIndex: 1, brokerIds: [4] },
      { partitionIndex: 2, brokerIds: [5] }
    ]
  })

  inspect('metadata(promises)', await admin.metadata(['temp1', 'temp2']))

  await admin.close()
} else if (process.env.SINGLE) {
  admin.deleteTopics(['temp1', 'temp2'], () => {
    admin.createTopics(['temp1', 'temp2'], error => {
      if (error) {
        console.error(error)
        admin.close()
        return
      }

      admin.metadata(['temp1', 'temp2'], (error, result) => {
        if (error) {
          console.error('ERROR', error)
          return
        }

        inspect('metadata(callbacks)', result)
        admin.close()
      })
    })
  })
} else {
  admin.deleteTopics(['temp1', 'temp2'], error => {
    if (error) {
      console.error(error)
    }

    admin.createTopics(['temp1'], { partitions: 3, replicas: 1 }, error => {
      if (error) {
        console.error(error)
        admin.close()
        return
      }

      admin.createTopics(
        ['temp2'],
        {
          partitions: -1,
          replicas: -1,
          assignments: [
            { partitionIndex: 0, brokerIds: [6] },
            { partitionIndex: 1, brokerIds: [4] },
            { partitionIndex: 2, brokerIds: [5] }
          ]
        },
        error => {
          if (error) {
            console.error(error)
            admin.close()
            return
          }

          admin.metadata(['temp1', 'temp2'], (error, result) => {
            if (error) {
              console.error('ERROR', error)
              return
            }

            inspect('metadata(callbacks)', result)
            admin.close()
          })
        }
      )
    })
  })
}

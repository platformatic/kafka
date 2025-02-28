import { FindCoordinatorKeyTypes } from '../../src/apis/enumerations.ts'
import { findCoordinatorV6 } from '../../src/apis/metadata/find-coordinator.ts'
import { addOffsetsToTxnV4 } from '../../src/apis/producer/add-offsets-to-txn.ts'
import { addPartitionsToTxnV5 } from '../../src/apis/producer/add-partitions-to-txn.ts'
import { endTxnV4 } from '../../src/apis/producer/end-txn.ts'
import { initProducerIdV5 } from '../../src/apis/producer/init-producer-id.ts'
import { produceV11 } from '../../src/apis/producer/produce.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('kafka')
await connection.start('localhost', 9092)

const transactionalId = '11'
const groupId = 'g1'

await performAPICallWithRetry('FindCoordinator (TRANSACTION)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.TRANSACTION, [transactionalId])
)

const { producerId, producerEpoch } = await performAPICallWithRetry('InitProducerId', () =>
  initProducerIdV5.async(connection, transactionalId, 15000, -1n, -1)
)

await performAPICallWithRetry('AddPartitionsToTnx', () =>
  addPartitionsToTxnV5.async(connection, [
    {
      transactionalId,
      producerId,
      producerEpoch,
      verifyOnly: false,
      topics: [{ name: 'temp', partitions: [0] }]
    }
  ])
)

await performAPICallWithRetry('Produce', () =>
  produceV11.async(
    connection,
    1,
    0,
    [
      { topic: 'temp', partition: 0, key: '111', value: '222', headers: { a: '123', b: Buffer.from([97, 98, 99]) } },
      { topic: 'temp', partition: 0, key: '333', value: '444', timestamp: 12345678n }
    ],
    { compression: 'zstd', producerId, producerEpoch, transactionalId }
  )
)

await performAPICallWithRetry('Produce', () =>
  produceV11.async(
    connection,
    1,
    0,
    [
      { topic: 'temp', partition: 0, key: '222', value: '333', headers: { a: '123', b: Buffer.from([97, 98, 99]) } },
      { topic: 'temp', partition: 0, key: '444', value: '555', timestamp: 12345678n }
    ],
    { compression: 'gzip', producerId, producerEpoch, transactionalId, firstSequence: 2 }
  )
)

await performAPICallWithRetry('AddOffsetsToTnx', () =>
  addOffsetsToTxnV4.async(connection, transactionalId, producerId, producerEpoch, groupId)
)

// This is only needed if the transactional producer is also a consumer
// await performAPICallWithRetry('TxnOffsetCommitV4', () =>
//   txnOffsetCommitV4.async(connection, transactionalId, groupId, producerId, producerEpoch, 3, member, null, [
//     {
//       name: 'temp',
//       partitions: [{ partitionIndex: 0, committedOffset: 0n, committedLeaderEpoch: 0, committedMetadata: null }]
//     }
//   ])
// )

// Commit
await performAPICallWithRetry('EndTxn', () =>
  endTxnV4.async(connection, transactionalId, producerId, producerEpoch, true)
)

// Abort
// await performAPICallWithRetry('EndTxn', () =>
//   endTxnV4.async(connection, transactionalId, producerId, producerEpoch, false)
// )

await connection.close()

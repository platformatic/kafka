import { FindCoordinatorKeyTypes } from '../../../src/apis/enumerations.ts'
import { api as findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator-v6.ts'
import { api as initProducerIdV5 } from '../../../src/apis/producer/init-producer-id-v5.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const transactionalId = 'eeeee'

const connection = new Connection('111')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('FindCoordinator (TRANSACTION)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.TRANSACTION, [transactionalId])
)

await performAPICallWithRetry('InitProducerId', () =>
  initProducerIdV5.async(connection, transactionalId, 60000, -1n, -1)
)

await connection.close()

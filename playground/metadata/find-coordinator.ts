import { FindCoordinatorKeyTypes } from '../../src/apis/enumerations.ts'
import { findCoordinatorV6 } from '../../src/apis/metadata/find-coordinator.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('foo')
await connection.start('localhost', 9092)

await performAPICallWithRetry('FindCoordinator (GROUP)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.GROUP, ['f1'])
)

await performAPICallWithRetry('FindCoordinator (TRANSACTION)', () =>
  findCoordinatorV6.async(connection, FindCoordinatorKeyTypes.TRANSACTION, ['f1'])
)

await connection.close()

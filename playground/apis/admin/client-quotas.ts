import { api as alterClientQuotasV1 } from '../../../src/apis/admin/alter-client-quotas.ts'
import { api as describeClientQuotasV0 } from '../../../src/apis/admin/describe-client-quotas.ts'
import { ClientQuotaMatchTypes } from '../../../src/apis/enumerations.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('DescribeClientQuotas', () =>
  describeClientQuotasV0.async(
    connection,
    [
      {
        entityType: 'client-id',
        matchType: ClientQuotaMatchTypes.EXACT,
        match: 'test-id'
      }
    ],
    false
  )
)

await performAPICallWithRetry('AlterClientQuotas', () =>
  alterClientQuotasV1.async(
    connection,
    [
      {
        entities: [
          {
            entityType: 'client-id',
            entityName: 'test-id'
          }
        ],
        ops: [
          {
            key: 'producer_byte_rate',
            value: 1000,
            remove: false
          }
        ]
      }
    ],
    false
  )
)

await performAPICallWithRetry('DescribeClientQuotas', () =>
  describeClientQuotasV0.async(
    connection,
    [
      {
        entityType: 'client-id',
        matchType: ClientQuotaMatchTypes.EXACT,
        match: 'test-id'
      }
    ],
    false
  )
)

await connection.close()

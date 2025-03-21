import { alterConfigsV2 } from '../../../src/apis/admin/alter-configs.ts'
import { describeConfigsV4 } from '../../../src/apis/admin/describe-configs.ts'
import { incrementalAlterConfigsV1 } from '../../../src/apis/admin/incremental-alter-configs.ts'
import { IncrementalAlterConfigTypes, ResourceTypes } from '../../../src/apis/enumerations.ts'
import { Connection } from '../../../src/connection/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('DescribeConfigs', () =>
  describeConfigsV4.async(
    connection,
    [
      {
        resourceType: ResourceTypes.CLUSTER,
        resourceName: '1',
        configurationKeys: ['compression.type']
      }
    ],
    false,
    false
  )
)

await performAPICallWithRetry('AlterConfig', () =>
  alterConfigsV2.async(
    connection,
    [
      {
        resourceType: ResourceTypes.CLUSTER,
        resourceName: '1',
        configs: [
          {
            name: 'compression.type',
            value: 'gzip'
          }
        ]
      }
    ],
    false
  )
)

await performAPICallWithRetry('IncrementalAlterConfig', () =>
  incrementalAlterConfigsV1.async(
    connection,
    [
      {
        resourceType: ResourceTypes.CLUSTER,
        resourceName: '1',
        configs: [
          {
            name: 'compression.type',
            configOperation: IncrementalAlterConfigTypes.SET,
            value: 'gzip'
          }
        ]
      }
    ],
    false
  )
)

await performAPICallWithRetry('DescribeConfigs', () =>
  describeConfigsV4.async(
    connection,
    [
      {
        resourceType: ResourceTypes.CLUSTER,
        resourceName: '1',
        configurationKeys: ['compression.type']
      }
    ],
    false,
    false
  )
)

await connection.close()

import { api as alterConfigsV2 } from '../../../src/apis/admin/alter-configs.ts'
import { api as describeConfigsV4 } from '../../../src/apis/admin/describe-configs.ts'
import { api as incrementalAlterConfigsV1 } from '../../../src/apis/admin/incremental-alter-configs.ts'
import { IncrementalAlterConfigTypes, ResourceTypes } from '../../../src/apis/enumerations.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 29092)

await performAPICallWithRetry('DescribeConfigs', () =>
  describeConfigsV4.async(
    connection,
    [
      {
        resourceType: ResourceTypes.CLUSTER,
        resourceName: '4',
        configurationKeys: ['log.retention.ms', 'log.retention.bytes', 'offsets.retention.minutes']
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

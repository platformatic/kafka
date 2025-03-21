import { consumerGroupDescribeV0 } from '../../../src/apis/admin/consumer-group-describe.ts'
import { describeClusterV1 } from '../../../src/apis/admin/describe-cluster.ts'
import { describeProducersV0 } from '../../../src/apis/admin/describe-producers.ts'
import { describeQuorumV2 } from '../../../src/apis/admin/describe-quorum.ts'
import { describeTopicPartitionsV0 } from '../../../src/apis/admin/describe-topic-partitions.ts'
import { describeTransactionsV0 } from '../../../src/apis/admin/describe-transactions.ts'
import { envelopeV0 } from '../../../src/apis/admin/envelope.ts'
import { listTransactionsV0 } from '../../../src/apis/admin/list-transactions.ts'
import { unregisterBrokerV0 } from '../../../src/apis/admin/unregister-broker.ts'
import { updateFeaturesV1 } from '../../../src/apis/admin/update-features.ts'
import { DescribeClusterEndpointTypes, FeatureUpgradeTypes } from '../../../src/apis/enumerations.ts'
import { Connection } from '../../../src/connection/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const performDescribeQuorum = false
const performUpdateFeatures = false
const performEnvelope = false
const performUnregisterBroker = false
const performConsumerGroupDescribe = false

const connection = new Connection('123')
await connection.connect('localhost', 9092)

if (performDescribeQuorum) {
  await performAPICallWithRetry('DescribeQuorum', () =>
    describeQuorumV2.async(connection, [
      {
        topicName: 'temp',
        partitions: [{ partitionIndex: 0 }]
      }
    ])
  )
}

if (performUpdateFeatures) {
  await performAPICallWithRetry('UpdateFeatures', () =>
    updateFeaturesV1.async(
      connection,
      1000,
      [
        {
          feature: 'feature',
          maxVersionLevel: 3,
          upgradeType: FeatureUpgradeTypes.UPGRADE
        }
      ],
      false
    )
  )
}

if (performEnvelope) {
  await performAPICallWithRetry('Envelope', () =>
    envelopeV0.async(
      connection,
      Buffer.from('request_data'),
      Buffer.from('request_principal'),
      Buffer.from('127.0.0.1:80')
    )
  )
}

await performAPICallWithRetry('DescribeCluster (BROKERS)', () =>
  describeClusterV1.async(connection, true, DescribeClusterEndpointTypes.BROKERS)
)

await performAPICallWithRetry('DescribeProducers', () =>
  describeProducersV0.async(connection, [{ name: 'temp', partitionIndexes: [0] }])
)

if (performUnregisterBroker) {
  await performAPICallWithRetry('UnregisterBroker', () => unregisterBrokerV0.async(connection, 1))
}

await performAPICallWithRetry('DescribeTransactions', () => describeTransactionsV0.async(connection, ['11']))

await performAPICallWithRetry('ListTransactions', () => listTransactionsV0.async(connection, ['EMPTY'], [], -1n))

if (performConsumerGroupDescribe) {
  await performAPICallWithRetry('ConsumerGroupDescribe', () => consumerGroupDescribeV0.async(connection, ['g2'], true))
}

await performAPICallWithRetry('DescribeTopicPartitions', () =>
  describeTopicPartitionsV0.async(connection, [{ name: 'temp' }], 0, {
    topicName: 'temp',
    partitionIndex: 0
  })
)

await connection.close()

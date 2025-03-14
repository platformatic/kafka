import { CompressionTypes } from 'kafkajs'
import { getTelemetrySubscriptionsV0 } from '../../src/apis/telemetry/get-telemetry-subscriptions.ts'
import { listClientMetricsResourcesV0 } from '../../src/apis/telemetry/list-client-metrics-resources.ts'
import { pushTelemetryV0 } from '../../src/apis/telemetry/push-telemetry.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

const { clientInstanceId, subscriptionId } = await performAPICallWithRetry('GetTelemetrySubscriptions', () =>
  getTelemetrySubscriptionsV0.async(connection)
)

await performAPICallWithRetry('ListClientMetricsResources', () => listClientMetricsResourcesV0.async(connection))

await performAPICallWithRetry('PushTelemetry', () =>
  pushTelemetryV0.async(
    connection,
    clientInstanceId,
    subscriptionId,
    false,
    CompressionTypes.None,
    Buffer.from('metrics')
  )
)
await connection.close()

import { CompressionTypes } from 'kafkajs'
import { api as getTelemetrySubscriptionsV0 } from '../../../src/apis/telemetry/get-telemetry-subscriptions-v0.ts'
import { api as listClientMetricsResourcesV0 } from '../../../src/apis/telemetry/list-client-metrics-resources-v0.ts'
import { api as pushTelemetryV0 } from '../../../src/apis/telemetry/push-telemetry-v0.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

const { clientInstanceId, subscriptionId } = await performAPICallWithRetry('GetTelemetrySubscriptions', () =>
  getTelemetrySubscriptionsV0.async(connection))

await performAPICallWithRetry('ListClientMetricsResources', () => listClientMetricsResourcesV0.async(connection))

await performAPICallWithRetry('PushTelemetry', () =>
  pushTelemetryV0.async(
    connection,
    clientInstanceId,
    subscriptionId,
    false,
    CompressionTypes.None,
    Buffer.from('metrics')
  ))
await connection.close()

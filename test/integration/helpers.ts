import { randomUUID } from 'node:crypto'
import { type TestContext } from 'node:test'
import { type AdminOptions } from '../../src/clients/admin/index.ts'
import {
  type ConsumerOptions,
  type ProducerOptions,
  stringDeserializers,
  stringSerializers
} from '../../src/index.ts'
import {
  createAdmin as baseCreateAdmin,
  createConsumer as baseCreateConsumer,
  createProducer as baseCreateProducer,
  kafkaBootstrapServers,
  kafkaSingleBootstrapServers,
  waitFor
} from '../helpers.ts'

export { stringDeserializers, stringSerializers }
export { kafkaBootstrapServers, kafkaSingleBootstrapServers, waitFor }
export {
  forEachVersion,
  implementedVersions,
  isUnsupportedVersion,
  legacyBroker,
  pinApiVersions,
  runAtVersion,
  usableVersions
} from '../helpers/api-versions.ts'

// The version sweeps run against the single broker: they repeat the same scenario many times over
// and the cluster's rebalances make that both slower and flakier for no extra coverage. The
// scenarios which genuinely need three brokers or an authorizer ask for the cluster explicitly.

export function createAdmin (t: TestContext, options: Partial<AdminOptions> = {}) {
  return baseCreateAdmin(t, { bootstrapBrokers: kafkaSingleBootstrapServers, ...options })
}

export function createProducer<K = Buffer, V = Buffer, HK = Buffer, HV = Buffer> (
  t: TestContext,
  options: Partial<ProducerOptions<K, V, HK, HV>> = {}
) {
  return baseCreateProducer<K, V, HK, HV>(t, {
    bootstrapBrokers: kafkaSingleBootstrapServers,
    autocreateTopics: false,
    ...options
  })
}

export function createConsumer<K = Buffer, V = Buffer, HK = Buffer, HV = Buffer> (
  t: TestContext,
  options: Partial<ConsumerOptions<K, V, HK, HV>> = {}
) {
  return baseCreateConsumer<K, V, HK, HV>(t, {
    bootstrapBrokers: kafkaSingleBootstrapServers,
    autocreateTopics: false,
    ...options
  })
}

/**
 * Creates a topic with an explicit partition count and replication factor.
 *
 * The shared helper omits both and lets Admin send -1, meaning "use the broker default". That is
 * KIP-464, which brokers only accept from Apache Kafka 2.4 (CreateTopics v4): older ones answer
 * INVALID_PARTITIONS or INVALID_REPLICATION_FACTOR, so the sweeps have to be explicit to run
 * against them at all.
 */
export async function createTopic (
  t: TestContext,
  partitions = 1,
  bootstrapBrokers = kafkaSingleBootstrapServers
): Promise<string> {
  const topic = `compat-topic-${randomUUID()}`
  const replicas = bootstrapBrokers === kafkaBootstrapServers ? 3 : 1
  const admin = createAdmin(t, { bootstrapBrokers })

  await admin.createTopics({ topics: [topic], partitions, replicas })

  await waitFor(
    async () => {
      const metadata = await admin.metadata({ topics: [topic], forceUpdate: true })
      const described = metadata.topics.get(topic)

      if (described?.partitions.length !== partitions || described.partitions.some(p => !p || p.leader < 0)) {
        throw new Error(`Topic ${topic} is not ready.`)
      }

      return true
    },
    { interval: 100, timeout: 30000 }
  )

  return topic
}

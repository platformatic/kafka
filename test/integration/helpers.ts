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
  createTopic as baseCreateTopic,
  kafkaBootstrapServers,
  kafkaSingleBootstrapServers,
  waitFor
} from '../helpers.ts'

export { stringDeserializers, stringSerializers }
export { kafkaBootstrapServers, kafkaSingleBootstrapServers, waitFor }
export { forEachVersion, implementedVersions, pinApiVersions, usableVersions } from '../helpers/api-versions.ts'

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

export function createTopic (t: TestContext, partitions = 1, bootstrapBrokers = kafkaSingleBootstrapServers) {
  return baseCreateTopic(t, true, partitions, bootstrapBrokers)
}

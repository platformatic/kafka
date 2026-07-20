import { readFileSync } from 'node:fs'
import { type BaseOptions } from '../../clients/base/types.ts'
import { type ConsumerOptions } from '../../clients/consumer/types.ts'
import { type ProducerOptions } from '../../clients/producer/types.ts'
import { rangeAssigner } from '../../clients/consumer/partitions-assigners.ts'
import { CODES, compatibilityError } from './errors.ts'
import { type RdkafkaConfig } from './types.ts'

export interface OauthBearerToken {
  value: string
}

function value<T> (config: RdkafkaConfig, key: string): T | undefined {
  return config[key] as T | undefined
}

function brokers (config: RdkafkaConfig): string[] {
  const raw = value<string>(config, 'bootstrap.servers') ?? value<string>(config, 'metadata.broker.list') ?? 'localhost:9092'
  return raw.split(',').map(broker => broker.trim()).filter(Boolean)
}

function certificate (config: RdkafkaConfig, pemKey: string, locationKey: string): string | Buffer | undefined {
  const pem = value<string | Buffer>(config, pemKey)
  if (pem !== undefined) {
    return pem
  }

  const location = value<string>(config, locationKey)
  return location === undefined ? undefined : readFileSync(location)
}

function baseOptions (config: RdkafkaConfig, oauthBearerToken?: OauthBearerToken): BaseOptions {
  const options: BaseOptions = {
    clientId: value<string>(config, 'client.id') ?? 'node-rdkafka',
    bootstrapBrokers: brokers(config)
  }

  options.connectTimeout = value<number>(config, 'socket.timeout.ms')
  options.requestTimeout = value<number>(config, 'request.timeout.ms')
  options.metadataMaxAge = value<number>(config, 'topic.metadata.refresh.interval.ms')
  options.maxInflights = value<number>(config, 'max.in.flight.requests.per.connection')
  options.autocreateTopics = value<boolean>(config, 'allow.auto.create.topics')

  const protocol = value<string>(config, 'security.protocol')?.toUpperCase()
  if (protocol?.includes('SSL')) {
    options.tls = {
      ca: certificate(config, 'ssl.ca.pem', 'ssl.ca.location'),
      cert: certificate(config, 'ssl.certificate.pem', 'ssl.certificate.location'),
      key: certificate(config, 'ssl.key.pem', 'ssl.key.location'),
      passphrase: value<string>(config, 'ssl.key.password'),
      rejectUnauthorized: value<boolean>(config, 'enable.ssl.certificate.verification') ?? true
    }
  }

  if (protocol?.includes('SASL')) {
    const mechanism = (value<string>(config, 'sasl.mechanisms') ?? value<string>(config, 'sasl.mechanism') ?? 'PLAIN').toUpperCase()
    if (mechanism !== 'PLAIN' && mechanism !== 'SCRAM-SHA-256' && mechanism !== 'SCRAM-SHA-512' && mechanism !== 'OAUTHBEARER') {
      throw compatibilityError(`Unsupported SASL mechanism: ${mechanism}`, CODES.ERRORS.ERR__NOT_IMPLEMENTED)
    }
    options.sasl = {
      mechanism,
      username: value<string>(config, 'sasl.username'),
      password: value<string>(config, 'sasl.password'),
      token: oauthBearerToken ? () => oauthBearerToken.value : value<string>(config, 'oauthbearer.token')
    }
  }

  return options
}

export function mapProducerConfig (
  config: RdkafkaConfig,
  topicConfig: RdkafkaConfig = {},
  oauthBearerToken?: OauthBearerToken
): ProducerOptions<Buffer | string | null, Buffer | null, string, Buffer | string> {
  const merged = { ...topicConfig, ...config }
  const transactionalId = value<string>(merged, 'transactional.id')
  const rawAcks = merged.acks ?? merged['request.required.acks']
  const compression = value<string>(merged, 'compression.codec') ?? value<string>(merged, 'compression.type')
  return {
    ...baseOptions(merged, oauthBearerToken),
    timeout: value<number>(merged, 'message.timeout.ms'),
    retries: value<number>(merged, 'message.send.max.retries') ?? value<number>(merged, 'retries'),
    retryDelay: value<number>(merged, 'retry.backoff.ms'),
    acks: rawAcks === 'all' ? -1 : rawAcks as number | undefined,
    compression: compression === 'inherit' ? undefined : compression as 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd' | undefined,
    idempotent: value<boolean>(merged, 'enable.idempotence') ?? transactionalId !== undefined,
    transactionalId
  }
}

export function mapConsumerConfig (
  config: RdkafkaConfig,
  topicConfig: RdkafkaConfig = {},
  oauthBearerToken?: OauthBearerToken
): ConsumerOptions<Buffer, Buffer | null, Buffer, Buffer> {
  const autocommit = value<boolean>(config, 'enable.auto.commit') ?? true
  const interval = value<number>(config, 'auto.commit.interval.ms')
  return {
    ...baseOptions(config, oauthBearerToken),
    groupId: requiredString(config, 'group.id'),
    groupInstanceId: value<string>(config, 'group.instance.id'),
    sessionTimeout: value<number>(config, 'session.timeout.ms') ?? 45_000,
    heartbeatInterval: value<number>(config, 'heartbeat.interval.ms'),
    rebalanceTimeout: value<number>(config, 'max.poll.interval.ms'),
    minBytes: value<number>(config, 'fetch.min.bytes'),
    maxBytes: value<number>(config, 'fetch.max.bytes'),
    maxBytesPerPartition: value<number>(config, 'max.partition.fetch.bytes') ?? value<number>(config, 'fetch.message.max.bytes'),
    maxWaitTime: value<number>(config, 'fetch.wait.max.ms'),
    autocommit: autocommit ? interval ?? true : false,
    isolationLevel: value<string>(config, 'isolation.level') === 'read_uncommitted' ? 0 : 1,
    partitionAssigner: value<string>(config, 'partition.assignment.strategy')?.split(',').map(strategy => strategy.trim()).includes('range') ? rangeAssigner : undefined,
    context: {
      offsetReset: value<string>(topicConfig, 'auto.offset.reset') ?? value<string>(config, 'auto.offset.reset')
    }
  }
}

export function mapAdminConfig (config: RdkafkaConfig, oauthBearerToken?: OauthBearerToken): BaseOptions {
  return baseOptions(config, oauthBearerToken)
}

function requiredString (config: RdkafkaConfig, key: string): string {
  const configured = value<string>(config, key)
  if (!configured) {
    throw compatibilityError(`Missing required configuration property: ${key}`, CODES.ERRORS.ERR__INVALID_ARG)
  }
  return configured
}

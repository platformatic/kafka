import { Admin } from '../../clients/admin/admin.ts'
import { mapAdminConfig, type OauthBearerToken } from './config.ts'
import { CODES, compatibilityError, toLibrdKafkaError } from './errors.ts'
import { type GlobalConfig, type IAdminClient, type NewTopic, type NodeCallback, type RdkafkaConfig } from './types.ts'

class AdminClientImpl implements IAdminClient {
  readonly #admin: Admin
  readonly #oauthBearerToken: OauthBearerToken | null
  #disconnected = false

  constructor (config: GlobalConfig) {
    const internalConfig = config as unknown as RdkafkaConfig
    const token = internalConfig['oauthbearer.token']
    const mechanism = internalConfig['sasl.mechanisms'] ?? internalConfig['sasl.mechanism']
    this.#oauthBearerToken = typeof token === 'string' || (typeof mechanism === 'string' && mechanism.toUpperCase() === 'OAUTHBEARER')
      ? { value: typeof token === 'string' ? token : '' }
      : null
    this.#admin = new Admin(mapAdminConfig(internalConfig, this.#oauthBearerToken ?? undefined))
  }

  refreshOauthBearerToken (token: string): void {
    if (!this.#oauthBearerToken) {
      throw compatibilityError('OAuth bearer authentication is not configured')
    }
    if (typeof token !== 'string' || token.length === 0) {
      throw compatibilityError('OAuth bearer token must be a non-empty string', CODES.ERRORS.ERR__INVALID_ARG)
    }
    this.#oauthBearerToken.value = token
  }

  createTopic (topic: NewTopic, timeoutOrCallback?: number | NodeCallback, callback?: NodeCallback): void {
    this.#assertConnected()
    const timeout = typeof timeoutOrCallback === 'number' && timeoutOrCallback !== 0 ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    this.#withTimeout(timeout, callback, done => {
      this.#admin.createTopics({
        topics: [topic.topic],
        timeout,
        partitions: topic.num_partitions,
        replicas: topic.replication_factor,
        configs: Object.entries(topic.config ?? {}).map(([name, value]) => ({ name, value }))
      }, done)
    })
  }

  deleteTopic (topic: string, timeoutOrCallback?: number | NodeCallback, callback?: NodeCallback): void {
    this.#assertConnected()
    const timeout = typeof timeoutOrCallback === 'number' && timeoutOrCallback !== 0 ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    this.#withTimeout(timeout, callback, done => this.#admin.deleteTopics({ topics: [topic], timeout }, done))
  }

  createPartitions (topic: string, desiredPartitions: number, timeoutOrCallback?: number | NodeCallback, callback?: NodeCallback): void {
    this.#assertConnected()
    const timeout = typeof timeoutOrCallback === 'number' && timeoutOrCallback !== 0 ? timeoutOrCallback : 5000
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback
    }
    this.#withTimeout(timeout, callback, done => this.#admin.createPartitions({ topics: [{ name: topic, count: desiredPartitions }], timeout }, done))
  }

  disconnect (): void {
    this.#disconnected = true
    this.#admin.close().catch(() => {})
  }

  #assertConnected (): void {
    if (this.#disconnected) {
      throw compatibilityError('Client is disconnected', CODES.ERRORS.ERR__STATE)
    }
  }

  #withTimeout (timeout: number, callback: NodeCallback | undefined, operation: (done: (error: Error | null) => void) => void): void {
    let completed = false
    const finish = (error: Error | null): void => {
      if (completed) {
        return
      }
      completed = true
      clearTimeout(timer)
      if (error) {
        callback?.(toLibrdKafkaError(error))
      } else {
        callback?.()
      }
    }
    const timer = setTimeout(() => finish(compatibilityError('Admin operation timed out', CODES.ERRORS.ERR__TIMED_OUT)), timeout)
    timer.unref()
    operation(finish)
  }
}

export const AdminClient = {
  create (config: GlobalConfig, initialOauthBearerToken?: string): IAdminClient {
    return new AdminClientImpl(initialOauthBearerToken === undefined
      ? config
      : { ...config, 'oauthbearer.token': initialOauthBearerToken })
  }
}

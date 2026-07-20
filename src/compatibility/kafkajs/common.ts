import { EventEmitter } from 'node:events'
import type { CallbackWithPromise } from '../../apis/callbacks.ts'
import type { SASLAuthenticationAPI, SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import type { Connection } from '../../network/connection.ts'
import type { KafkaConfig, Logger, RetryOptions, SASLOptions } from './types.ts'
import type { BaseOptions } from '../../clients/base/types.ts'
import { KafkaJSNonRetriableError, wrapError } from './errors.ts'

export type EventListener = (event: { id: string; type: string; timestamp: number; payload: unknown }) => unknown

export abstract class CompatibilityClient {
  protected readonly emitter = new EventEmitter()
  protected connected = false
  abstract readonly events: Record<string, string>
  abstract logger (): Logger

  on (eventName: string, listener: EventListener): () => void {
    const eventNames = Object.values(this.events)
    if (!eventNames.includes(eventName)) {
      const eventKeys = Object.keys(this.events)
        .map(key => `${this.constructor.name.toLowerCase()}.events.${key}`)
        .join(', ')
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }
    const wrapped = (event: Parameters<EventListener>[0]) => {
      Promise.resolve(listener(event)).catch(error => {
        const cause = error instanceof Error ? error : new Error(String(error))
        this.logger().error(`Failed to execute listener: ${cause.message}`, { eventName, stack: cause.stack })
      })
    }
    this.emitter.on(eventName, wrapped)
    return () => this.emitter.off(eventName, wrapped)
  }

  protected emit (type: string, payload: unknown = null): void {
    this.emitter.emit(type, { id: `${Date.now()}-${Math.random()}`, type, timestamp: Date.now(), payload })
  }
}

export function retryOptions (retry?: RetryOptions): Partial<Pick<BaseOptions, 'retries' | 'retryDelay'>> {
  if (!retry) {
    return {}
  }
  const output: Partial<Pick<BaseOptions, 'retries' | 'retryDelay'>> = {}
  if (retry.retries !== undefined) {
    output.retries = retry.retries
  }
  if (
    retry.initialRetryTime !== undefined ||
    retry.factor !== undefined ||
    retry.multiplier !== undefined ||
    retry.maxRetryTime !== undefined
  ) {
    const initialRetryTime = retry.initialRetryTime ?? 300
    const factor = retry.factor ?? 0.2
    const multiplier = retry.multiplier ?? 2
    const maxRetryTime = retry.maxRetryTime ?? 30_000
    output.retryDelay = (_client, _operationId, attempt) => {
      const base = initialRetryTime * multiplier ** Math.max(0, attempt - 1)
      const randomized = base + (Math.random() * 2 - 1) * factor * base
      return Math.min(Math.ceil(randomized), maxRetryTime)
    }
  }
  return output
}

export function mergeRetryOptions (...options: Array<RetryOptions | undefined>): RetryOptions {
  const merged: RetryOptions = {}
  for (const option of options) {
    for (const [key, value] of Object.entries(option ?? {})) {
      if (value !== undefined) {
        Object.assign(merged, { [key]: value })
      }
    }
  }
  return merged
}

function kafkaJSAuthenticator (
  provider: Extract<NonNullable<KafkaConfig['sasl']>, { authenticationProvider: unknown }>['authenticationProvider'],
  logger: Logger
) {
  return (
    mechanism: string,
    connection: Connection,
    authenticate: SASLAuthenticationAPI,
    _username: unknown,
    _password: unknown,
    _token: unknown,
    callback: CallbackWithPromise<SaslAuthenticateResponse>
  ) => {
    const complete = async () => {
      let lastResponse: SaslAuthenticateResponse | undefined
      const saslAuthenticate = async <Result>(args: {
        request: { encode: () => Buffer | Promise<Buffer> }
        response?: { decode: (rawResponse: Buffer) => Buffer | Promise<Buffer>; parse: (data: Buffer) => Result }
      }): Promise<Result | void> => {
        const encoded = await args.request.encode()
        if (!Buffer.isBuffer(encoded) || encoded.length < 4) {
          throw new KafkaJSNonRetriableError('SASL request encoder must return a length-prefixed Buffer')
        }
        const length = encoded.readInt32BE(0)
        if (length < 0 || length !== encoded.length - 4) {
          throw new KafkaJSNonRetriableError('Invalid SASL request encoding')
        }
        lastResponse = await authenticate.async(connection, encoded.subarray(4))
        if (!args.response) {
          return undefined
        }
        const response = Buffer.allocUnsafe(lastResponse.authBytes.length + 4)
        response.writeInt32BE(lastResponse.authBytes.length, 0)
        lastResponse.authBytes.copy(response, 4)
        return args.response.parse(await args.response.decode(response))
      }
      const authenticator = provider({
        host: connection.host!,
        port: connection.port!,
        logger: logger.namespace(`SaslAuthenticator-${mechanism}`),
        saslAuthenticate
      })
      await authenticator.authenticate()
      if (!lastResponse) {
        throw new KafkaJSNonRetriableError('SASL authenticator completed without an authentication exchange')
      }
      callback(null, lastResponse)
    }
    complete().catch(error => callback(error as Error))
  }
}

function normalizeSasl (sasl: KafkaConfig['sasl'], logger: Logger): BaseOptions['sasl'] {
  if (!sasl) {
    return undefined
  }
  if ('authenticationProvider' in sasl) {
    const mechanism = sasl.mechanism.toUpperCase()
    return { mechanism, authenticate: kafkaJSAuthenticator(sasl.authenticationProvider, logger) }
  }
  if (sasl.mechanism === 'aws') {
    const { authorizationIdentity, accessKeyId, secretAccessKey, sessionToken = '' } = sasl
    return {
      mechanism: 'AWS',
      authenticate: (_mechanism, connection, authenticate, _username, _password, _token, callback) => {
        const authBytes = Buffer.from(`${authorizationIdentity}\0${accessKeyId}\0${secretAccessKey}\0${sessionToken}`)
        authenticate(connection, authBytes, callback)
      }
    }
  }
  const mechanism = sasl.mechanism.toUpperCase().replaceAll('-', '_')
  if (sasl.mechanism === 'oauthbearer') {
    return {
      mechanism: 'OAUTHBEARER',
      token: async () => (await sasl.oauthBearerProvider()).value
    }
  }
  return {
    mechanism: mechanism.replaceAll('_', '-') as NonNullable<BaseOptions['sasl']>['mechanism'],
    username: sasl.username,
    password: sasl.password
  }
}

export function normalizeConfig (config: KafkaConfig, logger: Logger): BaseOptions {
  const retry = mergeRetryOptions(
    {
      maxRetryTime: 30_000,
      initialRetryTime: 300,
      factor: 0.2,
      multiplier: 2,
      retries: 5
    },
    config.retry
  )
  return {
    clientId: config.clientId ?? 'kafkajs',
    bootstrapBrokers: config.brokers,
    connectTimeout: config.connectionTimeout ?? 1000,
    authenticationTimeout: config.authenticationTimeout ?? 10_000,
    reauthenticationThreshold: config.reauthenticationThreshold ?? 10_000,
    requestTimeout: config.requestTimeout ?? 30_000,
    enforceRequestTimeout: config.enforceRequestTimeout ?? true,
    timeout: config.requestTimeout ?? 30_000,
    tls: config.ssl === true ? {} : config.ssl || undefined,
    socketFactory: config.socketFactory
      ? ({ host, port, tls, onConnect }) => config.socketFactory!({ host, port, ssl: tls ?? {}, onConnect })
      : undefined,
    sasl: normalizeSasl(config.sasl as SASLOptions | undefined, logger),
    ...retryOptions(retry)
  }
}

export async function call<T> (operation: Promise<T>): Promise<T> {
  try {
    return await operation
  } catch (error) {
    throw wrapError(error)
  }
}

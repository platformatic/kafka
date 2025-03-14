import { protocolAPIsById } from './protocol/apis.ts'
import { protocolErrors, protocolErrorsCodesById } from './protocol/errors.ts'

export const ERROR_PREFIX = 'KFK_'

export const errorCodes = [
  'KFK_UNSUPPORTED',
  'KFK_UNFINISHED_WRITE_BUFFER',
  'KFK_UNEXPECTED_CORRELATION_ID',
  'KFK_RESPONSE',
  'KFK_UNSUPPORTED_COMPRESSION',
  'KFK_NETWORK',
  'KFK_PROTOCOL',
  'KFK_AUTHENTICATION'
] as const

export type ErrorCode = (typeof errorCodes)[number]

export type ErrorProperties = { cause?: Error } & Record<string, any>

export class KafkaError extends Error {
  code: string;
  [index: string]: any

  constructor (code: ErrorCode, message: string, { cause, ...rest }: ErrorProperties = {}) {
    super(message, cause ? { cause } : {})
    this.code = code

    Reflect.defineProperty(this, 'message', { enumerable: true })
    Reflect.defineProperty(this, 'code', { enumerable: true })

    if ('stack' in this) {
      Reflect.defineProperty(this, 'stack', { enumerable: true })
    }

    for (const [key, value] of Object.entries(rest)) {
      Reflect.defineProperty(this, key, { value, enumerable: true })
    }
  }
}

export * from './protocol/errors.ts'

export class UnsupportedError extends KafkaError {
  static code: ErrorCode = 'KFK_UNSUPPORTED'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedError.code, message, properties)
  }
}

export class UnfinishedWriteBufferError extends KafkaError {
  static code: ErrorCode = 'KFK_UNFINISHED_WRITE_BUFFER'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnfinishedWriteBufferError.code, message, properties)
  }
}

export class UnexpectedCorrelationIdError extends KafkaError {
  static code: ErrorCode = 'KFK_UNEXPECTED_CORRELATION_ID'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnexpectedCorrelationIdError.code, message, properties)
  }
}

export class ResponseError extends KafkaError {
  static code: ErrorCode = 'KFK_RESPONSE'

  constructor (apiName: number, apiVersion: number, properties: ErrorProperties = {}) {
    const { errors, ...restProperties } = properties

    const message = `Received response with error while executing API ${protocolAPIsById[apiName]}(v${apiVersion})`

    if (typeof errors === 'object') {
      properties = restProperties
      properties.cause = new AggregateError(
        Object.entries(errors).map(([path, errorCode]) => new ProtocolError(errorCode as number, { path }))
      )
    }

    super(ResponseError.code, message, properties)
  }
}

export class UnsupportedCompressionError extends KafkaError {
  static code: ErrorCode = 'KFK_UNSUPPORTED_COMPRESSION'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedCompressionError.code, message, properties)
  }
}

export class NetworkError extends KafkaError {
  static code: ErrorCode = 'KFK_NETWORK'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(NetworkError.code, message, properties)
  }
}

export class AuthenticationError extends KafkaError {
  static code: ErrorCode = 'KFK_AUTHENTICATION'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(AuthenticationError.code, message, properties)
  }
}

export class ProtocolError extends KafkaError {
  constructor (codeOrId: string | number, properties: ErrorProperties = {}) {
    const { id, code, message, canRetry } =
      protocolErrors[typeof codeOrId === 'number' ? protocolErrorsCodesById[codeOrId] : codeOrId]

    super('KFK_PROTOCOL', message, {
      apiId: id,
      apiCode: code,
      canRetry,
      ...properties
    })
  }
}

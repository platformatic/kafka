import { protocolAPIsById } from './protocol/apis.ts'
import { protocolErrors, protocolErrorsCodesById } from './protocol/errors.ts'

const kGenericError = Symbol('plt.kafka.genericError')
const kMultipleErrors = Symbol('plt.kafka.multipleErrors')

export const ERROR_PREFIX = 'PLT_KFK_'

export const errorCodes = [
  'PLT_KFK_AUTHENTICATION',
  'PLT_KFK_MULTIPLE',
  'PLT_KFK_NETWORK',
  'PLT_KFK_PROTOCOL',
  'PLT_KFK_RESPONSE',
  'PLT_KFK_TIMEOUT',
  'PLT_KFK_UNEXPECTED_CORRELATION_ID',
  'PLT_KFK_UNFINISHED_WRITE_BUFFER',
  'PLT_KFK_UNSUPPORTED_COMPRESSION',
  'PLT_KFK_UNSUPPORTED',
  'PLT_KFK_USER'
] as const

export type ErrorCode = (typeof errorCodes)[number]

export type ErrorProperties = { cause?: Error } & Record<string, any>

export class GenericError extends Error {
  code: string;
  [index: string]: any
  [kGenericError]: true

  static isGenericError (error: Error): boolean {
    return (error as GenericError)[kGenericError] === true
  }

  constructor (code: ErrorCode, message: string, { cause, ...rest }: ErrorProperties = {}) {
    super(message, cause ? { cause } : {})
    this.code = code
    this[kGenericError] = true

    Reflect.defineProperty(this, 'message', { enumerable: true })
    Reflect.defineProperty(this, 'code', { enumerable: true })

    if ('stack' in this) {
      Reflect.defineProperty(this, 'stack', { enumerable: true })
    }

    for (const [key, value] of Object.entries(rest)) {
      Reflect.defineProperty(this, key, { value, enumerable: true })
    }

    Reflect.defineProperty(this, kGenericError, { value: true, enumerable: false })
  }

  hasAny (property: string, value: unknown): boolean {
    if (this[property] === value) {
      return true
    }

    return false
  }
}

export class MultipleErrors extends AggregateError {
  code: string;
  [index: string]: any
  [kGenericError]: true;
  [kMultipleErrors]: true

  static isGenericError (error: Error): boolean {
    return (error as GenericError)[kGenericError] === true
  }

  static isMultipleErrors (error: Error): boolean {
    return (error as MultipleErrors)[kMultipleErrors] === true
  }

  constructor (
    message: string,
    errors: Error[],
    { cause, ...rest }: ErrorProperties = {},
    code: ErrorCode = 'PLT_KFK_MULTIPLE'
  ) {
    super(errors, message, cause ? { cause } : {})
    this.code = code
    this[kGenericError] = true
    this[kMultipleErrors] = true

    Reflect.defineProperty(this, 'message', { enumerable: true })
    Reflect.defineProperty(this, 'code', { enumerable: true })

    if ('stack' in this) {
      Reflect.defineProperty(this, 'stack', { enumerable: true })
    }

    for (const [key, value] of Object.entries(rest)) {
      Reflect.defineProperty(this, key, { value, enumerable: true })
    }

    Reflect.defineProperty(this, kGenericError, { value: true, enumerable: false })
    Reflect.defineProperty(this, kMultipleErrors, { value: true, enumerable: false })
  }

  hasAny (property: string, value: unknown): boolean {
    if (this[property] === value) {
      return true
    }

    for (const error of this.errors) {
      if (error[property] === value) {
        return true
      }

      if (error[kMultipleErrors]) {
        if (error.hasAny(property, value)) {
          return true
        }
      }
    }

    return false
  }
}

export * from './protocol/errors.ts'

export class AuthenticationError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_AUTHENTICATION'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(AuthenticationError.code, message, properties)
  }
}

export class NetworkError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_NETWORK'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(NetworkError.code, message, properties)
  }
}

export class ProtocolError extends GenericError {
  constructor (codeOrId: string | number, properties: ErrorProperties = {}) {
    const { id, code, message, canRetry } =
      protocolErrors[typeof codeOrId === 'number' ? protocolErrorsCodesById[codeOrId] : codeOrId]

    super('PLT_KFK_PROTOCOL', message, {
      apiId: id,
      apiCode: code,
      canRetry,
      hasStaleMetadata: ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_OR_FOLLOWER'].includes(id),
      ...properties
    })
  }
}

export class ResponseError extends MultipleErrors {
  static code: ErrorCode = 'PLT_KFK_RESPONSE'

  constructor (
    apiName: number,
    apiVersion: number,
    errors: Record<string, number>,
    response: unknown,
    properties: ErrorProperties = {}
  ) {
    super(
      `Received response with error while executing API ${protocolAPIsById[apiName]}(v${apiVersion})`,
      Object.entries(errors).map(([path, errorCode]) => new ProtocolError(errorCode as number, { path })),
      {
        ...properties,
        response
      }
    )
  }
}

export class TimeoutError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_TIMEOUT'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(NetworkError.code, message, properties)
  }
}

export class UnexpectedCorrelationIdError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNEXPECTED_CORRELATION_ID'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnexpectedCorrelationIdError.code, message, properties)
  }
}

export class UnfinishedWriteBufferError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNFINISHED_WRITE_BUFFER'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnfinishedWriteBufferError.code, message, properties)
  }
}

export class UnsupportedCompressionError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNSUPPORTED_COMPRESSION'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedCompressionError.code, message, properties)
  }
}

export class UnsupportedError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNSUPPORTED'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedError.code, message, properties)
  }
}

export class UserError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_USER'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UserError.code, message, properties)
  }
}

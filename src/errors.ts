import { type JoinGroupResponse } from './apis/consumer/join-group-v9.ts'
import { protocolAPIsById } from './protocol/apis.ts'
import { protocolErrors, protocolErrorsCodesById } from './protocol/errors.ts'

const kGenericError = Symbol('plt.kafka.genericError')
const kMultipleErrors = Symbol('plt.kafka.multipleErrors')

export const ERROR_PREFIX = 'PLT_KFK_'

export const errorCodes = [
  'PLT_KFK_AUTHENTICATION',
  'PLT_KFK_MULTIPLE',
  'PLT_KFK_NETWORK',
  'PLT_KFK_OUT_OF_BOUNDS',
  'PLT_KFK_PROTOCOL',
  'PLT_KFK_RESPONSE',
  'PLT_KFK_TIMEOUT',
  'PLT_KFK_UNEXPECTED_CORRELATION_ID',
  'PLT_KFK_UNFINISHED_WRITE_BUFFER',
  'PLT_KFK_UNSUPPORTED_API',
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

  static isGenericError (error: Error): error is GenericError | MultipleErrors {
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

  findBy<ErrorType extends GenericError = GenericError> (property: string, value: unknown): ErrorType | null {
    if (this[property] === value) {
      return this as unknown as ErrorType
    }

    return null
  }
}

export class MultipleErrors extends AggregateError {
  code: string;
  [index: string]: any
  [kGenericError]: true;
  [kMultipleErrors]: true

  static code: ErrorCode = 'PLT_KFK_MULTIPLE'

  static isGenericError (error: Error): error is GenericError | MultipleErrors {
    return (error as GenericError)[kGenericError] === true
  }

  static isMultipleErrors (error: Error): error is MultipleErrors {
    return (error as MultipleErrors)[kMultipleErrors] === true
  }

  constructor (message: string, errors: Error[], { cause, ...rest }: ErrorProperties = {}) {
    super(errors, message, cause ? { cause } : {})
    this.code = MultipleErrors.code
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

  findBy<ErrorType extends GenericError | MultipleErrors = MultipleErrors> (
    property: string,
    value: unknown
  ): ErrorType | null {
    if (this[property] === value) {
      return this as unknown as ErrorType
    }

    for (const error of this.errors) {
      if (error[property] === value) {
        return error as unknown as ErrorType
      }

      const found = error[kGenericError] ? error.findBy(property, value) : undefined

      if (found) {
        return found as unknown as ErrorType
      }
    }

    return null
  }
}

export * from './protocol/errors.ts'

export class AuthenticationError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_AUTHENTICATION'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(AuthenticationError.code, message, { canRetry: false, ...properties })
  }
}

export class NetworkError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_NETWORK'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(NetworkError.code, message, properties)
  }
}

export class ProtocolError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_PROTOCOL'

  constructor (codeOrId: string | number, properties: ErrorProperties = {}, response: unknown = undefined) {
    const { id, code, message, canRetry } =
      protocolErrors[typeof codeOrId === 'number' ? protocolErrorsCodesById[codeOrId] : codeOrId]

    super(ProtocolError.code, message, {
      apiId: id,
      apiCode: code,
      canRetry,
      hasStaleMetadata: ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_OR_FOLLOWER'].includes(id),
      needsRejoin: ['MEMBER_ID_REQUIRED', 'UNKNOWN_MEMBER_ID', 'REBALANCE_IN_PROGRESS'].includes(id),
      rebalanceInProgress: id === 'REBALANCE_IN_PROGRESS',
      unknownMemberId: id === 'UNKNOWN_MEMBER_ID',
      memberId: (response as JoinGroupResponse)?.memberId as string | undefined,
      ...properties
    })
  }
}

export class OutOfBoundsError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_OUT_OF_BOUNDS'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(OutOfBoundsError.code, message, { isOut: true, ...properties })
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
      Object.entries(errors).map(([path, errorCode]) => new ProtocolError(errorCode as number, { path }, response)),
      {
        ...properties,
        response
      }
    )

    this.code = ResponseError.code
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
    super(UnexpectedCorrelationIdError.code, message, { canRetry: false, ...properties })
  }
}

export class UnfinishedWriteBufferError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNFINISHED_WRITE_BUFFER'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnfinishedWriteBufferError.code, message, { canRetry: false, ...properties })
  }
}

export class UnsupportedApiError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNSUPPORTED_API'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedApiError.code, message, { canRetry: false, ...properties })
  }
}

export class UnsupportedCompressionError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNSUPPORTED_COMPRESSION'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedCompressionError.code, message, { canRetry: false, ...properties })
  }
}

export class UnsupportedError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_UNSUPPORTED'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UnsupportedError.code, message, { canRetry: false, ...properties })
  }
}

export class UserError extends GenericError {
  static code: ErrorCode = 'PLT_KFK_USER'

  constructor (message: string, properties: ErrorProperties = {}) {
    super(UserError.code, message, { canRetry: false, ...properties })
  }
}

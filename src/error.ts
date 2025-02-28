export const ERROR_PREFIX = 'KFK_'
export const ERROR_UNSUPPORTED = ERROR_PREFIX + 'UNSUPPORTED'
export const ERROR_UNFINISHED_WRITE_BUFFER = ERROR_PREFIX + 'UNFINISHED_WRITE_BUFFER'
export const ERROR_UNEXPECTED_CORRELATION_ID = ERROR_PREFIX + 'UNEXPECTED_CORRELATION_ID'
export const ERROR_RESPONSE_WITH_ERROR = ERROR_PREFIX + 'RESPONSE_WITH_ERROR'
export const ERROR_UNSUPPORTED_COMPRESSION = ERROR_PREFIX + 'UNSUPPORTED_COMPRESSION'

export class KafkaError extends Error {
  constructor (message: string, { cause, ...rest }: any = {}) {
    super(message, { cause })

    for (const [key, value] of Object.entries(rest)) {
      Reflect.defineProperty(this, key, { value, enumerable: true })
    }
  }
}

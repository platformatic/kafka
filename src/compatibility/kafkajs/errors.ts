interface ErrorLike {
  message?: string
  name?: string
  stack?: string
  retriable?: boolean
  type?: string
  code?: number | string
  helpUrl?: string
  canRetry?: boolean
  apiCode?: number
  apiId?: string
  apiKey?: number
  apiName?: string
  memberId?: string
  partition?: number
  topic?: string
  cause?: unknown
  errors?: unknown[]
  retryExhausted?: boolean
  retryCount?: number
  retryTime?: number
}

export interface KafkaJSErrorMetadata {
  retriable?: boolean
  cause?: Error
  [key: string]: unknown
}

function messageOf (error: Error | string): string {
  return typeof error === 'string' ? error : error.message
}

export class KafkaJSError extends Error {
  retriable: boolean
  helpUrl?: string

  constructor (error: Error | string, { retriable = true, cause }: KafkaJSErrorMetadata = {}) {
    super(messageOf(error), { cause })
    Error.captureStackTrace(this, this.constructor)
    this.message = messageOf(error)
    this.name = 'KafkaJSError'
    this.retriable = retriable
    this.helpUrl = typeof error === 'string' ? undefined : (error as ErrorLike).helpUrl
    this.cause = cause
  }
}

export class KafkaJSNonRetriableError extends KafkaJSError {
  constructor (error: Error | string, metadata: KafkaJSErrorMetadata = {}) {
    super(error, { ...metadata, retriable: false })
    this.name = 'KafkaJSNonRetriableError'
  }
}

export class KafkaJSProtocolError extends KafkaJSError {
  code: number
  type: string

  constructor (
    error: Error | string,
    metadata: KafkaJSErrorMetadata = {}
  ) {
    const details = typeof error === 'string' ? {} : (error as ErrorLike)
    const retriable = metadata.retriable ?? details.retriable
    super(error, { ...metadata, retriable })
    this.code = details.code as number
    this.type = details.type as string
    this.name = 'KafkaJSProtocolError'
  }
}

export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  topic: string
  partition: number
  constructor (
    error: Error | string,
    metadata: KafkaJSErrorMetadata & { topic?: string; partition?: number } = {}
  ) {
    super(error, metadata)
    this.topic = metadata.topic as string
    this.partition = metadata.partition as number
    this.name = 'KafkaJSOffsetOutOfRange'
  }
}

export class KafkaJSMemberIdRequired extends KafkaJSProtocolError {
  memberId: string
  constructor (error: Error | string, metadata: KafkaJSErrorMetadata & { memberId?: string } = {}) {
    super(error, metadata)
    this.memberId = metadata.memberId as string
    this.name = 'KafkaJSMemberIdRequired'
  }
}

export class KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
  retryCount: number
  retryTime: number
  constructor (error: Error | string, metadata: { retryCount?: number; retryTime?: number } = {}) {
    super(error, { cause: typeof error === 'string' ? undefined : error })
    this.name = 'KafkaJSNumberOfRetriesExceeded'
    if (typeof error !== 'string') {
      this.stack = `${this.name}\n  Caused by: ${error.stack}`
    }
    this.retryCount = metadata.retryCount as number
    this.retryTime = metadata.retryTime as number
  }
}

export class KafkaJSConnectionError extends KafkaJSError {
  broker?: string
  code?: string
  constructor (error: Error | string, metadata: { broker?: string; code?: string } = {}) {
    super(error)
    this.broker = metadata.broker
    this.code = metadata.code
    this.name = 'KafkaJSConnectionError'
  }
}

export class KafkaJSConnectionClosedError extends KafkaJSConnectionError {
  host?: string
  port?: number
  constructor (error: Error | string, metadata: { host?: string; port?: number } = {}) {
    super(error, { broker: `${metadata.host}:${metadata.port}` })
    this.host = metadata.host
    this.port = metadata.port
    this.name = 'KafkaJSConnectionClosedError'
  }
}

export class KafkaJSRequestTimeoutError extends KafkaJSError {
  broker?: string
  correlationId?: number
  createdAt?: number
  sentAt?: number
  pendingDuration?: number
  constructor (error: Error | string, metadata: Record<string, unknown> = {}) {
    super(error)
    Object.assign(this, metadata)
    this.name = 'KafkaJSRequestTimeoutError'
  }
}

function namedRetriableError (name: string): typeof KafkaJSError {
  return class extends KafkaJSError {
    constructor (error: Error | string, metadata: KafkaJSErrorMetadata = {}) {
      super(error, metadata)
      this.name = name
    }
  }
}

function namedNonRetriableError (name: string): typeof KafkaJSNonRetriableError {
  return class extends KafkaJSNonRetriableError {
    constructor (error: Error | string, metadata: KafkaJSErrorMetadata = {}) {
      super(error, metadata)
      this.name = name
    }
  }
}

export const KafkaJSMetadataNotLoaded = namedRetriableError('KafkaJSMetadataNotLoaded')
export class KafkaJSTopicMetadataNotLoaded extends KafkaJSMetadataNotLoaded {
  topic: string
  constructor (error: Error | string, metadata: { topic?: string } = {}) {
    super(error)
    this.topic = metadata.topic as string
    this.name = 'KafkaJSTopicMetadataNotLoaded'
  }
}
export class KafkaJSStaleTopicMetadataAssignment extends KafkaJSError {
  topic: string
  unknownPartitions: unknown
  constructor (error: Error | string, metadata: { topic?: string; unknownPartitions?: unknown } = {}) {
    super(error)
    this.topic = metadata.topic ?? ''
    this.unknownPartitions = metadata.unknownPartitions
    this.name = 'KafkaJSStaleTopicMetadataAssignment'
  }
}
export class KafkaJSDeleteGroupsError extends KafkaJSError {
  groups: unknown[]
  constructor (error: Error | string, groups: unknown[] = []) {
    super(error)
    this.groups = groups
    this.name = 'KafkaJSDeleteGroupsError'
  }
}
export class KafkaJSServerDoesNotSupportApiKey extends KafkaJSNonRetriableError {
  apiKey?: number
  apiName?: string
  constructor (error: Error | string, metadata: { apiKey?: number; apiName?: string } = {}) {
    super(error)
    Object.assign(this, metadata)
    this.name = 'KafkaJSServerDoesNotSupportApiKey'
  }
}
export class KafkaJSDeleteTopicRecordsError extends KafkaJSError {
  partitions: unknown[]
  constructor (metadata: { partitions?: unknown[] }) {
    const partitions = metadata.partitions ?? []
    const retriable = partitions
      .filter(partition => (partition as { error?: unknown }).error != null)
      .every(partition => (partition as { error: { retriable?: boolean } }).error.retriable === true)
    super('Error while deleting records', { retriable })
    this.partitions = partitions
    this.name = 'KafkaJSDeleteTopicRecordsError'
  }
}
export class KafkaJSCreateTopicError extends KafkaJSProtocolError {
  topic: string
  constructor (error: Error | string, topic: string) {
    super(error)
    this.topic = topic
    this.name = 'KafkaJSCreateTopicError'
  }
}
export class KafkaJSAlterPartitionReassignmentsError extends KafkaJSProtocolError {
  topic?: string
  partition?: number
  constructor (error: Error | string, topic?: string, partition?: number) {
    super(error)
    this.topic = topic
    this.partition = partition
    this.name = 'KafkaJSAlterPartitionReassignmentsError'
  }
}
export class KafkaJSAggregateError extends Error {
  errors: (Error | string)[]
  constructor (message: Error | string, errors: (Error | string)[]) {
    super(messageOf(message))
    this.errors = errors
    this.name = 'KafkaJSAggregateError'
  }
}
export class KafkaJSFetcherRebalanceError extends Error {}
export const KafkaJSBrokerNotFound = namedRetriableError('KafkaJSBrokerNotFound')
export const KafkaJSPartialMessageError = namedNonRetriableError('KafkaJSPartialMessageError')
export const KafkaJSSASLAuthenticationError = namedNonRetriableError('KafkaJSSASLAuthenticationError')
export const KafkaJSGroupCoordinatorNotFound = namedNonRetriableError('KafkaJSGroupCoordinatorNotFound')
export const KafkaJSNotImplemented = namedNonRetriableError('KafkaJSNotImplemented')
export const KafkaJSTimeout = namedNonRetriableError('KafkaJSTimeout')
export class KafkaJSLockTimeout extends KafkaJSTimeout {
  constructor (error: Error | string) {
    super(error)
    this.name = 'KafkaJSLockTimeout'
  }
}
export const KafkaJSUnsupportedMagicByteInMessageSet = namedNonRetriableError('KafkaJSUnsupportedMagicByteInMessageSet')
export const KafkaJSInvalidVarIntError = namedNonRetriableError('KafkaJSNonRetriableError')
export const KafkaJSInvalidLongError = namedNonRetriableError('KafkaJSNonRetriableError')
export class KafkaJSInvariantViolation extends KafkaJSNonRetriableError {
  constructor (error: Error | string) {
    const message = messageOf(error)
    super(`Invariant violated: ${message}. This is likely a bug and should be reported.`)
    this.name = 'KafkaJSInvariantViolation'
    this.helpUrl = `https://github.com/tulios/kafkajs/issues/new?assignees=&labels=bug&template=bug_report.md&title=${encodeURIComponent(`Invariant violation: ${message}`)}`
  }
}
export class KafkaJSNoBrokerAvailableError extends KafkaJSError {
  constructor () {
    super('No broker available')
    this.name = 'KafkaJSNoBrokerAvailableError'
  }
}

export function isRebalancing (error: { type?: string }): boolean {
  return ['REBALANCE_IN_PROGRESS', 'NOT_COORDINATOR_FOR_GROUP', 'ILLEGAL_GENERATION'].includes(error.type ?? '')
}

export function isKafkaJSError (error: unknown): error is KafkaJSError {
  return error instanceof KafkaJSError
}

export function wrapError (error: unknown): KafkaJSError {
  if (error instanceof KafkaJSError) {
    return error
  }
  const source = error instanceof Error ? error : new Error(String(error))
  const sourceDetails = source as ErrorLike
  const queue: unknown[] = [source]
  const visited = new Set<unknown>()
  let retriable = true
  let protocolError: Error | undefined
  let protocolDetails: ErrorLike | undefined
  let timeoutError: Error | undefined
  let networkError: Error | undefined
  let authenticationError: Error | undefined
  let unsupportedApiError: Error | undefined
  let retryExhaustion: ErrorLike | undefined
  let retryError: Error | undefined
  while (queue.length > 0) {
    const candidate = queue.shift()
    if (!candidate || typeof candidate !== 'object' || visited.has(candidate)) {
      continue
    }
    visited.add(candidate)
    const details = candidate as ErrorLike
    if (details.retryExhausted && !retryExhaustion) {
      retryExhaustion = details
      retryError = candidate instanceof Error ? candidate : source
    }
    if (
      candidate instanceof TypeError ||
      candidate instanceof RangeError ||
      candidate instanceof ReferenceError ||
      candidate instanceof SyntaxError
    ) {
      retriable = false
    }
    if (details.canRetry === false) {
      retriable = false
    }
    if (!protocolError && typeof details.apiCode === 'number') {
      protocolError = candidate instanceof Error ? candidate : source
      protocolDetails = details
    }
    if (!timeoutError && details.code === 'PLT_KFK_TIMEOUT') {
      timeoutError = candidate instanceof Error ? candidate : source
    }
    if (!networkError && details.code === 'PLT_KFK_NETWORK') {
      networkError = candidate instanceof Error ? candidate : source
    }
    if (!authenticationError && details.code === 'PLT_KFK_AUTHENTICATION') {
      authenticationError = candidate instanceof Error ? candidate : source
    }
    if (!unsupportedApiError && details.code === 'PLT_KFK_UNSUPPORTED_API') {
      unsupportedApiError = candidate instanceof Error ? candidate : source
    }
    if (details.cause) {
      queue.push(details.cause)
    }
    if (details.errors) {
      queue.push(...details.errors)
    }
  }
  if (retryExhaustion && retryError) {
    const retryDetails = retryError as ErrorLike
    const cause = retryDetails.errors?.length
      ? [...retryDetails.errors].reverse().find(candidate => candidate instanceof Error) ?? retryError
      : retryError
    return new KafkaJSNumberOfRetriesExceeded(wrapError(cause), {
      retryCount: retryExhaustion.retryCount,
      retryTime: retryExhaustion.retryTime
    })
  }
  if (authenticationError) {
    return new KafkaJSSASLAuthenticationError(authenticationError)
  }
  if (unsupportedApiError) {
    const details = unsupportedApiError as ErrorLike
    return new KafkaJSServerDoesNotSupportApiKey(unsupportedApiError, {
      apiKey: typeof details.apiKey === 'number' ? details.apiKey : undefined,
      apiName: typeof details.apiName === 'string' ? details.apiName : undefined
    })
  }
  if (protocolError && protocolDetails) {
    const converted = Object.assign(protocolError, {
      code: protocolDetails.apiCode,
      type: protocolDetails.apiId,
      retriable: retriable && protocolDetails.canRetry !== false
    })
    const metadata = { retriable: converted.retriable, cause: protocolError }
    if (protocolDetails.apiId === 'OFFSET_OUT_OF_RANGE') {
      return new KafkaJSOffsetOutOfRange(converted, {
        ...metadata,
        topic: protocolDetails.topic,
        partition: protocolDetails.partition
      })
    }
    if (protocolDetails.apiId === 'MEMBER_ID_REQUIRED') {
      return new KafkaJSMemberIdRequired(converted, { ...metadata, memberId: protocolDetails.memberId })
    }
    return new KafkaJSProtocolError(converted, metadata)
  }
  if (timeoutError) {
    const converted = new KafkaJSRequestTimeoutError(timeoutError, timeoutError as unknown as Record<string, unknown>)
    converted.retriable = retriable
    return converted
  }
  if (networkError) {
    const converted = new KafkaJSConnectionError(networkError)
    converted.retriable = retriable
    return converted
  }
  if (!retriable || sourceDetails.canRetry === false) {
    return new KafkaJSNonRetriableError(source, { cause: source })
  }
  return new KafkaJSError(source, { retriable: true, cause: source })
}

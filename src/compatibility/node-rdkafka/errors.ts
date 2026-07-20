import { protocolErrors } from '../../protocol/errors.ts'

const internalErrors = {
  ERR__BEGIN: -200,
  ERR__BAD_MSG: -199,
  ERR__BAD_COMPRESSION: -198,
  ERR__DESTROY: -197,
  ERR__FAIL: -196,
  ERR__TRANSPORT: -195,
  ERR__CRIT_SYS_RESOURCE: -194,
  ERR__RESOLVE: -193,
  ERR__MSG_TIMED_OUT: -192,
  ERR__PARTITION_EOF: -191,
  ERR__UNKNOWN_PARTITION: -190,
  ERR__FS: -189,
  ERR__UNKNOWN_TOPIC: -188,
  ERR__ALL_BROKERS_DOWN: -187,
  ERR__INVALID_ARG: -186,
  ERR__TIMED_OUT: -185,
  ERR__QUEUE_FULL: -184,
  ERR__ISR_INSUFF: -183,
  ERR__NODE_UPDATE: -182,
  ERR__SSL: -181,
  ERR__WAIT_COORD: -180,
  ERR__UNKNOWN_GROUP: -179,
  ERR__IN_PROGRESS: -178,
  ERR__PREV_IN_PROGRESS: -177,
  ERR__EXISTING_SUBSCRIPTION: -176,
  ERR__ASSIGN_PARTITIONS: -175,
  ERR__REVOKE_PARTITIONS: -174,
  ERR__CONFLICT: -173,
  ERR__STATE: -172,
  ERR__UNKNOWN_PROTOCOL: -171,
  ERR__NOT_IMPLEMENTED: -170,
  ERR__AUTHENTICATION: -169,
  ERR__NO_OFFSET: -168,
  ERR__OUTDATED: -167,
  ERR__TIMED_OUT_QUEUE: -166,
  ERR__UNSUPPORTED_FEATURE: -165,
  ERR__WAIT_CACHE: -164,
  ERR__INTR: -163,
  ERR__KEY_SERIALIZATION: -162,
  ERR__VALUE_SERIALIZATION: -161,
  ERR__KEY_DESERIALIZATION: -160,
  ERR__VALUE_DESERIALIZATION: -159,
  ERR__PARTIAL: -158,
  ERR__READ_ONLY: -157,
  ERR__NOENT: -156,
  ERR__UNDERFLOW: -155,
  ERR__INVALID_TYPE: -154,
  ERR__RETRY: -153,
  ERR__PURGE_QUEUE: -152,
  ERR__PURGE_INFLIGHT: -151,
  ERR__FATAL: -150,
  ERR__INCONSISTENT: -149,
  ERR__GAPLESS_GUARANTEE: -148,
  ERR__MAX_POLL_EXCEEDED: -147,
  ERR__UNKNOWN_BROKER: -146,
  ERR__NOT_CONFIGURED: -145,
  ERR__FENCED: -144,
  ERR__APPLICATION: -143,
  ERR__ASSIGNMENT_LOST: -142,
  ERR__NOOP: -141,
  ERR__AUTO_OFFSET_RESET: -140,
  ERR__LOG_TRUNCATION: -139,
  ERR__INVALID_DIFFERENT_RECORD: -138,
  ERR__DESTROY_BROKER: -137,
  ERR__END: -100
} as const

const brokerAliases: Record<string, string> = {
  NONE: 'NO_ERROR',
  CORRUPT_MESSAGE: 'INVALID_MSG',
  UNKNOWN_TOPIC_OR_PARTITION: 'UNKNOWN_TOPIC_OR_PART',
  INVALID_FETCH_SIZE: 'INVALID_MSG_SIZE',
  NOT_LEADER_OR_FOLLOWER: 'NOT_LEADER_FOR_PARTITION',
  MESSAGE_TOO_LARGE: 'MSG_SIZE_TOO_LARGE',
  STALE_CONTROLLER_EPOCH: 'STALE_CTRL_EPOCH',
  INVALID_TOPIC_EXCEPTION: 'TOPIC_EXCEPTION'
}

const brokerErrors: Record<string, number> = { ERR_UNKNOWN: -1 }
for (const definition of Object.values(protocolErrors)) {
  const id = brokerAliases[definition.id] ?? definition.id
  brokerErrors[`ERR_${id}`] = definition.code
}

brokerErrors.ERR_GROUP_LOAD_IN_PROGRESS = 14
brokerErrors.ERR_GROUP_COORDINATOR_NOT_AVAILABLE = 15
brokerErrors.ERR_NOT_COORDINATOR_FOR_GROUP = 16
brokerErrors.ERR_REBOOTSTRAP_REQUIRED = 129

const allErrors: Readonly<Record<string, number>> & typeof internalErrors = { ...internalErrors, ...brokerErrors }
export const CODES = { ERRORS: allErrors } as const

export class LibrdKafkaError extends Error {
  code: number
  errno: number
  origin: string
  isFatal: boolean
  isRetriable: boolean
  isTxnRequiresAbort: boolean

  constructor (message: string, code: number = CODES.ERRORS.ERR__FAIL, options: ErrorOptions & { origin?: string } = {}) {
    super(message, options)
    this.name = 'LibrdKafkaError'
    this.code = code
    this.errno = code
    this.origin = options.origin ?? (code >= 0 ? 'broker' : 'local')
    this.isFatal = code === CODES.ERRORS.ERR__FATAL
    this.isRetriable = false
    this.isTxnRequiresAbort = false
  }
}

export function compatibilityError (message: string, code: number = CODES.ERRORS.ERR__NOT_IMPLEMENTED): LibrdKafkaError {
  return new LibrdKafkaError(message, code)
}

export function toLibrdKafkaError (error: unknown): LibrdKafkaError {
  if (error instanceof LibrdKafkaError) {
    return error
  }

  const source = error instanceof Error ? error : new Error(String(error))
  const record = source as Error & {
    apiCode?: number
    canRetry?: boolean
    code?: string | number
    isFatal?: boolean
    isTxnRequiresAbort?: boolean
    errors?: Error[]
  }
  const nested = record.errors?.find(error => error instanceof Error)
  if (nested) {
    return toLibrdKafkaError(nested)
  }
  let code = typeof record.apiCode === 'number'
    ? record.apiCode
    : typeof record.code === 'number' ? record.code : CODES.ERRORS.ERR__FAIL

  if (record.code === 'PLT_KFK_TIMEOUT') {
    code = CODES.ERRORS.ERR__TIMED_OUT
  } else if (record.code === 'PLT_KFK_NETWORK') {
    code = CODES.ERRORS.ERR__TRANSPORT
  } else if (record.code === 'PLT_KFK_AUTHENTICATION') {
    code = CODES.ERRORS.ERR__AUTHENTICATION
  }

  const converted = new LibrdKafkaError(source.message, code, { cause: source })
  converted.isRetriable = record.canRetry === true
  converted.isFatal = record.isFatal === true || converted.isFatal
  converted.isTxnRequiresAbort = record.isTxnRequiresAbort === true
  return converted
}

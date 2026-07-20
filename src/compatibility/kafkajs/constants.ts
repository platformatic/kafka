import { compressionsAlgorithms } from '../../protocol/compression.ts'

export const logLevel = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5
} as const

export const CompressionTypes = { None: 0, GZIP: 1, Snappy: 2, LZ4: 3, ZSTD: 4 } as const
export type CompressionType = (typeof CompressionTypes)[keyof typeof CompressionTypes]

export const ConfigResourceTypes = { UNKNOWN: 0, TOPIC: 2, BROKER: 4, BROKER_LOGGER: 8 } as const
export const ConfigSource = {
  UNKNOWN: 0,
  TOPIC_CONFIG: 1,
  DYNAMIC_BROKER_CONFIG: 2,
  DYNAMIC_DEFAULT_BROKER_CONFIG: 3,
  STATIC_BROKER_CONFIG: 4,
  DEFAULT_CONFIG: 5,
  DYNAMIC_BROKER_LOGGER_CONFIG: 6
} as const
export const AclResourceTypes = {
  UNKNOWN: 0,
  ANY: 1,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER: 4,
  TRANSACTIONAL_ID: 5,
  DELEGATION_TOKEN: 6
} as const
export const AclOperationTypes = {
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12
} as const
export const AclPermissionTypes = { UNKNOWN: 0, ANY: 1, DENY: 2, ALLOW: 3 } as const
export const ResourcePatternTypes = { UNKNOWN: 0, ANY: 1, MATCH: 2, LITERAL: 3, PREFIXED: 4 } as const

function codec (algorithm: (typeof compressionsAlgorithms)[keyof typeof compressionsAlgorithms]) {
  return () => ({
    async compress (encoder: { buffer: Buffer }): Promise<Buffer> {
      return algorithm.compressSync(encoder.buffer)
    },
    async decompress (buffer: Buffer): Promise<Buffer> {
      return algorithm.decompressSync(buffer)
    }
  })
}

export const CompressionCodecs = {
  [CompressionTypes.GZIP]: codec(compressionsAlgorithms.gzip),
  [CompressionTypes.Snappy]: codec(compressionsAlgorithms.snappy),
  [CompressionTypes.LZ4]: codec(compressionsAlgorithms.lz4),
  [CompressionTypes.ZSTD]: codec(compressionsAlgorithms.zstd)
}

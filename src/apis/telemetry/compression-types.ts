export const TelemetryCompressionTypes = {
  NONE: 0,
  GZIP: 1,
  SNAPPY: 2,
  LZ4: 3,
  ZSTD: 4
} as const

export type TelemetryCompressionType = (typeof TelemetryCompressionTypes)[keyof typeof TelemetryCompressionTypes]

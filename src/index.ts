// General
export { ajv, type OptionsValidator } from './ajv.ts'
export * from './diagnostic.ts'
export * from './errors.ts'
export * from './events.ts'
export * from './utils.ts'

// Networking
export * from './network/index.ts'
export * from './protocol/index.ts'

// APIs. The codecs of each API version are exported from @platformatic/kafka/apis.
// Here they are only loaded when used, and they are deprecated.
export * from './apis/callbacks.ts'
export * from './apis/definitions.ts'
export * from './apis/deprecated.ts'
export * from './apis/enumerations.ts'
export * from './apis/tagged-fields.ts'
export { TelemetryCompressionTypes, type TelemetryCompressionType } from './apis/telemetry/compression-types.ts'
export * from './apis/types.ts'

// Clients
export * from './clients/index.ts'

// Registries
export * from './registries/index.ts'

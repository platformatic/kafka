import { allowedSASLMechanisms } from '../../apis/enumerations.ts'
import { ajv } from '../../utils.ts'
import { version } from '../../version.ts'
import { type BaseOptions } from './types.ts'

// Note: clientSoftwareName can only contain alphanumeric characters, hyphens and dots
export const clientSoftwareName = 'platformatic-kafka'
export const clientSoftwareVersion = version

export const idProperty = { type: 'string', pattern: '^\\S+$' }

export const topicWithPartitionAndOffsetProperties = {
  topic: idProperty,
  partition: { type: 'number', minimum: 0 },
  offset: { bigint: true }
}

export const baseOptionsSchema = {
  type: 'object',
  properties: {
    clientId: idProperty,
    bootstrapBrokers: {
      oneOf: [
        { type: 'array', items: { type: 'string' } },
        {
          type: 'array',
          items: {
            type: 'object',
            properties: { host: { type: 'string' }, port: { type: 'number', minimum: 0, maximum: 65535 } }
          }
        }
      ]
    },
    timeout: { type: 'number', minimum: 0 },
    connectTimeout: { type: 'number', minimum: 0 },
    retries: { oneOf: [{ type: 'number', minimum: 0 }, { type: 'boolean' }] },
    retryDelay: { type: 'number', minimum: 0 },
    maxInflights: { type: 'number', minimum: 0 },
    handleBackPressure: { type: 'boolean', default: false },
    tls: { type: 'object', additionalProperties: true }, // No validation as they come from Node.js
    tlsServerName: { oneOf: [{ type: 'boolean' }, { type: 'string' }] },
    sasl: {
      type: 'object',
      properties: {
        mechanism: { type: 'string', enum: allowedSASLMechanisms },
        username: { oneOf: [{ type: 'string' }, { function: true }] },
        password: { oneOf: [{ type: 'string' }, { function: true }] },
        token: { oneOf: [{ type: 'string' }, { function: true }] },
        keytab: { type: 'string' },
        authBytesValidator: { function: true }
      },
      required: ['mechanism'],
      additionalProperties: false
    },
    metadataMaxAge: { type: 'number', minimum: 0 },
    autocreateTopics: { type: 'boolean' },
    strict: { type: 'boolean' },
    metrics: { type: 'object', additionalProperties: true }
  },
  required: ['clientId', 'bootstrapBrokers'],
  additionalProperties: true
}

export const metadataOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty },
    autocreateTopics: { type: 'boolean' },
    forceUpdate: { type: 'boolean' },
    metadataMaxAge: { type: 'number', minimum: 0 }
  },
  required: ['topics'],
  additionalProperties: false
}

export const baseOptionsValidator = ajv.compile(baseOptionsSchema)

export const metadataOptionsValidator = ajv.compile(metadataOptionsSchema)

export const defaultPort = 9092
export const defaultBaseOptions: Partial<BaseOptions> = {
  connectTimeout: 5000,
  maxInflights: 5,
  timeout: 5000,
  retries: 3,
  retryDelay: 1000,
  metadataMaxAge: 5000, // 5 seconds
  autocreateTopics: false,
  strict: false
}

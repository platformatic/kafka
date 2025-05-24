import { readFileSync } from 'node:fs'
import { SASLMechanisms } from '../../apis/enumerations.ts'
import { ajv } from '../../utils.ts'
import { type BaseOptions } from './types.ts'

const packageJson = JSON.parse(readFileSync(new URL('../../../package.json', import.meta.url), 'utf-8'))

// Note: clientSoftwareName can only contain alphanumeric characters, hyphens and dots
export const clientSoftwareName = 'platformatic-kafka'
export const clientSoftwareVersion = packageJson.version

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
    retries: { type: 'number', minimum: 0 },
    retryDelay: { type: 'number', minimum: 0 },
    maxInflights: { type: 'number', minimum: 0 },
    tls: { type: 'object', additionalProperties: true }, // No validation as they come from Node.js
    sasl: {
      type: 'object',
      properties: {
        mechanism: { type: 'string', enum: SASLMechanisms },
        username: { type: 'string' },
        password: { type: 'string' }
      },
      required: ['mechanism', 'username', 'password'],
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
  metadataMaxAge: 5000, // 5 minutes
  autocreateTopics: false,
  strict: false
}

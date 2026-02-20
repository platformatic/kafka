import { allowedProduceAcks, ProduceAcks } from '../../apis/enumerations.ts'
import { allowedCompressionsAlgorithms, compressionsAlgorithms } from '../../protocol/compression.ts'
import { messageSchema } from '../../protocol/records.ts'
import { ajv, enumErrorMessage } from '../../utils.ts'
import { serdeProperties } from '../serde.ts'
import { allowedProducerStreamReportModes, type ProducerStreamOptions, ProducerStreamReportModes } from './types.ts'

export const produceOptionsProperties = {
  producerId: { bigint: true },
  producerEpoch: { type: 'number' },
  idempotent: { type: 'boolean' },
  transactionalId: { type: 'string' },
  acks: {
    type: 'number',
    enumeration: {
      allowed: allowedProduceAcks,
      errorMessage: enumErrorMessage(ProduceAcks)
    }
  },
  compression: {
    type: 'string',
    enumeration: {
      allowed: allowedCompressionsAlgorithms,
      errorMessage: enumErrorMessage(compressionsAlgorithms)
    }
  },
  partitioner: { function: true },
  autocreateTopics: { type: 'boolean' },
  repeatOnStaleMetadata: { type: 'boolean' }
}

export const produceOptionsSchema = {
  type: 'object',
  properties: produceOptionsProperties,
  additionalProperties: false
}

export const produceOptionsValidator = ajv.compile(produceOptionsSchema)
export const producerOptionsValidator = ajv.compile({
  type: 'object',
  properties: {
    ...produceOptionsProperties,
    serializers: serdeProperties,
    beforeSerialization: { function: true },
    registry: { type: 'object' }
  },
  additionalProperties: true
})

export const sendOptionsSchema = {
  type: 'object',
  properties: {
    messages: { type: 'array', items: messageSchema },
    ...produceOptionsProperties
  },
  required: ['messages'],
  additionalProperties: false
}

export const sendOptionsValidator = ajv.compile(sendOptionsSchema)

export const defaultProducerStreamOptions = {
  batchSize: 100,
  batchTime: 10,
  reportMode: ProducerStreamReportModes.NONE
} satisfies Partial<ProducerStreamOptions<Buffer, Buffer, Buffer, Buffer>>

export const producerStreamOptionsSchema = {
  type: 'object',
  properties: {
    highWaterMark: { type: 'integer', minimum: 1 },
    batchSize: { type: 'integer', minimum: 1 },
    batchTime: { type: 'integer', minimum: 0 },
    reportMode: { type: 'string', enum: allowedProducerStreamReportModes },
    ...produceOptionsProperties
  },
  additionalProperties: false
}

export const producerStreamOptionsValidator = ajv.compile(producerStreamOptionsSchema)

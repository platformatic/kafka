import { allowedProduceAcks, ProduceAcks } from '../../apis/enumerations.ts'
import { allowedCompressionsAlgorithms, compressionsAlgorithms } from '../../protocol/compression.ts'
import { messageSchema } from '../../protocol/records.ts'
import { ajv, enumErrorMessage } from '../../utils.ts'
import { serdeProperties } from '../serde.ts'

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

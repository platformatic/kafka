import { ProduceAcks } from '../../apis/enumerations.ts'
import { compressionsAlgorithms } from '../../protocol/compression.ts'
import { messageSchema } from '../../protocol/records.ts'
import { ajv, enumErrorMessage } from '../../utils.ts'
import { serdeProperties } from '../serde.ts'

const produceOptionsProperties = {
  producerId: { bigint: true },
  producerEpoch: { type: 'number' },
  idempotent: { type: 'boolean' },
  acks: {
    type: 'number',
    enum: Object.values(ProduceAcks),
    errorMessage: enumErrorMessage(ProduceAcks)
  },
  compression: {
    type: 'string',
    enum: Object.keys(compressionsAlgorithms),
    errorMessage: enumErrorMessage(compressionsAlgorithms, true)
  },
  partitioner: { function: true },
  repeatOnStaleMetadata: { type: 'boolean' },
  serializers: serdeProperties
}

export const produceOptionsSchema = {
  type: 'object',
  properties: produceOptionsProperties,
  additionalProperties: false
}

export const produceOptionsValidator = ajv.compile(produceOptionsSchema)
export const producerOptionsValidator = ajv.compile({ ...produceOptionsSchema, additionalProperties: true })

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

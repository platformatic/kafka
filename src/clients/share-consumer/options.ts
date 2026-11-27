import { idProperty } from '../base/options.ts'
import { serdeProperties } from '../serde.ts'

export const shareFetchOptionsProperties = {
  deserializers: serdeProperties,
  minBytes: { type: 'number', minimum: 0 },
  maxBytes: { type: 'number', minimum: 0 },
  maxWaitTime: { type: 'number', minimum: 0 },
  maxRecords: { type: 'number', minimum: 1 },
  highWaterMark: { type: 'number', minimum: 1 }
}

export const shareConsumerOptionsSchema = {
  type: 'object',
  properties: {
    groupId: idProperty,
    context: true,
    leaveGroupTimeout: { type: 'number', minimum: 0 },
    ...shareFetchOptionsProperties
  },
  required: ['groupId'],
  additionalProperties: true
}

export const shareGroupOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty, minItems: 1 }
  },
  required: ['topics'],
  additionalProperties: false
}

export const shareConsumeOptionsSchema = {
  type: 'object',
  properties: {
    ...shareGroupOptionsSchema.properties,
    ...shareFetchOptionsProperties
  },
  required: ['topics'],
  additionalProperties: false
}

// Used until the coordinator sends the interval in the first heartbeat response.
// It matches the default of the group.share.heartbeat.interval.ms broker setting.
export const defaultShareHeartbeatInterval = 5000

export const defaultShareFetchOptions = {
  minBytes: 1,
  maxBytes: 1_048_576 * 50, // 50 MB
  maxWaitTime: 5_000,
  maxRecords: 500
}

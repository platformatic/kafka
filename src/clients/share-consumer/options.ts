import { idProperty } from '../base/options.ts'

export const shareConsumerOptionsSchema = {
  type: 'object',
  properties: {
    groupId: idProperty,
    context: true,
    leaveGroupTimeout: { type: 'number', minimum: 0 }
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

// Used until the coordinator sends the interval in the first heartbeat response.
// It matches the default of the group.share.heartbeat.interval.ms broker setting.
export const defaultShareHeartbeatInterval = 5000

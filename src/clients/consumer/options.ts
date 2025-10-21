import { allowedFetchIsolationLevels, allowedGroupProtocols } from '../../apis/enumerations.ts'
import { ajv } from '../../utils.ts'
import { idProperty, topicWithPartitionAndOffsetProperties } from '../base/options.ts'
import { serdeProperties } from '../serde.ts'
import { allowedMessagesStreamFallbackModes, allowedMessagesStreamModes, type ConsumerOptions } from './types.ts'

export const groupOptionsProperties = {
  sessionTimeout: { type: 'number', minimum: 0 },
  rebalanceTimeout: { type: 'number', minimum: 0 },
  heartbeatInterval: { type: 'number', minimum: 0 },
  groupProtocol: { type: 'string', enum: allowedGroupProtocols },
  groupRemoteAssignor: { type: 'string' },
  protocols: {
    type: 'array',
    items: {
      type: 'object',
      properties: {
        name: idProperty,
        version: { type: 'number', minimum: 0 },
        topics: {
          type: 'array',
          items: { type: 'string' }
        },
        metadata: { oneOf: [{ type: 'string' }, { buffer: true }] }
      }
    }
  },
  partitionAssigner: { function: true }
}

export const groupOptionsAdditionalValidations = {
  rebalanceTimeout: {
    properties: {
      rebalanceTimeout: {
        type: 'number',
        minimum: 0,
        gteProperty: 'sessionTimeout'
      }
    }
  },
  heartbeatInterval: {
    properties: {
      heartbeatInterval: {
        type: 'number',
        minimum: 0,
        allOf: [
          {
            lteProperty: 'sessionTimeout'
          },
          {
            lteProperty: 'rebalanceTimeout'
          }
        ]
      }
    }
  }
}

export const consumeOptionsProperties = {
  autocommit: { oneOf: [{ type: 'boolean' }, { type: 'number', minimum: 100 }] },
  minBytes: { type: 'number', minimum: 0 },
  maxBytes: { type: 'number', minimum: 0 },
  maxWaitTime: { type: 'number', minimum: 0 },
  isolationLevel: { type: 'string', enum: allowedFetchIsolationLevels },
  deserializers: serdeProperties,
  highWaterMark: { type: 'number', minimum: 1 }
}

export const groupOptionsSchema = {
  type: 'object',
  properties: groupOptionsProperties,
  additionalProperties: true // This is needed as we might forward options from consume
}

export const consumeOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty },
    mode: { type: 'string', enum: allowedMessagesStreamModes },
    fallbackMode: { type: 'string', enum: allowedMessagesStreamFallbackModes },
    maxFetches: { type: 'number', minimum: 0, default: 0 },
    offsets: {
      type: 'array',
      items: {
        type: 'object',
        properties: topicWithPartitionAndOffsetProperties,
        required: ['topic', 'partition', 'offset'],
        additionalProperties: false
      }
    },
    onCorruptedMessage: { function: true },
    ...groupOptionsProperties,
    ...consumeOptionsProperties
  },
  required: ['topics'],
  additionalProperties: false
}

export const consumerOptionsSchema = {
  type: 'object',
  properties: {
    groupId: idProperty,
    ...groupOptionsProperties,
    ...consumeOptionsProperties
  },
  required: ['groupId'],
  additionalProperties: true
}

export const fetchOptionsSchema = {
  type: 'object',
  properties: {
    node: { type: 'number', minimum: 0 },
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          topicId: { type: 'string' },
          partitions: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                partition: { type: 'integer' },
                currentLeaderEpoch: { type: 'integer' },
                fetchOffset: { bigint: true },
                lastFetchedEpoch: { type: 'integer' },
                partitionMaxBytes: { type: 'integer' }
              },
              required: ['partition', 'currentLeaderEpoch', 'fetchOffset', 'lastFetchedEpoch', 'partitionMaxBytes']
            }
          }
        },
        required: ['topicId', 'partitions']
      }
    },
    ...groupOptionsProperties,
    ...consumeOptionsProperties
  },
  required: ['node', 'topics'],
  additionalProperties: false
}

export const commitOptionsSchema = {
  type: 'object',
  properties: {
    offsets: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          ...topicWithPartitionAndOffsetProperties,
          leaderEpoch: { type: 'integer' }
        },
        required: ['topic', 'partition', 'offset', 'leaderEpoch'],
        additionalProperties: false
      }
    }
  },
  required: ['offsets'],
  additionalProperties: false
}

export const listCommitsOptionsSchema = {
  type: 'object',
  properties: {
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          topic: idProperty,
          partitions: {
            type: 'array',
            items: {
              type: 'number',
              minimum: 0
            }
          }
        },
        required: ['topic', 'partitions'],
        additionalProperties: false
      }
    }
  },
  required: ['topics'],
  additionalProperties: false
}

export const listOffsetsOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty },
    partitions: {
      type: 'object',
      additionalProperties: {
        type: 'array',
        items: { type: 'number', minimum: 0 }
      }
    },
    isolationLevel: { type: 'string', enum: allowedFetchIsolationLevels },
    timestamp: { bigint: true }
  },
  required: ['topics'],
  additionalProperties: false
}

export const getLagOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty },
    partitions: {
      type: 'object',
      additionalProperties: {
        type: 'array',
        items: { type: 'number', minimum: 0 }
      }
    }
  },
  required: ['topics'],
  additionalProperties: false
}

export const groupOptionsValidator = ajv.compile({
  ...groupOptionsSchema,
  dependentSchemas: groupOptionsAdditionalValidations
})

export const groupIdAndOptionsValidator = ajv.compile({
  type: 'object',
  properties: {
    groupId: idProperty,
    ...groupOptionsProperties
  },
  required: ['groupId'],
  additionalProperties: true,
  dependentSchemas: groupOptionsAdditionalValidations
})

export const consumeOptionsValidator = ajv.compile(consumeOptionsSchema)
export const consumerOptionsValidator = ajv.compile(consumerOptionsSchema)
export const fetchOptionsValidator = ajv.compile(fetchOptionsSchema)
export const commitOptionsValidator = ajv.compile(commitOptionsSchema)
export const listCommitsOptionsValidator = ajv.compile(listCommitsOptionsSchema)
export const listOffsetsOptionsValidator = ajv.compile(listOffsetsOptionsSchema)
export const getLagOptionsValidator = ajv.compile(getLagOptionsSchema)

export const defaultConsumerOptions = {
  autocommit: true,
  sessionTimeout: 60_000, // One minute
  rebalanceTimeout: 102_000, // Two minutes,
  heartbeatInterval: 3000,
  protocols: [{ name: 'roundrobin', version: 1 }],
  minBytes: 1,
  maxBytes: 1_048_576 * 10, // 10 MB
  maxWaitTime: 5_000,
  isolationLevel: 'READ_COMMITTED',
  highWaterMark: 1024
} satisfies Partial<ConsumerOptions<Buffer, Buffer, Buffer, Buffer>>

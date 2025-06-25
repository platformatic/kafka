import { ConsumerGroupStates } from '../../apis/enumerations.ts'
import { ajv, listErrorMessage } from '../../utils.ts'
import { idProperty } from '../base/options.ts'

export const groupsProperties = {
  groups: {
    type: 'array',
    items: idProperty,
    minItems: 1
  }
}

export const createTopicOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty },
    partitions: { type: 'number' },
    replicas: { type: 'number' },
    assignments: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          partition: { type: 'number', minimum: 0 },
          brokers: { type: 'array', items: { type: 'number' }, minItems: 1 }
        },
        required: ['partition', 'brokers'],
        additionalProperties: false
      },
      minItems: 1
    }
  },
  required: ['topics'],
  additionalProperties: false
}

export const deleteTopicOptionsSchema = {
  type: 'object',
  properties: {
    topics: { type: 'array', items: idProperty }
  },
  required: ['topics'],
  additionalProperties: false
}

export const listGroupsOptionsSchema = {
  type: 'object',
  properties: {
    states: {
      type: 'array',
      items: {
        type: 'string',
        enumeration: {
          allowed: ConsumerGroupStates,
          errorMessage: listErrorMessage(ConsumerGroupStates as unknown as string[])
        }
      },
      minItems: 0
    },
    types: {
      type: 'array',
      items: idProperty,
      minItems: 0
    }
  },
  additionalProperties: false
}

export const describeGroupsOptionsSchema = {
  type: 'object',
  properties: {
    ...groupsProperties,
    includeAuthorizedOperations: { type: 'boolean' }
  },
  required: ['groups'],
  additionalProperties: false
}

export const deleteGroupsOptionsSchema = {
  type: 'object',
  properties: groupsProperties,
  required: ['groups'],
  additionalProperties: false
}

export const createTopicsOptionsValidator = ajv.compile(createTopicOptionsSchema)
export const deleteTopicsOptionsValidator = ajv.compile(deleteTopicOptionsSchema)
export const listGroupsOptionsValidator = ajv.compile(listGroupsOptionsSchema)
export const describeGroupsOptionsValidator = ajv.compile(describeGroupsOptionsSchema)
export const deleteGroupsOptionsValidator = ajv.compile(deleteGroupsOptionsSchema)

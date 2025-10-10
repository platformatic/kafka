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

export const listTopicOptionsSchema = {
  type: 'object',
  properties: {
    includeInternals: { type: 'boolean', default: false }
  },
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

export const describeClientQuotasOptionsSchema = {
  type: 'object',
  properties: {
    components: {
      type: 'array',
      items: {
        oneOf: [
          {
            type: 'object',
            properties: {
              entityType: { type: 'string', minLength: 1 },
              matchType: { type: 'number', const: 0 }, // EXACT
              match: { type: 'string' }
            },
            required: ['entityType', 'matchType', 'match'],
            additionalProperties: false
          },
          {
            type: 'object',
            properties: {
              entityType: { type: 'string', minLength: 1 },
              matchType: { type: 'number', enum: [1, 2] } // DEFAULT or ANY
            },
            required: ['entityType', 'matchType'],
            additionalProperties: false
          }
        ]
      },
      minItems: 1
    },
    strict: { type: 'boolean' }
  },
  required: ['components'],
  additionalProperties: false
}

export const alterClientQuotasOptionsSchema = {
  type: 'object',
  properties: {
    entries: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          entities: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                entityType: { type: 'string', minLength: 1 },
                entityName: { type: ['string', 'null'] }
              },
              required: ['entityType'],
              additionalProperties: false
            },
            minItems: 1
          },
          ops: {
            type: 'array',
            items: {
              oneOf: [
                {
                  type: 'object',
                  properties: {
                    key: { type: 'string', minLength: 1 },
                    value: { type: 'number' },
                    remove: { type: 'boolean', const: false }
                  },
                  required: ['key', 'value', 'remove'],
                  additionalProperties: false
                },
                {
                  type: 'object',
                  properties: {
                    key: { type: 'string', minLength: 1 },
                    remove: { type: 'boolean', const: true }
                  },
                  required: ['key', 'remove'],
                  additionalProperties: false
                }
              ]
            },
            minItems: 1
          }
        },
        required: ['entities', 'ops'],
        additionalProperties: false
      },
      minItems: 1
    },
    validateOnly: { type: 'boolean' }
  },
  required: ['entries'],
  additionalProperties: false
}

export const createTopicsOptionsValidator = ajv.compile(createTopicOptionsSchema)
export const listTopicsOptionsValidator = ajv.compile(listTopicOptionsSchema)
export const deleteTopicsOptionsValidator = ajv.compile(deleteTopicOptionsSchema)
export const listGroupsOptionsValidator = ajv.compile(listGroupsOptionsSchema)
export const describeGroupsOptionsValidator = ajv.compile(describeGroupsOptionsSchema)
export const deleteGroupsOptionsValidator = ajv.compile(deleteGroupsOptionsSchema)
export const describeClientQuotasOptionsValidator = ajv.compile(describeClientQuotasOptionsSchema)
export const alterClientQuotasOptionsValidator = ajv.compile(alterClientQuotasOptionsSchema)

import {
  AclOperations,
  AclPermissionTypes,
  ClientQuotaMatchTypes,
  ConsumerGroupStates,
  PatternTypes,
  ResourceTypes
} from '../../apis/enumerations.ts'
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
        type: 'object',
        properties: {
          entityType: { type: 'string', minLength: 1 },
          matchType: { type: 'number', enum: Object.values(ClientQuotaMatchTypes) },
          match: { type: 'string' }
        },
        required: ['entityType', 'matchType'],
        additionalProperties: false
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

export const describeLogDirsOptionsSchema = {
  type: 'object',
  properties: {
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          name: { type: 'string', minLength: 1 },
          partitions: {
            type: 'array',
            items: { type: 'number', minimum: 0 },
            minItems: 1
          }
        },
        required: ['name', 'partitions'],
        additionalProperties: false
      },
      minItems: 1
    }
  },
  required: ['topics'],
  additionalProperties: false
}

const aclSchema = {
  type: 'object',
  properties: {
    resourceType: { type: 'number', enum: Object.values(ResourceTypes) },
    resourceName: { type: 'string', minLength: 1 },
    patternType: { type: 'number', enum: Object.values(PatternTypes) },
    principal: { type: 'string', minLength: 1 },
    host: { type: 'string', minLength: 1 },
    operation: { type: 'number', enum: Object.values(AclOperations) },
    permissionType: { type: 'number', enum: Object.values(AclPermissionTypes) }
  },
  required: ['resourceType', 'resourceName', 'patternType', 'principal', 'host', 'operation', 'permissionType'],
  additionalProperties: false
}

const aclFilterSchema = {
  type: 'object',
  properties: {
    resourceType: { type: 'number', enum: Object.values(ResourceTypes) },
    resourceName: {
      anyOf: [{ type: 'string', minLength: 1 }, { type: 'null' }]
    },
    patternType: { type: 'number', enum: Object.values(PatternTypes) },
    principal: {
      anyOf: [{ type: 'string', minLength: 1 }, { type: 'null' }]
    },
    host: {
      anyOf: [{ type: 'string', minLength: 1 }, { type: 'null' }]
    },
    operation: { type: 'number', enum: Object.values(AclOperations) },
    permissionType: { type: 'number', enum: Object.values(AclPermissionTypes) }
  },
  required: ['resourceType', 'patternType', 'operation', 'permissionType'],
  additionalProperties: false
}

export const createAclsOptionsSchema = {
  type: 'object',
  properties: {
    creations: {
      type: 'array',
      items: aclSchema,
      minItems: 1
    }
  },
  required: ['creations'],
  additionalProperties: false
}

export const describeAclsOptionsSchema = {
  type: 'object',
  properties: {
    filter: aclFilterSchema
  },
  required: ['filter'],
  additionalProperties: false
}

export const deleteAclsOptionsSchema = {
  type: 'object',
  properties: {
    filters: {
      type: 'array',
      items: aclFilterSchema,
      minItems: 1
    }
  },
  required: ['filters'],
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
export const describeLogDirsOptionsValidator = ajv.compile(describeLogDirsOptionsSchema)
export const createAclsOptionsValidator = ajv.compile(createAclsOptionsSchema)
export const describeAclsOptionsValidator = ajv.compile(describeAclsOptionsSchema)
export const deleteAclsOptionsValidator = ajv.compile(deleteAclsOptionsSchema)

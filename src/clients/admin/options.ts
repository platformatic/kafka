import {
  allowedIncrementalAlterConfigOperationTypes,
  allowedResourceTypes,
  ConsumerGroupStates,
  IncrementalAlterConfigOperationTypes,
  allowedAclOperations,
  allowedAclPermissionTypes,
  allowedResourcePatternTypes,
  allowedFetchIsolationLevels,
  allowedClientQuotaMatchTypes
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

export const createPartitionsOptionsSchema = {
  type: 'object',
  properties: {
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          name: idProperty,
          count: { type: 'number', minimum: 1 },
          assignments: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                brokerIds: { type: 'array', items: { type: 'number' }, minItems: 1 }
              },
              required: ['brokerIds'],
              additionalProperties: false
            }
          }
        },
        required: ['name', 'count'],
        additionalProperties: false
      },
      minItems: 1
    },
    validateOnly: { type: 'boolean', default: false }
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

export const removeMembersFromConsumerGroupOptionsSchema = {
  type: 'object',
  properties: {
    groupId: idProperty,
    members: {
      type: ['array', 'null'],
      items: {
        type: 'object',
        properties: {
          memberId: idProperty,
          reason: { type: 'string', minLength: 1 }
        },
        required: ['memberId'],
        additionalProperties: false
      },
      default: null
    }
  },
  required: ['groupId'],
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
          matchType: { type: 'number', enum: allowedClientQuotaMatchTypes },
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

export const listConsumerGroupOffsetsOptionsSchema = {
  type: 'object',
  properties: {
    groups: {
      type: 'array',
      items: {
        oneOf: [
          { type: 'string', minLength: 1 },
          {
            type: 'object',
            properties: {
              groupId: { type: 'string', minLength: 1 },
              topics: {
                type: ['array', 'null'],
                items: {
                  type: 'object',
                  properties: {
                    name: { type: 'string', minLength: 1 },
                    partitionIndexes: {
                      type: 'array',
                      items: { type: 'number', minimum: 0 },
                      minItems: 1
                    }
                  },
                  required: ['name', 'partitionIndexes'],
                  additionalProperties: false
                },
                minItems: 1
              }
            },
            required: ['groupId'],
            additionalProperties: false
          }
        ]
      },
      minItems: 1
    },
    requireStable: {
      type: 'boolean'
    }
  },
  required: ['groups'],
  additionalProperties: false
}

export const alterConsumerGroupOffsetsOptionsSchema = {
  type: 'object',
  properties: {
    groupId: { type: 'string', minLength: 1 },
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          name: { type: 'string', minLength: 1 },
          partitionOffsets: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                partition: { type: 'number', minimum: 0 },
                offset: { bigint: true }
              },
              required: ['partition', 'offset'],
              additionalProperties: false
            },
            minItems: 1
          }
        },
        required: ['name', 'partitionOffsets'],
        additionalProperties: false
      },
      minItems: 1
    }
  },
  required: ['groupId', 'topics'],
  additionalProperties: false
}

export const deleteConsumerGroupOffsetsOptionsSchema = {
  type: 'object',
  properties: {
    groupId: { type: 'string', minLength: 1 },
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          name: { type: 'string', minLength: 1 },
          partitionIndexes: {
            type: 'array',
            items: { type: 'number', minimum: 0 },
            minItems: 1
          }
        },
        required: ['name', 'partitionIndexes'],
        additionalProperties: false
      },
      minItems: 1
    }
  },
  required: ['groupId', 'topics'],
  additionalProperties: false
}

export const describeConfigsOptionsSchema = {
  type: 'object',
  properties: {
    resources: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          resourceType: { type: 'number', enum: allowedResourceTypes },
          resourceName: { type: 'string', minLength: 1 },
          configurationKeys: {
            type: ['array', 'null'],
            items: { type: 'string', minLength: 1 }
          }
        },
        required: ['resourceType', 'resourceName'],
        additionalProperties: false
      },
      minItems: 1
    },
    includeSynonyms: { type: 'boolean' },
    includeDocumentation: { type: 'boolean' }
  },
  required: ['resources'],
  additionalProperties: false
}

export const alterConfigsOptionsSchema = {
  type: 'object',
  properties: {
    resources: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          resourceType: { type: 'number', enum: allowedResourceTypes },
          resourceName: { type: 'string', minLength: 1 },
          configs: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                name: { type: 'string', minLength: 1 },
                value: { type: ['string', 'null'] }
              },
              required: ['name'],
              additionalProperties: false
            },
            minItems: 1
          }
        },
        required: ['resourceType', 'resourceName', 'configs'],
        additionalProperties: false
      },
      minItems: 1
    },
    validateOnly: { type: 'boolean' }
  },
  required: ['resources'],
  additionalProperties: false
}

export const incrementalAlterConfigsOptionsSchema = {
  type: 'object',
  properties: {
    resources: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          resourceType: { type: 'number', enum: allowedResourceTypes },
          resourceName: { type: 'string', minLength: 1 },
          configs: {
            type: 'array',
            items: {
              oneOf: [
                {
                  type: 'object',
                  properties: {
                    name: { type: 'string', minLength: 1 },
                    configOperation: {
                      type: 'number',
                      enum: [
                        ...allowedIncrementalAlterConfigOperationTypes.filter(
                          v => v !== IncrementalAlterConfigOperationTypes.DELETE
                        )
                      ]
                    },
                    value: { type: 'string' }
                  },
                  required: ['name', 'configOperation', 'value'],
                  additionalProperties: false
                },
                {
                  type: 'object',
                  properties: {
                    name: { type: 'string', minLength: 1 },
                    configOperation: { type: 'number', enum: [IncrementalAlterConfigOperationTypes.DELETE] }
                  },
                  required: ['name', 'configOperation'],
                  additionalProperties: false
                }
              ]
            },
            minItems: 1
          }
        },
        required: ['resourceType', 'resourceName', 'configs'],
        additionalProperties: false
      },
      minItems: 1
    },
    validateOnly: { type: 'boolean' }
  },
  required: ['resources'],
  additionalProperties: false
}

const aclSchema = {
  type: 'object',
  properties: {
    resourceType: { type: 'number', enum: allowedResourceTypes },
    resourceName: { type: 'string', minLength: 1 },
    resourcePatternType: { type: 'number', enum: allowedResourcePatternTypes },
    principal: { type: 'string', minLength: 1 },
    host: { type: 'string', minLength: 1 },
    operation: { type: 'number', enum: allowedAclOperations },
    permissionType: { type: 'number', enum: allowedAclPermissionTypes }
  },
  required: ['resourceType', 'resourceName', 'resourcePatternType', 'principal', 'host', 'operation', 'permissionType'],
  additionalProperties: false
}

const aclFilterSchema = {
  type: 'object',
  properties: {
    resourceType: { type: 'number', enum: allowedResourceTypes },
    resourceName: {
      anyOf: [{ type: 'string', minLength: 1 }, { type: 'null' }]
    },
    resourcePatternType: { type: 'number', enum: allowedResourcePatternTypes },
    principal: {
      anyOf: [{ type: 'string', minLength: 1 }, { type: 'null' }]
    },
    host: {
      anyOf: [{ type: 'string', minLength: 1 }, { type: 'null' }]
    },
    operation: { type: 'number', enum: allowedAclOperations },
    permissionType: { type: 'number', enum: allowedAclPermissionTypes }
  },
  required: ['resourceType', 'resourcePatternType', 'operation', 'permissionType'],
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

export const listOffsetsOptionsSchema = {
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
            items: {
              type: 'object',
              properties: {
                partitionIndex: { type: 'number', minimum: 0 },
                timestamp: { bigint: true }
              },
              required: ['partitionIndex', 'timestamp'],
              additionalProperties: false
            },
            minItems: 1
          }
        },
        required: ['name', 'partitions'],
        additionalProperties: false
      },
      minItems: 1
    },
    isolationLevel: { type: ['number', 'null'], enum: [null, ...allowedFetchIsolationLevels] }
  },
  required: ['topics'],
  additionalProperties: false
}

export const createTopicsOptionsValidator = ajv.compile(createTopicOptionsSchema)
export const createPartitionsOptionsValidator = ajv.compile(createPartitionsOptionsSchema)
export const listTopicsOptionsValidator = ajv.compile(listTopicOptionsSchema)
export const deleteTopicsOptionsValidator = ajv.compile(deleteTopicOptionsSchema)
export const listGroupsOptionsValidator = ajv.compile(listGroupsOptionsSchema)
export const describeGroupsOptionsValidator = ajv.compile(describeGroupsOptionsSchema)
export const deleteGroupsOptionsValidator = ajv.compile(deleteGroupsOptionsSchema)
export const removeMembersFromConsumerGroupOptionsValidator = ajv.compile(removeMembersFromConsumerGroupOptionsSchema)
export const describeClientQuotasOptionsValidator = ajv.compile(describeClientQuotasOptionsSchema)
export const alterClientQuotasOptionsValidator = ajv.compile(alterClientQuotasOptionsSchema)
export const describeLogDirsOptionsValidator = ajv.compile(describeLogDirsOptionsSchema)
export const alterConsumerGroupOffsetsOptionsValidator = ajv.compile(alterConsumerGroupOffsetsOptionsSchema)
export const deleteConsumerGroupOffsetsOptionsValidator = ajv.compile(deleteConsumerGroupOffsetsOptionsSchema)
export const listConsumerGroupOffsetsOptionsValidator = ajv.compile(listConsumerGroupOffsetsOptionsSchema)
export const describeConfigsOptionsValidator = ajv.compile(describeConfigsOptionsSchema)
export const alterConfigsOptionsValidator = ajv.compile(alterConfigsOptionsSchema)
export const incrementalAlterConfigsOptionsValidator = ajv.compile(incrementalAlterConfigsOptionsSchema)
export const createAclsOptionsValidator = ajv.compile(createAclsOptionsSchema)
export const describeAclsOptionsValidator = ajv.compile(describeAclsOptionsSchema)
export const deleteAclsOptionsValidator = ajv.compile(deleteAclsOptionsSchema)
export const listOffsetsOptionsValidator = ajv.compile(listOffsetsOptionsSchema)

import {
  type CreateTopicsRequestTopicAssignment,
  type CreateTopicsResponse,
  createTopicsV7
} from '../apis/admin/create-topics.ts'
import { type DeleteTopicsResponse, deleteTopicsV6 } from '../apis/admin/delete-topics.ts'
import { type Callback } from '../apis/definitions.ts'
import { type Broker } from '../connection/definitions.ts'
import { UserError } from '../errors.ts'
import { type NullableString } from '../protocol/definitions.ts'
import { ajv } from '../utils.ts'
import { type CallbackWithPromise, createPromisifiedCallback, kCallbackPromise } from './callbacks.ts'
import { Client, type ClientOptions } from './client.ts'

export interface AdminOptions extends ClientOptions {}

export interface CreatedTopic {
  id: string
  partitions: number
  replicas: number
  configuration: Record<string, NullableString>
}

export interface CreateTopicOptions {
  partitions: number
  replicas: number
  assignments: CreateTopicsRequestTopicAssignment[]
}

export const createTopicOptionsSchema = {
  type: 'object',
  properties: {
    partitions: { type: 'number', minimum: 1 },
    replicas: { type: 'number', minimum: 1 },
    assignments: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          partition: { type: 'number', minimum: 0 },
          replicas: { type: 'array', items: { type: 'number' }, minItems: 1 }
        },
        required: ['partition', 'replicas'],
        additionalProperties: false
      },
      minItems: 1
    }
  },
  additionalProperties: false
}

const createTopicsOptionsValidator = ajv.compile(createTopicOptionsSchema)

export class Admin extends Client<AdminOptions> {
  constructor (
    clientId: string,
    bootstrapBrokers: Broker | string | Broker[] | string[],
    options: Partial<AdminOptions> = {}
  ) {
    super(clientId, bootstrapBrokers, options as Partial<ClientOptions>)
  }

  createTopics (topics: string[], callback: Callback<Record<string, CreatedTopic>>): void
  createTopics (topics: string[], options?: Partial<CreateTopicOptions>): Promise<Record<string, CreatedTopic>>
  createTopics (
    topics: string[],
    options: Partial<CreateTopicOptions>,
    callback: CallbackWithPromise<Record<string, CreatedTopic>>
  ): void
  createTopics (
    topics: string[],
    options: Partial<CreateTopicOptions> | Callback<Record<string, CreatedTopic>> = {},
    callback?: CallbackWithPromise<Record<string, CreatedTopic>>
  ): void | Promise<Record<string, CreatedTopic>> {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

    if (!callback) {
      callback = createPromisifiedCallback<Record<string, CreatedTopic>>()
    }

    for (let i = 0; i < topics.length; i++) {
      if (typeof topics[i] !== 'string' || !topics[i]) {
        callback(
          new UserError(`/topics/${i} must be a non-empty string.`),
          undefined as unknown as Record<string, CreatedTopic>
        )

        return callback[kCallbackPromise]
      }
    }

    const validationError = this.validateOptions(options, createTopicsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Record<string, CreatedTopic>)
      return callback[kCallbackPromise]
    }

    const numPartitions = options.partitions ?? 1
    const replicationFactor = options.replicas ?? 1
    const assignments = (options.assignments ?? []) as CreateTopicsRequestTopicAssignment[]

    const requests = topics.map(name => {
      return {
        name,
        numPartitions,
        replicationFactor,
        assignments,
        configs: []
      }
    })

    return this.performDeduplicated(
      'createTopics',
      deduplicateCallback => {
        this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
          if (error) {
            deduplicateCallback(error, undefined as unknown as Record<string, CreatedTopic>)
            return
          }

          this.performWithRetry(
            'Creating topics failed.',
            cb => {
              createTopicsV7(
                connection,
                requests,
                this.options.operationTimeout,
                false,
                cb as unknown as Callback<CreateTopicsResponse>
              )
            },
            (error: Error | null, response: CreateTopicsResponse) => {
              if (error) {
                deduplicateCallback(error, undefined as unknown as Record<string, CreatedTopic>)
                return
              }

              const created: Record<string, CreatedTopic> = {}

              for (const {
                name,
                topicId: id,
                numPartitions: partitions,
                replicationFactor: replicas,
                configs
              } of response.topics) {
                const configuration: CreatedTopic['configuration'] = {}

                for (const { name, value } of configs) {
                  configuration[name] = value
                }

                created[name] = { id, partitions, replicas, configuration }
              }

              deduplicateCallback(null, created)
            },
            0,
            this.options.retries
          )
        })
      },
      callback
    )
  }

  deleteTopics (topics: string[], callback: CallbackWithPromise<void>): void
  deleteTopics (topics: string[]): Promise<void>
  deleteTopics (topics: string[], callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    for (let i = 0; i < topics.length; i++) {
      if (typeof topics[i] !== 'string' || !topics[i]) {
        callback(new UserError(`/topics/${i} must be a non-empty string.`))
        return callback[kCallbackPromise]
      }
    }

    return this.performDeduplicated(
      'deleteTopics',
      deduplicateCallback => {
        this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
          if (error) {
            deduplicateCallback(error, undefined)
            return
          }

          this.performWithRetry(
            'Deleting topics failed.',
            cb => {
              deleteTopicsV6(
                connection,
                topics.map(name => ({ name })),
                this.options.operationTimeout,
                cb as unknown as Callback<DeleteTopicsResponse>
              )
            },
            deduplicateCallback,
            0,
            this.options.retries
          )
        })
      },
      callback
    )
  }
}

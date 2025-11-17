import {
  type AlterClientQuotasRequest,
  type AlterClientQuotasResponse,
  type AlterClientQuotasResponseEntries
} from '../../apis/admin/alter-client-quotas-v1.ts'
import { type CreatePartitionsRequest, type CreatePartitionsResponse } from '../../apis/admin/create-partitions-v3.ts'
import { type OffsetDeleteRequest, type OffsetDeleteResponse } from '../../apis/admin/offset-delete-v0.ts'
import {
  type OffsetFetchRequest,
  type OffsetFetchResponse,
  type OffsetFetchRequestGroup
} from '../../apis/consumer/offset-fetch-v9.ts'
import {
  type OffsetCommitRequest,
  type OffsetCommitRequestTopic,
  type OffsetCommitResponse
} from '../../apis/consumer/offset-commit-v9.ts'
import {
  type CreateTopicsRequest,
  type CreateTopicsRequestTopic,
  type CreateTopicsRequestTopicAssignment,
  type CreateTopicsResponse
} from '../../apis/admin/create-topics-v7.ts'
import { type DeleteGroupsRequest, type DeleteGroupsResponse } from '../../apis/admin/delete-groups-v2.ts'
import {
  type DeleteTopicsRequest,
  type DeleteTopicsRequestTopic,
  type DeleteTopicsResponse
} from '../../apis/admin/delete-topics-v6.ts'
import {
  type DescribeClientQuotasRequest,
  type DescribeClientQuotasResponse,
  type DescribeClientQuotasResponseEntry
} from '../../apis/admin/describe-client-quotas-v0.ts'
import { type DescribeGroupsRequest, type DescribeGroupsResponse } from '../../apis/admin/describe-groups-v5.ts'
import { type DescribeLogDirsRequest, type DescribeLogDirsResponse } from '../../apis/admin/describe-log-dirs-v4.ts'
import { type ListGroupsRequest as ListGroupsRequestV4 } from '../../apis/admin/list-groups-v4.ts'
import {
  type LeaveGroupRequest,
  type LeaveGroupRequestMember,
  type LeaveGroupResponse
} from '../../apis/consumer/leave-group-v5.ts'
import {
  type ListGroupsRequest as ListGroupsRequestV5,
  type ListGroupsResponse
} from '../../apis/admin/list-groups-v5.ts'
import {
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks,
  type CallbackWithPromise
} from '../../apis/callbacks.ts'
import { type Callback } from '../../apis/definitions.ts'
import { FindCoordinatorKeyTypes, type ConsumerGroupStateValue } from '../../apis/enumerations.ts'
import { type FindCoordinatorRequest, type FindCoordinatorResponse } from '../../apis/metadata/find-coordinator-v6.ts'
import { type MetadataRequest, type MetadataResponse } from '../../apis/metadata/metadata-v12.ts'
import {
  adminClientQuotasChannel,
  adminConsumerGroupOffsetsChannel,
  adminGroupsChannel,
  adminLogDirsChannel,
  adminTopicsChannel,
  createDiagnosticContext
} from '../../diagnostic.ts'
import { MultipleErrors } from '../../errors.ts'
import { type Broker, type Connection } from '../../index.ts'
import { Reader } from '../../protocol/reader.ts'
import {
  Base,
  kAfterCreate,
  kCheckNotClosed,
  kConnections,
  kGetApi,
  kGetBootstrapConnection,
  kGetConnection,
  kMetadata,
  kOptions,
  kPerformDeduplicated,
  kPerformWithRetry,
  kValidateOptions
} from '../base/base.ts'
import { type BaseOptions } from '../base/types.ts'
import { type GroupAssignment } from '../consumer/types.ts'
import {
  alterClientQuotasOptionsValidator,
  createPartitionsOptionsValidator,
  createTopicsOptionsValidator,
  deleteGroupsOptionsValidator,
  deleteTopicsOptionsValidator,
  describeClientQuotasOptionsValidator,
  describeGroupsOptionsValidator,
  describeLogDirsOptionsValidator,
  listGroupsOptionsValidator,
  listTopicsOptionsValidator,
  removeMembersFromConsumerGroupOptionsValidator,
  alterConsumerGroupOffsetsOptionsValidator,
  deleteConsumerGroupOffsetsOptionsValidator,
  listConsumerGroupOffsetsOptionsValidator
} from './options.ts'
import {
  type AdminOptions,
  type AlterClientQuotasOptions,
  type BrokerLogDirDescription,
  type CreatedTopic,
  type CreatePartitionsOptions,
  type CreateTopicsOptions,
  type DeleteGroupsOptions,
  type DeleteTopicsOptions,
  type DescribeClientQuotasOptions,
  type DeleteConsumerGroupOffsetsOptions,
  type ListConsumerGroupOffsetsOptions,
  type AlterConsumerGroupOffsetsOptions,
  type DescribeGroupsOptions,
  type DescribeLogDirsOptions,
  type Group,
  type GroupBase,
  type GroupMember,
  type ListGroupsOptions,
  type ListTopicsOptions,
  type RemoveMembersFromConsumerGroupOptions,
  type ListConsumerGroupOffsetsGroup
} from './types.ts'

export class Admin extends Base<AdminOptions> {
  #controller: Broker | null = null

  constructor (options: AdminOptions) {
    super(options as BaseOptions)
    this[kAfterCreate]('admin')
  }

  listTopics (options: ListTopicsOptions, callback: Callback<string[]>): void
  listTopics (options?: ListTopicsOptions): Promise<string[]>
  listTopics (options?: ListTopicsOptions, callback?: CallbackWithPromise<string[]>): void | Promise<string[]> {
    if (!callback) {
      callback = createPromisifiedCallback<string[]>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!options) {
      options = {}
    }

    const validationError = this[kValidateOptions](options, listTopicsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as string[])
      return callback[kCallbackPromise]
    }

    adminTopicsChannel.traceCallback(
      this.#listTopics,
      1,
      createDiagnosticContext({ client: this, operation: 'listTopics', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  createTopics (options: CreateTopicsOptions, callback: Callback<CreatedTopic[]>): void
  createTopics (options: CreateTopicsOptions): Promise<CreatedTopic[]>
  createTopics (
    options: CreateTopicsOptions,
    callback?: CallbackWithPromise<CreatedTopic[]>
  ): void | Promise<CreatedTopic[]> {
    if (!callback) {
      callback = createPromisifiedCallback<CreatedTopic[]>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, createTopicsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as CreatedTopic[])
      return callback[kCallbackPromise]
    }

    adminTopicsChannel.traceCallback(
      this.#createTopics,
      1,
      createDiagnosticContext({ client: this, operation: 'createTopics', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  deleteTopics (options: DeleteTopicsOptions, callback: CallbackWithPromise<void>): void
  deleteTopics (options: DeleteTopicsOptions): Promise<void>
  deleteTopics (options: DeleteTopicsOptions, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, deleteTopicsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as void)
      return callback[kCallbackPromise]
    }

    adminTopicsChannel.traceCallback(
      this.#deleteTopics,
      1,
      createDiagnosticContext({ client: this, operation: 'deleteTopics', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  createPartitions (options: CreatePartitionsOptions, callback: CallbackWithPromise<void>): void
  createPartitions (options: CreatePartitionsOptions): Promise<void>
  createPartitions (options: CreatePartitionsOptions, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, createPartitionsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined)
      return callback[kCallbackPromise]
    }

    adminTopicsChannel.traceCallback(
      this.#createPartitions,
      1,
      createDiagnosticContext({ client: this, operation: 'createPartitions', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  listGroups (options: ListGroupsOptions, callback: CallbackWithPromise<Map<string, GroupBase>>): void
  listGroups (options?: ListGroupsOptions): Promise<Map<string, GroupBase>>
  listGroups (
    options?: ListGroupsOptions,
    callback?: CallbackWithPromise<Map<string, GroupBase>>
  ): void | Promise<Map<string, GroupBase>> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (!options) {
      options = {}
    }

    const validationError = this[kValidateOptions](options, listGroupsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Map<string, GroupBase>)
      return callback[kCallbackPromise]
    }

    options.types ??= ['classic']

    adminGroupsChannel.traceCallback(
      this.#listGroups,
      1,
      createDiagnosticContext({ client: this, operation: 'listGroups', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  describeGroups (options: DescribeGroupsOptions, callback: CallbackWithPromise<Map<string, Group>>): void
  describeGroups (options: DescribeGroupsOptions): Promise<Map<string, Group>>
  describeGroups (
    options: DescribeGroupsOptions,
    callback?: CallbackWithPromise<Map<string, Group>>
  ): void | Promise<Map<string, Group>> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, describeGroupsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Map<string, Group>)
      return callback[kCallbackPromise]
    }

    adminGroupsChannel.traceCallback(
      this.#describeGroups,
      1,
      createDiagnosticContext({ client: this, operation: 'describeGroups', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  deleteGroups (options: DeleteGroupsOptions, callback: CallbackWithPromise<void>): void
  deleteGroups (options: DeleteGroupsOptions): Promise<void>
  deleteGroups (options: DeleteGroupsOptions, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, deleteGroupsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as void)
      return callback[kCallbackPromise]
    }

    adminGroupsChannel.traceCallback(
      this.#deleteGroups,
      1,
      createDiagnosticContext({ client: this, operation: 'deleteGroups', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  removeMembersFromConsumerGroup (
    options: RemoveMembersFromConsumerGroupOptions,
    callback: CallbackWithPromise<void>
  ): void
  removeMembersFromConsumerGroup (options: RemoveMembersFromConsumerGroupOptions): Promise<void>
  removeMembersFromConsumerGroup (
    options: RemoveMembersFromConsumerGroupOptions,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](
      options,
      removeMembersFromConsumerGroupOptionsValidator,
      '/options',
      false
    )
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    adminGroupsChannel.traceCallback(
      this.#removeMembersFromConsumerGroup,
      1,
      createDiagnosticContext({ client: this, operation: 'removeMembersFromConsumerGroup', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  describeClientQuotas (
    options: DescribeClientQuotasOptions,
    callback: CallbackWithPromise<DescribeClientQuotasResponseEntry[]>
  ): void
  describeClientQuotas (options: DescribeClientQuotasOptions): Promise<DescribeClientQuotasResponseEntry[]>
  describeClientQuotas (
    options: DescribeClientQuotasOptions,
    callback?: CallbackWithPromise<DescribeClientQuotasResponseEntry[]>
  ): void | Promise<DescribeClientQuotasResponseEntry[]> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, describeClientQuotasOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as DescribeClientQuotasResponseEntry[])
      return callback[kCallbackPromise]
    }

    adminClientQuotasChannel.traceCallback(
      this.#describeClientQuotas,
      1,
      createDiagnosticContext({ client: this, operation: 'describeClientQuotas', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  alterClientQuotas (
    options: AlterClientQuotasOptions,
    callback: CallbackWithPromise<AlterClientQuotasResponseEntries[]>
  ): void
  alterClientQuotas (options: AlterClientQuotasOptions): Promise<AlterClientQuotasResponseEntries[]>
  alterClientQuotas (
    options: AlterClientQuotasOptions,
    callback?: CallbackWithPromise<AlterClientQuotasResponseEntries[]>
  ): void | Promise<AlterClientQuotasResponseEntries[]> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, alterClientQuotasOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as AlterClientQuotasResponseEntries[])
      return callback[kCallbackPromise]
    }

    adminClientQuotasChannel.traceCallback(
      this.#alterClientQuotas,
      1,
      createDiagnosticContext({ client: this, operation: 'alterClientQuotas', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  describeLogDirs (options: DescribeLogDirsOptions, callback: CallbackWithPromise<BrokerLogDirDescription[]>): void
  describeLogDirs (options: DescribeLogDirsOptions): Promise<BrokerLogDirDescription[]>
  describeLogDirs (
    options: DescribeLogDirsOptions,
    callback?: CallbackWithPromise<BrokerLogDirDescription[]>
  ): void | Promise<BrokerLogDirDescription[]> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    /* c8 ignore next 3 - Hard to test */
    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, describeLogDirsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as BrokerLogDirDescription[])
      return callback[kCallbackPromise]
    }

    adminLogDirsChannel.traceCallback(
      this.#describeLogDirs,
      1,
      createDiagnosticContext({ client: this, operation: 'describeLogDirs', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  listConsumerGroupOffsets (
    options: ListConsumerGroupOffsetsOptions,
    callback: CallbackWithPromise<ListConsumerGroupOffsetsGroup[]>
  ): void
  listConsumerGroupOffsets (options: ListConsumerGroupOffsetsOptions): Promise<ListConsumerGroupOffsetsGroup[]>
  listConsumerGroupOffsets (
    options: ListConsumerGroupOffsetsOptions,
    callback?: CallbackWithPromise<ListConsumerGroupOffsetsGroup[]>
  ): void | Promise<ListConsumerGroupOffsetsGroup[]> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, listConsumerGroupOffsetsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as ListConsumerGroupOffsetsGroup[])
      return callback[kCallbackPromise]
    }

    adminConsumerGroupOffsetsChannel.traceCallback(
      this.#listConsumerGroupOffsets,
      1,
      createDiagnosticContext({ client: this, operation: 'listConsumerGroupOffsets', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  alterConsumerGroupOffsets (options: AlterConsumerGroupOffsetsOptions, callback: CallbackWithPromise<void>): void
  alterConsumerGroupOffsets (options: AlterConsumerGroupOffsetsOptions): Promise<void>
  alterConsumerGroupOffsets (
    options: AlterConsumerGroupOffsetsOptions,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](
      options,
      alterConsumerGroupOffsetsOptionsValidator,
      '/options',
      false
    )
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    adminConsumerGroupOffsetsChannel.traceCallback(
      this.#alterConsumerGroupOffsets,
      1,
      createDiagnosticContext({ client: this, operation: 'alterConsumerGroupOffsets', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  deleteConsumerGroupOffsets (
    options: DeleteConsumerGroupOffsetsOptions,
    callback: CallbackWithPromise<{ name: string; partitionIndexes: number[] }[]>
  ): void
  deleteConsumerGroupOffsets (
    options: DeleteConsumerGroupOffsetsOptions
  ): Promise<{ name: string; partitionIndexes: number[] }[]>
  deleteConsumerGroupOffsets (
    options: DeleteConsumerGroupOffsetsOptions,
    callback?: CallbackWithPromise<{ name: string; partitionIndexes: number[] }[]>
  ): void | Promise<{ name: string; partitionIndexes: number[] }[]> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](
      options,
      deleteConsumerGroupOffsetsOptionsValidator,
      '/options',
      false
    )
    if (validationError) {
      callback(validationError, undefined as unknown as { name: string; partitionIndexes: number[] }[])
      return callback[kCallbackPromise]
    }

    adminConsumerGroupOffsetsChannel.traceCallback(
      this.#deleteConsumerGroupOffsets,
      1,
      createDiagnosticContext({ client: this, operation: 'deleteConsumerGroupOffsets', options }),
      this,
      options,
      callback
    )

    return callback[kCallbackPromise]
  }

  #getControllerConnection (callback: Callback<Connection>): void {
    if (this.#controller) {
      this[kConnections].get(this.#controller, callback)
    } else {
      this[kGetBootstrapConnection](callback)
    }
  }

  #handleNotControllerError<T> (error: Error | null, value: T, callback: Callback<T>): void {
    if (error && (error as MultipleErrors)?.findBy?.('apiCode', 41)) {
      this.metadata({ topics: [] }, (metadataError, metadata) => {
        if (metadataError) {
          callback(metadataError, undefined as unknown as T)
        }
        this.#controller = metadata.brokers.get(metadata.controllerId) || null
        callback(error, undefined as unknown as T)
      })
    } else {
      callback(error, value)
    }
  }

  #listTopics (options: ListTopicsOptions, callback: CallbackWithPromise<string[]>): void {
    const includeInternals = options.includeInternals ?? false

    this[kPerformDeduplicated](
      'metadata',
      deduplicateCallback => {
        this[kPerformWithRetry]<MetadataResponse>(
          'metadata',
          retryCallback => {
            this[kGetBootstrapConnection]((error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as MetadataResponse)
                return
              }

              this[kGetApi]<MetadataRequest, MetadataResponse>('Metadata', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as MetadataResponse)
                  return
                }

                api(connection, null, false, false, retryCallback)
              })
            })
          },
          (error: Error | null, metadata: MetadataResponse) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as string[])
              return
            }

            const topics: Set<string> = new Set()

            for (const { name, isInternal } of metadata.topics) {
              /* c8 ignore next 3 - Sometimes internal topics might be returned by Kafka */
              if (isInternal && !includeInternals) {
                continue
              }

              topics.add(name!)
            }

            deduplicateCallback(null, Array.from(topics).sort())
          },
          0
        )
      },
      callback
    )
  }

  #createTopics (options: CreateTopicsOptions, callback: CallbackWithPromise<CreatedTopic[]>): void {
    // -1 is required if manual assignments are used. If no manual assignments are used, -1 will default to broker settings.
    const numPartitions = options.partitions ?? -1
    const replicationFactor = options.replicas ?? -1
    const assignments: CreateTopicsRequestTopicAssignment[] = []
    const configs = options.configs ?? []

    for (const { partition, brokers } of options.assignments ?? []) {
      assignments.push({ partitionIndex: partition, brokerIds: brokers })
    }

    const requests: CreateTopicsRequestTopic[] = []
    for (const topic of options.topics) {
      requests.push({
        name: topic,
        numPartitions,
        replicationFactor,
        assignments,
        configs
      })
    }

    this[kPerformDeduplicated](
      `createTopics-${options.topics.join(',')}`,
      deduplicateCallback => {
        this[kPerformWithRetry](
          'createTopics',
          retryCallback => {
            this.#getControllerConnection((error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as CreateTopicsResponse)
                return
              }

              this[kGetApi]<CreateTopicsRequest, CreateTopicsResponse>('CreateTopics', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as CreateTopicsResponse)
                  return
                }

                api(connection, requests, this[kOptions].timeout!, false, (error, response) => {
                  this.#handleNotControllerError(error, response, retryCallback)
                })
              })
            })
          },
          (error: Error | null, response: CreateTopicsResponse) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as CreatedTopic[])
              return
            }

            const created: CreatedTopic[] = []

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

              created.push({ id, name, partitions, replicas, configuration })
            }

            deduplicateCallback(null, created)
          },
          0
        )
      },
      callback
    )
  }

  #deleteTopics (options: DeleteTopicsOptions, callback: CallbackWithPromise<void>): void {
    this[kPerformDeduplicated](
      `deleteTopics-${options.topics.join(',')}`,
      deduplicateCallback => {
        this[kPerformWithRetry](
          'deleteTopics',
          retryCallback => {
            this.#getControllerConnection((error, connection) => {
              if (error) {
                retryCallback(error, undefined)
                return
              }

              const requests: DeleteTopicsRequestTopic[] = []
              for (const topic of options.topics) {
                requests.push({ name: topic })
              }

              this[kGetApi]<DeleteTopicsRequest, DeleteTopicsResponse>('DeleteTopics', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as DeleteTopicsResponse)
                  return
                }

                api(connection, requests, this[kOptions].timeout!, (error, response) => {
                  this.#handleNotControllerError(error, response, retryCallback)
                })
              })
            })
          },
          deduplicateCallback,
          0
        )
      },
      error => callback(error)
    )
  }

  #createPartitions (options: CreatePartitionsOptions, callback: CallbackWithPromise<void>): void {
    this[kPerformDeduplicated](
      'createPartitions',
      deduplicateCallback => {
        this[kPerformWithRetry](
          'createPartitions',
          retryCallback => {
            this.#getControllerConnection((error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as CreatePartitionsResponse)
                return
              }

              this[kGetApi]<CreatePartitionsRequest, CreatePartitionsResponse>('CreatePartitions', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as CreatePartitionsResponse)
                  return
                }

                api(connection, options.topics, this[kOptions].timeout!, options.validateOnly ?? false, (
                  error,
                  response
                ) => {
                  this.#handleNotControllerError(error, response, retryCallback)
                })
              })
            })
          },
          deduplicateCallback,
          0
        )
      },
      error => {
        if (error) {
          callback(new MultipleErrors('Creating partitions failed.', [error]))
        } else {
          callback(null)
        }
      }
    )
  }

  #listGroups (options: ListGroupsOptions, callback: CallbackWithPromise<Map<string, GroupBase>>): void {
    // Find all the brokers in the cluster
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error, undefined as unknown as Map<string, GroupBase>)
        return
      }

      runConcurrentCallbacks<ListGroupsResponse>(
        'Listing groups failed.',
        metadata.brokers,
        ([, broker], concurrentCallback) => {
          this[kGetConnection](broker, (error, connection) => {
            if (error) {
              concurrentCallback(error, undefined as unknown as ListGroupsResponse)
              return
            }

            this[kPerformWithRetry]<ListGroupsResponse>(
              'listGroups',
              retryCallback => {
                this[kGetApi]<ListGroupsRequestV4 | ListGroupsRequestV5, ListGroupsResponse>('ListGroups', (
                  error,
                  api
                ) => {
                  if (error) {
                    retryCallback(error, undefined as unknown as ListGroupsResponse)
                    return
                  }

                  /* c8 ignore next 5 */
                  if (api.version === 4) {
                    api(connection, (options.states as ConsumerGroupStateValue[]) ?? [], retryCallback)
                  } else {
                    api(connection, (options.states as ConsumerGroupStateValue[]) ?? [], options.types!, retryCallback)
                  }
                })
              },
              concurrentCallback,
              0
            )
          })
        },
        (error, results) => {
          if (error) {
            callback(error, undefined as unknown as Map<string, GroupBase>)
            return
          }

          const groups: Map<string, GroupBase> = new Map()
          for (const result of results) {
            for (const raw of result.groups) {
              groups.set(raw.groupId, {
                id: raw.groupId,
                state: raw.groupState.toUpperCase() as ConsumerGroupStateValue,
                groupType: raw.groupType,
                protocolType: raw.protocolType
              })
            }
          }

          callback(null, groups)
        }
      )
    })
  }

  #describeGroups (options: DescribeGroupsOptions, callback: CallbackWithPromise<Map<string, Group>>): void {
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error, undefined as unknown as Map<string, Group>)
        return
      }

      this.#findGroupCoordinator(options.groups, (error, response) => {
        if (error) {
          callback(error, undefined as unknown as Map<string, Group>)
          return
        }

        // Group the groups by coordinator
        const coordinators: Map<number, string[]> = new Map()
        for (const { key: group, nodeId: node } of response.coordinators) {
          let coordinator = coordinators.get(node)
          if (!coordinator) {
            coordinator = []
            coordinators.set(node, coordinator)
          }

          coordinator.push(group)
        }

        runConcurrentCallbacks<DescribeGroupsResponse>(
          'Describing groups failed.',
          coordinators,
          ([node, groups], concurrentCallback) => {
            this[kGetConnection](metadata.brokers.get(node)!, (error, connection) => {
              if (error) {
                concurrentCallback(error, undefined as unknown as DescribeGroupsResponse)
                return
              }

              this[kPerformWithRetry]<DescribeGroupsResponse>(
                'describeGroups',
                retryCallback => {
                  this[kGetApi]<DescribeGroupsRequest, DescribeGroupsResponse>('DescribeGroups', (error, api) => {
                    if (error) {
                      retryCallback(error, undefined as unknown as DescribeGroupsResponse)
                      return
                    }

                    api(connection, groups, options.includeAuthorizedOperations ?? false, retryCallback)
                  })
                },
                concurrentCallback,
                0
              )
            })
          },
          (error, results) => {
            if (error) {
              callback(error, undefined as unknown as Map<string, Group>)
              return
            }

            const groups: Map<string, Group> = new Map()
            for (const result of results) {
              for (const raw of result.groups) {
                const group: Group = {
                  id: raw.groupId,
                  state: raw.groupState.toUpperCase() as ConsumerGroupStateValue,
                  protocolType: raw.protocolType,
                  protocol: raw.protocolData,
                  members: new Map(),
                  authorizedOperations: raw.authorizedOperations
                }

                for (const member of raw.members) {
                  const reader = Reader.from(member.memberMetadata)

                  let memberMetadata: GroupMember['metadata'] | undefined
                  let memberAssignments: Map<string, GroupAssignment> | undefined

                  if (reader.remaining > 0) {
                    memberMetadata = {
                      version: reader.readInt16(),
                      topics: reader.readArray(r => r.readString(false), false, false),
                      metadata: reader.readBytes(false)
                    }

                    reader.reset(member.memberAssignment)
                    reader.skip(2) // Ignore Version information

                    memberAssignments = reader.readMap(
                      r => {
                        const topic = r.readString(false)

                        return [topic, { topic, partitions: reader.readArray(r => r.readInt32(), false, false) }]
                      },
                      false,
                      false
                    )

                    reader.readBytes() // Ignore the user data
                  }

                  group.members.set(member.memberId, {
                    id: member.memberId,
                    groupInstanceId: member.groupInstanceId,
                    clientId: member.clientId,
                    clientHost: member.clientHost,
                    metadata: memberMetadata,
                    assignments: memberAssignments
                  })
                }

                groups.set(group.id, group)
              }
            }

            callback(null, groups)
          }
        )
      })
    })
  }

  #deleteGroups (options: DeleteGroupsOptions, callback: CallbackWithPromise<void>): void {
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error)
        return
      }

      this.#findGroupCoordinator(options.groups, (error, response) => {
        if (error) {
          callback(error)
          return
        }

        // Group the groups by coordinator
        const coordinators: Map<number, string[]> = new Map()
        for (const { key: group, nodeId: node } of response.coordinators) {
          let coordinator = coordinators.get(node)
          if (!coordinator) {
            coordinator = []
            coordinators.set(node, coordinator)
          }

          coordinator.push(group)
        }

        runConcurrentCallbacks(
          'Deleting groups failed.',
          coordinators,
          ([node, groups], concurrentCallback) => {
            this[kGetConnection](metadata.brokers.get(node)!, (error, connection) => {
              if (error) {
                concurrentCallback(error, undefined)
                return
              }

              this[kPerformWithRetry](
                'deleteGroups',
                retryCallback => {
                  this[kGetApi]<DeleteGroupsRequest, DeleteGroupsResponse>('DeleteGroups', (error, api) => {
                    if (error) {
                      retryCallback(error, undefined as unknown as CreateTopicsResponse)
                      return
                    }

                    api(connection, groups, retryCallback)
                  })
                },
                concurrentCallback,
                0
              )
            })
          },
          error => callback(error)
        )
      })
    })
  }

  #removeMembersFromConsumerGroup (
    options: RemoveMembersFromConsumerGroupOptions,
    callback: CallbackWithPromise<void>
  ): void {
    if (!options.members || options.members.length === 0) {
      this.#describeGroups({ groups: [options.groupId] }, (error, groupsMap) => {
        if (error) {
          callback(new MultipleErrors('Removing members from consumer group failed.', [error]))
          return
        }

        const group = groupsMap.get(options.groupId)
        if (!group) {
          callback(new MultipleErrors('Removing members from consumer group failed.', []))
          return
        }

        const allMemberIds = Array.from(group.members.keys())
        if (allMemberIds.length === 0) {
          callback(null)
          return
        }

        this.#removeMembersFromConsumerGroup(
          { ...options, members: allMemberIds.map(memberId => ({ memberId })) },
          callback
        )
      })
      return
    }

    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error, undefined)
        return
      }

      this.#findGroupCoordinator([options.groupId], (error, response) => {
        if (error) {
          callback(new MultipleErrors('Removing members from consumer group failed.', [error]))
          return
        }

        const coordinator = response.coordinators.find(c => c.key === options.groupId)
        if (!coordinator) {
          callback(
            new MultipleErrors('Removing members from consumer group failed.', [
              new Error(`No coordinator found for group ${options.groupId}`)
            ])
          )
          return
        }

        const broker = metadata.brokers.get(coordinator.nodeId)
        if (!broker) {
          callback(
            new MultipleErrors('Removing members from consumer group failed.', [
              new Error(`Broker ${coordinator.nodeId} not found`)
            ])
          )
          return
        }

        this[kGetConnection](broker, (error, connection) => {
          if (error) {
            callback(new MultipleErrors('Removing members from consumer group failed.', [error]))
            return
          }

          this[kPerformWithRetry]<LeaveGroupResponse>(
            'removeMembersFromConsumerGroup',
            retryCallback => {
              this[kGetApi]<LeaveGroupRequest, LeaveGroupResponse>('LeaveGroup', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as LeaveGroupResponse)
                  return
                }

                const members: LeaveGroupRequestMember[] = options.members!.map(member => ({
                  memberId: typeof member === 'string' ? member : member.memberId,
                  groupInstanceId: null,
                  reason: typeof member === 'string' ? 'Not specified' : member.reason
                }))

                api(connection, options.groupId, members, retryCallback as unknown as Callback<LeaveGroupResponse>)
              })
            },
            (error: Error | null) => {
              if (error) {
                callback(new MultipleErrors('Removing members from consumer group failed.', [error]))
                return
              }

              callback(null)
            },
            0
          )
        })
      })
    })
  }

  #findGroupCoordinator (groups: string[], callback: CallbackWithPromise<FindCoordinatorResponse>): void {
    this[kPerformWithRetry]<FindCoordinatorResponse>(
      'findGroupCoordinator',
      retryCallback => {
        this[kGetBootstrapConnection]((error, connection) => {
          if (error) {
            retryCallback(error, undefined as unknown as FindCoordinatorResponse)
            return
          }

          this[kGetApi]<FindCoordinatorRequest, FindCoordinatorResponse>('FindCoordinator', (error, api) => {
            if (error) {
              retryCallback(error, undefined as unknown as FindCoordinatorResponse)
              return
            }

            api(connection, FindCoordinatorKeyTypes.GROUP, groups, retryCallback)
          })
        })
      },
      (error, response) => {
        if (error) {
          callback(error, undefined as unknown as FindCoordinatorResponse)
          return
        }

        callback(null, response)
      },
      0
    )
  }

  #describeClientQuotas (
    options: DescribeClientQuotasOptions,
    callback: CallbackWithPromise<DescribeClientQuotasResponseEntry[]>
  ): void {
    this[kPerformWithRetry](
      'describeClientQuotas',
      retryCallback => {
        this[kGetBootstrapConnection]((error, connection) => {
          if (error) {
            retryCallback(error, undefined as unknown as DescribeClientQuotasResponse)
            return
          }

          this[kGetApi]<DescribeClientQuotasRequest, DescribeClientQuotasResponse>('DescribeClientQuotas', (
            error,
            api
          ) => {
            if (error) {
              retryCallback(error, undefined as unknown as DescribeClientQuotasResponse)
              return
            }

            api(
              connection,
              options.components,
              options.strict ?? false,
              retryCallback as unknown as Callback<DescribeClientQuotasResponse>
            )
          })
        })
      },
      (error: Error | null, response: DescribeClientQuotasResponse) => {
        if (error) {
          callback(
            new MultipleErrors('Describing client quotas failed.', [error]),
            undefined as unknown as DescribeClientQuotasResponseEntry[]
          )
          return
        }

        callback(null, response.entries)
      },
      0
    )
  }

  #alterClientQuotas (
    options: AlterClientQuotasOptions,
    callback: CallbackWithPromise<AlterClientQuotasResponseEntries[]>
  ): void {
    this[kPerformWithRetry](
      'alterClientQuotas',
      retryCallback => {
        this[kGetBootstrapConnection]((error, connection) => {
          if (error) {
            retryCallback(error, undefined as unknown as AlterClientQuotasResponse)
            return
          }

          this[kGetApi]<AlterClientQuotasRequest, AlterClientQuotasResponse>('AlterClientQuotas', (error, api) => {
            if (error) {
              retryCallback(error, undefined as unknown as AlterClientQuotasResponse)
              return
            }

            api(
              connection,
              options.entries,
              options.validateOnly ?? false,
              retryCallback as unknown as Callback<AlterClientQuotasResponse>
            )
          })
        })
      },
      (error: Error | null, response: AlterClientQuotasResponse) => {
        if (error) {
          callback(
            new MultipleErrors('Altering client quotas failed.', [error]),
            undefined as unknown as AlterClientQuotasResponseEntries[]
          )
          return
        }

        callback(null, response.entries)
      },
      0
    )
  }

  #describeLogDirs (options: DescribeLogDirsOptions, callback: CallbackWithPromise<BrokerLogDirDescription[]>): void {
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      /* c8 ignore next 4 - Hard to test */
      if (error) {
        callback(error, undefined as unknown as BrokerLogDirDescription[])
        return
      }

      runConcurrentCallbacks<BrokerLogDirDescription>(
        'Describing log dirs failed.',
        metadata.brokers,
        ([id, broker], concurrentCallback) => {
          this[kGetConnection](broker, (error, connection) => {
            if (error) {
              concurrentCallback(error, undefined as unknown as BrokerLogDirDescription)
              return
            }

            this[kPerformWithRetry]<DescribeLogDirsResponse>(
              'describeLogDirs',
              retryCallback => {
                this[kGetApi]<DescribeLogDirsRequest, DescribeLogDirsResponse>('DescribeLogDirs', (error, api) => {
                  if (error) {
                    retryCallback(error, undefined as unknown as DescribeLogDirsResponse)
                    return
                  }

                  api(connection, options.topics, retryCallback as unknown as Callback<DescribeLogDirsResponse>)
                })
              },
              (error, response) => {
                if (error) {
                  concurrentCallback(error, undefined as unknown as BrokerLogDirDescription)
                  return
                }

                concurrentCallback(null, {
                  broker: id,
                  throttleTimeMs: response.throttleTimeMs,
                  results: response.results.map(result => ({
                    logDir: result.logDir,
                    topics: result.topics,
                    totalBytes: result.totalBytes,
                    usableBytes: result.usableBytes
                  }))
                })
              },
              0
            )
          })
        },
        callback
      )
    })
  }

  #listConsumerGroupOffsets (
    options: ListConsumerGroupOffsetsOptions,
    callback: CallbackWithPromise<ListConsumerGroupOffsetsGroup[]>
  ): void {
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error, undefined as unknown as ListConsumerGroupOffsetsGroup[])
        return
      }

      const groupIds = options.groups.map(group => (typeof group === 'string' ? group : group.groupId))

      this.#findGroupCoordinator(groupIds, (error, response) => {
        if (error) {
          callback(
            new MultipleErrors('Listing consumer group offsets failed.', [error]),
            undefined as unknown as ListConsumerGroupOffsetsGroup[]
          )
          return
        }

        const coordinators: Map<number, OffsetFetchRequestGroup[]> = new Map()
        for (const { key: groupId, nodeId: node } of response.coordinators) {
          const groupRequest: OffsetFetchRequestGroup = {
            groupId,
            memberId: null,
            memberEpoch: -1
          }

          for (const group of options.groups) {
            if (typeof group !== 'string' && group.groupId === groupId && !!group.topics && group.topics.length > 0) {
              groupRequest.topics = group.topics
              break
            }
          }

          let coordinator = coordinators.get(node)
          if (!coordinator) {
            coordinator = []
            coordinators.set(node, coordinator)
          }

          coordinator.push(groupRequest)
        }

        runConcurrentCallbacks<OffsetFetchResponse>(
          'Listing consumer group offsets failed.',
          coordinators,
          ([node, groups], concurrentCallback) => {
            this[kGetConnection](metadata.brokers.get(node)!, (error, connection) => {
              if (error) {
                concurrentCallback(error, undefined as unknown as OffsetFetchResponse)
                return
              }

              this[kPerformWithRetry]<OffsetFetchResponse>(
                'offsetFetch',
                retryCallback => {
                  this[kGetApi]<OffsetFetchRequest, OffsetFetchResponse>('OffsetFetch', (error, api) => {
                    if (error) {
                      retryCallback(error, undefined as unknown as OffsetFetchResponse)
                      return
                    }

                    api(connection, groups, options.requireStable ?? false, retryCallback)
                  })
                },
                concurrentCallback,
                0
              )
            })
          },
          (error, responses) => {
            if (error) {
              callback(error, undefined as unknown as ListConsumerGroupOffsetsGroup[])
              return
            }

            callback(
              null,
              responses.flatMap(r =>
                r.groups.map(group => ({
                  groupId: group.groupId,
                  topics: group.topics.map(topic => ({
                    name: topic.name,
                    partitions: topic.partitions.map(p => ({
                      partitionIndex: p.partitionIndex,
                      committedOffset: p.committedOffset,
                      committedLeaderEpoch: p.committedLeaderEpoch,
                      metadata: p.metadata
                    }))
                  }))
                })))
            )
          }
        )
      })
    })
  }

  #alterConsumerGroupOffsets (options: AlterConsumerGroupOffsetsOptions, callback: CallbackWithPromise<void>): void {
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error, undefined)
        return
      }

      this.#findGroupCoordinator([options.groupId], (error, response) => {
        if (error) {
          callback(new MultipleErrors('Altering consumer group offsets failed.', [error]))
          return
        }

        const coordinator = response.coordinators.find(c => c.key === options.groupId)
        if (!coordinator) {
          callback(
            new MultipleErrors('Altering consumer group offsets failed.', [
              new Error(`No coordinator found for group ${options.groupId}`)
            ])
          )
          return
        }

        const broker = metadata.brokers.get(coordinator.nodeId)
        if (!broker) {
          callback(
            new MultipleErrors('Altering consumer group offsets failed.', [
              new Error(`Broker ${coordinator.nodeId} not found`)
            ])
          )
          return
        }

        this[kGetConnection](broker, (error, connection) => {
          if (error) {
            callback(new MultipleErrors('Altering consumer group offsets failed.', [error]))
            return
          }

          this[kPerformWithRetry]<OffsetCommitResponse>(
            'alterConsumerGroupOffsets',
            retryCallback => {
              this[kGetApi]<OffsetCommitRequest, OffsetCommitResponse>('OffsetCommit', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as OffsetCommitResponse)
                  return
                }

                const topics: OffsetCommitRequestTopic[] = []
                for (const topic of options.topics) {
                  const partitions = topic.partitionOffsets.map(p => ({
                    partitionIndex: p.partition,
                    committedOffset: p.offset,
                    committedLeaderEpoch: -1,
                    committedMetadata: null
                  }))
                  topics.push({ name: topic.name, partitions })
                }

                api(
                  connection,
                  options.groupId,
                  -1,
                  '',
                  null,
                  topics,
                  retryCallback as unknown as Callback<OffsetCommitResponse>
                )
              })
            },
            (error: Error | null) => {
              if (error) {
                callback(new MultipleErrors('Altering consumer group offsets failed.', [error]))
                return
              }

              callback(null)
            },
            0
          )
        })
      })
    })
  }

  #deleteConsumerGroupOffsets (
    options: DeleteConsumerGroupOffsetsOptions,
    callback: CallbackWithPromise<{ name: string; partitionIndexes: number[] }[]>
  ): void {
    this[kMetadata]({ topics: [] }, (error, metadata) => {
      if (error) {
        callback(error, undefined as unknown as { name: string; partitionIndexes: number[] }[])
        return
      }

      this.#findGroupCoordinator([options.groupId], (error, response) => {
        if (error) {
          callback(
            new MultipleErrors('Deleting consumer group offsets failed.', [error]),
            undefined as unknown as { name: string; partitionIndexes: number[] }[]
          )
          return
        }

        const coordinator = response.coordinators.find(c => c.key === options.groupId)
        if (!coordinator) {
          callback(
            new MultipleErrors('Deleting consumer group offsets failed.', [
              new Error(`No coordinator found for group ${options.groupId}`)
            ]),
            undefined as unknown as { name: string; partitionIndexes: number[] }[]
          )
          return
        }

        const broker = metadata.brokers.get(coordinator.nodeId)
        if (!broker) {
          callback(
            new MultipleErrors('Deleting consumer group offsets failed.', [
              new Error(`Broker ${coordinator.nodeId} not found`)
            ]),
            undefined as unknown as { name: string; partitionIndexes: number[] }[]
          )
          return
        }

        this[kGetConnection](broker, (error, connection) => {
          if (error) {
            callback(
              new MultipleErrors('Deleting consumer group offsets failed.', [error]),
              undefined as unknown as { name: string; partitionIndexes: number[] }[]
            )
            return
          }

          this[kPerformWithRetry]<OffsetDeleteResponse>(
            'deleteOffset',
            retryCallback => {
              this[kGetApi]<OffsetDeleteRequest, OffsetDeleteResponse>('OffsetDelete', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as OffsetDeleteResponse)
                  return
                }

                api(
                  connection,
                  options.groupId,
                  options.topics.map(t => ({
                    name: t.name,
                    partitions: t.partitionIndexes.map(p => ({ partitionIndex: p }))
                  })),
                  retryCallback as unknown as Callback<OffsetDeleteResponse>
                )
              })
            },
            (error: Error | null, response: OffsetDeleteResponse) => {
              if (error) {
                callback(
                  new MultipleErrors('Deleting consumer group offsets failed.', [error]),
                  undefined as unknown as { name: string; partitionIndexes: number[] }[]
                )
                return
              }

              callback(
                null,
                response.topics.map(topic => ({
                  name: topic.name,
                  partitionIndexes: topic.partitions.map(p => p.partitionIndex)
                }))
              )
            },
            0
          )
        })
      })
    })
  }
}

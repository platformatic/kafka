import { type FetchResponse, fetchV17 } from '../../apis/consumer/fetch.ts'
import { type HeartbeatResponse, heartbeatV4 } from '../../apis/consumer/heartbeat.ts'
import { type JoinGroupRequestProtocol, type JoinGroupResponse, joinGroupV9 } from '../../apis/consumer/join-group.ts'
import { type LeaveGroupResponse, leaveGroupV5 } from '../../apis/consumer/leave-group.ts'
import {
  type ListOffsetsRequestTopic,
  type ListOffsetsResponse,
  listOffsetsV9
} from '../../apis/consumer/list-offsets.ts'
import {
  type OffsetCommitRequestTopic,
  type OffsetCommitResponse,
  offsetCommitV9
} from '../../apis/consumer/offset-commit.ts'
import {
  type OffsetFetchRequestTopic,
  type OffsetFetchResponse,
  offsetFetchV9
} from '../../apis/consumer/offset-fetch.ts'
import { type SyncGroupRequestAssignment, type SyncGroupResponse, syncGroupV5 } from '../../apis/consumer/sync-group.ts'
import { FetchIsolationLevels, FindCoordinatorKeyTypes } from '../../apis/enumerations.ts'
import { type FindCoordinatorResponse, findCoordinatorV6 } from '../../apis/metadata/find-coordinator.ts'
import { type Connection } from '../../connection/connection.ts'
import { type GenericError, type ProtocolError, UserError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { Base } from '../base/base.ts'
import { defaultBaseOptions } from '../base/options.ts'
import { type ClusterMetadata } from '../base/types.ts'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from '../callbacks.ts'
import { MessagesStream } from './messages-stream.ts'
import {
  commitOptionsValidator,
  consumeOptionsValidator,
  consumerOptionsValidator,
  defaultConsumerOptions,
  fetchOptionsValidator,
  groupOptionsValidator,
  listCommitsOptionsValidator,
  listOffsetsOptionsValidator
} from './options.ts'
import {
  type CommitOptions,
  type ConsumeOptions,
  type ConsumerOptions,
  type ExtendedGroupProtocolSubscription,
  type FetchOptions,
  type GroupAssignment,
  type GroupOptions,
  type GroupProtocolSubscription,
  type ListCommitsOptions,
  type ListOffsetsOptions,
  type Offsets
} from './types.ts'
import { TopicsMap } from './utils.ts'

interface GroupRequestAssignment {
  memberId: string
  assignments: Map<string, GroupAssignment>
}

export class Consumer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> extends Base<
  ConsumerOptions<Key, Value, HeaderKey, HeaderValue>
> {
  groupId: string
  generationId: number
  memberId: string | undefined
  topics: TopicsMap
  assignments: GroupAssignment[] | undefined

  #members: Map<string, ExtendedGroupProtocolSubscription> | undefined
  #isLeader: boolean | undefined
  #protocol: string | undefined
  #coordinatorId: number | undefined
  #heartbeatInterval: NodeJS.Timeout | undefined
  #streams: Set<MessagesStream<Key, Value, HeaderKey, HeaderValue>>

  constructor (options: ConsumerOptions<Key, Value, HeaderKey, HeaderValue>) {
    super(options)

    this.options = Object.assign({}, defaultBaseOptions, defaultConsumerOptions, options)
    this.validateOptions(options, consumerOptionsValidator, '/options')

    this.groupId = options.groupId
    this.topics = new TopicsMap()
    this.generationId = 0

    this.#streams = new Set()

    if (this.options.heartbeatInterval! > this.options.sessionTimeout!) {
      throw new UserError('/options/heartbeatInterval must be less than or equal to /options/sessionTimeout.')
    }
  }

  close (force: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void
  close (force?: boolean): Promise<void>
  close (force?: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (typeof force === 'function') {
      callback = force
      force = false
    }

    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.closed) {
      callback(null)
      return callback[kCallbackPromise]
    }

    this.closed = true

    this.#leaveGroup(force as boolean, error => {
      if (error) {
        const unknownMemberError = (error as GenericError).findBy<ProtocolError>?.('unknownMemberId', true)

        // This is to avoid throwin an error if a group join was cancelled.
        if (!unknownMemberError) {
          callback(error)
          return
        }
      }

      super.close(callback)
    })

    return callback[kCallbackPromise]
  }

  consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void
  consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  ): Promise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void | Promise<MessagesStream<Key, Value, HeaderKey, HeaderValue>> {
    if (!callback) {
      callback = createPromisifiedCallback<MessagesStream<Key, Value, HeaderKey, HeaderValue>>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this.validateOptions(options, consumeOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as MessagesStream<Key, Value, HeaderKey, HeaderValue>)
      return callback[kCallbackPromise]
    }

    options.autocommit ??= this.options.autocommit! ?? true
    options.maxBytes ??= this.options.maxBytes!
    options.deserializers = Object.assign({}, options.deserializers, this.options.deserializers)
    options.highWaterMark ??= this.options.highWaterMark!

    this.#consume(options, callback)
    return callback![kCallbackPromise]
  }

  fetch (options: FetchOptions<Key, Value, HeaderKey, HeaderValue>, callback: CallbackWithPromise<FetchResponse>): void
  fetch (options: FetchOptions<Key, Value, HeaderKey, HeaderValue>): Promise<FetchResponse>
  fetch (
    options: FetchOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<FetchResponse>
  ): void | Promise<FetchResponse> {
    if (!callback) {
      callback = createPromisifiedCallback<FetchResponse>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this.validateOptions(options, fetchOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as FetchResponse)
      return callback[kCallbackPromise]
    }

    this.#fetch(options, callback)
    return callback![kCallbackPromise]
  }

  commit (options: CommitOptions, callback: CallbackWithPromise<void>): void
  commit (options: CommitOptions): Promise<void>
  commit (options: CommitOptions, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this.validateOptions(options, commitOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    this.#commit(options, callback)
    return callback![kCallbackPromise]
  }

  listOffsets (options: ListOffsetsOptions, callback: CallbackWithPromise<Offsets>): void
  listOffsets (options: ListOffsetsOptions): Promise<Offsets>
  listOffsets (options: ListOffsetsOptions, callback?: CallbackWithPromise<Offsets>): void | Promise<Offsets> {
    if (!callback) {
      callback = createPromisifiedCallback<Offsets>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this.validateOptions(options, listOffsetsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Offsets)
      return callback[kCallbackPromise]
    }

    this.#listOffsets(options, callback)
    return callback![kCallbackPromise]
  }

  listCommittedOffsets (options: ListCommitsOptions, callback: CallbackWithPromise<Offsets>): void
  listCommittedOffsets (options: ListCommitsOptions): Promise<Offsets>
  listCommittedOffsets (options: ListCommitsOptions, callback?: CallbackWithPromise<Offsets>): void | Promise<Offsets> {
    if (!callback) {
      callback = createPromisifiedCallback<Offsets>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this.validateOptions(options, listCommitsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Offsets)
      return callback[kCallbackPromise]
    }

    this.#listCommittedOffsets(options, callback)
    return callback![kCallbackPromise]
  }

  findGroupCoordinator (callback: CallbackWithPromise<number>): void
  findGroupCoordinator (): Promise<number>
  findGroupCoordinator (callback?: CallbackWithPromise<number>): void | Promise<number> {
    if (!callback) {
      callback = createPromisifiedCallback<number>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    if (this.#coordinatorId) {
      callback(null, this.#coordinatorId)
      return callback[kCallbackPromise]
    }

    this.#findGroupCoordinator(callback)
    return callback[kCallbackPromise]
  }

  joinGroup (options: GroupOptions, callback: CallbackWithPromise<string>): void
  joinGroup (options: GroupOptions): Promise<string>
  joinGroup (options: GroupOptions, callback?: CallbackWithPromise<string>): void | Promise<string> {
    if (!callback) {
      callback = createPromisifiedCallback<string>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this.validateOptions(options, groupOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as string)
      return callback[kCallbackPromise]
    }

    options.sessionTimeout ??= this.options.sessionTimeout!
    options.rebalanceTimeout ??= this.options.rebalanceTimeout!
    options.heartbeatInterval ??= this.options.heartbeatInterval!
    options.protocols ??= this.options.protocols!

    if (options.heartbeatInterval > options.sessionTimeout) {
      throw new UserError('/options/heartbeatInterval must be less than or equal to the current sessionTimeout.')
    }

    this.#joinGroup(options as Required<GroupOptions>, callback)
    return callback[kCallbackPromise]
  }

  leaveGroup (force: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void
  leaveGroup (force?: boolean): Promise<void>
  leaveGroup (force?: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (typeof force === 'function') {
      callback = force
      force = false
    }

    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.checkNotClosed(callback)) {
      return callback[kCallbackPromise]
    }

    this.#leaveGroup(force as boolean, callback)
    return callback[kCallbackPromise]
  }

  #consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void | Promise<MessagesStream<Key, Value, HeaderKey, HeaderValue>> {
    // Subscribe all topics
    let joinNeeded = this.memberId === undefined
    for (const topic of options.topics) {
      if (this.topics.track(topic)) {
        joinNeeded = true
      }
    }

    // If we need to (re)join the group, do that first and then try again
    if (joinNeeded) {
      this.joinGroup(options, error => {
        if (error) {
          callback(error, undefined as unknown as MessagesStream<Key, Value, HeaderKey, HeaderValue>)
          return
        }

        this.#consume(options, callback)
      })

      return callback![kCallbackPromise]
    }

    // Create the stream and start consuming
    const stream = new MessagesStream<Key, Value, HeaderKey, HeaderValue>(
      this,
      options as ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
    )
    this.#streams.add(stream)

    stream.once('close', () => {
      this.#streams.delete(stream)
    })

    callback(null, stream)

    return callback![kCallbackPromise]
  }

  #fetch (
    options: FetchOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<FetchResponse>
  ): void {
    this.performWithRetry<FetchResponse>(
      'fetch',
      retryCallback => {
        this._metadata({ topics: this.topics.current }, (error, metadata) => {
          if (error) {
            retryCallback(error, undefined as unknown as FetchResponse)
            return
          }

          const broker = metadata.brokers!.get(options.node)!

          if (!broker) {
            retryCallback(
              new UserError(`Cannot find broker with node id ${options.node}`),
              undefined as unknown as FetchResponse
            )
            return
          }

          this.connections.get(broker, (error, connection) => {
            if (error) {
              retryCallback(error, undefined as unknown as FetchResponse)
              return
            }

            fetchV17(
              connection,
              options.maxWaitTime ?? this.options.maxWaitTime!,
              options.minBytes ?? this.options.minBytes!,
              options.maxBytes ?? this.options.maxBytes!,
              FetchIsolationLevels[options.isolationLevel ?? this.options.isolationLevel!],
              0,
              0,
              options.topics,
              [],
              '',
              retryCallback
            )
          })
        })
      },
      callback,
      0
    )
  }

  #commit (options: CommitOptions, callback: CallbackWithPromise<void>): void {
    this.#performGroupOperation<OffsetCommitResponse>(
      'commit',
      (connection, groupCallback) => {
        const topics = new Map<string, OffsetCommitRequestTopic>()

        for (const { topic, partition, offset, leaderEpoch } of options.offsets) {
          let topicOffsets = topics.get(topic)
          if (!topicOffsets) {
            topicOffsets = { name: topic, partitions: [] }
            topics.set(topic, topicOffsets)
          }

          topicOffsets.partitions.push({
            partitionIndex: partition,
            committedOffset: offset,
            committedLeaderEpoch: leaderEpoch,
            committedMetadata: null
          })
        }

        offsetCommitV9(
          connection,
          this.groupId,
          this.generationId,
          this.memberId!,
          null,
          Array.from(topics.values()),
          groupCallback
        )
      },
      error => {
        callback(error)
      }
    )
  }

  #listOffsets (options: ListOffsetsOptions, callback: CallbackWithPromise<Offsets>): void {
    this._metadata({ topics: options.topics }, (error, metadata) => {
      if (error) {
        callback(error, undefined as unknown as Offsets)
        return
      }

      const requests = new Map<number, Map<string, ListOffsetsRequestTopic>>()

      for (const name of options.topics) {
        const topic = metadata.topics.get(name)!

        for (let i = 0; i < topic.partitionsCount; i++) {
          const partition = topic.partitions[i]
          const { leader, leaderEpoch } = partition

          let leaderRequests = requests.get(leader)
          if (!leaderRequests) {
            leaderRequests = new Map()
            requests.set(leader, leaderRequests)
          }

          let topicRequests = leaderRequests.get(name)
          if (!topicRequests) {
            topicRequests = { name, partitions: [] }
            leaderRequests.set(name, topicRequests)
          }

          topicRequests.partitions.push({
            partitionIndex: i,
            currentLeaderEpoch: leaderEpoch,
            timestamp: options.timestamp ?? -1n
          })
        }
      }

      runConcurrentCallbacks<ListOffsetsResponse>(
        'Listing offsets failed.',
        requests,
        ([leader, requests], concurrentCallback) => {
          this.performWithRetry<ListOffsetsResponse>(
            'listOffsets',
            retryCallback => {
              this.connections.get(metadata.brokers!.get(leader)!, (error, connection) => {
                if (error) {
                  retryCallback(error, undefined as unknown as ListOffsetsResponse)
                  return
                }

                listOffsetsV9(
                  connection,
                  -1,
                  FetchIsolationLevels[options.isolationLevel ?? this.options.isolationLevel!],
                  Array.from(requests.values()),
                  retryCallback
                )
              })
            },
            concurrentCallback,
            0
          )
        },
        (error, responses) => {
          if (error) {
            callback(error, undefined as unknown as Offsets)
            return
          }

          const offsets: Offsets = new Map()

          for (const response of responses) {
            for (const { name: topic, partitions } of response.topics) {
              let topicOffsets = offsets.get(topic)

              if (!topicOffsets) {
                topicOffsets = Array(metadata.topics.get(topic)!.partitionsCount)
                offsets.set(topic, topicOffsets)
              }

              for (const { partitionIndex: index, offset } of partitions) {
                topicOffsets[index] = offset
              }
            }
          }

          callback(null, offsets)
        }
      )
    })
  }

  #listCommittedOffsets (options: ListCommitsOptions, callback: CallbackWithPromise<Offsets>): void {
    const topics: OffsetFetchRequestTopic[] = []

    for (const { topic: name, partitions } of options.topics) {
      topics.push({ name, partitionIndexes: partitions })
    }

    this.#performGroupOperation<OffsetFetchResponse>(
      'listCommits',
      (connection, groupCallback) => {
        offsetFetchV9(
          connection,
          // Note: once we start implementing KIP-848, the memberEpoch must be obtained
          [{ groupId: this.groupId, memberId: this.memberId!, memberEpoch: -1, topics }],
          false,
          groupCallback
        )
      },
      (error, response) => {
        if (error) {
          callback(error, undefined as unknown as Offsets)
          return
        }

        const committed: Offsets = new Map()
        for (const responseGroup of response.groups) {
          for (const responseTopic of responseGroup.topics) {
            const topic = responseTopic.name

            const partitions = Array(responseTopic.partitions.length)
            for (const { partitionIndex: index, committedOffset } of responseTopic.partitions) {
              partitions[index] = committedOffset
            }

            committed.set(topic, partitions)
          }
        }

        callback(null, committed)
      }
    )
  }

  #findGroupCoordinator (callback: CallbackWithPromise<number>): void {
    if (this.#coordinatorId) {
      callback(null, this.#coordinatorId)
      return
    }

    this.performDeduplicated(
      'findGroupCoordinator',
      deduplicateCallback => {
        this.performWithRetry<FindCoordinatorResponse>(
          'findGroupCoordinator',
          retryCallback => {
            this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                return
              }

              findCoordinatorV6(connection, FindCoordinatorKeyTypes.GROUP, [this.groupId], retryCallback)
            })
          },
          (error, response) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as number)
              return
            }

            const groupInfo = response.coordinators.find(coordinator => coordinator.key === this.groupId)!
            this.#coordinatorId = groupInfo.nodeId
            deduplicateCallback(null, this.#coordinatorId)
          },
          0
        )
      },
      callback
    )
  }

  #joinGroup (options: Required<GroupOptions>, callback: CallbackWithPromise<string>): void {
    clearTimeout(this.#heartbeatInterval)

    const protocols: JoinGroupRequestProtocol[] = []
    for (const protocol of options.protocols) {
      protocols.push({
        name: protocol.name,
        metadata: this.#encodeProtocolSubscriptionMetadata(protocol, this.topics.current)
      })
    }

    this.#performDeduplicateGroupOperaton<JoinGroupResponse>(
      'joinGroup',
      (connection, groupCallback) => {
        joinGroupV9(
          connection,
          this.groupId,
          options.sessionTimeout,
          options.rebalanceTimeout,
          this.memberId ?? '',
          null,
          'consumer',
          protocols,
          '',
          groupCallback
        )
      },
      (error, response) => {
        if (error) {
          if (this.#getRejoinError(error)) {
            this.#joinGroup(options, callback)
            return
          }

          callback(error, undefined as unknown as string)
          return
        }

        this.generationId = response.generationId
        this.#isLeader = response.leader === this.memberId
        this.#protocol = response.protocolName!

        this.#members = new Map()
        for (const member of response.members) {
          this.#members.set(
            member.memberId,
            this.#decodeProtocolSubscriptionMetadata(member.memberId, member.metadata!)
          )
        }

        // Send a syncGroup request
        this.#syncGroup(null, (error, response) => {
          if (error) {
            if (this.#getRejoinError(error)) {
              this.#joinGroup(options, callback)
              return
            }

            callback(error, undefined as unknown as string)
            return
          }

          this.assignments = response

          clearTimeout(this.#heartbeatInterval)
          this.#heartbeatInterval = setTimeout(() => {
            this.#heartbeat(options)
          }, options.heartbeatInterval)

          this.emitWithDebug('consumer', 'group:join', {
            groupId: this.groupId,
            memberId: this.memberId,
            generationId: this.generationId,
            isLeader: this.#isLeader,
            assignments: this.assignments
          })

          callback(null, this.memberId!)
        })
      }
    )
  }

  #leaveGroup (force: boolean, callback: CallbackWithPromise<void>): void {
    if (!this.memberId) {
      callback(null)
      return
    }

    if (this.#streams.size) {
      if (!force) {
        callback(new UserError('Cannot leave group while consuming messages.'))
        return
      }

      runConcurrentCallbacks<void>(
        'Closing streams failed.',
        this.#streams,
        (stream, concurrentCallback) => {
          stream.close(concurrentCallback)
        },
        error => {
          if (error) {
            callback(error)
            return
          }

          // All streams are closed, try the operation again
          this.#leaveGroup(force, callback)
        }
      )

      return
    }

    const memberId = this.memberId
    this.memberId = undefined
    clearTimeout(this.#heartbeatInterval)

    this.#performDeduplicateGroupOperaton<LeaveGroupResponse>(
      'leaveGroup',
      (connection, groupCallback) => {
        leaveGroupV5(connection, this.groupId, [{ memberId }], groupCallback)
      },
      error => {
        if (!error) {
          this.emitWithDebug('consumer', 'group:leave', {
            groupId: this.groupId,
            memberId: this.memberId,
            generationId: this.generationId
          })
        }

        callback(error)
      }
    )
  }

  #syncGroup (assignments: SyncGroupRequestAssignment[] | null, callback: CallbackWithPromise<GroupAssignment[]>): void {
    if (assignments == null) {
      if (this.#isLeader) {
        // Get all the metadata for  the topics the consumer are listening to, then compute the assignments
        const topicsSubscriptions = new Map<string, ExtendedGroupProtocolSubscription[]>()

        for (const subscription of this.#members!.values()) {
          for (const topic of subscription.topics!) {
            let topicSubscriptions = topicsSubscriptions.get(topic)

            if (!topicSubscriptions) {
              topicSubscriptions = []
              topicsSubscriptions.set(topic, topicSubscriptions)
            }

            topicSubscriptions.push(subscription)
          }
        }

        this._metadata({ topics: Array.from(topicsSubscriptions.keys()) }, (error, metadata) => {
          if (error) {
            callback(error, undefined as unknown as GroupAssignment[])
            return
          }

          this.#syncGroup(this.#roundRobinAssignments(metadata), callback)
        })

        return
      } else {
        // Non leader simply do not send any assignments and wait
        assignments = []
      }
    }

    this.#performDeduplicateGroupOperaton<SyncGroupResponse>(
      'syncGroup',
      (connection, groupCallback) => {
        syncGroupV5(
          connection,
          this.groupId,
          this.generationId,
          this.memberId!,
          null,
          'consumer',
          this.#protocol!,
          assignments!,
          groupCallback
        )
      },
      (error, response) => {
        if (error) {
          callback(error, undefined as unknown as GroupAssignment[])
          return
        } else if (response.assignment.length === 0) {
          callback(null, [])
          return
        }

        // Read the assignment back
        const reader = Reader.from(response.assignment)

        const assignments: GroupAssignment[] = reader.readArray(
          r => {
            return {
              topic: r.readString()!,
              partitions: r.readArray(r => r.readInt32(), true, false)!
            }
          },
          true,
          false
        )!

        callback(error, assignments)
      }
    )
  }

  #heartbeat (options: Required<GroupOptions>): void {
    const eventPayload = { groupId: this.groupId, memberId: this.memberId, generationId: this.generationId }

    this.#performDeduplicateGroupOperaton<HeartbeatResponse>(
      'heartbeat',
      (connection, groupCallback) => {
        this.emitWithDebug('consumer:heartbeat', 'start')
        heartbeatV4(connection, this.groupId, this.generationId, this.memberId!, null, groupCallback)
      },
      error => {
        if (!error) {
          this.emitWithDebug('consumer:heartbeat', 'end', eventPayload)
          this.#heartbeatInterval?.refresh()
          return
        }

        clearTimeout(this.#heartbeatInterval)

        if (this.#getRejoinError(error)) {
          // We have left the group in the meanwhile, abort
          if (!this.memberId) {
            return
          }

          this.performWithRetry(
            'rejoinGroup',
            retryCallback => {
              this.#joinGroup(options, retryCallback)
            },
            error => {
              if (error) {
                this.emitWithDebug(null, 'error', error)
              }
            },
            0
          )

          return
        }

        this.emitWithDebug('consumer:heartbeat', 'error', { ...eventPayload, error })
      }
    )
  }

  #performDeduplicateGroupOperaton<ReturnType>(
    operationId: string,
    operation: (connection: Connection, callback: CallbackWithPromise<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>
  ): void | Promise<ReturnType> {
    return this.performDeduplicated(
      operationId,
      deduplicateCallback => {
        this.#performGroupOperation<ReturnType>(operationId, operation, deduplicateCallback)
      },
      callback
    )
  }

  #performGroupOperation<ReturnType>(
    operationId: string,
    operation: (connection: Connection, callback: CallbackWithPromise<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>
  ): void | Promise<ReturnType> {
    this.#findGroupCoordinator((error, coordinatorId) => {
      if (error) {
        callback(error, undefined as unknown as ReturnType)
        return
      }

      this._metadata({ topics: this.topics.current }, (error: Error | null, metadata: ClusterMetadata) => {
        if (error) {
          callback(error, undefined as unknown as ReturnType)
          return
        }

        this.performWithRetry<ReturnType>(
          operationId,
          retryCallback => {
            this.connections.get(metadata.brokers.get(coordinatorId)!, (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as ReturnType)
                return
              }

              operation(connection, retryCallback)
            })
          },
          callback
        )
      })
    })
  }

  /*
    The following two methods follow:
    https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
  */
  #encodeProtocolSubscriptionMetadata (metadata: GroupProtocolSubscription, topics: string[]): Buffer {
    return Writer.create()
      .appendInt16(metadata.version)
      .appendArray(topics, (w, t) => w.appendString(t, false), false, false)
      .appendBytes(typeof metadata.metadata === 'string' ? Buffer.from(metadata.metadata) : metadata.metadata, false)
      .buffer
  }

  #decodeProtocolSubscriptionMetadata (memberId: string, buffer: Buffer): ExtendedGroupProtocolSubscription {
    const reader = Reader.from(buffer)

    return {
      memberId,
      version: reader.readInt16(),
      topics: reader.readArray(r => r.readString(false)!, false, false)!,
      metadata: reader.readBytes(false)!
    }
  }

  /*
    This follows:
    https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
  */
  #encodeProtocolAssignment (assignments: GroupAssignment[]): Buffer {
    return Writer.create().appendArray(
      assignments,
      (w, { topic, partitions }) => {
        w.appendString(topic).appendArray(partitions, (w, a) => w.appendInt32(a), true, false)
      },
      true,
      false
    ).buffer
  }

  #roundRobinAssignments (metadata: ClusterMetadata): SyncGroupRequestAssignment[] {
    const partitionTracker: Map<string, { next: number; max: number }> = new Map()

    // First of all, layout topics-partitions in a list
    for (const [topic, partitions] of metadata.topics) {
      partitionTracker.set(topic, { next: 0, max: partitions.partitionsCount })
    }

    // We are the only member of the group, assign all partitions to us
    const membersSize = this.#members!.size
    if (membersSize === 1) {
      const assignments: GroupAssignment[] = []

      for (const topic of this.topics.current) {
        const partitionsCount = metadata.topics.get(topic)!.partitionsCount
        const partitions: number[] = []
        for (let i = 0; i < partitionsCount; i++) {
          partitions.push(i)
        }

        assignments.push({ topic, partitions })
      }

      return [{ memberId: this.memberId!, assignment: this.#encodeProtocolAssignment(assignments) }]
    }

    // Flat the list of members and subscribed topics
    const members: GroupRequestAssignment[] = []
    const subscribedTopics = new Set<string>()
    for (const [memberId, subscription] of this.#members!) {
      members.push({ memberId, assignments: new Map() })

      for (const topic of subscription.topics!) {
        subscribedTopics.add(topic)
      }
    }

    // Assign topic-partitions in round robin
    let currentMember = 0
    for (const topic of subscribedTopics) {
      const partitionsCount = metadata.topics.get(topic)!.partitionsCount

      for (let i = 0; i < partitionsCount; i++) {
        const member = members[currentMember++ % membersSize]
        let topicAssignments = member.assignments.get(topic)

        if (!topicAssignments) {
          topicAssignments = { topic, partitions: [] }
          member.assignments.set(topic, topicAssignments)
        }

        topicAssignments?.partitions.push(i)
      }
    }

    const assignments: SyncGroupRequestAssignment[] = []
    for (const member of members) {
      assignments.push({
        memberId: member.memberId,
        assignment: this.#encodeProtocolAssignment(Array.from(member.assignments.values()))
      })
    }

    return assignments
  }

  #getRejoinError (error: Error): ProtocolError | null {
    const protocolError = (error as GenericError).findBy<ProtocolError>?.('needsRejoin', true)

    if (!protocolError) {
      return null
    }

    if (protocolError.rebalanceInProgress) {
      this.emitWithDebug('consumer', 'group:rebalance', { groupId: this.groupId })
    }

    if (!this.memberId) {
      this.memberId = protocolError.memberId!
    }

    return protocolError
  }
}

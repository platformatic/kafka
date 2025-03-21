import { type HeartbeatResponse, heartbeatV4 } from '../apis/consumer/heartbeat.ts'
import { type JoinGroupRequestProtocol, type JoinGroupResponse, joinGroupV9 } from '../apis/consumer/join-group.ts'
import { type LeaveGroupResponse, leaveGroupV5 } from '../apis/consumer/leave-group.ts'
import { type SyncGroupRequestAssignment, type SyncGroupResponse, syncGroupV5 } from '../apis/consumer/sync-group.ts'
import { FindCoordinatorKeyTypes } from '../apis/enumerations.ts'
import { type FindCoordinatorResponse, findCoordinatorV6 } from '../apis/metadata/find-coordinator.ts'
import { type Connection } from '../connection/connection.ts'
import { type Broker } from '../connection/definitions.ts'
import { type MultipleErrors, protocolErrors, type ResponseError, UserError } from '../errors.ts'
import { Reader } from '../protocol/reader.ts'
import { Writer } from '../protocol/writer.ts'
import { ajv } from '../utils.ts'
import { type CallbackWithPromise, createPromisifiedCallback, kCallbackPromise, noopCallback } from './callbacks.ts'
import { Client, type ClientOptions, type ClusterMetadata } from './client.ts'

/*
  This follows:
  https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
*/
export type GroupProtocolSubscription = {
  version: number
  topics?: string[]
  metadata?: Buffer | string
}

/*
  This follows:
  https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
*/
export type GroupAssignments = Record<string, number[]>

export interface GroupOptions {
  sessionTimeout: number
  rebalanceTimeout: number
  heartbeatInterval: number
  protocols: Record<string, GroupProtocolSubscription>
}

export interface FetchOptions {
  minBytes: number
  maxBytes: number
  maxWaitTime: number
}

export type ConsumerOptions = ClientOptions & Partial<GroupOptions> & Partial<FetchOptions>

export const groupOptionsSchema = {
  type: 'object',
  properties: {
    sessionTimeout: { type: 'number', minimum: 0 },
    rebalanceTimeout: { type: 'number', minimum: 0 },
    heartbeatInterval: { type: 'number', minimum: 0 },
    protocols: {
      type: 'object',
      patternProperties: {
        '.+': {
          type: 'object',
          properties: {
            version: { type: 'number', minimum: 0 },
            topics: {
              type: 'array',
              items: { type: 'string' }
            },
            metadata: { anyOf: [{ type: 'string' }, { buffer: true }] }
          }
        }
      }
    }
  },
  additionalProperties: true
}

export const groupOptionsValidator = ajv.compile(groupOptionsSchema)

export const fetchOptionsSchema = {
  type: 'object',
  properties: {
    minBytes: { type: 'number', minimum: 0 },
    maxBytes: { type: 'number', minimum: 0 },
    maxWaitTime: { type: 'number', minimum: 0 }
  },
  additionalProperties: true
}

export const fetchOptionsValidator = ajv.compile(fetchOptionsSchema)

export const DEFAULT_MAX_WAIT_TIME = 5_000
export const DEFAULT_SESSION_TIMEOUT = 60_000
export const DEFAULT_REBALANCE_TIMEOUT = 2_000
// export const DEFAULT_PROTOCOL = '@platformatic/kafka/round-robin'
export const DEFAULT_PROTOCOL = 'roundrobin'
export const DEFAULT_PROTOCOL_VERSION = 1

export class Consumer extends Client<ConsumerOptions> {
  #metadata: ClusterMetadata | undefined
  #groupId: string
  #generationId: number
  #memberId: string | undefined
  #members: string[] | undefined
  #isLeader: boolean | undefined
  #protocol: string | undefined
  #coordinatorId: number | undefined
  #assignments: GroupAssignments | undefined
  #heartbeatInterval: NodeJS.Timeout | undefined

  constructor (
    groupId: string,
    clientId: string,
    bootstrapBrokers: Broker | string | Broker[] | string[],
    options: Partial<ConsumerOptions> = {}
  ) {
    if (typeof groupId !== 'string' || !groupId) {
      throw new UserError('/groupId must be a non-empty string.')
    }

    super(clientId, bootstrapBrokers, options as Partial<ClientOptions>)

    this.#groupId = groupId
    this.#generationId = 0

    this.options.minBytes ??= 0
    this.options.maxBytes ??= 0
    this.options.maxWaitTime ??= DEFAULT_MAX_WAIT_TIME
    this.options.sessionTimeout ??= DEFAULT_SESSION_TIMEOUT
    // See: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#heartbeat-interval-ms
    this.options.heartbeatInterval ??= this.options.sessionTimeout / 3
    this.options.rebalanceTimeout ??= DEFAULT_REBALANCE_TIMEOUT
    this.options.protocols ??= { [DEFAULT_PROTOCOL]: { version: DEFAULT_PROTOCOL_VERSION } }

    this.validateOptions(options, groupOptionsValidator, '/options')
    this.validateOptions(options, fetchOptionsValidator, '/options')

    if (this.options.heartbeatInterval > this.options.sessionTimeout) {
      throw new UserError('/options/heartbeatInterval must be less than or equal to /options/sessionTimeout.')
    }
  }

  close (): Promise<void>
  close (callback: CallbackWithPromise<void>): void
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this.leaveGroup(error => {
      if (error) {
        callback(error)
        return
      }

      super.close(callback)
    })

    return callback[kCallbackPromise]
  }

  findGroupCoordinator (callback: CallbackWithPromise<number>): void
  findGroupCoordinator (): Promise<number>
  findGroupCoordinator (callback?: CallbackWithPromise<number>): void | Promise<number> {
    if (!callback) {
      callback = createPromisifiedCallback<number>()
    }

    if (this.#coordinatorId) {
      callback(null, this.#coordinatorId)
      return callback[kCallbackPromise]
    }

    return this.performDeduplicated(
      'findGroupCoordinator',
      deduplicateCallback => {
        this.performWithRetry<FindCoordinatorResponse>(
          'Finding group coordinator failed.',
          retryCallback => {
            this.connections.getFromMultiple(this.bootstrapBrokers, (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                return
              }

              findCoordinatorV6(connection, FindCoordinatorKeyTypes.GROUP, [this.#groupId], retryCallback)
            })
          },
          (error, response) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as number)
              return
            }

            const groupInfo = response.coordinators.find(coordinator => coordinator.key === this.#groupId)!
            this.#coordinatorId = groupInfo.nodeId
            deduplicateCallback(null, this.#coordinatorId)
          },
          0,
          this.options.retries
        )
      },
      callback
    )
  }

  joinGroup (callback: CallbackWithPromise<string>): void
  joinGroup (options?: Partial<GroupOptions>): Promise<string>
  joinGroup (options: Partial<GroupOptions> | CallbackWithPromise<string>, callback: CallbackWithPromise<string>): void
  joinGroup (
    options?: Partial<GroupOptions> | CallbackWithPromise<string>,
    callback?: CallbackWithPromise<string>
  ): void | Promise<string> {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }

    const joinOptions: GroupOptions = Object.assign({}, this.options, options as GroupOptions)

    if (!callback) {
      callback = createPromisifiedCallback<string>()
    }

    const validationError = this.validateOptions(joinOptions, groupOptionsValidator, '/options', false)

    if (validationError) {
      callback(validationError, undefined as unknown as string)
      return callback[kCallbackPromise]
    }

    if (joinOptions.heartbeatInterval > joinOptions.sessionTimeout) {
      throw new UserError('/options/heartbeatInterval must be less than or equal to the current sessionTimeout.')
    }

    this.#joinGroup(joinOptions, (error, response) => {
      if (error) {
        const responseError = (error as MultipleErrors).errors[0] as ResponseError

        // The group is rebalancing, we need to rejoin - This is also needed for the initial join
        if (
          (!this.#memberId && responseError.errors[0].apiId === protocolErrors.MEMBER_ID_REQUIRED.id) ||
          responseError.errors[0].apiId === protocolErrors.REBALANCE_IN_PROGRESS.id
        ) {
          this.#memberId = (responseError.response as JoinGroupResponse).memberId!

          if (this.#memberId) {
            this.joinGroup(joinOptions, callback)
            return
          }
        }

        callback(error, undefined as unknown as string)
        return
      }

      callback(null, response)
    })

    return callback[kCallbackPromise]
  }

  leaveGroup (callback: CallbackWithPromise<void>): void
  leaveGroup (): Promise<void>
  leaveGroup (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (!this.#memberId) {
      callback(null)
      return callback[kCallbackPromise]
    }

    const memberId = this.#memberId
    this.#memberId = undefined
    clearInterval(this.#heartbeatInterval)

    this.#performGroupOperation<LeaveGroupResponse>(
      'leaveGroup',
      (connection, groupCallback) => {
        leaveGroupV5(connection, this.#groupId, [{ memberId }], groupCallback)
      },
      error => {
        callback(error)
      }
    )

    return callback[kCallbackPromise]
  }

  #joinGroup (options: GroupOptions, callback: CallbackWithPromise<string>): void {
    const protocols: JoinGroupRequestProtocol[] = Object.entries(options.protocols).map(([name, metadata]) => ({
      name,
      metadata: this.#encodeProtocolSubscriptionMetadata(metadata)
    }))

    this.#performGroupOperation<JoinGroupResponse>(
      'joinGroup',
      (connection, groupCallback) => {
        joinGroupV9(
          connection,
          this.#groupId,
          options.sessionTimeout,
          options.rebalanceTimeout,
          this.#memberId ?? '',
          null,
          'consumer',
          protocols,
          '',
          groupCallback
        )
      },
      (error, response) => {
        if (error) {
          callback(error, undefined as unknown as string)
          return
        }

        this.#generationId = response.generationId
        this.#members = response.members.map(m => m.memberId)
        this.#isLeader = response.leader === this.#memberId
        this.#protocol = response.protocolName!

        // Send a syncGroup request
        this.#syncGroup((error, response) => {
          if (error) {
            callback(error, undefined as unknown as string)
            return
          }

          this.#assignments = response

          clearInterval(this.#heartbeatInterval)
          this.#heartbeatInterval = setInterval(() => {
            this.#heartbeat(options)
          }, options.heartbeatInterval)

          callback(null, this.#memberId!)
        })
      }
    )
  }

  #syncGroup (callback: CallbackWithPromise<GroupAssignments>): void {
    const assignments: SyncGroupRequestAssignment[] = []

    if (this.#isLeader) {
      try {
        let i = 0

        const topicAssignments: Record<string, Record<string, number[]>> = {}

        // For now, just round-robin on all topic-partitions
        for (const [topic, partitions] of Object.entries(this.#metadata!.topics)) {
          for (let j = 0; i < partitions.partitionsCount; j++) {
            const memberId = this.#members![i++ % this.#members!.length]
            topicAssignments[memberId] ??= {}
            topicAssignments[memberId][topic] ??= []
            topicAssignments[memberId][topic].push(j)
          }
        }

        for (const [memberId, memberAssignments] of Object.entries(topicAssignments)) {
          assignments.push({ memberId, assignment: this.#encodeProtocolAssignment(memberAssignments) })
        }
      } catch (e) {
        callback(new UserError('Failed to assign partitions to members.'), { cause: e })
        return
      }
    }

    this.#performGroupOperation<SyncGroupResponse>(
      'syncGroup',
      (connection, groupCallback) => {
        syncGroupV5(
          connection,
          this.#groupId,
          this.#generationId,
          this.#memberId!,
          null,
          'consumer',
          this.#protocol!,
          assignments,
          groupCallback
        )
      },
      (error, response) => {
        if (error) {
          callback(error, undefined as unknown as GroupAssignments)
          return
        } else if (response.assignment.length === 0) {
          callback(null, {})
          return
        }

        // Read the assignment back
        const reader = Reader.from(response.assignment)

        const assignments: GroupAssignments = Object.fromEntries(
          reader.readArray(
            r => {
              return [r.readString()!, r.readArray(r => r.readInt32(), true, false)!]
            },
            true,
            false
          )!
        )

        callback(error, assignments)
      }
    )
  }

  #heartbeat (options: GroupOptions): void {
    this.#performGroupOperation<HeartbeatResponse>(
      'heartbeat',
      (connection, groupCallback) => {
        heartbeatV4(connection, this.#groupId, this.#generationId, this.#memberId!, null, groupCallback)
      },
      error => {
        if (!error) {
          return
        }

        clearInterval(this.#heartbeatInterval)

        const responseError = (error as MultipleErrors).errors[0] as ResponseError
        if (responseError.errors[0].apiId === protocolErrors.REBALANCE_IN_PROGRESS.id) {
          this.joinGroup(options, noopCallback)
          return
        }

        this.emit('error', error)
      }
    )
  }

  #performGroupOperation<T>(
    operationId: string,
    operation: (connection: Connection, callback: CallbackWithPromise<T>) => void,
    callback: CallbackWithPromise<T>
  ): void | Promise<T> {
    return this.performDeduplicated(
      operationId,
      deduplicateCallback => {
        this.findGroupCoordinator((error, coordinatorId) => {
          if (error) {
            deduplicateCallback(null, undefined as unknown as T)
            return
          }

          this.metadata([], (error: Error | null, metadata: ClusterMetadata) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as T)
              return
            }

            this.#metadata = metadata
            this.performWithRetry<T>(
              operationId,
              retryCallback => {
                this.connections.get(metadata.brokers[coordinatorId], (error, connection) => {
                  if (error) {
                    retryCallback(error, undefined as unknown as T)
                    return
                  }

                  operation(connection, retryCallback)
                })
              },
              deduplicateCallback,
              0,
              this.options.retries,
              []
            )
          })
        })
      },
      callback
    )
  }

  /*
    This follows:
    https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
  */
  #encodeProtocolSubscriptionMetadata (metadata: GroupProtocolSubscription): Buffer {
    return Writer.create()
      .appendInt16(metadata.version)
      .appendArray(metadata.topics ?? [], (w, t) => w.appendString(t, false), false, false)
      .appendBytes(typeof metadata.metadata === 'string' ? Buffer.from(metadata.metadata) : metadata.metadata, false)
      .buffer
  }

  /*
    This follows:
    https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
  */
  #encodeProtocolAssignment (assignments: Record<string, number[]>): Buffer {
    return Writer.create().appendArray(
      Object.entries(assignments),
      (w, [topic, topicAssignments]) => {
        w.appendString(topic).appendArray(topicAssignments, (w, a) => w.appendInt32(a), true, false)
      },
      true,
      false
    ).buffer
  }
}

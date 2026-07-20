import {
  Consumer,
  kCreateGroupAssignments,
  kCreateMessagesStream,
  kGetCommittedMetadata,
  kHandleTerminalGroupFailure,
  kRequestHeartbeat
} from '../../clients/consumer/consumer.ts'
import { createPromisifiedCallback, kCallbackPromise } from '../../apis/callbacks.ts'
import type { MessagesStream } from '../../clients/consumer/messages-stream.ts'
import type {
  CommitOptionsPartition,
  ConsumeOptions,
  ConsumerOptions,
  ExtendedGroupProtocolSubscription,
  GroupPartitionsAssigner
} from '../../clients/consumer/types.ts'
import type { ClusterMetadata } from '../../clients/base/types.ts'
import type { Callback } from '../../apis/definitions.ts'
import type { SyncGroupRequestAssignment } from '../../apis/consumer/sync-group-v5.ts'
import type { KafkaJSGroupAssignments } from './partitioners.ts'
import { KafkaJSMessagesStream } from './messages-stream.ts'
import {
  kKafkaJSCommitMetadata,
  kKafkaJSCreateAssignments,
  kKafkaJSFallbackModes,
  type KafkaJSFallbackModes
} from './symbols.ts'

export type KafkaJSCommitOptionsPartition = CommitOptionsPartition & {
  [kKafkaJSCommitMetadata]?: string | null
}

export type KafkaJSConsumeOptions = ConsumeOptions<Buffer, Buffer, Buffer, Buffer> & {
  [kKafkaJSFallbackModes]?: KafkaJSFallbackModes
}

export class KafkaJSConsumerBridge extends Consumer<Buffer, Buffer, Buffer, Buffer> {
  readonly #streams = new Set<MessagesStream<Buffer, Buffer, Buffer, Buffer>>()
  readonly #createKafkaJSAssignments?: KafkaJSGroupAssignments
  #terminalGroupFailure?: Error

  constructor (options: ConsumerOptions<Buffer, Buffer, Buffer, Buffer> & {
    [kKafkaJSCreateAssignments]?: KafkaJSGroupAssignments
  }) {
    super(options)
    this.#createKafkaJSAssignments = options[kKafkaJSCreateAssignments]
  }

  [kCreateMessagesStream] (
    options: KafkaJSConsumeOptions
  ): MessagesStream<Buffer, Buffer, Buffer, Buffer> {
    const stream = new KafkaJSMessagesStream(this, options, options[kKafkaJSFallbackModes] ?? {})
    this.#streams.add(stream)
    stream.once('close', () => this.#streams.delete(stream))
    if (this.#terminalGroupFailure) {
      stream.destroy(this.#terminalGroupFailure)
      this.#terminalGroupFailure = undefined
    }
    return stream
  }

  [kGetCommittedMetadata] (offset: KafkaJSCommitOptionsPartition): string | null {
    return offset[kKafkaJSCommitMetadata] ?? null
  }

  [kCreateGroupAssignments] (
    partitionsAssigner: GroupPartitionsAssigner | null,
    members: Map<string, ExtendedGroupProtocolSubscription>,
    topics: Set<string>,
    metadata: ClusterMetadata,
    protocol: string | null,
    callback: Callback<SyncGroupRequestAssignment[]>
  ): void {
    const createAssignments = this.#createKafkaJSAssignments
    if (!createAssignments) {
      super[kCreateGroupAssignments](partitionsAssigner, members, topics, metadata, protocol, callback)
      return
    }

    createAssignments(members, topics, metadata, protocol).then(
      assignments => {
        const encoded: SyncGroupRequestAssignment[] = []
        for (const { memberId, assignment } of assignments) {
          if (!assignment) {
            callback(new Error('KafkaJS partition assigner did not return a member assignment.'))
            return
          }
          encoded.push({ memberId, assignment })
        }
        callback(null, encoded)
      },
      error => callback(error as Error)
    )
  }

  heartbeat (): Promise<void> {
    const callback = createPromisifiedCallback<void>()
    this[kRequestHeartbeat](callback)
    return callback[kCallbackPromise]!
  }

  [kHandleTerminalGroupFailure] (error: Error): boolean {
    this.#terminalGroupFailure = error
    for (const stream of this.#streams) {
      stream.destroy(error)
    }
    return true
  }
}

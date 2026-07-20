import {
  Consumer,
  kCreateMessagesStream,
  kGetCommitGroupMetadata,
  kShouldJoinGroupForConsume
} from '../../clients/consumer/consumer.ts'
import {
  kAssignmentsForOffsetRestore,
  kAssignmentsForTopic,
  kHandleFetchPartitionProgress,
  MessagesStream
} from '../../clients/consumer/messages-stream.ts'
import type { FetchResponsePartition } from '../../apis/consumer/fetch-v17.ts'
import type { ConsumeOptions, ConsumerOptions, GroupAssignment } from '../../clients/consumer/types.ts'

export const kNodeRdkafkaAssignments = Symbol('plt.kafka.nodeRdkafka.assignments')
export const kNodeRdkafkaAssignmentFilter = Symbol('plt.kafka.nodeRdkafka.assignmentFilter')
export const kNodeRdkafkaPausedPartitions = Symbol('plt.kafka.nodeRdkafka.pausedPartitions')
export const kNodeRdkafkaPartitionEof = Symbol('plt.kafka.nodeRdkafka.partitionEof')

export type NodeRdkafkaConsumeOptions = ConsumeOptions<Buffer, Buffer | null, Buffer, Buffer> & {
  [kNodeRdkafkaAssignments]?: GroupAssignment[]
  [kNodeRdkafkaAssignmentFilter]?: Set<string>
  [kNodeRdkafkaPausedPartitions]?: Set<string>
  [kNodeRdkafkaPartitionEof]?: boolean
}

class NodeRdkafkaMessagesStream extends MessagesStream<Buffer, Buffer | null, Buffer, Buffer> {
  readonly #assignments?: GroupAssignment[]
  readonly #assignmentFilter?: Set<string>
  readonly #pausedPartitions?: Set<string>
  readonly #partitionEof: boolean
  readonly #eofOffsets = new Map<string, bigint>()

  constructor (consumer: Consumer<Buffer, Buffer | null, Buffer, Buffer>, options: NodeRdkafkaConsumeOptions) {
    super(consumer, options)
    this.#assignments = options[kNodeRdkafkaAssignments]
    this.#assignmentFilter = options[kNodeRdkafkaAssignmentFilter]
    this.#pausedPartitions = options[kNodeRdkafkaPausedPartitions]
    this.#partitionEof = options[kNodeRdkafkaPartitionEof] === true
  }

  [kAssignmentsForTopic] (topic: string): GroupAssignment | undefined {
    const assignment = this.#assignments
      ? this[kAssignmentsForOffsetRestore](topic)
      : super[kAssignmentsForTopic](topic)
    if (!assignment) {
      return assignment
    }
    const partitions = assignment.partitions.filter(partition => {
      const key = `${topic}:${partition}`
      return (!this.#assignmentFilter || this.#assignmentFilter.has(key)) && !this.#pausedPartitions?.has(key)
    })
    return partitions.length === 0 ? undefined : { ...assignment, partitions }
  }

  [kAssignmentsForOffsetRestore] (topic: string): GroupAssignment | undefined {
    const assignments = this.#assignments?.filter(assignment => assignment.topic === topic)
    const assignment = !assignments || assignments.length === 0
      ? super[kAssignmentsForOffsetRestore](topic)
      : { topic, partitions: assignments.flatMap(assignment => assignment.partitions) }
    const filter = this.#assignmentFilter
    if (!assignment || !filter) {
      return assignment
    }
    const partitions = assignment.partitions.filter(partition => filter.has(`${topic}:${partition}`))
    return partitions.length === 0 ? undefined : { ...assignment, partitions }
  }

  [kHandleFetchPartitionProgress] (
    topic: string,
    partition: number,
    response: FetchResponsePartition,
    lastOffset: bigint,
    seekVersion: number,
    membershipVersion: number
  ): boolean {
    this.emit('watermark', {
      topic,
      partition,
      lowOffset: Number(response.logStartOffset),
      highOffset: Number(response.highWatermark)
    })
    if (this.#partitionEof && response.highWatermark === lastOffset + 1n) {
      const key = `${topic}:${partition}`
      if (this.#eofOffsets.get(key) !== response.highWatermark) {
        this.#eofOffsets.set(key, response.highWatermark)
        this.emit('partition.eof', { topic, partition, offset: Number(response.highWatermark) })
      }
    }
    return super[kHandleFetchPartitionProgress](topic, partition, response, lastOffset, seekVersion, membershipVersion)
  }
}

export class NodeRdkafkaConsumerBridge extends Consumer<Buffer, Buffer | null, Buffer, Buffer> {
  [kCreateMessagesStream] (options: NodeRdkafkaConsumeOptions): MessagesStream<Buffer, Buffer | null, Buffer, Buffer> {
    return new NodeRdkafkaMessagesStream(this, options)
  }

  [kShouldJoinGroupForConsume] (options: NodeRdkafkaConsumeOptions): boolean {
    return options[kNodeRdkafkaAssignments] === undefined
  }

  [kGetCommitGroupMetadata] (): { generationId: number, memberId: string } {
    if (!this.memberId) {
      return { generationId: -1, memberId: '' }
    }

    return super[kGetCommitGroupMetadata]()
  }
}

export type NodeRdkafkaConsumerOptions = ConsumerOptions<Buffer, Buffer | null, Buffer, Buffer>

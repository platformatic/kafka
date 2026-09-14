import { createRecordsBatchAsync, type CreateRecordsBatchOptions, type MessageRecord } from '../../protocol/records.ts'
import { type Writer } from '../../protocol/writer.ts'
import { groupByProperty } from '../../utils.ts'

const gzipConcurrency = 4

export interface EncodedProducePartition {
  partition: number
  records: Writer
}

export interface EncodedProduceTopic {
  topic: string
  partitions: EncodedProducePartition[]
}

interface ProducePartitionTask {
  partition: number
  messages: MessageRecord[]
  records?: Writer
}

interface ProduceTopicTask {
  topic: string
  partitions: ProducePartitionTask[]
}

export async function encodeProduceTopicData (
  topicData: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions>
): Promise<EncodedProduceTopic[]> {
  const now = BigInt(Date.now())
  for (const message of topicData) {
    message.partition ??= 0
    message.timestamp ??= now
  }

  const topics: ProduceTopicTask[] = groupByProperty<string, MessageRecord>(topicData, 'topic').map(
    ([topic, messages]) => ({
      topic,
      partitions: groupByProperty<number, MessageRecord>(messages, 'partition').map(([partition, messages]) => ({
        partition: Number(partition),
        messages
      }))
    })
  )
  const tasks = topics.flatMap(topic => topic.partitions)
  let nextTask = 0

  async function worker (): Promise<void> {
    while (nextTask < tasks.length) {
      const task = tasks[nextTask++]
      task.records = await createRecordsBatchAsync(task.messages, options)
    }
  }

  const workers = Array.from({ length: Math.min(gzipConcurrency, tasks.length) }, worker)
  await Promise.all(workers)

  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, records }) => ({ partition, records: records! }))
  }))
}

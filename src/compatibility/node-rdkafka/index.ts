import { AdminClient } from './admin.ts'
import { Consumer, KafkaConsumer } from './consumer.ts'
import { CODES } from './errors.ts'
import { HighLevelProducer, Producer } from './producer.ts'
import { createReadStream, createWriteStream } from './streams.ts'

export { AdminClient, CODES, Consumer, createReadStream, createWriteStream, HighLevelProducer, KafkaConsumer, Producer }
export type { ConsumerStream, ProducerStream, ReadStreamOptions, WriteStreamOptions } from './streams.ts'
export type * from './types.ts'

export function Topic (name: string): string {
  return name
}

Topic.PARTITION_UA = -1
Topic.OFFSET_BEGINNING = -2
Topic.OFFSET_END = -1
Topic.OFFSET_STORED = -1000
Topic.OFFSET_INVALID = -1001

export const features = ['gzip', 'snappy', 'ssl', 'sasl', 'regex', 'lz4', 'sasl_plain', 'sasl_scram', 'zstd', 'sasl_oauthbearer']
export const librdkafkaVersion = '2.12.0'

export default {
  Consumer,
  Producer,
  HighLevelProducer,
  AdminClient,
  KafkaConsumer,
  createReadStream,
  createWriteStream,
  CODES,
  Topic,
  features,
  librdkafkaVersion
}

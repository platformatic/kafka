import {
  kCreateProduceResultOffset,
  kGetProduceTimeout,
  Producer
} from '../../clients/producer/producer.ts'
import { kResetConnections } from '../../clients/base/base.ts'
import type { ProduceResponsePartition } from '../../apis/producer/produce-v11.ts'
import type { SendOptions } from '../../clients/producer/types.ts'
import { kKafkaJSProduceTimeout } from './symbols.ts'

export type KafkaJSSendOptions = SendOptions<Buffer, Buffer, Buffer, Buffer> & {
  [kKafkaJSProduceTimeout]?: number
}

export class KafkaJSProducerBridge extends Producer<Buffer, Buffer, Buffer, Buffer> {
  resetConnections (): Promise<void> {
    return this[kResetConnections]()
  }

  [kGetProduceTimeout] (options: KafkaJSSendOptions): number {
    return options[kKafkaJSProduceTimeout] ?? super[kGetProduceTimeout](options)
  }

  [kCreateProduceResultOffset] (topic: string, partition: ProduceResponsePartition) {
    return {
      ...super[kCreateProduceResultOffset](topic, partition),
      errorCode: partition.errorCode,
      logAppendTime: partition.logAppendTimeMs,
      logStartOffset: partition.logStartOffset
    }
  }
}

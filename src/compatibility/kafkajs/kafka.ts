import type { AdminConfig, ConsumerConfig, KafkaConfig, Logger, ProducerConfig } from './types.ts'
import type { AdminOptions } from '../../clients/admin/types.ts'
import type { ConsumerOptions } from '../../clients/consumer/types.ts'
import type { ProducerOptions } from '../../clients/producer/types.ts'
import { Admin } from './admin.ts'
import { mergeRetryOptions, normalizeConfig, retryOptions } from './common.ts'
import { Consumer } from './consumer.ts'
import { createLogger } from './logger.ts'
import { Producer } from './producer.ts'

export class Kafka {
  private readonly options: ReturnType<typeof normalizeConfig>
  private readonly log: Logger
  private readonly retry: KafkaConfig['retry']

  constructor (config: KafkaConfig) {
    this.log = createLogger(config.logLevel, config.logCreator)
    this.options = normalizeConfig(config, this.log)
    this.retry = config.retry
  }

  producer (config: ProducerConfig = {}): Producer {
    return new Producer(
      { ...this.options } as ProducerOptions<Buffer, Buffer, Buffer, Buffer>,
      { ...config, retry: mergeRetryOptions(this.retry, config.retry) },
      this.log
    )
  }

  consumer (config: ConsumerConfig): Consumer {
    return new Consumer(
      { ...this.options, groupId: config.groupId } as ConsumerOptions<Buffer, Buffer, Buffer, Buffer>,
      { ...config, retry: mergeRetryOptions(this.retry, { retries: config.retry?.retries ?? 5 }, config.retry) },
      this.log
    )
  }

  admin (config: AdminConfig = {}): Admin {
    return new Admin(
      {
        ...this.options,
        ...retryOptions(mergeRetryOptions(this.retry, config.retry))
      } as AdminOptions,
      this.log
    )
  }

  logger (): Logger {
    return this.log
  }
}

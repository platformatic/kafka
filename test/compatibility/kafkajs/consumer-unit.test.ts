import { deepStrictEqual, equal, ok, rejects, throws } from 'node:assert'
import { describe, test } from 'node:test'
import KafkaJS from 'kafkajs'
import { Kafka } from '../../../src/compatibility/kafkajs/index.ts'
import { KafkaJSConsumerBridge } from '../../../src/compatibility/kafkajs/consumer-native.ts'
import {
  kCreateGroupAssignments,
  kCreateMessagesStream,
  kHandleTerminalGroupFailure
} from '../../../src/clients/consumer/consumer.ts'
import { createPromisifiedCallback, kCallbackPromise } from '../../../src/apis/callbacks.ts'
import { kSeekPartition } from '../../../src/clients/consumer/messages-stream.ts'
import { createPartitionAssignerOptions } from '../../../src/compatibility/kafkajs/partitioners.ts'
import { AssignerProtocol } from '../../../src/compatibility/kafkajs/protocol.ts'
import {
  kKafkaJSFetchProgress
} from '../../../src/compatibility/kafkajs/symbols.ts'

const brokers = ['localhost:9092']

function consumers (config: { groupId: string; heartbeatInterval?: number; sessionTimeout?: number }) {
  return [new Kafka({ brokers }).consumer(config), new KafkaJS.Kafka({ brokers }).consumer(config)]
}

function nativeOptions (consumer: object): Record<string, unknown> {
  const native = (consumer as { native: object }).native
  const optionsSymbol = Object.getOwnPropertySymbols(native).find(
    symbol => symbol.description === 'plt.kafka.base.options'
  )
  ok(optionsSymbol)
  return (native as Record<symbol, Record<string, unknown>>)[optionsSymbol]
}

describe('KafkaJS consumer differential validation', () => {
  test('uses the compatibility consumer subclass', () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    ok((consumer as unknown as { native: unknown }).native instanceof KafkaJSConsumerBridge)
  })

  test('uses KafkaJS metadata and group protocol defaults', () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const options = nativeOptions(consumer)
    equal(options.metadataMaxAge, 300_000)
    deepStrictEqual(options.protocols, [{ name: 'RoundRobinAssigner', version: 0 }])
  })

  test('validates group and heartbeat configuration like KafkaJS 2.2.4', () => {
    for (const KafkaImplementation of [Kafka, KafkaJS.Kafka]) {
      throws(
        () => new KafkaImplementation({ brokers }).consumer({ groupId: '' }),
        /Consumer groupId must be a non-empty string/
      )
      throws(
        () =>
          new KafkaImplementation({ brokers }).consumer({
            groupId: 'group',
            heartbeatInterval: 1_000,
            sessionTimeout: 1_000
          }),
        /heartbeatInterval \(1000\) must be lower than sessionTimeout \(1000\)/
      )
    }
  })

  test('configures custom partition assigners', () => {
    const consumer = new Kafka({ brokers }).consumer({
      groupId: 'group',
      partitionAssigners: [() => ({
        name: 'CustomAssigner',
        version: 1,
        protocol: () => ({ name: 'CustomAssigner', metadata: Buffer.alloc(0) }),
        assign: async () => []
      })]
    })
    const options = nativeOptions(consumer)
    deepStrictEqual(options.protocols, [{ name: 'CustomAssigner', version: 1 }])
  })

  test('adapts custom partition assigner metadata and assignments', async () => {
    const assigned = Buffer.from('assigned')
    let memberMetadata: Buffer | undefined
    const options = createPartitionAssignerOptions(
      [({ cluster }) => ({
        name: 'CustomAssigner',
        version: 1,
        protocol: ({ topics }) => ({
          name: 'CustomAssigner',
          metadata: AssignerProtocol.MemberMetadata.encode({ version: 0, topics, userData: Buffer.alloc(0) })
        }),
        assign: async ({ members }) => {
          deepStrictEqual(cluster.findTopicPartitionMetadata('topic').map(partition => partition.partitionId), [0, 1])
          memberMetadata = members[0].memberMetadata
          return members.map(member => ({ memberId: member.memberId, memberAssignment: assigned }))
        }
      })],
      'group',
      {} as never
    )
    const metadata = {
      brokers: new Map(),
      topics: new Map([
        ['topic', {
          partitions: [
            { leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
            { leader: 1, replicas: [1], isr: [1], offlineReplicas: [] }
          ]
        }]
      ])
    } as never
    const protocolMetadata = options.protocolMetadata!({ name: 'CustomAssigner', version: 1 }, ['topic'], metadata)
    deepStrictEqual(
      AssignerProtocol.MemberMetadata.decode(protocolMetadata as Buffer),
      { version: 0, topics: ['topic'], userData: Buffer.alloc(0) }
    )

    const assignments = await options.createAssignments(
      new Map([
        ['member-a', { memberId: 'member-a', version: 0, topics: ['topic'], metadata: Buffer.alloc(0) }]
      ]),
      new Set(['topic']),
      metadata,
      'CustomAssigner'
    )
    equal(assignments[0].assignment, assigned)
    deepStrictEqual(AssignerProtocol.MemberMetadata.decode(memberMetadata!).topics, ['topic'])
  })

  test('adapts asynchronous assignments through the native callback hook', async () => {
    const assignment = Buffer.from('assignment')
    const consumer = new Kafka({ brokers }).consumer({
      groupId: 'group',
      partitionAssigners: [() => ({
        name: 'CustomAssigner',
        version: 1,
        protocol: () => ({ name: 'CustomAssigner', metadata: Buffer.alloc(0) }),
        assign: async ({ members }) => members.map(member => ({ memberId: member.memberId, memberAssignment: assignment }))
      })]
    })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    const callback = createPromisifiedCallback<Array<{ memberId: string; assignment: Buffer }>>()
    const metadata = { brokers: new Map(), topics: new Map() } as never

    native[kCreateGroupAssignments](
      null,
      new Map([
        ['member-a', { memberId: 'member-a', version: 0, topics: [], metadata: Buffer.alloc(0) }]
      ]),
      new Set(),
      metadata,
      'CustomAssigner',
      callback
    )

    deepStrictEqual(await callback[kCallbackPromise], [{ memberId: 'member-a', assignment }])
  })

  test('validates subscriptions like KafkaJS 2.2.4', async () => {
    for (const consumer of consumers({ groupId: 'group' })) {
      await rejects(consumer.subscribe({ topics: [42] } as never), /topic name has to be a String or a RegExp/)
    }
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    await rejects(consumer.subscribe({ topics: [] }), /topics array, it cannot be empty/)
  })

  test('rejects group-only operations before run', async () => {
    for (const consumer of consumers({ groupId: 'group' })) {
      await rejects(consumer.commitOffsets([]), /consumer#run must be called first/)
      throws(() => consumer.seek({ topic: 'topic', partition: 0, offset: '0' }), /consumer#run must be called first/)
      throws(() => consumer.pause([{ topic: 'topic' }]), /consumer#run must be called first/)
      throws(() => consumer.resume([{ topic: 'topic' }]), /consumer#run must be called first/)
      equal(consumer.paused().length, 0)
    }
  })

  test('validates offsets and topic partitions before lifecycle state', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    await rejects(consumer.commitOffsets([{ topic: '', partition: 0, offset: '0' }]), /Invalid topic/)
    throws(() => consumer.seek({ topic: 'topic', partition: 0, offset: '-3' }), /Offset must not be a negative number/)
    throws(() => consumer.pause([{ topic: 'topic', partitions: [Number.NaN] }]), /Array of valid partitions/)
  })

  test('waits for native group startup before resolving run', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    const startup = Promise.withResolvers<never>()
    const stream = {
      offsetsToFetch: new Map(),
      close: async () => {},
      async * [Symbol.asyncIterator] () {}
    }
    native.consume = (() => startup.promise) as never
    native.leaveGroup = (async () => {}) as never

    let started = false
    const run = consumer.run({ eachMessage: async () => {} }).then(() => {
      started = true
    })
    await Promise.resolve()
    equal(started, false)

    startup.resolve(stream as never)
    await run
    equal(started, true)
    await consumer.stop()
  })

  test('resolves run after an initial crash is handled', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    native.consume = (async () => {
      throw new TypeError('startup failed')
    }) as never
    native.leaveGroup = (async () => {}) as never
    let crashed = false
    consumer.on(consumer.events.CRASH, event => {
      crashed = true
      equal((event.payload as { restart: boolean }).restart, false)
    })

    await consumer.run({ eachMessage: async () => {} })
    equal(crashed, true)
    await consumer.stop()
  })

  test('unblocks run when stopped during startup', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    const startup = Promise.withResolvers<never>()
    const stream = {
      offsetsToFetch: new Map(),
      close: async () => {},
      async * [Symbol.asyncIterator] () {}
    }
    native.consume = (() => startup.promise) as never
    native.leaveGroup = (async () => {}) as never

    const run = consumer.run({ eachMessage: async () => {} })
    await Promise.resolve()
    const stop = consumer.stop()
    await run
    startup.resolve(stream as never)
    await stop
  })

  test('keeps control-only fetch progress internal to the KafkaJS runner', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const progress = {
      [kKafkaJSFetchProgress]: {
        topic: 'topic',
        partition: 1,
        highWatermark: 12n,
        lastOffset: 10n
      }
    }
    const nativeMessage = { topic: 'topic', partition: 1, offset: 9n }
    const stream = {
      async * [Symbol.asyncIterator] () {
        yield nativeMessage
        yield progress
      }
    }
    ;(consumer as unknown as { running: boolean }).running = true
    const batches = (consumer as unknown as {
      batches: (stream: unknown) => AsyncGenerator<{
        topic: string
        partition: number
        messages: unknown[]
        highWatermark: bigint
        lastOffset: bigint
      }>
    }).batches(stream)

    deepStrictEqual(await batches.next(), {
      done: false,
      value: {
        topic: 'topic',
        partition: 1,
        messages: [nativeMessage],
        highWatermark: 12n,
        lastOffset: 10n,
        seekVersion: 0
      }
    })
  })

  test('commits control-only fetch progress without invoking a handler', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    let committed: unknown
    native.commit = (async (options: unknown) => {
      committed = options
    }) as never
    ;(consumer as unknown as { running: boolean }).running = true

    await (consumer as unknown as {
      processBatch: (pending: unknown, config: unknown) => Promise<void>
    }).processBatch(
      {
        topic: 'topic',
        partition: 1,
        messages: [],
        highWatermark: 12n,
        lastOffset: 10n,
        seekVersion: 0
      },
      { autoCommit: true }
    )

    deepStrictEqual((committed as { offsets: Array<{ topic: string; partition: number; offset: bigint; leaderEpoch: number }> }).offsets.map(
      ({ topic, partition, offset, leaderEpoch }) => ({ topic, partition, offset, leaderEpoch })
    ), [{ topic: 'topic', partition: 1, offset: 11n, leaderEpoch: -1 }])
  })

  test('routes terminal group failures by destroying bridge streams', async () => {
    const native = new KafkaJSConsumerBridge({
      clientId: 'compat-test',
      bootstrapBrokers: brokers,
      groupId: 'group'
    })
    const stream = native[kCreateMessagesStream]({ topics: [], autocommit: false } as never)
    const error = new Error('heartbeat failed')
    const destroyed = new Promise<Error>(resolve => stream.once('error', resolve))

    equal(native[kHandleTerminalGroupFailure](error), true)
    equal(await destroyed, error)
  })

  test('uses the native throttled heartbeat from handlers', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    let heartbeats = 0
    native.heartbeat = async () => {
      heartbeats++
    }

    await (consumer as unknown as { heartbeat: () => Promise<void> }).heartbeat()
    equal(heartbeats, 1)
  })

  test('applies assigned seeks without restarting the stream', async () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' })
    const native = (consumer as unknown as { native: KafkaJSConsumerBridge }).native
    native.assignments = [{ topic: 'topic', partitions: [0] }]
    const seeks: Array<[string, number, bigint, number]> = []
    const stream = {
      [kSeekPartition] (topic: string, partition: number, offset: bigint, seekVersion: number) {
        seeks.push([topic, partition, offset, seekVersion])
      }
    }
    const state = consumer as unknown as {
      stream: unknown
      autoCommitEnabled: boolean
      seekVersions: Map<string, number>
      applySeek: (topic: string, partition: number, offset: bigint, seekVersion: number) => void
    }
    state.stream = stream
    state.autoCommitEnabled = false
    state.seekVersions.set('topic:0', 1)

    state.applySeek('topic', 0, 5n, 1)
    await new Promise(resolve => setImmediate(resolve))
    deepStrictEqual(seeks, [['topic', 0, 5n, 1]])
  })

  test('clears compatibility run state between runs', () => {
    const consumer = new Kafka({ brokers }).consumer({ groupId: 'group' }) as unknown as {
      pausedPartitions: Map<string, Set<number>>
      pendingSeeks: Map<string, bigint>
      seekVersions: Map<string, number>
      resolvedOffsets: Map<string, bigint>
      committedOffsets: Map<string, bigint>
      resolvedCount: number
      resetRunState: () => void
    }
    consumer.pausedPartitions.set('topic', new Set([0]))
    consumer.pendingSeeks.set('topic:0', 2n)
    consumer.seekVersions.set('topic:0', 1)
    consumer.resolvedOffsets.set('topic:0', 2n)
    consumer.committedOffsets.set('topic:0', 2n)
    consumer.resolvedCount = 1

    consumer.resetRunState()

    equal(consumer.pausedPartitions.size, 0)
    equal(consumer.pendingSeeks.size, 0)
    equal(consumer.seekVersions.size, 0)
    equal(consumer.resolvedOffsets.size, 0)
    equal(consumer.committedOffsets.size, 0)
    equal(consumer.resolvedCount, 0)
  })
})

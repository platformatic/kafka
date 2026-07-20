import { deepStrictEqual, equal, ok } from 'node:assert'
import { describe, test } from 'node:test'
import KafkaJS from 'kafkajs'
import * as Platformatic from '../../../src/index.ts'
import * as Compatibility from '../../../src/compatibility/kafkajs/index.ts'
import {
  AclOperationTypes,
  AssignerProtocol,
  CompressionTypes,
  Kafka,
  KafkaJSError,
  KafkaJSNonRetriableError,
  logLevel,
  Partitioners
} from '../../../src/compatibility/kafkajs/index.ts'
import {
  isKafkaJSError,
  isRebalancing,
  KafkaJSMemberIdRequired,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSOffsetOutOfRange,
  wrapError
} from '../../../src/compatibility/kafkajs/errors.ts'
import { Transaction } from '../../../src/compatibility/kafkajs/producer.ts'
import { MultipleErrors, NetworkError, ProtocolError } from '../../../src/errors.ts'

describe('KafkaJS compatibility runtime shape', () => {
  test('exports the KafkaJS 2.2.4 root surface', () => {
    deepStrictEqual(Object.keys(Compatibility).sort(), Object.keys(KafkaJS).sort())
  })

  test('does not export internal consumer extension symbols', () => {
    for (const name of [
      'kCreateMessagesStream',
      'kGetCommittedMetadata',
      'kHandleOffsetOutOfRange',
      'kDecorateMessage',
      'kGetProduceTimeout',
      'kHandleFetchPartitionProgress',
      'kHandleTerminalGroupFailure'
    ]) {
      equal(name in Platformatic, false)
    }
  })

  test('exports KafkaJS 2.2.4 constants', () => {
    deepStrictEqual(logLevel, KafkaJS.logLevel)
    deepStrictEqual(CompressionTypes, KafkaJS.CompressionTypes)
    deepStrictEqual(AclOperationTypes, KafkaJS.AclOperationTypes)
  })

  test('implements KafkaJS error semantics', () => {
    const retryable = new KafkaJSError('retry')
    const fatal = new KafkaJSNonRetriableError(new Error('fatal'))
    equal(retryable.name, 'KafkaJSError')
    equal(retryable.retriable, true)
    equal(fatal.retriable, false)
    ok(isKafkaJSError(fatal))
    equal(isRebalancing({ type: 'REBALANCE_IN_PROGRESS' }), true)
    equal(isRebalancing({ type: 'UNKNOWN' }), false)
  })

  test('converts specialized protocol and retry errors', () => {
    const offsetError = wrapError(
      new ProtocolError('OFFSET_OUT_OF_RANGE', null, { topic: 'topic', partition: 2 })
    )
    ok(offsetError instanceof KafkaJSOffsetOutOfRange)
    equal(offsetError.topic, 'topic')
    equal(offsetError.partition, 2)

    const memberError = wrapError(new ProtocolError('MEMBER_ID_REQUIRED', null, {}, { memberId: 'member' }))
    ok(memberError instanceof KafkaJSMemberIdRequired)
    equal(memberError.memberId, 'member')

    const retryError = wrapError(
      new MultipleErrors('1 failed 3 times.', [new NetworkError('first'), new NetworkError('last')], {
        retryExhausted: true,
        retryCount: 2,
        retryTime: 300
      })
    )
    ok(retryError instanceof KafkaJSNumberOfRetriesExceeded)
    equal(retryError.retryCount, 2)
    equal(retryError.retryTime, 300)
    equal((retryError.cause as Error).message, 'last')
  })

  test('does not retry programming errors', () => {
    equal(wrapError(new Error('unknown')).retriable, true)
    equal(wrapError(new TypeError('invalid')).retriable, false)
    equal(wrapError(new RangeError('invalid')).retriable, false)
    equal(wrapError(new ReferenceError('invalid')).retriable, false)
    equal(wrapError(new SyntaxError('invalid')).retriable, false)
  })

  test('encodes assigner protocol identically to KafkaJS', () => {
    const member = { version: 0, topics: ['a', 'b'], userData: Buffer.from('data') }
    const assignment = { version: 0, assignment: { a: [0, 2], b: [1] }, userData: Buffer.from('data') }

    const encodedMember = AssignerProtocol.MemberMetadata.encode(member)
    const encodedAssignment = AssignerProtocol.MemberAssignment.encode(assignment)
    deepStrictEqual(encodedMember, KafkaJS.AssignerProtocol.MemberMetadata.encode(member))
    deepStrictEqual(encodedAssignment, KafkaJS.AssignerProtocol.MemberAssignment.encode(assignment))
    deepStrictEqual(AssignerProtocol.MemberMetadata.decode(encodedMember), member)
    deepStrictEqual(AssignerProtocol.MemberAssignment.decode(encodedAssignment), assignment)
    equal(AssignerProtocol.MemberAssignment.decode(Buffer.alloc(0)), null)
  })

  test('honors explicit and keyed partitioning', () => {
    const partitioner = Partitioners.DefaultPartitioner()
    const partitionMetadata = [0, 1, 2].map(partitionId => ({
      partitionErrorCode: 0,
      partitionId,
      leader: partitionId,
      replicas: [partitionId],
      isr: [partitionId]
    }))
    equal(partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x', partition: 2 } }), 2)
    const first = partitioner({ topic: 'topic', partitionMetadata, message: { key: 'same', value: 'x' } })
    const second = partitioner({ topic: 'topic', partitionMetadata, message: { key: 'same', value: 'y' } })
    equal(first, second)
  })

  test('matches KafkaJS 2.2.4 partitioners for golden keys', () => {
    const partitionMetadata = [0, 1, 2, 3, 4, 5, 6].map(partitionId => ({
      partitionErrorCode: 0,
      partitionId,
      leader: partitionId,
      replicas: [partitionId],
      isr: [partitionId]
    }))
    const keys = ['', 'a', 'same', 'kafkajs', '\u{1f525}', Buffer.from([0, 1, 2, 255])]
    for (const createPartitioner of [Partitioners.DefaultPartitioner, Partitioners.LegacyPartitioner]) {
      const actual = createPartitioner()
      const expected =
        createPartitioner === Partitioners.DefaultPartitioner
          ? KafkaJS.Partitioners.DefaultPartitioner()
          : KafkaJS.Partitioners.LegacyPartitioner()
      for (const key of keys) {
        const input = { topic: 'topic', partitionMetadata, message: { key, value: 'value' } }
        equal(actual(input), expected(input), `${createPartitioner.name} key ${key.toString()}`)
      }
      const explicit = { topic: 'topic', partitionMetadata, message: { value: 'value', partition: 4 } }
      equal(actual(explicit), expected(explicit))
    }
  })

  test('normalizes factories and exposes namespaced logger', () => {
    const entries: Array<{ namespace: string; level: number; message: string }> = []
    const kafka = new Kafka({
      brokers: ['localhost:9092'],
      clientId: 'compat-test',
      logLevel: logLevel.DEBUG,
      logCreator: () => entry =>
        entries.push({
          namespace: entry.namespace,
          level: entry.level,
          message: entry.log.message
        })
    })
    kafka.logger().namespace('unit').debug('message')
    equal(entries[0].namespace, 'unit')
    equal(entries[0].message, 'message')
    ok(kafka.producer())
    ok(kafka.consumer({ groupId: 'group' }))
    ok(kafka.admin())
    ok(new Kafka({ brokers: async () => ['localhost:9092'] }))
  })

  test('accepts supported connection configuration', () => {
    ok(new Kafka({ brokers: ['localhost:9092'], authenticationTimeout: 1 }))
    ok(new Kafka({ brokers: ['localhost:9092'], reauthenticationThreshold: 1 }))
    ok(new Kafka({ brokers: ['localhost:9092'], enforceRequestTimeout: false }))
    ok(new Kafka({ brokers: ['localhost:9092'], socketFactory: (() => {}) as never }))
    ok(new Kafka({ brokers: ['localhost:9092'], enforceRequestTimeout: true }))
  })

  test('accepts custom SASL providers and KafkaJS AWS configuration', () => {
    ok(
      new Kafka({
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'custom',
          authenticationProvider: () => ({ authenticate: async () => {} })
        }
      })
    )
    ok(
      new Kafka({
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'aws',
          authorizationIdentity: 'identity',
          accessKeyId: 'access',
          secretAccessKey: 'secret'
        }
      })
    )
  })

  test('defaults producer and transactional sends to all acknowledgements', async () => {
    const producer = new Kafka({ brokers: ['localhost:9092'] }).producer()
    const acknowledgements: number[] = []
    const native = producer as unknown as {
      native: {
        send: (options: { acks?: number }) => Promise<{ offsets: [] }>
        metadata: () => Promise<unknown>
      }
    }
    native.native.send = async options => {
      acknowledgements.push(options.acks!)
      return { offsets: [] }
    }
    native.native.metadata = async () => ({})
    ;(producer as unknown as { state: string; connected: boolean }).state = 'connected'
    ;(producer as unknown as { state: string; connected: boolean }).connected = true
    await producer.send({ topic: 'topic', messages: [{ value: 'one' }] })
    await producer.sendBatch({ topicMessages: [{ topic: 'topic', messages: [{ value: 'two' }] }] })

    const transaction = new Transaction({
      completed: false,
      send: async (options: { acks?: number }) => {
        acknowledgements.push(options.acks!)
        return { offsets: [] }
      }
    } as never)
    await transaction.send({ topic: 'topic', messages: [{ value: 'three' }] })
    deepStrictEqual(acknowledgements, [-1, -1, -1])
  })
})

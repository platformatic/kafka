import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { EventEmitter } from 'node:events'
import { test } from 'node:test'
import { ProtocolError } from '../../../src/errors.ts'
import { kCreateConnectionPool, kPrometheus } from '../../../src/clients/base/base.ts'
import { MessagesStream, MessagesStreamFallbackModes, MessagesStreamModes } from '../../../src/index.ts'
import { kAutocommit, kGetFetchNode } from '../../../src/symbols.ts'
import { mockMethod } from '../../helpers.ts'

function createFakeConsumer (active: boolean) {
  const consumer = new EventEmitter() as any

  consumer.assignments = [{ topic: 'test-topic', partitions: [0] }]
  consumer.topics = { current: ['test-topic'] }
  consumer.groupId = 'test-group'
  consumer.memberId = 'test-member'
  consumer.generationId = 1
  consumer.coordinatorId = 1
  consumer[kGetFetchNode] = (metadata: any, topic: string, partition: number) => {
    return metadata.topics.get(topic).partitions[partition].leader
  }
  consumer[kPrometheus] = undefined
  consumer[kCreateConnectionPool] = () => {
    return { close (callback: (error: Error | null) => void) { callback(null) } }
  }
  consumer.metadata = (_: object, callback: (error: null, metadata: unknown) => void) => {
    callback(null, { topics: new Map([['test-topic', { id: 'test-topic-id', partitions: [{ leader: 1, leaderEpoch: 0 }] }]]) })
  }
  consumer.listOffsets = (_: object, callback: (error: null, offsets: unknown) => void) => {
    callback(null, new Map([['test-topic', [0n]]]))
  }
  consumer.listCommittedOffsets = (_: object, callback: (error: null, offsets: unknown) => void) => {
    callback(null, new Map([['test-topic', [0n]]]))
  }
  consumer.fetch = () => {}
  consumer.isActive = () => active
  consumer.commit = (_: object, callback: (error: Error | null) => void) => {
    consumer.commitCalls = (consumer.commitCalls ?? 0) + 1
    callback(null)
  }

  return consumer
}

function createStream (consumer: any) {
  return new MessagesStream(consumer, {
    topics: ['test-topic'],
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    maxWaitTime: 1000,
    maxBytes: 1024,
    autocommit: false
  })
}

test('kAutocommit skips committing while the consumer is not active (mid-rebalance)', () => {
  const consumer = createFakeConsumer(false)
  const stream = createStream(consumer)

  stream.offsetsToCommit.set('test-topic:0', { topic: 'test-topic', partition: 0, offset: 5n, leaderEpoch: 0 })

  stream[kAutocommit]()

  ok(!consumer.commitCalls, 'commit must not be attempted while the consumer is not active')
  // The pending offset must remain queued so it is committed once the consumer becomes active again.
  deepStrictEqual(stream.offsetsToCommit.get('test-topic:0'), { topic: 'test-topic', partition: 0, offset: 5n, leaderEpoch: 0 })

  stream.destroy()
})

test('kAutocommit commits normally once the consumer is active again', () => {
  const consumer = createFakeConsumer(true)
  const stream = createStream(consumer)

  stream.offsetsToCommit.set('test-topic:0', { topic: 'test-topic', partition: 0, offset: 5n, leaderEpoch: 0 })

  stream[kAutocommit]()

  ok(consumer.commitCalls === 1, 'commit must be attempted once the consumer is active')
  deepStrictEqual(stream.offsetsToCommit.size, 0)

  stream.destroy()
})

test('kAutocommit keeps offsets queued after a transient commit error', () => {
  const consumer = createFakeConsumer(true)
  const error = new ProtocolError('NOT_COORDINATOR', null, {}, {})
  mockMethod(consumer, 'commit', 1, error)
  const stream = createStream(consumer)
  const offset = { topic: 'test-topic', partition: 0, offset: 5n, leaderEpoch: 0 }
  stream.offsetsToCommit.set('test-topic:0', offset)

  stream[kAutocommit]()

  deepStrictEqual(stream.offsetsToCommit.get('test-topic:0'), offset)
  stream.destroy()
})

test('kAutocommit does not overlap an in-flight commit', () => {
  const consumer = createFakeConsumer(true)
  let commitCallback!: (error: Error | null) => void
  let commitCalls = 0
  mockMethod(consumer, 'commit', 1, undefined, undefined, (_original, ...args) => {
    commitCalls++
    commitCallback = args.at(-1)
    return true
  })
  const stream = createStream(consumer)
  stream.offsetsToCommit.set('test-topic:0', { topic: 'test-topic', partition: 0, offset: 5n, leaderEpoch: 0 })

  stream[kAutocommit]()
  stream.offsetsToCommit.set('test-topic:0', { topic: 'test-topic', partition: 0, offset: 6n, leaderEpoch: 0 })
  stream[kAutocommit]()

  strictEqual(commitCalls, 1)
  commitCallback(null)
  deepStrictEqual(stream.offsetsToCommit.get('test-topic:0'), {
    topic: 'test-topic',
    partition: 0,
    offset: 6n,
    leaderEpoch: 0
  })
  stream.destroy()
})

test('ILLEGAL_GENERATION ProtocolError has needsRejoin: true so kAutocommit destroys the stream', () => {
  const error = new ProtocolError('ILLEGAL_GENERATION', null, {}, {})
  ok(error.needsRejoin, 'ILLEGAL_GENERATION requires a group rejoin')
})

test('UNKNOWN_MEMBER_ID ProtocolError has needsRejoin: true', () => {
  const error = new ProtocolError('UNKNOWN_MEMBER_ID', null, {}, {})
  ok(error.needsRejoin)
})

test('REBALANCE_IN_PROGRESS ProtocolError has needsRejoin: true', () => {
  const error = new ProtocolError('REBALANCE_IN_PROGRESS', null, {}, {})
  ok(error.needsRejoin)
})

test('COORDINATOR_LOAD_IN_PROGRESS ProtocolError has needsRejoin: false — kAutocommit must not destroy the stream', () => {
  const error = new ProtocolError('COORDINATOR_LOAD_IN_PROGRESS', null, {}, {})
  ok(!error.needsRejoin, 'transient coordinator error must not trigger stream destruction')
})

test('NOT_COORDINATOR ProtocolError has needsRejoin: false — kAutocommit must not destroy the stream', () => {
  const error = new ProtocolError('NOT_COORDINATOR', null, {}, {})
  ok(!error.needsRejoin, 'NOT_COORDINATOR is a transient routing error, not a rejoin signal')
})

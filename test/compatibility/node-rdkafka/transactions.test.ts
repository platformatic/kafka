import assert from 'node:assert/strict'
import { setTimeout as sleep } from 'node:timers/promises'
import { test } from 'node:test'
import type { Transaction } from '../../../src/clients/producer/transaction.ts'
import type { NodeRdkafkaProducerBridge } from '../../../src/compatibility/node-rdkafka/producer-native.ts'
import { KafkaConsumer } from '../../../src/compatibility/node-rdkafka/consumer.ts'
import { CODES, type LibrdKafkaError } from '../../../src/compatibility/node-rdkafka/errors.ts'
import { Producer } from '../../../src/compatibility/node-rdkafka/producer.ts'

type Callback = (error: Error | null) => void

interface TransactionStub {
  commit: (callback: Callback) => void
  abort: (callback: Callback) => void
  addConsumer: (consumer: unknown, callback: Callback) => void
  addOffset: (message: TransactionOffset, callback: Callback) => void
}

interface TransactionOffset {
  topic: string
  partition: number
  offset: bigint
  metadata: {
    consumer: {
      groupId: string
      generationId: number
      memberId: string
      coordinatorId: number
    }
  }
}

interface ProducerBridgeStub {
  initIdempotentProducer: (options: object, callback: Callback) => void
  beginTransaction: (options: object, callback: (error: Error | null, transaction?: TransactionStub) => void) => void
}

class TestProducer extends Producer {
  constructor () {
    super({ 'bootstrap.servers': 'localhost:9092', 'transactional.id': 'transaction-test' })
    this.connected = true
  }

  get nativeBridge (): NodeRdkafkaProducerBridge {
    return this.producer
  }

  activate (transaction: TransactionStub): void {
    this.transaction = transaction as unknown as Transaction<Buffer | string | null, Buffer | null, string, Buffer | string>
  }

  setPending (pending: number): void {
    this.pending = pending
  }

  hasActiveTransaction (): boolean {
    return this.transaction !== null
  }
}

test('initTransactions initializes the native producer without opening a transaction', async () => {
  const producer = new TestProducer()
  const bridge = producer.nativeBridge as unknown as ProducerBridgeStub
  const transaction = createTransactionStub()
  let initializationOptions: object | undefined
  let beginCalls = 0

  bridge.initIdempotentProducer = (options, callback) => {
    initializationOptions = options
    queueMicrotask(() => callback(null))
  }
  bridge.beginTransaction = (_options, callback) => {
    beginCalls++
    callback(null, transaction)
  }

  await initTransactions(producer)
  assert.deepEqual(initializationOptions, {})
  assert.equal(beginCalls, 0)

  await beginTransaction(producer)
  assert.equal(beginCalls, 1)
  assert.equal(producer.hasActiveTransaction(), true)
})

test('transaction methods report state errors without an active transaction', async () => {
  const producer = new TestProducer()
  const consumer = new KafkaConsumer({ 'group.id': 'transaction-test' })

  await assert.rejects(beginTransaction(producer), hasCode(CODES.ERRORS.ERR__STATE))
  assertErrorCode(await transactionCallback(callback => producer.commitTransaction(callback)), CODES.ERRORS.ERR__STATE)
  assertErrorCode(await transactionCallback(callback => producer.abortTransaction(callback)), CODES.ERRORS.ERR__STATE)
  assertErrorCode(await transactionCallback(callback => producer.sendOffsetsToTransaction([], consumer, callback)), CODES.ERRORS.ERR__STATE)
})

test('commit and abort enforce callback deadlines and ignore late callbacks', async () => {
  for (const operation of ['commit', 'abort'] as const) {
    const producer = new TestProducer()
    let nativeCallback: Callback | undefined
    let callbackCalls = 0
    const transaction = createTransactionStub({
      [operation]: callback => {
        nativeCallback = callback
      }
    })
    producer.activate(transaction)

    const error = await new Promise<Error | null>(resolve => {
      producer[`${operation}Transaction`](10, result => {
        callbackCalls++
        resolve(result)
      })
    })
    assertErrorCode(error, CODES.ERRORS.ERR__TIMED_OUT)
    assert.equal(producer.hasActiveTransaction(), true)

    nativeCallback!(null)
    await sleep(0)
    assert.equal(callbackCalls, 1)
    assert.equal(producer.hasActiveTransaction(), true)
  }
})

test('commit and abort do not start after pending deliveries miss their deadline', async () => {
  for (const operation of ['commit', 'abort'] as const) {
    const producer = new TestProducer()
    let nativeCalls = 0
    producer.activate(createTransactionStub({
      [operation]: callback => {
        nativeCalls++
        callback(null)
      }
    }))
    producer.setPending(1)

    const error = await transactionCallback(callback => producer[`${operation}Transaction`](10, callback))
    assertErrorCode(error, CODES.ERRORS.ERR__TIMED_OUT)

    producer.setPending(0)
    await sleep(20)
    assert.equal(nativeCalls, 0)
    assert.equal(producer.hasActiveTransaction(), true)
  }
})

test('commit and abort cannot end the same transaction concurrently', async () => {
  for (const selected of ['commit', 'abort'] as const) {
    const rejected = selected === 'commit' ? 'abort' : 'commit'
    const producer = new TestProducer()
    const calls = { commit: 0, abort: 0 }
    producer.activate(createTransactionStub({
      commit: callback => {
        calls.commit++
        callback(null)
      },
      abort: callback => {
        calls.abort++
        callback(null)
      }
    }))
    producer.setPending(1)

    const completed = transactionCallback(callback => producer[`${selected}Transaction`](100, callback))
    assertErrorCode(await transactionCallback(callback => producer[`${rejected}Transaction`](100, callback)), CODES.ERRORS.ERR__STATE)

    producer.setPending(0)
    assert.equal(await completed, null)
    assert.equal(calls[selected], 1)
    assert.equal(calls[rejected], 0)
  }
})

test('sendOffsetsToTransaction validates offsets and active consumer membership', async () => {
  const producer = new TestProducer()
  const consumer = new KafkaConsumer({ 'group.id': 'transaction-test' })
  const transaction = createTransactionStub()
  producer.activate(transaction)

  assertErrorCode(
    await transactionCallback(callback => producer.sendOffsetsToTransaction([
      { topic: 'events', partition: 0, offset: 1 }
    ], consumer, callback)),
    CODES.ERRORS.ERR__STATE
  )

  setConsumerMembership(consumer)
  for (const offsets of [
    [{ topic: '', partition: 0, offset: 1 }],
    [{ topic: 'events', partition: -1, offset: 1 }],
    [{ topic: 'events', partition: 0.5, offset: 1 }],
    [{ topic: 'events', partition: 0, offset: -1 }],
    [{ topic: 'events', partition: 0, offset: Number.MAX_SAFE_INTEGER + 1 }]
  ]) {
    assertErrorCode(
      await transactionCallback(callback => producer.sendOffsetsToTransaction(offsets, consumer, callback)),
      CODES.ERRORS.ERR__INVALID_ARG
    )
  }
})

test('sendOffsetsToTransaction adds membership before normalized offsets', async () => {
  const producer = new TestProducer()
  const consumer = new KafkaConsumer({ 'group.id': 'transaction-test' })
  const calls: string[] = []
  const added: TransactionOffset[] = []
  producer.activate(createTransactionStub({
    addConsumer: (_consumer, callback) => {
      calls.push('consumer')
      callback(null)
    },
    addOffset: (message, callback) => {
      calls.push('offset')
      added.push(message)
      callback(null)
    }
  }))
  setConsumerMembership(consumer)

  const error = await transactionCallback(callback => producer.sendOffsetsToTransaction([
    { topic: 'events', partition: 1, offset: 7 },
    { topic: 'events', partition: 2, offset: 0 }
  ], consumer, callback))

  assert.equal(error, null)
  assert.deepEqual(calls, ['consumer', 'offset', 'offset'])
  assert.deepEqual(added, [
    {
      topic: 'events',
      partition: 1,
      offset: 6n,
      metadata: { consumer: { groupId: 'transaction-test', generationId: 3, memberId: 'member', coordinatorId: 1 } }
    },
    {
      topic: 'events',
      partition: 2,
      offset: -1n,
      metadata: { consumer: { groupId: 'transaction-test', generationId: 3, memberId: 'member', coordinatorId: 1 } }
    }
  ])
})

function createTransactionStub (overrides: Partial<TransactionStub> = {}): TransactionStub {
  return {
    commit: callback => callback(null),
    abort: callback => callback(null),
    addConsumer: (_consumer, callback) => callback(null),
    addOffset: (_message, callback) => callback(null),
    ...overrides
  }
}

function setConsumerMembership (consumer: KafkaConsumer): void {
  consumer.isConnected = () => true
  consumer.consumer.memberId = 'member'
  consumer.consumer.generationId = 3
  Object.defineProperty(consumer.consumer, 'coordinatorId', { configurable: true, value: 1 })
}

function initTransactions (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.initTransactions(error => error ? reject(error) : resolve())
  })
}

function beginTransaction (producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.beginTransaction(error => error ? reject(error) : resolve())
  })
}

function transactionCallback (operation: (callback: Callback) => void): Promise<Error | null> {
  return new Promise(resolve => operation(resolve))
}

function assertErrorCode (error: Error | null, code: number): void {
  assert.equal((error as LibrdKafkaError | null)?.code, code)
}

function hasCode (code: number): (error: unknown) => boolean {
  return error => (error as LibrdKafkaError).code === code
}

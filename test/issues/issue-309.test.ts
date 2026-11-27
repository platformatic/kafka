// Related issue: https://github.com/platformatic/kafka/issues/309

import { deepStrictEqual, rejects, strictEqual } from 'node:assert'
import { test } from 'node:test'
import type { Callback } from '../../src/apis/definitions.ts'
import type { ProduceResponse } from '../../src/apis/producer/produce-v11.ts'
import {
  kGetApi,
  kGetBootstrapConnection,
  kGetConnection,
  kMetadata
} from '../../src/clients/base/base.ts'
import type { ClusterMetadata } from '../../src/clients/base/types.ts'
import type { CreateRecordsBatchOptions, MessageRecord } from '../../src/protocol/records.ts'
import { type Connection, type Producer, ProtocolError } from '../../src/index.ts'
import { createProducer, createTopic, mockMethod } from '../helpers.ts'

type ProduceApiCallback = Callback<boolean | ProduceResponse>
type ProduceApi = (
  connection: Connection,
  acks: number | undefined,
  timeout: number | undefined,
  messages: MessageRecord[],
  produceOptions: Partial<CreateRecordsBatchOptions> | undefined,
  apiCallback: ProduceApiCallback | undefined
) => void

function createMetadata (topic: string): ClusterMetadata {
  return {
    id: 'test-cluster',
    brokers: new Map([[0, { host: 'localhost', port: 9092, rack: null }]]),
    controllerId: 0,
    topics: new Map([
      [
        topic,
        {
          id: 'test-topic-id',
          partitions: Array.from({ length: 2 }, () => ({
            leader: 0, leaderEpoch: 0, replicas: [0], isr: [0], offlineReplicas: []
          })),
          partitionsCount: 2,
          lastUpdate: Date.now()
        }
      ]
    ]),
    lastUpdate: Date.now()
  }
}

function createProduceResponse (topic: string, partition: number, offset: bigint = 0n): ProduceResponse {
  return {
    responses: [
      {
        name: topic,
        partitionResponses: [
          {
            index: partition,
            errorCode: 0,
            baseOffset: offset,
            logAppendTimeMs: 0n,
            logStartOffset: 0n,
            recordErrors: [],
            errorMessage: null
          }
        ]
      }
    ],
    throttleTimeMs: 0
  }
}

function mockProducerNetwork (producer: Producer, topic: string): void {
  const connection = {} as Connection

  mockMethod(producer, kMetadata, () => true, null, undefined, (_original, _options, callback) => {
    callback(null, createMetadata(topic))
    return true
  })

  mockMethod(producer, kGetConnection, () => true, null, undefined, (_original, _broker, callback) => {
    callback(null, connection)
    return true
  })

  mockMethod(producer, kGetBootstrapConnection, () => true, null, undefined, (_original, callback) => {
    callback(null, connection)
    return true
  })
}

function mockProducerApis (
  producer: Producer,
  getProducerId: (id?: bigint, epoch?: number) => bigint,
  produceApi: ProduceApi,
  version: number = 3
): void {
  mockMethod(producer, kGetApi, () => true, null, undefined, (original, name, callback) => {
    if (name === 'InitProducerId') {
      const api = (
        _connection: Connection,
        _transactionalId: string,
        _timeout: number,
        producerId: bigint | undefined,
        producerEpoch: number | undefined,
        apiCallback: Callback<unknown>
      ) => {
        apiCallback(null, {
          throttleTimeMs: 0,
          errorCode: 0,
          producerId: getProducerId(producerId, producerEpoch),
          producerEpoch: producerEpoch === 7 ? 8 : 7
        })
      }

      callback(null, Object.assign(api, { version }))
      return true
    }

    if (name === 'Produce') {
      callback(null, produceApi as any)
      return true
    }

    original(name, callback)
    return true
  })
}

for (const apiId of ['UNKNOWN_PRODUCER_ID', 'OUT_OF_ORDER_SEQUENCE_NUMBER'] as const) {
  test(`issue-309: idempotent producer recovers after ${apiId}`, async t => {
    const producer = createProducer(t, { idempotent: true })
    const topic = await createTopic(t)
    let initCalls = 0
    let produceCalls = 0

    mockProducerNetwork(producer, topic)
    mockProducerApis(
      producer,
      () => {
        initCalls++
        return BigInt(initCalls)
      },
      (_connection, _acks, _timeout, messages, _produceOptions, apiCallback) => {
        produceCalls++

        if (produceCalls === 1) {
          apiCallback!(new ProtocolError(apiId))
          return
        }

        apiCallback!(null, createProduceResponse(messages[0].topic, messages[0].partition!))
      }
    )

    await producer.send({ messages: [{ topic, value: Buffer.from('message'), partition: 0 }] })

    strictEqual(initCalls, 2)
    strictEqual(produceCalls, 2)
  })
}

for (const version of [2, 3]) {
  test(`issue-472: recover a sequence gap with InitProducerId v${version}`, async t => {
    const producer = createProducer(t, { idempotent: true })
    const topic = await createTopic(t)
    const identities: [bigint | undefined, number | undefined][] = []
    const batches: [bigint, number, number][] = []
    mockProducerNetwork(producer, topic)
    mockProducerApis(
      producer,
      (id, epoch) => {
        identities.push([id, epoch])
        return id === 42n || identities.length === 1 ? 42n : 43n
      },
      (_connection, _acks, _timeout, _messages, options, callback) => {
        batches.push([options!.producerId!, options!.producerEpoch!, options!.sequences!.get(`${topic}:0`)!])
        if (batches.length === 2) {
          callback!(new ProtocolError('OUT_OF_ORDER_SEQUENCE_NUMBER'))
          return
        }

        callback!(null, createProduceResponse(topic, 0))
      },
      version
    )

    for (const value of ['A', 'B', 'C']) {
      await producer.send({ messages: [{ topic, partition: 0, value: Buffer.from(value) }] })
    }

    deepStrictEqual(
      identities,
      version === 3 ? [[-1n, -1], [42n, 7]] : [[undefined, undefined], [undefined, undefined]]
    )
    const recovered = version === 3 ? [42n, 8] : [43n, 7]
    deepStrictEqual(batches, [[42n, 7, 0], [42n, 7, 1], [...recovered, 0], [...recovered, 1]])
  })
}

test('issue-472: a late failure on another partition preserves the recovered epoch', async t => {
  const producer = createProducer(t, { idempotent: true })
  const topic = await createTopic(t)
  const pending = Promise.withResolvers<ProduceApiCallback>()
  const sequences: number[] = []
  let initCalls = 0

  mockProducerNetwork(producer, topic)
  mockProducerApis(
    producer,
    () => {
      initCalls++
      return 42n
    },
    (_connection, _acks, _timeout, messages, options, callback) => {
      if (messages[0].partition === 1) {
        pending.resolve(callback!)
        return
      }

      sequences.push(options!.sequences!.get(`${topic}:0`)!)
      if (sequences.length === 1) {
        callback!(new ProtocolError('UNKNOWN_PRODUCER_ID'))
      } else {
        callback!(null, createProduceResponse(topic, 0))
      }
    }
  )

  const send = (partition: number) => producer.send({ messages: [{ topic, partition, value: Buffer.from('test') }] })
  const error = new ProtocolError('MESSAGE_TOO_LARGE')
  const failure = rejects(send(1), { errors: [error] })
  const fail = await pending.promise
  await send(0)
  fail(error)
  await failure
  await send(0)

  strictEqual(initCalls, 2)
  deepStrictEqual(sequences, [0, 0, 1])
})

test('issue-309: concurrent idempotent sends claim distinct partition sequences', async t => {
  const producer = createProducer(t, { idempotent: true })
  const topic = await createTopic(t)
  const firstSequences: number[] = []
  let initCalls = 0

  mockProducerNetwork(producer, topic)
  mockProducerApis(
    producer,
    () => {
      initCalls++
      return BigInt(initCalls)
    },
    (_connection, _acks, _timeout, messages, produceOptions, apiCallback) => {
      const { partition } = messages[0]

      firstSequences.push(produceOptions!.sequences!.get(`${topic}:${partition}`)!)
      setImmediate(() => {
        apiCallback!(null, createProduceResponse(topic, partition!))
      })
    }
  )

  await Promise.all([
    producer.send({ messages: [{ topic, value: Buffer.from('message-1'), partition: 0 }] }),
    producer.send({ messages: [{ topic, value: Buffer.from('message-2'), partition: 0 }] })
  ])

  deepStrictEqual(firstSequences, [0, 1])
  strictEqual(initCalls, 1)
})

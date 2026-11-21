// Related issue: https://github.com/platformatic/kafka/issues/309

import { deepStrictEqual, strictEqual } from 'node:assert'
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
          partitions: [{ leader: 0, leaderEpoch: 0, replicas: [0], isr: [0], offlineReplicas: [] }],
          partitionsCount: 1,
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

function mockProducerApis (producer: Producer, getProducerId: () => bigint, produceApi: ProduceApi): void {
  mockMethod(producer, kGetApi, () => true, null, undefined, (original, name, callback) => {
    if (name === 'InitProducerId') {
      const api = (
        _connection: Connection,
        _transactionalId: string,
        _timeout: number,
        _producerId: bigint,
        _producerEpoch: number,
        apiCallback: Callback<unknown>
      ) => {
        apiCallback(null, { throttleTimeMs: 0, errorCode: 0, producerId: getProducerId(), producerEpoch: 0 })
      }

      callback(null, api as any)
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

test('issue-309: concurrent idempotent sends claim distinct partition sequences', async t => {
  const producer = createProducer(t, { idempotent: true })
  const topic = await createTopic(t)
  const firstSequences: number[] = []
  const callbacks: (() => void)[] = []
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
      callbacks.push(() => {
        apiCallback!(null, createProduceResponse(topic, partition!, BigInt(callbacks.length)))
      })

      if (callbacks.length === 2) {
        callbacks[1]()
        callbacks[0]()
      }
    }
  )

  await Promise.all([
    producer.send({ messages: [{ topic, value: Buffer.from('message-1'), partition: 0 }] }),
    producer.send({ messages: [{ topic, value: Buffer.from('message-2'), partition: 0 }] })
  ])

  deepStrictEqual(firstSequences, [0, 1])
  strictEqual(initCalls, 1)
})

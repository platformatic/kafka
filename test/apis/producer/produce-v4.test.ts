import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { type MessageRecord, ProduceAcks, produceV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = produceV4

test('Produce v4 creates a non-flexible request with Record Batch payloads', () => {
  const messages: MessageRecord[] = [{ topic: 'orders', value: Buffer.from('created') }]
  const writer = createRequest(ProduceAcks.ALL, 30_000, messages, { transactionalId: 'transaction-1' })
  const reader = Reader.from(writer)

  strictEqual(api.version, 4)
  strictEqual(reader.readString(false), 'transaction-1')
  strictEqual(reader.readInt16(), ProduceAcks.ALL)
  strictEqual(reader.readInt32(), 30_000)
  strictEqual(reader.readArray(r => r.readString(false), false, false)[0], 'orders')
  strictEqual(messages[0].partition, 0)
  ok(messages[0].timestamp)
})

test('Produce v4 marks no-ack requests as having no response', () => {
  const writer = createRequest(ProduceAcks.NO_RESPONSE, 10, [{ topic: 'orders', value: Buffer.from('created') }])

  strictEqual(writer.context.noResponse, true)
  strictEqual(writer.context.requestTimeout, 10)
})

test('Produce v4 adapts successful responses to the current producer response API', () => {
  const writer = Writer.create()
    .appendArray(
      [{ name: 'orders', index: 2 }],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          [topic],
          (w, partition) => w.appendInt32(partition.index).appendInt16(0).appendInt64(42n).appendInt64(3n),
          false,
          false
        )
      },
      false,
      false
    )
    .appendInt32(5)

  deepStrictEqual(parseResponse(1, 0, 4, Reader.from(writer)), {
    responses: [
      {
        name: 'orders',
        partitionResponses: [
          {
            index: 2,
            errorCode: 0,
            baseOffset: 42n,
            logAppendTimeMs: 3n,
            logStartOffset: -1n,
            recordErrors: [],
            errorMessage: null
          }
        ]
      }
    ],
    throttleTimeMs: 5
  })
})

test('Produce v4 reports partition errors while retaining the adapted response', () => {
  const writer = Writer.create()
    .appendArray(
      [{ name: 'orders', index: 2 }],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          [topic],
          (w, partition) => w.appendInt32(partition.index).appendInt16(6).appendInt64(-1n).appendInt64(0n),
          false,
          false
        )
      },
      false,
      false
    )
    .appendInt32(0)

  throws(
    () => parseResponse(1, 0, 4, Reader.from(writer)),
    (error: unknown) => {
      ok(error instanceof ResponseError)
      deepStrictEqual(error.response, {
        responses: [
          {
            name: 'orders',
            partitionResponses: [
              {
                index: 2,
                errorCode: 6,
                baseOffset: -1n,
                logAppendTimeMs: 0n,
                logStartOffset: -1n,
                recordErrors: [],
                errorMessage: null
              }
            ]
          }
        ],
        throttleTimeMs: 0
      })
      return true
    }
  )
})

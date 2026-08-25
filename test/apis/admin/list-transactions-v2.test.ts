import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, listTransactionsV2 } from '../../../src/index.ts'

const { createRequest, parseResponse } = listTransactionsV2

test('createRequest serializes transactional id pattern correctly', () => {
  const stateFilters: listTransactionsV2.TransactionState[] = ['Ongoing']
  const producerIdFilters = [1000n]
  const durationFilter = 60000n
  const transactionalIdPattern = '^app-.*$'

  const writer = createRequest(stateFilters, producerIdFilters, durationFilter, transactionalIdPattern)

  ok(writer instanceof Writer)

  const reader = Reader.from(writer)
  deepStrictEqual(
    reader.readArray(() => reader.readString(), true, false),
    ['Ongoing']
  )
  deepStrictEqual(
    reader.readArray(() => reader.readInt64(), true, false),
    [1000n]
  )
  deepStrictEqual(reader.readInt64(), 60000n)
  deepStrictEqual(reader.readNullableString(), '^app-.*$')
})

test('parseResponse correctly processes a successful response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendArray([], () => {})
    .appendArray([{ transactionalId: 'transaction-1', producerId: 1000n, transactionState: 'Ongoing' }], (w, state) => {
      w.appendString(state.transactionalId).appendInt64(state.producerId).appendString(state.transactionState)
    })
    .appendTaggedFields()

  const response = parseResponse(1, 66, 2, Reader.from(writer))

  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    unknownStateFilters: [],
    transactionStates: [{ transactionalId: 'transaction-1', producerId: 1000n, transactionState: 'Ongoing' }]
  })
})

test('parseResponse throws ResponseError on error response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(42)
    .appendArray([], () => {})
    .appendArray([], () => {})
    .appendTaggedFields()

  throws(
    () => {
      parseResponse(1, 66, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      deepStrictEqual(err.response.errorCode, 42)
      deepStrictEqual(err.response.errorCode, 42)
      return true
    }
  )
})

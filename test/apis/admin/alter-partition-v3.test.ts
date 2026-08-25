import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { alterPartitionV3, Reader } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterPartitionV3
const topicId = '12345678-1234-1234-1234-123456789abc'

test('AlterPartition v3 writes distinct ISR broker epochs', () => {
  const request = createRequest(1, 2n, [{
    topicId,
    partitions: [{
      partitionIndex: 0,
      leaderEpoch: 3,
      newIsrWithEpochs: [{ brokerId: 1, brokerEpoch: 5n }, { brokerId: 2, brokerEpoch: 6n }],
      leaderRecoveryState: 1,
      partitionEpoch: 4
    }]
  }])

  deepStrictEqual(request.buffer, Buffer.from('0000000100000000000000020212345678123412341234123456789abc0200000000000000030300000001000000000000000500000000020000000000000006000100000004000000', 'hex'))

  const reader = Reader.from(Buffer.from('0000000100000212345678123412341234123456789abc0200000000000000000001000000030300000001000000020100000004000000', 'hex'))
  const response: alterPartitionV3.AlterPartitionResponse = parseResponse(1, 56, 3, reader)
  deepStrictEqual(response, {
    throttleTimeMs: 1,
    errorCode: 0,
    topics: [{ topicId, partitions: [{ partitionIndex: 0, errorCode: 0, leaderId: 1, leaderEpoch: 3, isr: [1, 2], leaderRecoveryState: 1, partitionEpoch: 4 }] }]
  })
  strictEqual(reader.remaining, 0)
})

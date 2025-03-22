import { strictEqual } from 'node:assert'
import test from 'node:test'
import * as producerAPIs from '../../../src/apis/producer/index.ts'

test('producer index exports all APIs', () => {
  // Verify that all expected APIs are exported
  strictEqual(typeof producerAPIs.addOffsetsToTxnV4, 'function')
  strictEqual(typeof producerAPIs.addPartitionsToTxnV5, 'function')
  strictEqual(typeof producerAPIs.endTxnV4, 'function')
  strictEqual(typeof producerAPIs.initProducerIdV5, 'function')
  strictEqual(typeof producerAPIs.produceV11, 'function')
  strictEqual(typeof producerAPIs.txnOffsetCommitV4, 'function')
})
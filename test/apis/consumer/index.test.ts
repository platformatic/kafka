import { strictEqual } from 'node:assert'
import test from 'node:test'
import * as consumerAPIs from '../../../src/apis/consumer/index.ts'

test('consumer index exports all APIs', () => {
  // Verify that all expected APIs are exported
  strictEqual(typeof consumerAPIs.listOffsetsV9, 'function')
  strictEqual(typeof consumerAPIs.fetchV17, 'function')
  strictEqual(typeof consumerAPIs.heartbeatV4, 'function')
  strictEqual(typeof consumerAPIs.joinGroupV9, 'function')
  strictEqual(typeof consumerAPIs.leaveGroupV5, 'function')
  strictEqual(typeof consumerAPIs.offsetCommitV9, 'function')
  strictEqual(typeof consumerAPIs.offsetFetchV9, 'function')
  strictEqual(typeof consumerAPIs.syncGroupV5, 'function')
  strictEqual(typeof consumerAPIs.consumerGroupHeartbeatV0, 'function')
})
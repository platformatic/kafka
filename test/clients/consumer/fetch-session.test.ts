import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import { nextFetchEpoch } from '../../../src/clients/consumer/utils.ts'

test('nextFetchEpoch - INITIAL_EPOCH (0) advances to 1', () => {
  strictEqual(nextFetchEpoch(0), 1, 'First request (epoch 0) must be followed by epoch 1')
})

test('nextFetchEpoch - monotonically increments positive epochs', () => {
  strictEqual(nextFetchEpoch(1), 2)
  strictEqual(nextFetchEpoch(2), 3)
  strictEqual(nextFetchEpoch(41), 42)
})

test('nextFetchEpoch - wraps back to 1 (not 0) at INT32_MAX', () => {
  strictEqual(nextFetchEpoch(0x7fffffff), 1, 'Wrap must skip 0 to avoid restarting the session')
})

test('nextFetchEpoch - resets to INITIAL_EPOCH (0) for negative epochs', () => {
  strictEqual(nextFetchEpoch(-1), 0)
})

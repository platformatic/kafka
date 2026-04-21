import { ok } from 'node:assert'
import { test } from 'node:test'
import { ProtocolError } from '../../../src/errors.ts'

// Unit tests — no Kafka cluster required.

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

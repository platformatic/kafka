import { deepStrictEqual, rejects, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { kGetApi, kGetBootstrapConnection, kGetConnection, kMetadata } from '../../../src/clients/base/base.ts'
import { ProtocolError, ResponseError } from '../../../src/index.ts'
import { createConsumer, mockMethod } from '../../helpers.ts'

const offsets = [
  { topic: 'topic-a', partition: 0, offset: 10n, leaderEpoch: 0 },
  { topic: 'topic-a', partition: 1, offset: 20n, leaderEpoch: 0 },
  { topic: 'topic-b', partition: 0, offset: 30n, leaderEpoch: 0 }
]

const scenarios = [
  {
    name: 'all partitions retained',
    assignments: [
      { topic: 'topic-a', partitions: [0, 1] },
      { topic: 'topic-b', partitions: [0] }
    ],
    retry: true
  },
  {
    name: 'one partition revoked',
    assignments: [
      { topic: 'topic-a', partitions: [0] },
      { topic: 'topic-b', partitions: [0] }
    ],
    retry: false
  },
  { name: 'one topic revoked', assignments: [{ topic: 'topic-a', partitions: [0, 1] }], retry: false },
  { name: 'all partitions revoked', assignments: [], retry: false },
  { name: 'assignment unavailable', assignments: null, retry: false }
]

for (const apiId of ['ILLEGAL_GENERATION', 'UNKNOWN_MEMBER_ID', 'REBALANCE_IN_PROGRESS']) {
  for (const scenario of scenarios) {
    for (const callbackAPI of [false, true]) {
      test(`commit rejoin after ${apiId}: ${scenario.name} (${callbackAPI ? 'callback' : 'promise'})`, async t => {
        const consumer = createConsumer(t, { retries: 2, retryDelay: 0 })
        consumer.assignments = [
          { topic: 'topic-a', partitions: [0, 1] },
          { topic: 'topic-b', partitions: [0] }
        ]
        consumer.memberId = 'test-member'
        consumer.generationId = 1

        const protocolError = new ProtocolError(apiId)
        const error = new ResponseError(8, 9, { 'topics.0.partitions.0': [protocolError.apiCode, null] }, {})
        const requests: any[][] = []
        let joinCalls = 0

        // Mock only network-facing operations so commits use the real coordinator and retry paths.
        mockMethod(
          consumer,
          kGetBootstrapConnection,
          () => true,
          null,
          null,
          (_original, callback) => {
            callback(null, {})
            return true
          }
        )
        mockMethod(
          consumer,
          kGetConnection,
          () => true,
          null,
          null,
          (_original, _broker, callback) => {
            callback(null, {})
            return true
          }
        )
        mockMethod(
          consumer,
          kMetadata,
          () => true,
          null,
          null,
          (_original, _options, callback) => {
            callback(null, { brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092 }]]), topics: new Map() })
            return true
          }
        )
        mockMethod(
          consumer,
          kGetApi,
          () => true,
          null,
          null,
          (_original, name, callback) => {
            if (name === 'FindCoordinator') {
              callback(
                null,
                Object.assign(
                  (...args: any[]) => {
                    args.at(-1)(null, { coordinators: [{ key: consumer.groupId, nodeId: 1, errorCode: 0 }] })
                  },
                  { version: 6 }
                )
              )
              return true
            }

            strictEqual(name, 'OffsetCommit')
            callback(null, (...args: any[]) => {
              requests.push(args.slice(1, -1))
              args.at(-1)(requests.length === 1 ? error : null)
            })
            return true
          }
        )
        mockMethod(
          consumer,
          'joinGroup',
          () => true,
          null,
          null,
          (_original, _options, callback) => {
            joinCalls++
            consumer.assignments = scenario.assignments
            consumer.generationId++
            consumer.memberId = 'rejoined-member'
            callback(null, consumer.memberId)
            return true
          }
        )

        let commit: Promise<void>
        if (callbackAPI) {
          commit = new Promise<void>((resolve, reject) => {
            consumer.commit({ offsets }, error => {
              if (error) {
                reject(error)
                return
              }

              resolve()
            })
          })
        } else {
          commit = consumer.commit({ offsets })
        }

        if (scenario.retry) {
          await commit
          deepStrictEqual(requests[1], [requests[0][0], 2, 'rejoined-member', ...requests[0].slice(3)])
        } else {
          await rejects(commit, returnedError => {
            strictEqual(returnedError, error)
            strictEqual(error.code, 'PLT_KFK_RESPONSE')
            strictEqual(error.findBy('apiId', apiId)?.code, 'PLT_KFK_PROTOCOL')
            return true
          })
        }

        strictEqual(joinCalls, 1)
        strictEqual(requests.length, scenario.retry ? 2 : 1)
        deepStrictEqual(
          offsets.map(({ offset }) => offset),
          [10n, 20n, 30n]
        )
      })
    }
  }
}

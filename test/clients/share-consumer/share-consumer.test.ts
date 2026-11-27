import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test, type TestContext } from 'node:test'
import * as Prometheus from 'prom-client'
import { type ShareGroupHeartbeatResponse } from '../../../src/apis/consumer/share-group-heartbeat-v1.ts'
import { type Callback } from '../../../src/apis/definitions.ts'
import {
  kConnections,
  kGetApi,
  kGetBootstrapConnection,
  kGetConnection,
  kMetadata
} from '../../../src/clients/base/base.ts'
import { type ClusterMetadata, type MetadataOptions } from '../../../src/clients/base/types.ts'
import {
  type ClientDiagnosticEvent,
  type Connection,
  instancesChannel,
  NetworkError,
  protocolErrors,
  ResponseError,
  ShareConsumer,
  type ShareConsumerOptions,
  shareConsumerGroupChannel,
  shareConsumerHeartbeatChannel,
  UnsupportedApiError,
  UserError
} from '../../../src/index.ts'
import { isUnsupportedVersion, usableVersions } from '../../helpers/api-versions.ts'
import {
  createAdmin,
  createCreationChannelVerifier,
  createTopic,
  createTracingChannelVerifier,
  kafkaBootstrapServers,
  kafkaSingleBootstrapServers,
  mockedOperationId,
  mockMethod,
  mockUnavailableAPI,
  waitFor
} from '../../helpers.ts'

interface HeartbeatRequest {
  groupId: string
  memberId: string
  memberEpoch: number
  rackId: string | null
  topics: string[] | null
}

type HeartbeatHandler = (request: HeartbeatRequest, callback: Callback<ShareGroupHeartbeatResponse>) => void

interface MockedShareGroup {
  requests: HeartbeatRequest[]
  connections: Connection[]
  metadataRequests: MetadataOptions[]
  findCoordinatorCalls: () => number
}

const heartbeatInterval = 20

function createShareConsumer (t: TestContext, overrideOptions: Partial<ShareConsumerOptions> = {}): ShareConsumer {
  const consumer = new ShareConsumer({
    clientId: `test-share-consumer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    groupId: `test-share-group-${randomUUID()}`,
    retryDelay: 10,
    ...overrideOptions
  })

  t.after(() => consumer.close())
  return consumer
}

function createMetadata (topics: Record<string, string>): ClusterMetadata {
  return {
    id: 'test-cluster',
    brokers: new Map([[0, { host: 'localhost', port: 9092, rack: null }]]),
    controllerId: 0,
    topics: new Map(
      Object.entries(topics).map(([topic, id]) => [
        topic,
        {
          id,
          partitions: [{ leader: 0, leaderEpoch: 0, replicas: [0], isr: [0], offlineReplicas: [] }],
          partitionsCount: 1,
          lastUpdate: Date.now()
        }
      ])
    ),
    lastUpdate: Date.now()
  }
}

function heartbeatResponse (overrides: Partial<ShareGroupHeartbeatResponse> = {}): ShareGroupHeartbeatResponse {
  return {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: null,
    memberEpoch: 1,
    heartbeatIntervalMs: heartbeatInterval,
    assignment: null,
    ...overrides
  }
}

function heartbeatError (errorCode: number): ResponseError {
  return new ResponseError(76, 1, { '/': [errorCode, null] }, heartbeatResponse({ errorCode }))
}

function assignment (topicId: string, partitions: number[]): ShareGroupHeartbeatResponse['assignment'] {
  return { topicPartitions: [{ topicId, partitions }] }
}

// Replaces the network with a single broker which is the group coordinator
function mockShareGroup (
  consumer: ShareConsumer,
  heartbeat: HeartbeatHandler,
  getTopics: (options: MetadataOptions) => Record<string, string> | Error = () => ({ topic: 'topic-id' })
): MockedShareGroup {
  const connection = { isConnected: () => true } as unknown as Connection
  const requests: HeartbeatRequest[] = []
  const connections: Connection[] = []
  const metadataRequests: MetadataOptions[] = []
  let findCoordinatorCalls = 0

  mockMethod(
    consumer,
    kMetadata,
    () => true,
    null,
    undefined,
    (_original, options, callback) => {
      metadataRequests.push(options)
      const topics = getTopics(options)

      if (topics instanceof Error) {
        callback(topics)
        return true
      }

      const unknown = options.topics?.find((topic: string) => !(topic in topics))
      if (unknown) {
        callback(new UserError(`Unknown topic ${unknown}.`))
        return true
      }

      callback(null, createMetadata(topics))
      return true
    }
  )

  for (const method of [kGetConnection, kGetBootstrapConnection]) {
    mockMethod(
      consumer,
      method,
      () => true,
      null,
      undefined,
      (_original, ...args) => {
        args.at(-1)(null, connection)
        return true
      }
    )
  }

  mockMethod(
    consumer,
    kGetApi,
    () => true,
    null,
    undefined,
    (original, name, callback) => {
      if (name === 'FindCoordinator') {
        const api = (_connection: Connection, _keyType: number, keys: string[], apiCallback: Callback<unknown>) => {
          findCoordinatorCalls++
          apiCallback(null, {
            throttleTimeMs: 0,
            coordinators: [{ key: keys[0], nodeId: 0, host: 'localhost', port: 9092, errorCode: 0, errorMessage: null }]
          })
        }

        callback(null, Object.assign(api, { version: 6 }))
        return true
      }

      if (name === 'ShareGroupHeartbeat') {
        const api = (
          requestConnection: Connection,
          groupId: string,
          memberId: string,
          memberEpoch: number,
          rackId: string | null,
          topics: string[] | null,
          apiCallback: Callback<ShareGroupHeartbeatResponse>
        ) => {
          const request = { groupId, memberId, memberEpoch, rackId, topics }
          requests.push(request)
          connections.push(requestConnection)
          heartbeat(request, apiCallback)
        }

        callback(null, Object.assign(api, { version: 1 }))
        return true
      }

      original(name, callback)
      return true
    }
  )

  return { requests, connections, metadataRequests, findCoordinatorCalls: () => findCoordinatorCalls }
}

// Grants the next member epoch on every heartbeat, and the given assignment on the first one
function acceptAll (firstAssignment: ShareGroupHeartbeatResponse['assignment'] = null): HeartbeatHandler {
  return (request, callback) => {
    if (request.memberEpoch === -1) {
      callback(null, heartbeatResponse({ memberEpoch: -1 }))
      return
    }

    callback(
      null,
      heartbeatResponse({
        memberEpoch: request.memberEpoch + 1,
        assignment: request.memberEpoch === 0 ? firstAssignment : null
      })
    )
  }
}

test('constructor should generate the member id and require the group id', t => {
  const consumer = createShareConsumer(t)

  ok(/^[0-9a-f-]{36}$/.test(consumer.memberId))
  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.coordinatorId, null)
  strictEqual(consumer.assignments, null)
  strictEqual(consumer.isActive(), false)

  throws(
    () => new ShareConsumer({ clientId: 'clientId', bootstrapBrokers: kafkaBootstrapServers } as ShareConsumerOptions),
    (error: Error) => error instanceof UserError && error.message === "/options must have required property 'groupId'."
  )
})

test('constructor should notify the creation of the client', t => {
  const created = createCreationChannelVerifier(instancesChannel, (data: any) => data.type === 'share-consumer')
  const consumer = createShareConsumer(t)

  deepStrictEqual(created(), { type: 'share-consumer', instance: consumer })
})

test('metrics should track the number of active share consumers', async t => {
  const registry = new Prometheus.Registry()
  const consumer1 = createShareConsumer(t, { metrics: { registry, client: Prometheus } })
  createShareConsumer(t, { metrics: { registry, client: Prometheus } })

  const active = async () => (await registry.getMetricsAsJSON()).find(m => m.name === 'kafka_share_consumers')!

  deepStrictEqual((await active()).values[0].value, 2)
  await consumer1.close()
  deepStrictEqual((await active()).values[0].value, 1)
})

test('joinGroup should join with member epoch 0 and the subscribed topics', async t => {
  const consumer = createShareConsumer(t, { clientRack: 'rack-1' })
  const { requests } = mockShareGroup(consumer, acceptAll(assignment('topic-id', [0, 1])))

  const joined = once(consumer, 'share-consumer:group:join')
  const memberId = await consumer.joinGroup({ topics: ['topic'] })

  strictEqual(memberId, consumer.memberId)
  deepStrictEqual(requests[0], {
    groupId: consumer.groupId,
    memberId: consumer.memberId,
    memberEpoch: 0,
    rackId: 'rack-1',
    topics: ['topic']
  })
  deepStrictEqual(await joined, [{ groupId: consumer.groupId, memberId: consumer.memberId, memberEpoch: 1 }])
  strictEqual(consumer.memberEpoch, 1)
  strictEqual(consumer.coordinatorId, 0)
  strictEqual(consumer.isActive(), true)
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0, 1] }])
})

test('joinGroup should keep heartbeating at the interval sent by the coordinator', async t => {
  const consumer = createShareConsumer(t)
  const { requests } = mockShareGroup(consumer, acceptAll(assignment('topic-id', [0])))

  await consumer.joinGroup({ topics: ['topic'] })
  await once(consumer, 'share-consumer:heartbeat:end')
  await once(consumer, 'share-consumer:heartbeat:end')

  deepStrictEqual(
    requests.map(request => request.memberEpoch),
    [0, 1, 2]
  )
  // The memberId never changes and the full subscription is sent every time
  ok(requests.every(request => request.memberId === consumer.memberId && request.topics![0] === 'topic'))
  // A null assignment means that the assignment did not change
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])
})

test('joinGroup should replace the assignment when the coordinator sends a new one', async t => {
  const consumer = createShareConsumer(t)
  mockShareGroup(consumer, (request, callback) => {
    const partitions = request.memberEpoch === 0 ? [0] : [0, 1]
    callback(
      null,
      heartbeatResponse({ memberEpoch: request.memberEpoch + 1, assignment: assignment('topic-id', partitions) })
    )
  })

  await consumer.joinGroup({ topics: ['topic'] })
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])

  await once(consumer, 'share-consumer:heartbeat:end')
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0, 1] }])
})

test('joinGroup should refresh the metadata when a topic id is unknown', async t => {
  const consumer = createShareConsumer(t)
  // Only a forced refresh sees that the topic was recreated with a new id
  const { metadataRequests } = mockShareGroup(consumer, acceptAll(assignment('topic-id', [0])), options => {
    return { topic: options.forceUpdate ? 'topic-id' : 'old-topic-id' }
  })

  await consumer.joinGroup({ topics: ['topic'] })

  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])
  deepStrictEqual(metadataRequests.at(-1), { topics: ['topic'], forceUpdate: true })
})

test('joinGroup should resolve the topics still unknown after the next heartbeats', async t => {
  const consumer = createShareConsumer(t)
  let known: Record<string, string> = { topic: 'topic-id' }
  mockShareGroup(
    consumer,
    acceptAll({
      topicPartitions: [
        { topicId: 'topic-id', partitions: [0] },
        { topicId: 'other-id', partitions: [1] }
      ]
    }),
    () => known
  )

  await consumer.joinGroup({ topics: ['topic', 'other'] })
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])

  known = { topic: 'topic-id', other: 'other-id' }
  await once(consumer, 'share-consumer:heartbeat:end')

  deepStrictEqual(consumer.assignments, [
    { topic: 'topic', partitions: [0] },
    { topic: 'other', partitions: [1] }
  ])
})

test('joinGroup should keep the membership when the assignment cannot be resolved yet', async t => {
  const consumer = createShareConsumer(t, { retries: 0 })
  let metadataCalls = 0
  mockShareGroup(consumer, acceptAll(assignment('topic-id', [0])), () => {
    return ++metadataCalls === 1 ? new Error('Metadata failed.') : { topic: 'topic-id' }
  })

  const failed = once(consumer, 'share-consumer:heartbeat:error')
  await consumer.joinGroup({ topics: ['topic'] })

  const [{ error }] = await failed
  strictEqual(error.message, 'Resolving the assignment failed.')
  strictEqual(error.errors[0].message, 'Metadata failed.')
  strictEqual(consumer.isActive(), true)
  strictEqual(consumer.assignments, null)

  // The next heartbeat gets a null assignment, but the pending one is resolved again
  await once(consumer, 'share-consumer:heartbeat:end')
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])
})

test('joinGroup should validate the options', async t => {
  const consumer = createShareConsumer(t)

  await rejects(consumer.joinGroup({} as any), {
    code: 'PLT_KFK_USER',
    message: "/options must have required property 'topics'."
  })
  await rejects(consumer.joinGroup({ topics: [] }), {
    code: 'PLT_KFK_USER',
    message: '/options/topics must NOT have fewer than 1 items.'
  })
})

test('joinGroup should fail when the group has already been joined', async t => {
  const consumer = createShareConsumer(t)
  mockShareGroup(consumer, acceptAll())

  await consumer.joinGroup({ topics: ['topic'] })
  await rejects(consumer.joinGroup({ topics: ['topic'] }), {
    code: 'PLT_KFK_USER',
    message: 'The consumer has already joined the group.'
  })
})

test('joinGroup should fail when the consumer is closed', async t => {
  const consumer = createShareConsumer(t)
  await consumer.close()

  await rejects(consumer.joinGroup({ topics: ['topic'] }), { message: 'Client is closed.' })
})

test('joinGroup should fail and stop heartbeating when the coordinator rejects the member', async t => {
  const consumer = createShareConsumer(t)
  let reject = true
  const { requests } = mockShareGroup(consumer, (request, callback) => {
    if (reject) {
      callback(heartbeatError(protocolErrors.GROUP_AUTHORIZATION_FAILED.code))
      return
    }

    acceptAll()(request, callback)
  })

  await rejects(consumer.joinGroup({ topics: ['topic'] }), (error: Error) => {
    ok(error.message.includes('ShareGroupHeartbeat'))
    return true
  })

  strictEqual(consumer.isActive(), false)
  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.assignments, null)

  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 3))
  strictEqual(requests.length, 1)

  // The group can be joined again
  reject = false
  await consumer.joinGroup({ topics: ['topic'] })
  strictEqual(consumer.isActive(), true)
})

test('joinGroup should fail when the broker does not support share groups', async t => {
  const consumer = createShareConsumer(t)
  mockShareGroup(consumer, acceptAll())
  mockUnavailableAPI(consumer, 'ShareGroupHeartbeat')

  await rejects(consumer.joinGroup({ topics: ['topic'] }), (error: Error) => {
    ok(error instanceof UnsupportedApiError || (error as any).errors?.[0] instanceof UnsupportedApiError)
    return true
  })
})

test('joinGroup should find the coordinator again when it moves', async t => {
  const consumer = createShareConsumer(t)
  let moved = true
  const group = mockShareGroup(consumer, (request, callback) => {
    if (moved) {
      moved = false
      callback(heartbeatError(protocolErrors.NOT_COORDINATOR.code))
      return
    }

    acceptAll()(request, callback)
  })

  await consumer.joinGroup({ topics: ['topic'] })

  strictEqual(group.findCoordinatorCalls(), 2)
  strictEqual(consumer.isActive(), true)
})

for (const [step, method] of [
  ['connecting to the bootstrap broker', kGetBootstrapConnection],
  ['loading the FindCoordinator API', kGetApi],
  ['connecting to the coordinator', kGetConnection]
] as const) {
  test(`joinGroup should fail when ${step} fails`, async t => {
    const consumer = createShareConsumer(t, { retries: 0 })
    const { requests } = mockShareGroup(consumer, acceptAll())
    mockMethod(consumer, method, 1, new Error(`Failed ${step}.`))

    await rejects(consumer.joinGroup({ topics: ['topic'] }), (error: Error) => {
      return error.message === `Failed ${step}.` || (error as any).errors?.[0]?.message === `Failed ${step}.`
    })

    strictEqual(requests.length, 0)
    strictEqual(consumer.isActive(), false)
  })
}

test('leaveGroup should fail when the ShareGroupHeartbeat API cannot be loaded', async t => {
  const consumer = createShareConsumer(t, { retries: 0 })
  mockShareGroup(consumer, acceptAll())

  await consumer.joinGroup({ topics: ['topic'] })
  mockUnavailableAPI(consumer, 'ShareGroupHeartbeat')

  await rejects(consumer.leaveGroup())
  strictEqual(consumer.isActive(), false)
})

test('joinGroup should fail when the group is left while joining', async t => {
  const consumer = createShareConsumer(t)
  let pendingJoin: (() => void) | null = null
  mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === 0) {
      pendingJoin = () => callback(null, heartbeatResponse({ memberEpoch: 1, assignment: assignment('topic-id', [0]) }))
      return
    }

    acceptAll()(request, callback)
  })

  const joining = consumer.joinGroup({ topics: ['topic'] })
  await consumer.leaveGroup()
  pendingJoin!()

  await rejects(joining, { code: 'PLT_KFK_USER', message: 'The group was left while joining it.' })
  strictEqual(consumer.isActive(), false)
  strictEqual(consumer.assignments, null)
})

for (const error of ['UNKNOWN_MEMBER_ID', 'FENCED_MEMBER_EPOCH'] as const) {
  test(`heartbeats should rejoin with the same member id after ${error}`, async t => {
    const consumer = createShareConsumer(t)
    let fenced = false
    const { requests } = mockShareGroup(consumer, (request, callback) => {
      if (request.memberEpoch === 1 && !fenced) {
        fenced = true
        callback(heartbeatError(protocolErrors[error].code))
        return
      }

      acceptAll(assignment('topic-id', [0]))(request, callback)
    })

    await consumer.joinGroup({ topics: ['topic'] })

    const [payload] = await once(consumer, 'share-consumer:group:rejoin')
    strictEqual(payload.memberId, consumer.memberId)
    strictEqual(payload.memberEpoch, 1)
    ok(payload.error instanceof ResponseError)
    strictEqual(consumer.memberEpoch, 0)
    strictEqual(consumer.assignments, null)
    strictEqual(consumer.isActive(), false)

    await once(consumer, 'share-consumer:heartbeat:end')

    deepStrictEqual(
      requests.map(request => request.memberEpoch),
      [0, 1, 0]
    )
    ok(requests.every(request => request.memberId === consumer.memberId))
    strictEqual(consumer.isActive(), true)
    deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])
  })
}

test('heartbeats should keep the membership and retry after other errors', async t => {
  const consumer = createShareConsumer(t, { retries: 0 })
  let failed = false
  const { requests } = mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === 1 && !failed) {
      failed = true
      callback(heartbeatError(protocolErrors.COORDINATOR_NOT_AVAILABLE.code))
      return
    }

    acceptAll()(request, callback)
  })

  await consumer.joinGroup({ topics: ['topic'] })

  const [{ error, memberEpoch }] = await once(consumer, 'share-consumer:heartbeat:error')
  ok(error instanceof ResponseError)
  strictEqual(memberEpoch, 1)
  strictEqual(consumer.coordinatorId, null)

  await once(consumer, 'share-consumer:heartbeat:end')
  deepStrictEqual(
    requests.map(request => request.memberEpoch),
    [0, 1, 1]
  )
})

test('heartbeats should be traced', async t => {
  const consumer = createShareConsumer(t)
  mockShareGroup(consumer, acceptAll())

  const verifyTracingChannel = createTracingChannelVerifier(
    shareConsumerHeartbeatChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, { client: consumer, operation: 'shareGroupHeartbeat', operationId: mockedOperationId })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        strictEqual(context.result, undefined)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label, data) => data.client === consumer
  )

  await consumer.joinGroup({ topics: ['topic'] })
  verifyTracingChannel()
})

test('joinGroup and leaveGroup should be traced', async t => {
  const consumer = createShareConsumer(t)
  mockShareGroup(consumer, acceptAll())

  for (const operation of ['joinGroup', 'leaveGroup'] as const) {
    const verifyTracingChannel = createTracingChannelVerifier(
      shareConsumerGroupChannel,
      'client',
      {
        start (context: ClientDiagnosticEvent) {
          deepStrictEqual(context, { client: consumer, operation, operationId: mockedOperationId })
        },
        error (context: ClientDiagnosticEvent) {
          ok(typeof context === 'undefined')
        }
      },
      (_label, data) => data.client === consumer && data.operation === operation
    )

    if (operation === 'joinGroup') {
      await consumer.joinGroup({ topics: ['topic'] })
    } else {
      await consumer.leaveGroup()
    }

    verifyTracingChannel()
  }
})

test('leaveGroup should send member epoch -1 and stop heartbeating', async t => {
  const consumer = createShareConsumer(t)
  const { requests } = mockShareGroup(consumer, acceptAll(assignment('topic-id', [0])))

  await consumer.joinGroup({ topics: ['topic'] })
  const left = once(consumer, 'share-consumer:group:leave')
  await consumer.leaveGroup()

  deepStrictEqual(requests.at(-1), {
    groupId: consumer.groupId,
    memberId: consumer.memberId,
    memberEpoch: -1,
    rackId: null,
    topics: null
  })
  deepStrictEqual(await left, [{ groupId: consumer.groupId, memberId: consumer.memberId, memberEpoch: 1 }])
  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.assignments, null)
  strictEqual(consumer.isActive(), false)

  const sent = requests.length
  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 3))
  strictEqual(requests.length, sent)

  // The group can be joined again with the same member id
  await consumer.joinGroup({ topics: ['topic'] })
  strictEqual(requests.at(-1)!.memberEpoch, 0)
  strictEqual(requests.at(-1)!.memberId, consumer.memberId)
})

test('leaveGroup should do nothing when the group has not been joined', async t => {
  const consumer = createShareConsumer(t)
  const { requests } = mockShareGroup(consumer, acceptAll())

  await consumer.leaveGroup()
  strictEqual(requests.length, 0)
})

test('leaveGroup should fail when the consumer is closed', async t => {
  const consumer = createShareConsumer(t)
  await consumer.close()

  await rejects(consumer.leaveGroup(), { message: 'Client is closed.' })
})

test('leaveGroup should succeed when the coordinator does not know the member anymore', async t => {
  const consumer = createShareConsumer(t)
  mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === -1) {
      callback(heartbeatError(protocolErrors.UNKNOWN_MEMBER_ID.code))
      return
    }

    acceptAll()(request, callback)
  })

  await consumer.joinGroup({ topics: ['topic'] })
  await consumer.leaveGroup()

  strictEqual(consumer.isActive(), false)
})

test('leaveGroup should end the membership even when leaving fails', async t => {
  const consumer = createShareConsumer(t, { retries: 0 })
  // The coordinator removed the member, but the response to the leave was lost
  let removed = false
  const { requests } = mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === -1) {
      removed = true
      callback(new NetworkError('Connection closed.'))
      return
    }

    if (removed) {
      callback(heartbeatError(protocolErrors.UNKNOWN_MEMBER_ID.code))
      return
    }

    acceptAll(assignment('topic-id', [0]))(request, callback)
  })

  await consumer.joinGroup({ topics: ['topic'] })
  let left = false
  consumer.once('share-consumer:group:leave', () => {
    left = true
  })
  await rejects(consumer.leaveGroup(), (error: Error) => error instanceof NetworkError)

  ok(left)
  strictEqual(consumer.isActive(), false)
  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.assignments, null)

  // No heartbeat is sent, so the consumer does not join the group again
  const sent = requests.length
  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 3))
  strictEqual(requests.length, sent)
})

test('heartbeats should not depend on the metadata of the subscribed topics', async t => {
  const consumer = createShareConsumer(t)
  // The topic deleted is not assigned anymore
  const { requests, metadataRequests } = mockShareGroup(consumer, acceptAll(assignment('topic-id', [0])), () => ({
    topic: 'topic-id'
  }))

  await consumer.joinGroup({ topics: ['topic', 'deleted'] })
  deepStrictEqual(consumer.assignments, [{ topic: 'topic', partitions: [0] }])
  const metadataCalls = metadataRequests.length

  await once(consumer, 'share-consumer:heartbeat:end')
  await once(consumer, 'share-consumer:heartbeat:end')

  deepStrictEqual(
    requests.map(request => request.memberEpoch),
    [0, 1, 2]
  )
  // Unchanged assignments are not resolved again
  strictEqual(metadataRequests.length, metadataCalls)
  strictEqual(consumer.isActive(), true)

  await consumer.leaveGroup()
  strictEqual(requests.at(-1)!.memberEpoch, -1)
})

test('heartbeat responses received after leaving the group should be ignored', async t => {
  const consumer = createShareConsumer(t)
  let pending: (() => void) | null = null
  const { requests } = mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === 1) {
      pending = () => callback(null, heartbeatResponse({ memberEpoch: 2, assignment: assignment('topic-id', [0]) }))
      return
    }

    acceptAll()(request, callback)
  })

  await consumer.joinGroup({ topics: ['topic'] })
  await waitFor(() => {
    ok(pending)
  })

  await consumer.leaveGroup()
  pending!()

  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.assignments, null)

  const sent = requests.length
  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 3))
  strictEqual(requests.length, sent)
})

function mockCoordinatorConnection (consumer: ShareConsumer): Connection {
  const connection = { isConnected: () => true } as unknown as Connection
  consumer[kConnections].getEstablishedConnection = () => connection
  return connection
}

test('close should leave the group through the connection to the coordinator', async t => {
  const consumer = createShareConsumer(t)
  const { requests, connections, metadataRequests } = mockShareGroup(consumer, acceptAll())
  const established = mockCoordinatorConnection(consumer)

  await consumer.joinGroup({ topics: ['topic'] })
  const metadataCalls = metadataRequests.length
  const left = once(consumer, 'share-consumer:group:leave')
  await consumer.close()

  strictEqual(requests.at(-1)!.memberEpoch, -1)
  strictEqual(connections.at(-1), established)
  strictEqual(metadataRequests.length, metadataCalls)
  strictEqual((await left)[0].memberId, consumer.memberId)
  strictEqual(consumer.isActive(), false)

  // Closing again does nothing
  await consumer.close()
  strictEqual(requests.at(-1)!.memberEpoch, -1)
})

test('close should not wait for the coordinator longer than leaveGroupTimeout', async t => {
  const consumer = createShareConsumer(t, { leaveGroupTimeout: 50 })
  mockShareGroup(consumer, (request, callback) => {
    // The leave request is never answered
    if (request.memberEpoch !== -1) {
      acceptAll()(request, callback)
    }
  })
  mockCoordinatorConnection(consumer)

  await consumer.joinGroup({ topics: ['topic'] })

  const start = Date.now()
  await consumer.close()
  ok(Date.now() - start >= 40)
  strictEqual(consumer.isActive(), false)
})

test('close should not leave the group when leaveGroupTimeout is 0', async t => {
  const consumer = createShareConsumer(t, { leaveGroupTimeout: 0 })
  const { requests } = mockShareGroup(consumer, acceptAll())
  mockCoordinatorConnection(consumer)

  await consumer.joinGroup({ topics: ['topic'] })
  await consumer.close()

  strictEqual(requests.at(-1)!.memberEpoch, 0)
})

test('close should not leave the group without an open connection to the coordinator', async t => {
  const consumer = createShareConsumer(t)
  const { requests } = mockShareGroup(consumer, acceptAll())

  await consumer.joinGroup({ topics: ['topic'] })
  await consumer.close()

  strictEqual(requests.at(-1)!.memberEpoch, 0)
  strictEqual(consumer.isActive(), false)
})

test('close should report errors from closing the connections', async t => {
  const consumer = createShareConsumer(t)
  mockMethod(consumer[kConnections], 'close')

  await rejects(consumer.close(), { message: 'Cannot connect to any broker.' })
  await consumer.close()
})

test('a share consumer should join a share group on the broker', async t => {
  const admin = createAdmin(t, { bootstrapBrokers: kafkaSingleBootstrapServers })
  if (!(await usableVersions(admin, 'ShareGroupHeartbeat')).length) {
    t.skip('the broker does not support share groups')
    return
  }

  const topic = await createTopic(t, true, 3, kafkaSingleBootstrapServers)
  const consumer = createShareConsumer(t, { bootstrapBrokers: kafkaSingleBootstrapServers })

  try {
    await consumer.joinGroup({ topics: [topic] })
  } catch (error) {
    // Some brokers advertise the API but ship with share groups disabled
    if (isUnsupportedVersion(error)) {
      t.skip('share groups are not enabled on the broker')
      return
    }

    throw error
  }

  ok(consumer.memberEpoch > 0)
  strictEqual(consumer.isActive(), true)

  // The coordinator might send the assignment with a later heartbeat
  await waitFor(
    () => {
      deepStrictEqual(consumer.assignments, [{ topic, partitions: [0, 1, 2] }])
    },
    { interval: 100, timeout: 30000 }
  )

  await consumer.leaveGroup()
  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.isActive(), false)
})

type HeartbeatReply = (request: HeartbeatRequest, callback: Callback<ShareGroupHeartbeatResponse>) => void

// Holds the requests with the given member epoch until release is called
function holdRequests (memberEpoch: number, reply: HeartbeatReply) {
  const held: (() => void)[] = []

  const handler: HeartbeatHandler = (request, callback) => {
    if (request.memberEpoch === memberEpoch) {
      held.push(() => reply(request, callback))
      return
    }

    acceptAll()(request, callback)
  }

  return {
    handler,
    held: () => held.length,
    release () {
      for (const fn of held.splice(0)) {
        fn()
      }
    }
  }
}

const leaveSucceeds: HeartbeatReply = (_request, callback) => {
  callback(null, heartbeatResponse({ memberEpoch: -1 }))
}

test('joinGroup should fail while leaving the group', async t => {
  const consumer = createShareConsumer(t)
  const leave = holdRequests(-1, leaveSucceeds)
  mockShareGroup(consumer, leave.handler)

  await consumer.joinGroup({ topics: ['topic'] })
  const leaving = consumer.leaveGroup()
  await waitFor(() => ok(leave.held()))

  await rejects(consumer.joinGroup({ topics: ['topic'] }), {
    code: 'PLT_KFK_USER',
    message: 'The consumer is leaving the group.'
  })

  leave.release()
  await leaving
  strictEqual(consumer.isActive(), false)

  // Once left, the group can be joined again
  await consumer.joinGroup({ topics: ['topic'] })
  strictEqual(consumer.isActive(), true)
})

test('concurrent leaveGroup calls should wait for the same request', async t => {
  const consumer = createShareConsumer(t)
  const leave = holdRequests(-1, leaveSucceeds)
  const { requests } = mockShareGroup(consumer, leave.handler)

  await consumer.joinGroup({ topics: ['topic'] })
  const leaving = [consumer.leaveGroup(), consumer.leaveGroup()]
  await waitFor(() => ok(leave.held()))

  let settled = false
  Promise.all(leaving).then(() => {
    settled = true
  })

  await new Promise(resolve => setImmediate(resolve))
  strictEqual(settled, false)

  leave.release()
  await Promise.all(leaving)
  strictEqual(requests.filter(request => request.memberEpoch === -1).length, 1)
})

test('close should leave the group again while a leave is inflight', async t => {
  const consumer = createShareConsumer(t, { retries: 0 })
  let firstLeave = true
  let releaseLeave: (() => void) | null = null
  const { requests } = mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === -1 && firstLeave) {
      firstLeave = false
      // The leave started by leaveGroup fails after the consumer is closed
      releaseLeave = () => callback(heartbeatError(protocolErrors.GROUP_AUTHORIZATION_FAILED.code))
      return
    }

    acceptAll()(request, callback)
  })
  mockCoordinatorConnection(consumer)

  await consumer.joinGroup({ topics: ['topic'] })
  const leaving = consumer.leaveGroup()
  await waitFor(() => ok(releaseLeave))

  let leaves = 0
  consumer.on('share-consumer:group:leave', () => leaves++)
  await consumer.close()

  strictEqual(requests.filter(request => request.memberEpoch === -1).length, 2)
  strictEqual(leaves, 1)

  // The failure of the first leave does not restore the membership of the closed consumer
  releaseLeave!()
  await leaving

  const sent = requests.length
  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 3))
  strictEqual(requests.length, sent)
  strictEqual(consumer.isActive(), false)
  strictEqual(leaves, 1)
})

test('close should fail a join which is inflight', async t => {
  const consumer = createShareConsumer(t)
  const join = holdRequests(0, (_request, callback) => {
    callback(null, heartbeatResponse({ memberEpoch: 1, assignment: assignment('topic-id', [0]) }))
  })
  const { requests } = mockShareGroup(consumer, join.handler)
  mockCoordinatorConnection(consumer)

  const joining = consumer.joinGroup({ topics: ['topic'] })
  await waitFor(() => ok(join.held()))
  await consumer.close()

  // The broker might have accepted the join, so it is told that the member leaves
  strictEqual(requests.at(-1)!.memberEpoch, -1)

  join.release()
  await rejects(joining, { code: 'PLT_KFK_USER', message: 'The group was left while joining it.' })
  strictEqual(consumer.assignments, null)

  const sent = requests.length
  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 3))
  strictEqual(requests.length, sent)
})

test('heartbeats waiting for a retry should not be sent after leaving the group', async t => {
  const consumer = createShareConsumer(t, { retries: 1, retryDelay: heartbeatInterval * 2 })
  let failed = false
  const { requests } = mockShareGroup(consumer, (request, callback) => {
    if (request.memberEpoch === 1 && !failed) {
      failed = true
      callback(heartbeatError(protocolErrors.COORDINATOR_NOT_AVAILABLE.code))
      return
    }

    acceptAll()(request, callback)
  })

  await consumer.joinGroup({ topics: ['topic'] })
  await once(consumer, 'client:performWithRetry:retry')
  await consumer.leaveGroup()

  const sent = requests.length
  await new Promise(resolve => setTimeout(resolve, heartbeatInterval * 4))

  strictEqual(requests.length, sent)
  strictEqual(requests.at(-1)!.memberEpoch, -1)
  strictEqual(consumer.memberEpoch, 0)
  strictEqual(consumer.assignments, null)
})

test('close should run the cleanup once when the leave is answered after leaveGroupTimeout', async t => {
  const consumer = createShareConsumer(t, { leaveGroupTimeout: 20 })
  const leave = holdRequests(-1, leaveSucceeds)
  mockShareGroup(consumer, leave.handler)
  mockCoordinatorConnection(consumer)

  await consumer.joinGroup({ topics: ['topic'] })

  let leaves = 0
  consumer.on('share-consumer:group:leave', () => leaves++)
  await consumer.close()

  strictEqual(leaves, 1)
  leave.release()
  strictEqual(leaves, 1)
})

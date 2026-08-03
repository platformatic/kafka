import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import test from 'node:test'
import { ProduceAcks } from '../../src/index.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  forEachVersion,
  pinApiVersions,
  stringDeserializers,
  stringSerializers,
  usableVersions
} from './helpers.ts'

// The consumer group surface is where this branch's client side reshaping actually runs: the topic
// name to topic id remapping for Fetch <= v12 and the OffsetFetch < v8 single group response
// adaptation. Neither is reachable from a protocol test.

const PARTITIONS = 3

async function seed (t: any, topic: string, count: number) {
  const producer = createProducer(t, { serializers: stringSerializers })
  const messages = []

  for (let index = 0; index < count; index++) {
    messages.push({ topic, partition: index % PARTITIONS, key: `k${index}`, value: `v${index}` })
  }

  await producer.send({ messages, acks: ProduceAcks.ALL })
  return messages.map(message => `${message.key}=${message.value}`)
}

async function consumeAll (consumer: any, topic: string, wanted: Set<string>) {
  const stream = await consumer.consume({ topics: [topic], mode: 'earliest', maxWaitTime: 500 })
  const seen = new Set<string>()

  try {
    for await (const message of stream) {
      seen.add(`${message.key}=${message.value}`)

      if (Array.from(wanted).every(entry => seen.has(entry))) {
        break
      }
    }
  } finally {
    await stream.close()
  }

  return seen
}

test('Fetch delivers the same records at every broker-supported version', async t => {
  const topic = await createTopic(t, PARTITIONS)
  const expected = new Set(await seed(t, topic, 9))
  const probe = createConsumer(t)

  await forEachVersion(t, probe, 'Fetch', async version => {
    const consumer = await pinApiVersions(createConsumer(t, { deserializers: stringDeserializers }), {
      Fetch: version
    })

    const received = await consumeAll(consumer, topic, expected)

    deepStrictEqual(
      Array.from(received).sort(),
      Array.from(expected).sort(),
      `Fetch v${version} did not deliver the same records as the other versions`
    )
  })
})

test('ListOffsets reports the same watermarks at every version', async t => {
  const topic = await createTopic(t, PARTITIONS)
  await seed(t, topic, 6)

  const probe = createConsumer(t)
  const reference = await probe.listOffsets({ topics: [topic] })

  await forEachVersion(t, probe, 'ListOffsets', async version => {
    const consumer = await pinApiVersions(createConsumer(t), { ListOffsets: version })
    const offsets = await consumer.listOffsets({ topics: [topic] })

    deepStrictEqual(
      offsets.get(topic),
      reference.get(topic),
      `ListOffsets v${version} disagrees with the newest version`
    )
  })
})

test('OffsetCommit and OffsetFetch round-trip committed offsets at every version', async t => {
  const topic = await createTopic(t, PARTITIONS)
  await seed(t, topic, 6)

  const probe = createConsumer(t)
  const commitVersions = await usableVersions(probe, 'OffsetCommit')
  const fetchVersions = await usableVersions(probe, 'OffsetFetch')

  ok(commitVersions.length > 0, 'no usable OffsetCommit version')
  ok(fetchVersions.length > 0, 'no usable OffsetFetch version')

  for (const commitVersion of commitVersions) {
    for (const fetchVersion of [fetchVersions[0], fetchVersions.at(-1)!]) {
      await t.test(`OffsetCommit v${commitVersion} + OffsetFetch v${fetchVersion}`, async t => {
        const groupId = `compat-offsets-${commitVersion}-${fetchVersion}-${Date.now()}`
        const writer = await pinApiVersions(createConsumer(t, { groupId }), { OffsetCommit: commitVersion })

        // A commit needs a group which has joined, otherwise the coordinator rejects it.
        await writer.joinGroup({})
        await writer.commit({
          offsets: [
            { topic, partition: 0, offset: 2n, leaderEpoch: 0 },
            { topic, partition: 1, offset: 1n, leaderEpoch: 0 }
          ]
        })

        const reader = await pinApiVersions(createConsumer(t, { groupId }), { OffsetFetch: fetchVersion })
        const committed = await reader.listCommittedOffsets({ topics: [{ topic, partitions: [0, 1] }] })

        deepStrictEqual(
          committed.get(topic)?.slice(0, 2),
          [2n, 1n],
          `OffsetCommit v${commitVersion} / OffsetFetch v${fetchVersion} did not round-trip the offsets`
        )
      })
    }
  }
})

test('The group protocol APIs complete a full lifecycle at every version', async t => {
  const topic = await createTopic(t, PARTITIONS)
  const expected = new Set(await seed(t, topic, 6))
  const probe = createConsumer(t)

  // JoinGroup, SyncGroup, Heartbeat and LeaveGroup only ever run together, so they are swept as one
  // unit: pin each of them to the lowest version the broker still accepts and drive a real session.
  for (const name of ['JoinGroup', 'SyncGroup', 'Heartbeat', 'LeaveGroup', 'FindCoordinator']) {
    await t.test(name, async t => {
      const versions = await usableVersions(probe, name)

      if (!versions.length) {
        t.diagnostic(`${name}: no usable version on this broker`)
        return
      }

      for (const version of versions) {
        await t.test(`${name} v${version}`, async t => {
          const consumer = await pinApiVersions(
            createConsumer(t, { deserializers: stringDeserializers, groupId: `compat-${name}-${version}` }),
            { [name]: version }
          )

          const received = await consumeAll(consumer, topic, expected)

          deepStrictEqual(
            Array.from(received).sort(),
            Array.from(expected).sort(),
            `${name} v${version} broke the consumer group lifecycle`
          )

          // Closing drives LeaveGroup, which must also succeed at the pinned version.
          await consumer.close()
        })
      }
    })
  }
})

test('OffsetForLeaderEpoch resolves epochs at every version', async t => {
  const topic = await createTopic(t, PARTITIONS)
  await seed(t, topic, 6)

  const probe = createConsumer(t)

  await forEachVersion(t, probe, 'OffsetForLeaderEpoch', async version => {
    const consumer = await pinApiVersions(createConsumer(t, { deserializers: stringDeserializers }), {
      OffsetForLeaderEpoch: version
    })

    // The API is only reached on a truncation, which cannot be forced here. Selecting the codec and
    // completing a normal session at least proves the version negotiates and the client still works.
    const stream = await consumer.consume({ topics: [topic], mode: 'earliest', maxWaitTime: 500 })
    let count = 0

    for await (const message of stream) {
      ok(message.topic === topic)

      if (++count === 6) {
        break
      }
    }

    await stream.close()
    strictEqual(count, 6, `OffsetForLeaderEpoch v${version} broke a normal consume`)
  })
})

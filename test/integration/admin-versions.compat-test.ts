import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import test from 'node:test'
import { ConfigResourceTypes, IncrementalAlterConfigOperationTypes, ProduceAcks } from '../../src/index.ts'
import {
  createAdmin,
  createConsumer,
  createProducer,
  createTopic,
  forEachVersion,
  pinApiVersions,
  stringSerializers,
  usableVersions,
  waitFor
} from './helpers.ts'

// Metadata v0-v12 is the widest sweep in the branch, and the one whose client side backfill
// (topic.topicId = topic.name for v0-v8, in base.ts) has no protocol level coverage at all.

// Config changes are applied asynchronously by the broker, so a single read after the write races
// the propagation. This is a broker behaviour, not a codec one, and it affects every version.
async function expectConfig (admin: any, topic: string, name: string, value: string, label: string) {
  await waitFor(
    async () => {
      const described = await admin.describeConfigs({
        resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: topic }]
      })
      const actual = described[0].configs.find((config: any) => config.name === name)?.value

      if (actual !== value) {
        throw new Error(`${label} did not apply the change: ${name} is ${actual}, expected ${value}`)
      }

      return true
    },
    { interval: 200, timeout: 15000 }
  )
}

// Topic and partition creation are applied through the metadata log, so a read straight after the
// write races the propagation. This is broker behaviour and affects every version equally.
async function expectPartitions (admin: any, topic: string, count: number, label: string) {
  await waitFor(
    async () => {
      const metadata = await admin.metadata({ topics: [topic], forceUpdate: true })
      const actual = metadata.topics.get(topic)?.partitionsCount

      if (actual !== count) {
        throw new Error(`${label}: ${topic} has ${actual} partitions, expected ${count}`)
      }

      return true
    },
    { interval: 200, timeout: 30000 }
  )
}

function normalizeTopic (topic: any, version: number) {
  const normalized: Record<string, unknown> = {
    partitionsCount: topic.partitionsCount,
    partitions: topic.partitions.map((partition: any) => ({
      partition: partition.partition,
      leader: partition.leader,
      replicas: partition.replicas.slice().sort(),
      isr: partition.isr?.slice().sort()
    }))
  }

  // topic_authorized_operations only exists from Metadata v8, and offline_replicas from v5.
  if (version >= 5) {
    normalized.offlineReplicas = topic.partitions.map((partition: any) => partition.offlineReplicas?.slice().sort())
  }

  return normalized
}

test('Metadata describes topics identically at every version', async t => {
  const topic = await createTopic(t, 3)
  const probe = createAdmin(t)
  const reference = await probe.metadata({ topics: [topic], forceUpdate: true })

  await forEachVersion(t, probe, 'Metadata', async version => {
    const admin = await pinApiVersions(createAdmin(t), { Metadata: version })
    const metadata = await admin.metadata({ topics: [topic], forceUpdate: true })
    const described = metadata.topics.get(topic)

    ok(described, `Metadata v${version} did not describe ${topic}`)
    deepStrictEqual(
      normalizeTopic(described, version),
      normalizeTopic(reference.topics.get(topic), version),
      `Metadata v${version} disagrees with the newest version`
    )

    // Below v10 the wire carries no topic UUID, so base.ts backfills the name as a stable surrogate.
    ok(described!.id, `Metadata v${version} left the topic id empty`)

    ok(metadata.brokers.size > 0, `Metadata v${version} returned no brokers`)
    for (const broker of metadata.brokers.values()) {
      ok(broker.host.length > 0, `Metadata v${version} returned a broker without a host`)
      ok(broker.port > 0, `Metadata v${version} returned a broker without a port`)
    }
  })
})

test('CreateTopics and DeleteTopics manage a topic lifecycle at every version', async t => {
  const probe = createAdmin(t)

  await forEachVersion(t, probe, 'CreateTopics', async version => {
    const admin = await pinApiVersions(createAdmin(t), { CreateTopics: version })
    const topic = `compat-create-v${version}-${Date.now()}`

    const created = await admin.createTopics({ topics: [topic], partitions: 2, replicas: 1 })
    strictEqual(created.length, 1, `CreateTopics v${version} did not report the created topic`)
    strictEqual(created[0].name, topic, `CreateTopics v${version} reported the wrong name`)

    // numPartitions and replicationFactor are only echoed back from CreateTopics v5.
    if (version >= 5) {
      strictEqual(created[0].partitions, 2, `CreateTopics v${version} reported the wrong partition count`)
      strictEqual(created[0].replicas, 1, `CreateTopics v${version} reported the wrong replication factor`)
    }

    await expectPartitions(admin, topic, 2, `CreateTopics v${version}`)
    await admin.deleteTopics({ topics: [topic] })
  })

  await forEachVersion(t, probe, 'DeleteTopics', async version => {
    const admin = await pinApiVersions(createAdmin(t), { DeleteTopics: version })
    const topic = `compat-delete-v${version}-${Date.now()}`

    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 })
    await admin.deleteTopics({ topics: [topic] })

    await waitFor(
      async () => {
        try {
          await admin.metadata({ topics: [topic], forceUpdate: true, autocreateTopics: false })
        } catch {
          return true
        }

        throw new Error(`DeleteTopics v${version} left ${topic} behind`)
      },
      { interval: 200, timeout: 30000 }
    )
  })
})

test('CreatePartitions grows a topic at every version', async t => {
  const probe = createAdmin(t)

  await forEachVersion(t, probe, 'CreatePartitions', async version => {
    const admin = await pinApiVersions(createAdmin(t), { CreatePartitions: version })
    const topic = await createTopic(t, 1)

    await admin.createPartitions({ topics: [{ name: topic, count: 3, assignments: null }] })
    await expectPartitions(admin, topic, 3, `CreatePartitions v${version}`)
  })
})

test('DescribeConfigs reports the same configuration at every version', async t => {
  const topic = await createTopic(t, 1)
  const probe = createAdmin(t)

  const resources = [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: topic }]
  const reference = await probe.describeConfigs({ resources })
  const referenceValue = reference[0].configs.find(config => config.name === 'cleanup.policy')?.value

  await forEachVersion(t, probe, 'DescribeConfigs', async version => {
    const admin = await pinApiVersions(createAdmin(t), { DescribeConfigs: version })
    const described = await admin.describeConfigs({ resources })

    strictEqual(described.length, 1, `DescribeConfigs v${version} returned the wrong resource count`)
    strictEqual(
      described[0].configs.find(config => config.name === 'cleanup.policy')?.value,
      referenceValue,
      `DescribeConfigs v${version} disagrees with the newest version`
    )
  })
})

test('AlterConfigs and IncrementalAlterConfigs change a topic config at every version', async t => {
  const probe = createAdmin(t)

  await forEachVersion(t, probe, 'AlterConfigs', async version => {
    const admin = await pinApiVersions(createAdmin(t), { AlterConfigs: version })
    const topic = await createTopic(t, 1)

    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: topic,
          configs: [{ name: 'retention.ms', value: '123456789' }]
        }
      ]
    })

    await expectConfig(admin, topic, 'retention.ms', '123456789', `AlterConfigs v${version}`)
  })

  await forEachVersion(t, probe, 'IncrementalAlterConfigs', async version => {
    const admin = await pinApiVersions(createAdmin(t), { IncrementalAlterConfigs: version })
    const topic = await createTopic(t, 1)

    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: topic,
          configs: [
            { name: 'retention.ms', value: '987654321', configOperation: IncrementalAlterConfigOperationTypes.SET }
          ]
        }
      ]
    })

    await expectConfig(admin, topic, 'retention.ms', '987654321', `IncrementalAlterConfigs v${version}`)
  })
})

test('DeleteRecords truncates a partition at every version', async t => {
  const probe = createAdmin(t)

  await forEachVersion(t, probe, 'DeleteRecords', async version => {
    const admin = await pinApiVersions(createAdmin(t), { DeleteRecords: version })
    const topic = await createTopic(t, 1)
    const producer = createProducer(t, { serializers: stringSerializers })

    await producer.send({
      messages: Array.from({ length: 5 }, (_, index) => ({ topic, partition: 0, key: `k${index}`, value: `v${index}` })),
      acks: ProduceAcks.ALL
    })

    const deleted = await admin.deleteRecords({ topics: [{ name: topic, partitions: [{ partition: 0, offset: 3n }] }] })

    strictEqual(deleted.length, 1, `DeleteRecords v${version} returned the wrong topic count`)
    strictEqual(
      deleted[0].partitions[0].lowWatermark,
      3n,
      `DeleteRecords v${version} reported the wrong low watermark`
    )
  })
})

test('DescribeLogDirs reports log directories at every version', async t => {
  const topic = await createTopic(t, 1)
  const probe = createAdmin(t)

  await forEachVersion(t, probe, 'DescribeLogDirs', async version => {
    const admin = await pinApiVersions(createAdmin(t), { DescribeLogDirs: version })
    const described = await admin.describeLogDirs({ topics: [{ name: topic, partitions: [0] }] })

    ok(described.length > 0, `DescribeLogDirs v${version} returned no brokers`)
    ok(
      described.some(broker => broker.results.some(result => result.topics.some(entry => entry.name === topic))),
      `DescribeLogDirs v${version} did not report ${topic}`
    )
  })
})

test('ListGroups and DescribeGroups agree across versions', async t => {
  const topic = await createTopic(t, 1)
  const producer = createProducer(t, { serializers: stringSerializers })
  await producer.send({ messages: [{ topic, partition: 0, key: 'k', value: 'v' }], acks: ProduceAcks.ALL })

  const probe = createAdmin(t)
  const groupId = `compat-groups-${Date.now()}`
  const consumer = createConsumer(t, { groupId })
  const stream = await consumer.consume({ topics: [topic], mode: 'earliest' })
  t.after(() => stream.close())

  await forEachVersion(t, probe, 'ListGroups', async version => {
    const admin = await pinApiVersions(createAdmin(t), { ListGroups: version })
    const groups = await admin.listGroups({})

    ok(groups.has(groupId), `ListGroups v${version} did not return the active group`)

    // group_state only exists on the wire from ListGroups v4; below that the client reports the
    // 'Unknown' state Kafka defines for the case rather than an empty string.
    strictEqual(
      groups.get(groupId)!.state,
      version >= 4 ? 'Stable' : 'Unknown',
      `ListGroups v${version} reported the wrong state`
    )
  })

  await forEachVersion(t, probe, 'DescribeGroups', async version => {
    const admin = await pinApiVersions(createAdmin(t), { DescribeGroups: version })
    const described = await admin.describeGroups({ groups: [groupId] })

    const group = described.get(groupId)
    ok(group, `DescribeGroups v${version} did not describe the group`)
    strictEqual(group!.state, 'Stable', `DescribeGroups v${version} reported the wrong state`)
    strictEqual(group!.members.size, 1, `DescribeGroups v${version} reported the wrong member count`)
  })
})

test('DeleteGroups removes an empty group at every version', async t => {
  const probe = createAdmin(t)
  const versions = await usableVersions(probe, 'DeleteGroups')

  for (const version of versions) {
    await t.test(`DeleteGroups v${version}`, async t => {
      const topic = await createTopic(t, 1)
      const groupId = `compat-delete-group-v${version}-${Date.now()}`

      const consumer = createConsumer(t, { groupId })
      const stream = await consumer.consume({ topics: [topic], mode: 'earliest' })
      await stream.close()
      await consumer.close()

      const admin = await pinApiVersions(createAdmin(t), { DeleteGroups: version })
      await admin.deleteGroups({ groups: [groupId] })

      const groups = await admin.listGroups({})
      ok(!groups.has(groupId), `DeleteGroups v${version} did not remove the group`)
    })
  }
})

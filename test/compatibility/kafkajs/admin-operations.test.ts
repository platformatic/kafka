import { deepStrictEqual, equal, ok, rejects } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { test } from 'node:test'
import {
  AclOperationTypes,
  AclPermissionTypes,
  AclResourceTypes,
  ConfigResourceTypes,
  Kafka,
  ResourcePatternTypes
} from '../../../src/compatibility/kafkajs/index.ts'
import { kafkaBootstrapServers } from '../../helpers.ts'

test('KafkaJS admin topic, group, config, ACL, offset and reassignment operations', async t => {
  const suffix = randomUUID()
  const topic = `kafkajs-admin-${suffix}`
  const secondTopic = `kafkajs-admin-second-${suffix}`
  const validatedTopic = `kafkajs-admin-validated-${suffix}`
  const mixedTopic = `kafkajs-admin-mixed-${suffix}`
  const groupId = `kafkajs-admin-group-${suffix}`
  const principal = `User:kafkajs-admin-${suffix}`
  const kafka = new Kafka({ brokers: kafkaBootstrapServers, clientId: `kafkajs-admin-${suffix}` })
  const admin = kafka.admin()
  const producer = kafka.producer()
  const lifecycle: string[] = []
  const operations: string[] = []

  admin.on(admin.events.CONNECT, event => lifecycle.push(event.type))
  admin.on(admin.events.DISCONNECT, event => lifecycle.push(event.type))
  admin.on(admin.events.REQUEST, event => operations.push((event.payload as { apiName: string }).apiName))
  t.after(async () => {
    await admin.connect()
    const existingTopics = new Set(await admin.listTopics())
    const topicsToDelete = [topic, secondTopic, mixedTopic].filter(topic => existingTopics.has(topic))
    if (topicsToDelete.length > 0) {
      await admin.deleteTopics({ topics: topicsToDelete })
    }
    await Promise.allSettled([admin.disconnect(), producer.disconnect()])
  })

  await admin.connect()
  await producer.connect()
  equal(
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic, numPartitions: 1, replicationFactor: 1, configEntries: [{ name: 'cleanup.policy', value: 'delete' }] },
        { topic: secondTopic, numPartitions: 2, replicationFactor: 1 }
      ]
    }),
    true
  )
  const allMetadata = await admin.fetchTopicMetadata({ topics: [topic, secondTopic] })
  ok(allMetadata.topics.some(metadataTopic => metadataTopic.name === topic))
  ok(allMetadata.topics.some(metadataTopic => metadataTopic.name === secondTopic))
  equal(await admin.createTopics({ topics: [{ topic }], waitForLeaders: false }), false)
  await rejects(
    admin.createTopics({
      topics: [{ topic }, { topic: mixedTopic, numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: false
    }),
    { name: 'KafkaJSAggregateError' }
  )
  equal(
    await admin.createTopics({
      topics: [{ topic: validatedTopic, numPartitions: 1, replicationFactor: 1 }],
      validateOnly: true,
      waitForLeaders: false
    }),
    true
  )
  ok(!(await admin.listTopics()).includes(validatedTopic))

  await admin.createPartitions({ topicPartitions: [{ topic, count: 2 }] })
  let metadata = await admin.fetchTopicMetadata({ topics: [topic] })
  for (let attempt = 0; metadata.topics[0].partitions.length < 2 && attempt < 20; attempt++) {
    await sleep(100)
    metadata = await admin.fetchTopicMetadata({ topics: [topic] })
  }
  deepStrictEqual(
    metadata.topics[0].partitions.map(partition => partition.partitionId),
    [0, 1]
  )

  const configs = await admin.describeConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configNames: ['cleanup.policy'] }],
    includeSynonyms: true
  })
  equal(configs.resources[0].errorCode, 0)
  equal(configs.resources[0].configEntries[0].configName, 'cleanup.policy')
  const altered = await admin.alterConfigs({
    validateOnly: true,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configEntries: [{ name: 'retention.ms', value: '60000' }]
      }
    ]
  })
  equal(altered.resources[0].errorCode, 0)

  await producer.send({
    topic,
    messages: [
      { value: 'one', partition: 0 },
      { value: 'two', partition: 1 }
    ]
  })
  await admin.setOffsets({
    groupId,
    topic,
    partitions: [
      { partition: 0, offset: '0' },
      { partition: 1, offset: '0' }
    ]
  })
  const groupOffsets = await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: true })
  deepStrictEqual(
    groupOffsets[0].partitions.map(partition => partition.partition),
    [0, 1]
  )
  equal((await admin.describeGroups([groupId])).groups[0].state, 'Empty')

  await admin.createAcls({
    acl: [
      {
        resourceType: AclResourceTypes.TOPIC,
        resourceName: topic,
        resourcePatternType: ResourcePatternTypes.LITERAL,
        principal,
        host: '*',
        operation: AclOperationTypes.READ,
        permissionType: AclPermissionTypes.ALLOW
      }
    ]
  })
  const aclFilter = {
    resourceType: AclResourceTypes.TOPIC,
    resourceName: topic,
    resourcePatternType: ResourcePatternTypes.LITERAL,
    principal,
    host: '*',
    operation: AclOperationTypes.READ,
    permissionType: AclPermissionTypes.ALLOW
  } as const
  const describedAcls = await admin.describeAcls(aclFilter)
  ok(describedAcls.resources.some(resource => resource.resourceName === topic))
  const deletedAcls = await admin.deleteAcls({ filters: [aclFilter] })
  equal(deletedAcls.filterResponses.length, 1)

  const topicOffsets = await admin.fetchTopicOffsets(topic)
  const futureOffsets = await admin.fetchTopicOffsetsByTimestamp(topic, Date.now() + 60_000)
  deepStrictEqual(
    futureOffsets.map(partition => partition.offset),
    topicOffsets.map(partition => partition.high)
  )
  await admin.deleteTopicRecords({
    topic,
    partitions: topicOffsets.map(partition => ({ partition: partition.partition, offset: partition.high }))
  })

  const brokers = (await admin.describeCluster()).brokers
  await admin.alterPartitionReassignments({
    topics: [
      {
        topic,
        partitionAssignment: metadata.topics[0].partitions.map(partition => ({
          partition: partition.partitionId,
          replicas: partition.replicas.length > 0 ? partition.replicas : [brokers[0].nodeId]
        }))
      }
    ]
  })
  const reassignments = await admin.listPartitionReassignments({
    topics: [{ topic, partitions: [0, 1] }]
  })
  ok(Array.isArray(reassignments.topics))
  ok(Array.isArray((await admin.listPartitionReassignments()).topics))
  const deletedGroups = await admin.deleteGroups([groupId])
  deepStrictEqual(deletedGroups, [{ groupId, errorCode: 0 }])

  await admin.disconnect()
  await admin.connect()
  ok((await admin.listTopics()).includes(topic))
  await admin.disconnect()
  deepStrictEqual(lifecycle, ['admin.connect', 'admin.disconnect', 'admin.connect', 'admin.disconnect'])
  ok(operations.includes('CreateTopics'))
  ok(operations.includes('AlterPartitionReassignments'))
})

import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test, type TestContext } from 'node:test'
import { promisify } from 'node:util'
import { pathToFileURL } from 'node:url'
import { Kafka, PartitionAssigners, type PartitionAssigner } from 'kafkajs'
import semver from 'semver'
import { Admin, Consumer } from '../../src/index.ts'
import { retry } from '../helpers.ts'

const exec = promisify(execFile)
const bootstrapBrokers = [`localhost:${process.env.KAFKA_SINGLE_PORT ?? 9001}`]
const packageName = '@platformatic/kafka'

// Resolve at run time so this exercises the releases users actually upgrade from, not a stale pinned fixture.
async function previousVersions (): Promise<string[]> {
  const { version: current } = JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'))
  const { stdout } = await exec('npm', ['view', packageName, 'versions', '--json'])
  const published = (JSON.parse(stdout) as string[]).filter(version => semver.valid(version) && !semver.prerelease(version))
  const parsed = semver.parse(current)!
  const previousMinor = published.filter(version => {
    const candidate = semver.parse(version)!
    return semver.lt(version, current) && (candidate.major !== parsed.major || candidate.minor !== parsed.minor)
  }).sort(semver.rcompare)[0]
  const previousPatch = published.filter(version => {
    const candidate = semver.parse(version)!
    return candidate.major === parsed.major && candidate.minor === parsed.minor && semver.lt(version, current)
  }).sort(semver.rcompare)[0]

  ok(previousMinor, `No published previous minor release for ${current}`)
  return [...new Set([previousMinor, previousPatch].filter((version): version is string => !!version))]
}

async function installVersion (t: TestContext, version: string): Promise<typeof Consumer> {
  const directory = await mkdtemp(join(tmpdir(), 'kafka-upgrade-'))
  t.after(() => rm(directory, { recursive: true, force: true }))
  await exec('npm', [
    'install', '--prefix', directory, '--no-save', '--no-package-lock', '--ignore-scripts',
    '--no-audit', '--no-fund', `${packageName}@${version}`
  ], { timeout: 120_000 })
  const entry = join(directory, 'node_modules', '@platformatic', 'kafka', 'dist', 'index.js')
  const legacy = await import(pathToFileURL(entry).href)
  return legacy.Consumer as typeof Consumer
}

async function waitForMembers (admin: Admin, groupId: string, topic: string, count: number): Promise<void> {
  await retry(60, 500, async () => {
    const group = (await admin.describeGroups({ groups: [groupId] })).get(groupId)
    strictEqual(group?.state, 'Stable')
    strictEqual(group.members.size, count)
    const partitions = [...group.members.values()].flatMap(member => member.assignments?.get(topic)?.partitions ?? [])
    deepStrictEqual(partitions.sort((a, b) => a - b), [0, 1])
  })
}

test('real broker accepts KafkaJS v1 metadata during a mixed group rebalance (issue #420)', async t => {
  for (const first of ['current', 'kafkajs']) {
    await t.test(`${first} joins first`, async t => {
      const groupId = `upgrade-kafkajs-${randomUUID()}`
      const topic = `upgrade-kafkajs-${randomUUID()}`
      const admin = new Admin({ clientId: `admin-${randomUUID()}`, bootstrapBrokers })
      const current = new Consumer({
        clientId: `current-${randomUUID()}`, bootstrapBrokers, groupId, heartbeatInterval: 1000
      })
      const kafka = new Kafka({ clientId: `kafkajs-${randomUUID()}`, brokers: bootstrapBrokers, logLevel: 0 })
      // KafkaJS writes v1 metadata with only v0 fields; use a shared protocol name for the eager assigner.
      const assigner: PartitionAssigner = options => ({
        ...PartitionAssigners.roundRobin(options), name: 'roundrobin', version: 1
      })
      const kafkaJs = kafka.consumer({ groupId, partitionAssigners: [assigner], heartbeatInterval: 1000 })
      t.after(async () => {
        await kafkaJs.disconnect()
        await current.close(true)
        await admin.deleteTopics({ topics: [topic] }).catch(() => {})
        await admin.close()
      })

      await admin.createTopics({ topics: [topic], partitions: 2, replicas: 1 })
      await current.topics.trackAll(topic)
      const joinKafkaJs = async () => {
        await kafkaJs.connect()
        await kafkaJs.subscribe({ topics: [topic] })
        await kafkaJs.run({ eachMessage: async () => {} })
      }

      if (first === 'current') {
        await current.joinGroup()
        await joinKafkaJs()
      } else {
        await joinKafkaJs()
        await waitForMembers(admin, groupId, topic, 1)
        await current.joinGroup()
      }

      await waitForMembers(admin, groupId, topic, 2)
      const group = (await admin.describeGroups({ groups: [groupId] })).get(groupId)!
      const member = [...group.members.values()].find(member => member.clientId.startsWith('kafkajs-'))
      ok(member, 'KafkaJS member must be visible through Admin.describeGroups')
      strictEqual(member.metadata?.version, 1)
      deepStrictEqual(member.metadata?.ownedPartitions, [])

      await current.close(true)
      await waitForMembers(admin, groupId, topic, 1) // Admin.describeGroups on a KafkaJS-only group.
    })
  }
})

test('real broker supports mixed membership across dynamically selected previous releases', async t => {
  for (const version of await previousVersions()) {
    await t.test(`upgrade from ${version}`, async t => {
      const LegacyConsumer = await installVersion(t, version)
      const groupId = `upgrade-${randomUUID()}`
      const topic = `upgrade-${randomUUID()}`
      const admin = new Admin({ clientId: `admin-${randomUUID()}`, bootstrapBrokers })
      const previous = new LegacyConsumer({
        clientId: `previous-${randomUUID()}`, bootstrapBrokers, groupId, heartbeatInterval: 1000
      })
      const current = new Consumer({
        clientId: `current-${randomUUID()}`, bootstrapBrokers, groupId, heartbeatInterval: 1000
      })
      t.after(async () => {
        await current.close(true)
        await previous.close(true)
        await admin.deleteTopics({ topics: [topic] }).catch(() => {})
        await admin.close()
      })

      await admin.createTopics({ topics: [topic], partitions: 2, replicas: 1 })
      await previous.topics.trackAll(topic)
      await current.topics.trackAll(topic)
      await previous.joinGroup()
      await current.joinGroup()
      await waitForMembers(admin, groupId, topic, 2)

      await previous.close(true)
      await waitForMembers(admin, groupId, topic, 1) // The new client takes over after the old version leaves.
    })
  }
})

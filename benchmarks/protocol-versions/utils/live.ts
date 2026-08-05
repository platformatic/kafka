import { randomUUID } from 'node:crypto'
import { PerformanceObserver } from 'node:perf_hooks'
import { Admin } from '../../../src/clients/admin/index.ts'
import { type Base, kApis, kGetApi } from '../../../src/clients/base/base.ts'
import { Consumer, Producer, type ConsumerOptions, type ProducerOptions } from '../../../src/index.ts'
import { brokerApis } from '../../../test/helpers/api-versions.ts'

export const bootstrapBrokers = (process.env.PROTOCOL_BENCH_BROKERS ?? 'localhost:9001').split(',')

// The single broker, not the three broker cluster the other benchmarks use: replication adds
// variance that has nothing to do with which codec is being measured.
const closers: (() => Promise<void>)[] = []

export function createProducer (options: Partial<ProducerOptions<Buffer, Buffer, Buffer, Buffer>> = {}): Producer {
  const producer = new Producer({
    clientId: `protocol-bench-${randomUUID()}`,
    bootstrapBrokers,
    autocreateTopics: false,
    ...options
  })

  closers.push(() => producer.close())

  return producer
}

export function createConsumer (options: Partial<ConsumerOptions<Buffer, Buffer, Buffer, Buffer>> = {}): Consumer {
  const consumer = new Consumer({
    clientId: `protocol-bench-${randomUUID()}`,
    bootstrapBrokers,
    autocreateTopics: false,
    groupId: `protocol-bench-${randomUUID()}`,
    ...options
  })

  closers.push(() => consumer.close())

  return consumer
}

export function createAdmin (): Admin {
  const admin = new Admin({ clientId: `protocol-bench-admin-${randomUUID()}`, bootstrapBrokers })

  closers.push(() => admin.close())

  return admin
}

export async function closeAll (): Promise<void> {
  const pending = closers.splice(0, closers.length)

  for (const close of pending) {
    try {
      await close()
    } catch {
      // A client already closed by the benchmark is not an error worth failing the run for.
    }
  }
}

export async function createTopic (admin: Admin, partitions = 1): Promise<string> {
  const topic = `protocol-bench-${randomUUID()}`

  // Explicit partitions and replicas: sending -1 is KIP-464, which only brokers from Apache Kafka
  // 2.4 accept, and this harness is also pointed at the 1.1.0 stack in tier 2.
  await admin.createTopics({ topics: [topic], partitions, replicas: 1 })

  return topic
}

/**
 * The version the client will actually negotiate for an API.
 *
 * The pin assertion guard (see README.md). pinApiVersions throws when the broker rejects a pin, but it cannot
 * notice a client that negotiated before the pin was seeded. A run that silently used the newest
 * codec while reporting an old version's label is the one failure mode here that produces a
 * confident wrong answer, so every cell asserts the pin took effect.
 */
export function negotiatedVersion (client: Base<never>, name: string): Promise<number> {
  return new Promise((resolve, reject) => {
    client[kGetApi](name, (error, api) => {
      if (error) {
        reject(error)
        return
      }

      resolve(api!.version)
    })
  })
}

export async function assertNegotiated (client: Base<never>, name: string, expected: number): Promise<void> {
  const actual = await negotiatedVersion(client, name)

  if (actual !== expected) {
    throw new Error(`${name}: pinned v${expected} but the client negotiated v${actual}`)
  }
}

/** Versions of an API which this package implements and the broker under test accepts. */
export async function usableVersionsFor (client: Base<never>, name: string): Promise<number[]> {
  const { usableVersions } = await import('../../../test/helpers/api-versions.ts')

  return usableVersions(client, name)
}

export async function brokerRange (client: Base<never>, name: string): Promise<string> {
  const api = (await brokerApis(client)).get(name)

  return api ? `v${api.minVersion}-v${api.maxVersion}` : 'unsupported'
}

export interface LiveSample {
  /** Wall clock milliseconds of the timed region. */
  durationMs: number
  messages: number
  messagesPerSecond: number
  /** The primary metric: client CPU microseconds per message. */
  cpuUsPerMessage: number
  gcMs: number
  peakRssMb: number
}

/**
 * Measures an async region.
 *
 * CPU per message rather than throughput is the headline: with acks=ALL the broker round trip
 * dominates wall clock, so every version looks identical there even when one is doing measurably
 * more work per byte.
 */
export async function measure (
  messages: number,
  body: (control: { restart: () => void }) => Promise<void>
): Promise<LiveSample> {
  let gcMs = 0
  const observer = new PerformanceObserver(list => {
    for (const entry of list.getEntries()) {
      gcMs += entry.duration
    }
  })

  observer.observe({ entryTypes: ['gc'] })

  let peakRss = process.memoryUsage.rss()
  const sampler = setInterval(() => {
    peakRss = Math.max(peakRss, process.memoryUsage.rss())
  }, 50)

  let startCpu = process.cpuUsage()
  let start = process.hrtime.bigint()

  // The consumer uses this to drop group join out of the timed region: joining is unrelated to the
  // Fetch codec and a rebalance lands entirely in whichever version happened to trigger it.
  await body({
    restart: () => {
      gcMs = 0
      startCpu = process.cpuUsage()
      start = process.hrtime.bigint()
    }
  })

  const durationMs = Number(process.hrtime.bigint() - start) / 1e6
  const cpu = process.cpuUsage(startCpu)

  clearInterval(sampler)
  observer.disconnect()

  return {
    durationMs,
    messages,
    messagesPerSecond: messages / (durationMs / 1000),
    cpuUsPerMessage: (cpu.user + cpu.system) / messages,
    gcMs,
    peakRssMb: peakRss / 1024 / 1024
  }
}

/** Seeds client[kApis] so a later pin is not overwritten by the first real request. */
export async function primeApis (client: Base<never>): Promise<void> {
  if (!client[kApis].length) {
    await brokerApis(client)
    await negotiatedVersion(client, 'Metadata')
  }
}

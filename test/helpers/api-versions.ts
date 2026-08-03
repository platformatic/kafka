import { type TestContext } from 'node:test'
import * as apis from '../../src/apis/index.ts'
import { type ApiVersionsResponseApi } from '../../src/apis/metadata/api-versions-v3.ts'
import { type Base, kApis, kBootstrapBrokers, kListApis } from '../../src/clients/base/base.ts'

export type ApiPins = Record<string, number>

/**
 * True when the broker under test predates the features a sweep needs.
 *
 * Set by the legacy compose stack's CI job. It is an explicit opt out rather than a catch of
 * connection failures, so a genuine regression on a modern broker still fails.
 */
export function legacyBroker (): boolean {
  return process.env.COMPAT_LEGACY_BROKER === '1'
}

/**
 * The versions of an API this package actually implements, in ascending order.
 *
 * Mirrors the name mangling in Base[kGetApi]: the codec for Produce v3 is exported as produceV3.
 */
export function implementedVersions (name: string): number[] {
  const prefix = name.slice(0, 1).toLowerCase() + name.slice(1) + 'V'
  const versions: number[] = []

  for (let version = 0; version <= 32; version++) {
    if (apis[(prefix + version) as keyof typeof apis]) {
      versions.push(version)
    }
  }

  return versions
}

/** What the broker advertises, keyed by API name. Cached per bootstrap list, not per client. */
const brokerApisCache = new Map<string, Promise<ApiVersionsResponseApi[]>>()

function listApis (client: Base<any>): Promise<ApiVersionsResponseApi[]> {
  const key = (client[kBootstrapBrokers] as unknown[]).join(',')
  let cached = brokerApisCache.get(key)

  if (!cached) {
    cached = new Promise<ApiVersionsResponseApi[]>((resolve, reject) => {
      client[kListApis]((error, list) => {
        /* c8 ignore next 4 - Only reachable when the broker is unavailable */
        if (error) {
          reject(error)
          return
        }

        resolve(list!)
      })
    })

    brokerApisCache.set(key, cached)
  }

  return cached
}

export async function brokerApis (client: Base<any>): Promise<Map<string, ApiVersionsResponseApi>> {
  return new Map((await listApis(client)).map(api => [api.name, api]))
}

/**
 * The versions of an API which are both implemented here and accepted by the broker under test.
 *
 * The floors moved with Kafka 4.0 (KIP-896), so this differs between the ends of the CI matrix:
 * Fetch starts at v0 on Confluent 7.5.0 and at v4 on 8.2.0, for example.
 */
export async function usableVersions (client: Base<any>, name: string): Promise<number[]> {
  const advertised = (await brokerApis(client)).get(name)

  if (!advertised) {
    return []
  }

  return implementedVersions(name).filter(
    version => version >= advertised.minVersion && version <= advertised.maxVersion
  )
}

/**
 * Force a client to negotiate the given API versions instead of the highest it supports.
 *
 * The point is to exercise the legacy codecs against a real broker: Base[kGetApi] always walks down
 * from maxVersion, so against every broker in the CI matrix the newest codec always wins and the
 * legacy ones are never serialized, sent or parsed.
 *
 * Throws when a pin is outside what the broker accepts rather than letting the client fall back to
 * a version the test did not ask for, which would silently stop testing what the test claims to.
 */
export async function pinApiVersions<ClientType extends Base<any>> (
  client: ClientType,
  pins: ApiPins
): Promise<ClientType> {
  const advertised = await brokerApis(client)

  for (const [name, version] of Object.entries(pins)) {
    const api = advertised.get(name)

    if (!api) {
      throw new Error(`Cannot pin ${name} v${version}: the broker does not support ${name}.`)
    }

    if (version < api.minVersion || version > api.maxVersion) {
      throw new Error(
        `Cannot pin ${name} v${version}: the broker only accepts v${api.minVersion}-v${api.maxVersion}.`
      )
    }

    if (!implementedVersions(name).includes(version)) {
      throw new Error(`Cannot pin ${name} v${version}: this package does not implement it.`)
    }
  }

  // Base populates the list lazily on first use, so seed it before rewriting it, otherwise the
  // first operation would overwrite the pins with what the broker advertises.
  client[kApis] = (await listApis(client)).map(api =>
    pins[api.name] !== undefined ? { ...api, minVersion: pins[api.name], maxVersion: pins[api.name] } : api
  )

  return client
}

/**
 * True when an error, or anything it aggregates, is the broker rejecting the API version.
 *
 * Brokers do not always honour the range they advertise. Confluent 7.5.0 reports a minimum of v0
 * for OffsetCommit and OffsetFetch but refuses v0 requests, because those versions stored offsets
 * in ZooKeeper and KRaft has no such storage. That is a property of the broker, not a defect in the
 * codec, so it is reported and skipped rather than failed.
 */
export function isUnsupportedVersion (error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false
  }

  if ((error as { apiId?: string }).apiId === 'UNSUPPORTED_VERSION') {
    return true
  }

  const aggregated = (error as { errors?: unknown[] }).errors

  return Array.isArray(aggregated) && aggregated.some(isUnsupportedVersion)
}

/**
 * Runs body, turning a broker level version rejection into a reported skip.
 */
export async function runAtVersion (t: TestContext, label: string, body: () => Promise<void>): Promise<void> {
  try {
    await body()
  } catch (error) {
    if (!isUnsupportedVersion(error)) {
      throw error
    }

    t.diagnostic(`${label}: the broker advertises this version but rejects it, skipping`)
  }
}

/**
 * Runs body once per usable version of an API, as a subtest, and reports the versions which are not
 * reachable on this broker instead of quietly not testing them.
 */
export async function forEachVersion (
  t: TestContext,
  client: Base<any>,
  name: string,
  body: (version: number, t: TestContext) => Promise<void>
): Promise<void> {
  const implemented = implementedVersions(name)
  const usable = await usableVersions(client, name)
  const skipped = implemented.filter(version => !usable.includes(version))

  if (skipped.length) {
    t.diagnostic(`${name}: skipping v${skipped.join(', v')} — not accepted by this broker`)
  }

  if (!usable.length) {
    t.diagnostic(`${name}: no usable version on this broker, nothing exercised`)
    return
  }

  for (const version of usable) {
    await t.test(`${name} v${version}`, async t => runAtVersion(t, `${name} v${version}`, () => body(version, t)))
  }
}

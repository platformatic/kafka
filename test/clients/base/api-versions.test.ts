import { deepStrictEqual, rejects, strictEqual, throws } from 'node:assert'
import { test } from 'node:test'
import type { MetadataResponse } from '../../../src/apis/metadata/metadata-v12.ts'
import { kApis, kGetApi } from '../../../src/clients/base/base.ts'
import { Base, FetchId, MetadataId, type API, type ApiVersionContext, type Callback } from '../../../src/index.ts'
import { createBase, mockConnectionPoolGetFirstAvailable } from '../../helpers.ts'

const fetchVersions = { apiKey: FetchId, name: 'Fetch', minVersion: 4, maxVersion: 15 }

function getFetch (client: Base): Promise<API<unknown[], unknown>> {
  return new Promise((resolve, reject) => {
    client[kGetApi]('Fetch', (error, api) => {
      if (error) {
        reject(error)
        return
      }

      resolve(api!)
    })
  })
}

test('pins APIs using their numeric IDs, including version zero', async t => {
  const client = createBase(t, { strict: true, apiVersions: { [FetchId]: 11, [MetadataId]: 0 } })
  client[kApis] = [
    fetchVersions,
    { apiKey: MetadataId, name: 'Metadata', minVersion: 0, maxVersion: 12 }
  ]

  strictEqual((await getFetch(client)).version, 11)
  client[kGetApi]('Metadata', (error, api) => {
    strictEqual(error, null)
    strictEqual(api!.version, 0)
  })
})

test('passes the local and broker versions and reevaluates the selector with fresh metadata', async t => {
  const contexts: ApiVersionContext[] = []
  const client = createBase(t, {
    strict: true,
    apiVersions: {
      [FetchId]: context => {
        contexts.push(context)
        return context.metadata?.id === 'fallback' ? 11 : undefined
      }
    }
  })
  client[kApis] = [
    fetchVersions,
    { apiKey: MetadataId, name: 'Metadata', minVersion: 0, maxVersion: 12 }
  ]
  let clusterId = 'fallback'
  const connection = {
    send (
      key: number,
      _version: number,
      _payload: unknown,
      _parse: unknown,
      _requestTags: boolean,
      _responseTags: boolean,
      callback: Callback<MetadataResponse>
    ) {
      strictEqual(key, MetadataId)
      callback(null, { throttleTimeMs: 0, brokers: [], clusterId, controllerId: 0, topics: [] })
    }
  }
  mockConnectionPoolGetFirstAvailable(client.connections, () => true, null, null, (_original, _brokers, callback) => {
    callback(null, connection)
    return true
  })

  strictEqual((await getFetch(client)).version, 15)
  await client.metadata({ forceUpdate: true })
  strictEqual((await getFetch(client)).version, 11)
  clusterId = 'recovered'
  client.clearMetadata()
  await client.metadata({ forceUpdate: true })
  strictEqual((await getFetch(client)).version, 15)
  deepStrictEqual(contexts[0], {
    name: 'Fetch',
    id: FetchId,
    candidateVersion: 15,
    client: { minVersion: 4, maxVersion: 17 },
    broker: { minVersion: 4, maxVersion: 15 },
    metadata: undefined
  })
  strictEqual(contexts[1].metadata?.id, 'fallback')
  strictEqual(contexts[2].metadata?.id, 'recovered')
})

test('rejects pinned or selected versions not supported by both sides', async t => {
  const cases = [
    { minVersion: 0, maxVersion: 19, version: 3 },
    { minVersion: 0, maxVersion: 19, version: 18 },
    { minVersion: 12, maxVersion: 15, version: 11 },
    { minVersion: 12, maxVersion: 15, version: 16 },
    { minVersion: 4, maxVersion: 15, version: 11.5 },
    { minVersion: 4, maxVersion: 15, version: NaN }
  ]
  for (const { minVersion, maxVersion, version } of cases) {
    for (const override of [version, () => version]) {
      const client = createBase(t, { apiVersions: { [FetchId]: override } })
      client[kApis] = [{ ...fetchVersions, minVersion, maxVersion }]
      await rejects(getFetch(client), {
        code: 'PLT_KFK_UNSUPPORTED_API',
        candidateVersion: version,
        broker: { minVersion, maxVersion }
      })
    }
  }
})

test('reports synchronous selector exceptions', async t => {
  const cause = new Error('selector failed')
  const client = createBase(t, { apiVersions: { [FetchId]: () => { throw cause } } })
  client[kApis] = [fetchVersions]
  await rejects(getFetch(client), { code: 'PLT_KFK_USER', cause })
})

test('strict validation rejects names and invalid fixed versions', () => {
  const options = { clientId: 'test', bootstrapBrokers: ['localhost:9092'], strict: true }
  const invalidOverrides: Record<string, number>[] = [{ Fetch: 11 }, { [FetchId]: -1 }, { [FetchId]: 11.5 }]
  for (const apiVersions of invalidOverrides) {
    throws(() => new Base({ ...options, apiVersions }), { code: 'PLT_KFK_USER' })
  }
})

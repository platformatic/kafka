import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import * as codecs from '../../src/apis/index.ts'
import * as kafka from '../../src/index.ts'

test('exports a named API ID for every entry in the protocol maps', () => {
  const ids = [...new Set(Object.values(kafka.protocolAPIsByName))].sort((a, b) => a - b)
  deepStrictEqual(ids, Object.keys(kafka.protocolAPIsById).map(Number))

  for (const [name, id] of Object.entries(kafka.protocolAPIsByName)) {
    const exportName = `${name}Id` as keyof typeof kafka
    strictEqual(kafka[exportName], id)
    strictEqual(kafka.protocolAPIsByName[kafka.protocolAPIsById[id]], id)
  }
})

test('lists every protocol API with its client support and version bounds', () => {
  const versionsById = new Map<number, number[]>()

  for (const codec of Object.values(codecs)) {
    if (typeof codec !== 'object' || codec === null || !('api' in codec) || typeof codec.api !== 'function') {
      continue
    }

    const { key, version } = codec.api
    const versions = versionsById.get(key) ?? []
    versions.push(version)
    versionsById.set(key, versions)
  }

  deepStrictEqual(Object.keys(kafka.supportedApis), Object.keys(kafka.protocolAPIsById))

  for (const [key, name] of Object.entries(kafka.protocolAPIsById)) {
    const id = Number(key)
    const versions = versionsById.get(id)
    deepStrictEqual(kafka.supportedApis[id], {
      id,
      name,
      supported: versions !== undefined,
      minVersion: versions ? Math.min(...versions) : -1,
      maxVersion: versions ? Math.max(...versions) : -1
    })
  }
})

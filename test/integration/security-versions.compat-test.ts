import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import test from 'node:test'
import {
  AclOperations,
  AclPermissionTypes,
  ClientQuotaEntityTypes,
  ClientQuotaKeys,
  ClientQuotaMatchTypes,
  ResourcePatternTypes,
  ResourceTypes,
  SASLMechanisms
} from '../../src/index.ts'
import { kafkaSaslBootstrapServers } from '../helpers.ts'
import {
  createAdmin,
  createTopic,
  forEachVersion,
  kafkaBootstrapServers,
  pinApiVersions,
  waitFor
} from './helpers.ts'

// ACLs need the authorizer, which only the cluster brokers enable, and SASL needs its own broker.

function createClusterAdmin (t: any, options = {}) {
  return createAdmin(t, { bootstrapBrokers: kafkaBootstrapServers, ...options })
}

test('CreateAcls, DescribeAcls and DeleteAcls round-trip an ACL at every version', async t => {
  const probe = createClusterAdmin(t)

  for (const name of ['CreateAcls', 'DescribeAcls', 'DeleteAcls']) {
    await forEachVersion(t, probe, name, async version => {
      const admin = await pinApiVersions(createClusterAdmin(t), { [name]: version })
      const acl = {
        resourceType: ResourceTypes.TOPIC,
        resourceName: `compat-acl-${name}-v${version}-${randomUUID()}`,
        resourcePatternType: ResourcePatternTypes.LITERAL,
        principal: 'User:compat-user',
        host: '*',
        operation: AclOperations.READ,
        permissionType: AclPermissionTypes.ALLOW
      }

      await admin.createAcls({ creations: [acl] })

      const filter = {
        resourceType: acl.resourceType,
        resourceName: acl.resourceName,
        resourcePatternType: acl.resourcePatternType,
        principal: acl.principal,
        host: acl.host,
        operation: acl.operation,
        permissionType: acl.permissionType
      }

      // The authorizer applies ACLs asynchronously, so poll rather than read once.
      await waitFor(
        async () => {
          const described = await admin.describeAcls({ filter })

          if (described.length !== 1) {
            throw new Error(`${name} v${version}: the ACL is not visible yet`)
          }

          return true
        },
        { interval: 200, timeout: 15000 }
      )

      const described = await admin.describeAcls({ filter })
      deepStrictEqual(
        described,
        [
          {
            resourceType: acl.resourceType,
            resourceName: acl.resourceName,
            resourcePatternType: acl.resourcePatternType,
            acls: [
              {
                principal: acl.principal,
                host: acl.host,
                operation: acl.operation,
                permissionType: acl.permissionType
              }
            ]
          }
        ],
        `${name} v${version} described the ACL differently`
      )

      const deleted = await admin.deleteAcls({ filters: [acl] })
      strictEqual(deleted.length, 1, `${name} v${version} did not delete the ACL`)
    })
  }
})

test('DescribeClientQuotas and AlterClientQuotas round-trip a quota at every version', async t => {
  const probe = createClusterAdmin(t)

  for (const name of ['AlterClientQuotas', 'DescribeClientQuotas']) {
    await forEachVersion(t, probe, name, async version => {
      const admin = await pinApiVersions(createClusterAdmin(t), { [name]: version })
      const clientId = `compat-quota-${name}-v${version}-${randomUUID()}`
      const entity = [{ entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: clientId }]

      await admin.alterClientQuotas({
        entries: [{ entities: entity, ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1024, remove: false }] }]
      })

      await waitFor(
        async () => {
          const described = await admin.describeClientQuotas({
            components: [
              {
                entityType: ClientQuotaEntityTypes.CLIENT_ID,
                matchType: ClientQuotaMatchTypes.EXACT,
                match: clientId
              }
            ],
            strict: false
          })

          const quota = described.find(entry =>
            entry.entity.some(component => component.entityName === clientId)
          )

          if (quota?.values.find(value => value.key === ClientQuotaKeys.PRODUCER_BYTE_RATE)?.value !== 1024) {
            throw new Error(`${name} v${version}: the quota is not visible yet`)
          }

          return true
        },
        { interval: 200, timeout: 15000 }
      )

      // Remove it again so repeated runs do not accumulate quotas on the cluster.
      await admin.alterClientQuotas({
        entries: [{ entities: entity, ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, remove: true }] }]
      })
    })
  }
})

test('SaslHandshake and SaslAuthenticate authenticate at every version', async t => {
  const probe = createAdmin(t, {
    bootstrapBrokers: kafkaSaslBootstrapServers,
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'admin', password: 'admin' }
  })

  for (const name of ['SaslHandshake', 'SaslAuthenticate']) {
    await forEachVersion(t, probe, name, async version => {
      // Each client authenticates on connect, so pinning the codec and performing any operation is
      // enough to drive the whole handshake at that version.
      const admin = await pinApiVersions(
        createAdmin(t, {
          bootstrapBrokers: kafkaSaslBootstrapServers,
          sasl: { mechanism: SASLMechanisms.PLAIN, username: 'admin', password: 'admin' }
        }),
        { [name]: version }
      )

      const metadata = await admin.metadata({ topics: [], forceUpdate: true })
      ok(metadata.brokers.size > 0, `${name} v${version} did not complete authentication`)
    })
  }
})

test('ApiVersions negotiates at every version', async t => {
  const probe = createAdmin(t)

  await forEachVersion(t, probe, 'ApiVersions', async version => {
    const admin = await pinApiVersions(createAdmin(t), { ApiVersions: version })
    const topic = await createTopic(t, 1)

    // Pinning ApiVersions itself only takes effect on the next connection, so force a fresh one and
    // check the client can still describe a topic through it.
    const metadata = await admin.metadata({ topics: [topic], forceUpdate: true })
    ok(metadata.topics.has(topic), `ApiVersions v${version} broke the client`)
  })
})

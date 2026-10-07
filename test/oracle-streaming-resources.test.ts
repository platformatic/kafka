import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { test } from 'node:test'
import {
  OracleStreamingResources,
  resourceNames,
  type Command,
  type Configuration
} from '../scripts/oracle-streaming-resources.ts'
import { recover, renewingCommand } from '../scripts/oracle-streaming-ci.ts'
import { UserError } from '../src/errors.ts'

const config = { compartment: 'ocid1.compartment.test', region: 'eu-frankfurt-1' }
const run = 'local-test-1'

function fixture (configuration: Configuration = config, runId = run) {
  const names = resourceNames(configuration, runId)
  const tags = configuration.owner
    ? {
        purpose: 'kafka-oracle-streaming-regression',
        repository: configuration.owner.repository,
        run: runId,
        attempt: configuration.owner.attempt
      }
    : { purpose: 'kafka-oracle-streaming-local', repository: 'platformatic/kafka', run: runId }
  const pool = {
    id: 'ocid1.streampool.test',
    name: names.pool,
    'compartment-id': configuration.compartment,
    'lifecycle-state': 'ACTIVE',
    'freeform-tags': { ...tags },
    'kafka-settings': { 'bootstrap-servers': 'streaming.eu-frankfurt-1.oci.oraclecloud.com:9092' }
  }
  const stream = {
    id: 'ocid1.stream.test',
    name: names.topic,
    'compartment-id': configuration.compartment,
    'stream-pool-id': pool.id,
    'lifecycle-state': 'ACTIVE',
    'freeform-tags': { ...tags }
  }
  const state = {
    pools: [] as (typeof pool)[],
    streams: [] as (typeof stream)[],
    fail: '',
    loseCreateResponse: false,
    stuck: false,
    streamStuck: false
  }
  const calls: string[][] = []
  const command: Command = async args => {
    calls.push(args)
    const operation = args.slice(2, 4).join(' ')
    if (operation === state.fail) {
      throw new UserError('Simulated OCI failure.')
    }
    switch (operation) {
      case 'stream-pool list':
        ok(args.includes('--all'))
        return JSON.stringify({ data: state.pools })
      case 'stream list':
        ok(args.includes('--all'))
        ok(args.includes('--compartment-id'))
        ok(!args.includes('--stream-pool-id'))
        return JSON.stringify({ data: state.streams })
      case 'stream-pool create':
        state.pools.push(pool)
        if (state.loseCreateResponse) {
          throw new UserError('Simulated timeout after pool creation.')
        }
        return JSON.stringify({ data: pool })
      case 'stream create':
        state.streams.push(stream)
        return JSON.stringify({ data: stream })
      case 'stream-pool get':
        return JSON.stringify({ data: pool })
      case 'stream get':
        return JSON.stringify({ data: stream })
      case 'stream-pool delete':
        if (
          state.streams.some(
            item =>
              item['stream-pool-id'] === args[args.indexOf('--stream-pool-id') + 1] &&
              item['lifecycle-state'] !== 'DELETED'
          )
        ) {
          throw new UserError('Stream pool is not empty.')
        }
        if (!state.stuck) {
          for (const item of state.pools) {
            if (item.id === args[args.indexOf('--stream-pool-id') + 1]) {
              item['lifecycle-state'] = 'DELETED'
            }
          }
        }
        return ''
      case 'stream delete':
        for (const item of state.streams) {
          if (item.id === args[args.indexOf('--stream-id') + 1]) {
            item['lifecycle-state'] = state.streamStuck ? 'DELETING' : 'DELETED'
          }
        }
        return ''
      default:
        throw new UserError(`Unexpected mock operation: ${operation}.`)
    }
  }
  const resources = new OracleStreamingResources(configuration, command, async () => {}, 2)
  return { resources, names, pool, stream, state, calls, command }
}

test('CI ownership isolates repositories and attempts without changing local names', async () => {
  const ci = { ...config, owner: { repository: 'platformatic/kafka', attempt: '2' } }
  const id = '12345'
  const { resources, names, state, pool, calls } = fixture(ci, id)
  ok(names.pool.includes('-ci-'))
  ok(names.pool.endsWith('-12345-2'))
  ok(names.pool !== resourceNames({ ...ci, owner: { ...ci.owner, attempt: '3' } }, id).pool)
  ok(names.pool !== resourceNames({ ...ci, owner: { ...ci.owner, repository: 'other/kafka' } }, id).pool)
  await resources.provision(id)
  const create = calls.find(args => args[3] === 'create')!
  deepStrictEqual(JSON.parse(create[create.indexOf('--freeform-tags') + 1]), {
    purpose: 'kafka-oracle-streaming-regression',
    repository: 'platformatic/kafka',
    run: id,
    attempt: '2'
  })
  // A pool with the expected name but another attempt is never adopted or deleted.
  pool['freeform-tags'].attempt = '3'
  await rejects(resources.cleanup(id), { code: 'PLT_KFK_USER' })
  ok(!calls.some(args => args[3] === 'delete'))
  pool['freeform-tags'].attempt = '2'
  await resources.cleanup(id)
  ok(state.pools.every(item => item['lifecycle-state'] === 'DELETED'))
  throws(() => resourceNames(ci, 'local-id'), { code: 'PLT_KFK_USER' })
  throws(() => resourceNames({ ...ci, owner: { ...ci.owner, attempt: '../bad' } }, id), { code: 'PLT_KFK_USER' })
})

test('WIF sessions renew during polling and cleanup and renewal failures never invoke OCI', async () => {
  let clock = 0
  let logins = 0
  const used: string[] = []
  let fail = false
  const command = renewingCommand(
    async () => {
      logins++
      if (fail) {
        throw new UserError('Session renewal failed.')
      }
      return {
        environment: {
          OCI_CLI_CONFIG_FILE: `profile-${logins}`,
          OCI_CLI_AUTH: 'security_token',
          OCI_CLI_PROFILE: 'DEFAULT'
        },
        expiresAt: clock + 60
      }
    },
    async (args, environment) => {
      used.push(`${args[0]}:${environment.OCI_CLI_CONFIG_FILE}`)
      return ''
    },
    () => clock
  )
  await command(['list'])
  await command(['get'])
  strictEqual(logins, 1)
  clock = 60
  await command(['delete'])
  deepStrictEqual(used, ['list:profile-1', 'get:profile-1', 'delete:profile-2'])
  clock = 120
  fail = true
  await rejects(command(['delete']), { code: 'PLT_KFK_USER' })
  strictEqual(used.length, 3)
  fail = false
  await command(['delete'])
  strictEqual(used.at(-1), 'delete:profile-4')
})

test('recovery deletes completed CI runs but preserves active runs and local resources', async () => {
  const ci = { ...config, owner: { repository: 'platformatic/kafka', attempt: '1' } }
  const { resources, state, command, calls } = fixture(ci, '123')
  await resources.provision('123')
  const local = fixture()
  local.pool.id = 'ocid1.streampool.local'
  local.stream.id = 'ocid1.stream.local'
  local.stream['stream-pool-id'] = local.pool.id
  state.pools.push(local.pool)
  state.streams.push(local.stream)
  const status = { status: 'in_progress', path: '.github/workflows/regression.yml', head_branch: 'main', event: 'push' }
  let requests = 0
  const github = async (id: string) => {
    strictEqual(id, '123')
    requests++
    return status
  }
  await recover(ci, command, github, async () => {}, 2)
  ok(!calls.some(args => args[3] === 'delete'))
  status.status = 'completed'
  await recover(ci, command, github, async () => {}, 2)
  strictEqual(requests, 2)
  strictEqual(local.pool['lifecycle-state'], 'ACTIVE')
  strictEqual(local.stream['lifecycle-state'], 'ACTIVE')
})

test('recovery refuses unknown workflow identities, API errors and malformed ownership', async () => {
  const ci = { ...config, owner: { repository: 'platformatic/kafka', attempt: '1' } }
  for (const change of [
    { path: 'another.yml' },
    { head_branch: 'untrusted' },
    { status: 'unknown' },
    { head_branch: 'oracle-part-2', event: 'push' },
    { head_branch: 'oracle-part-2', event: 'workflow_dispatch' }
  ]) {
    const { resources, command, calls } = fixture(ci, '123')
    await resources.provision('123')
    await rejects(
      recover(
        ci,
        command,
        async () => ({
          status: 'completed',
          path: '.github/workflows/regression.yml',
          head_branch: 'main',
          event: 'push',
          ...change
        }),
        async () => {},
        2
      ),
      { code: 'PLT_KFK_USER' }
    )
    ok(!calls.some(args => args[3] === 'delete'))
  }
  const { resources, pool, command, calls } = fixture(ci, '123')
  await resources.provision('123')
  await rejects(
    recover(
      ci,
      command,
      async () => {
        throw new UserError('GitHub unavailable.')
      },
      async () => {},
      2
    ),
    { code: 'PLT_KFK_USER' }
  )
  pool.name = 'unexpected-pool'
  await rejects(
    recover(
      ci,
      command,
      async () => ({
        status: 'completed',
        path: '.github/workflows/regression.yml',
        head_branch: 'main',
        event: 'push'
      }),
      async () => {},
      2
    ),
    { code: 'PLT_KFK_USER' }
  )
  pool['freeform-tags'].attempt = 'bad'
  await rejects(
    recover(
      ci,
      command,
      async () => {
        throw new UserError('Must not request GitHub.')
      },
      async () => {},
      2
    ),
    { code: 'PLT_KFK_USER' }
  )
  ok(!calls.some(args => args[3] === 'delete'))
})

test('recovery fails rather than ignoring orphan streams from trusted runs', async () => {
  const ci = { ...config, owner: { repository: 'platformatic/kafka', attempt: '1' } }
  const { resources, state, command } = fixture(ci, '123')
  await resources.provision('123')
  const status = {
    status: 'completed',
    path: '.github/workflows/regression.yml',
    head_branch: 'main',
    event: 'workflow_dispatch'
  }
  state.pools = []
  await rejects(
    recover(
      ci,
      command,
      async () => status,
      async () => {},
      2
    ),
    { code: 'PLT_KFK_USER' }
  )
})

test('local lifecycle verifies stream deletion before deleting the empty pool', async () => {
  const { resources, pool, calls, state } = fixture()
  strictEqual(await resources.provision(run), pool.id)
  deepStrictEqual(await resources.connection(run, 'tenancy/Default/test-user'), {
    ORACLE_STREAMING_BOOTSTRAP_SERVERS: pool['kafka-settings']['bootstrap-servers'],
    ORACLE_STREAMING_TOPIC: 'kafka-smoke',
    ORACLE_STREAMING_USERNAME: `tenancy/Default/test-user/${pool.id}`
  })
  const createPool = calls.find(args => args[2] === 'stream-pool' && args[3] === 'create')!
  deepStrictEqual(JSON.parse(createPool[createPool.indexOf('--kafka-settings') + 1]), { autoCreateTopicsEnable: false })
  const createStream = calls.find(args => args[2] === 'stream' && args[3] === 'create')!
  strictEqual(createStream[createStream.indexOf('--partitions') + 1], '2')
  strictEqual(createStream[createStream.indexOf('--retention-in-hours') + 1], '24')
  ok(!createStream.includes('--compartment-id'))
  await resources.cleanup(run)
  ok(state.pools.every(item => item['lifecycle-state'] === 'DELETED'))
  ok(state.streams.every(item => item['lifecycle-state'] === 'DELETED'))
  const deleteStream = calls.findIndex(args => args[2] === 'stream' && args[3] === 'delete')
  const deletePool = calls.findIndex(args => args[2] === 'stream-pool' && args[3] === 'delete')
  ok(deleteStream >= 0 && deletePool > deleteStream)
  ok(calls.slice(deleteStream + 1, deletePool).some(args => args[2] === 'stream' && args[3] === 'list'))
  const deletes = calls.filter(args => args[3] === 'delete').length
  await resources.cleanup(run)
  strictEqual(calls.filter(args => args[3] === 'delete').length, deletes)
  ok(calls.every(args => args.includes(config.region)))
})

test('names isolate local executions, compartments and regions and reject unsafe run IDs', () => {
  const first = resourceNames(config, run).pool
  ok(first !== resourceNames(config, 'local-test-2').pool)
  ok(first !== resourceNames({ ...config, region: 'another-region' }, run).pool)
  ok(first !== resourceNames({ ...config, compartment: 'another-compartment' }, run).pool)
  for (const invalid of ['', '../test', '-test', 'a'.repeat(65)]) {
    throws(() => resourceNames(config, invalid), { code: 'PLT_KFK_USER' })
  }
})

test('partial provisioning and a lost create response trigger cleanup by tags', async () => {
  for (const failure of ['stream create', 'lost-response']) {
    const { resources, state } = fixture()
    state.fail = failure
    state.loseCreateResponse = failure === 'lost-response'
    await rejects(resources.provision(run), { code: 'PLT_KFK_USER' })
    ok(state.pools.every(item => item['lifecycle-state'] === 'DELETED'))
    strictEqual(state.streams.length, 0)
  }
})

test('a colliding run never executes the smoke or deletes existing resources', async () => {
  const { resources, state, pool, calls } = fixture()
  state.pools.push(pool)
  await rejects(
    resources.withResources(run, async () => {
      throw new UserError('Must not execute.')
    }),
    {
      code: 'PLT_KFK_USER',
      message: /already exists/
    }
  )
  ok(!calls.some(args => args.includes('delete')))
})

test('failed or stalled provisioning fails with a stable error and cleans up', async () => {
  for (const status of ['FAILED', 'CREATING', 'unknown']) {
    const { resources, pool, state } = fixture()
    pool['lifecycle-state'] = status
    await rejects(resources.provision(run), { code: 'PLT_KFK_USER' })
    if (status !== 'unknown') {
      ok(state.pools.every(item => item['lifecycle-state'] === 'DELETED'))
    }
  }
})

test('ownership validation precedes every deletion', async () => {
  for (const failure of ['repository', 'run', 'purpose', 'duplicate']) {
    const { resources, state, pool, calls } = fixture()
    state.pools.push(pool)
    if (failure === 'repository') {
      pool['freeform-tags'].repository = 'another/repository'
    } else if (failure === 'run') {
      pool['freeform-tags'].run = 'another-run'
    } else if (failure === 'purpose') {
      pool['freeform-tags'].purpose = 'manual'
    } else {
      state.pools.push({ ...pool, id: 'unowned-pool', 'freeform-tags': { ...pool['freeform-tags'], run: 'other' } })
    }
    await rejects(resources.cleanup(run), { code: 'PLT_KFK_USER', message: /ownership/ })
    ok(!calls.some(args => args.includes('delete')))
  }
})

test('cleanup is idempotent and ignores unrelated pools', async () => {
  const { resources, state, pool, calls } = fixture()
  state.pools.push({ ...pool, name: 'manual-pool' })
  await resources.cleanup(run)
  await resources.cleanup(run)
  strictEqual(state.pools.length, 1)
  ok(!calls.some(args => args.includes('delete')))
})

test('CLI failures and pending deletions cannot report successful cleanup', async () => {
  for (const failure of [
    'stream-pool list',
    'stream-pool delete',
    'stream list',
    'stream delete',
    'stuck',
    'stream-stuck'
  ]) {
    const { resources, state, calls } = fixture()
    await resources.provision(run)
    state.fail = failure
    state.stuck = failure === 'stuck'
    state.streamStuck = failure === 'stream-stuck'
    await rejects(resources.cleanup(run), { code: 'PLT_KFK_USER' })
    if (['stream list', 'stream delete', 'stream-stuck'].includes(failure)) {
      ok(!calls.some(args => args[2] === 'stream-pool' && args[3] === 'delete'))
    }
  }
})

test('invalid responses are not interpreted as absent resources', async () => {
  for (const response of ['null', '{}', '{"data":null}', '{"data":[{}]}', 'not-json']) {
    const resources = new OracleStreamingResources(
      config,
      async () => response,
      async () => {},
      1
    )
    await rejects(resources.cleanup(run), { code: 'PLT_KFK_USER' })
  }
})

test('successful empty CLI listings mean no resources, but empty create responses fail', async () => {
  const calls: string[][] = []
  const resources = new OracleStreamingResources(
    config,
    async args => {
      calls.push(args)
      return ''
    },
    async () => {},
    1
  )
  await resources.cleanup(run)
  ok(!calls.some(args => args.includes('delete')))
  await rejects(resources.provision(run), { code: 'PLT_KFK_USER', message: /invalid JSON/ })
})

test('already pending deletion is verified without issuing a second delete', async () => {
  const { state, pool, command, calls } = fixture()
  pool['lifecycle-state'] = 'DELETING'
  state.pools.push(pool)
  const resources = new OracleStreamingResources(
    config,
    command,
    async () => {
      state.pools = []
    },
    2
  )
  await resources.cleanup(run)
  ok(!calls.some(args => args.includes('delete')))
})

test('cleanup detects remaining tagged streams even after the pool has disappeared', async () => {
  const { resources, state, stream } = fixture()
  state.streams.push(stream)
  await rejects(resources.cleanup(run), { code: 'PLT_KFK_USER', message: /timed out/ })
})

test('cleanup waits for asynchronous stream deletion and resumes pending deletions', async () => {
  for (const pending of [false, true]) {
    const { state, pool, stream, command, calls } = fixture()
    state.pools.push(pool)
    state.streams.push(stream)
    state.streamStuck = true
    if (pending) {
      stream['lifecycle-state'] = 'DELETING'
    }
    let waited = false
    const resources = new OracleStreamingResources(
      config,
      command,
      async () => {
        strictEqual(stream['lifecycle-state'], 'DELETING')
        ok(!calls.some(args => args[2] === 'stream-pool' && args[3] === 'delete'))
        stream['lifecycle-state'] = 'DELETED'
        waited = true
      },
      2
    )
    await resources.cleanup(run)
    ok(waited)
    strictEqual(pool['lifecycle-state'], 'DELETED')
    strictEqual(calls.filter(args => args[2] === 'stream' && args[3] === 'delete').length, pending ? 0 : 1)
  }
})

test('cleanup validates every stream before issuing deletions and preserves unrelated streams', async () => {
  for (const mismatch of [true, false]) {
    const { resources, state, pool, stream, calls } = fixture()
    state.pools.push(pool)
    const other = {
      ...stream,
      id: 'other-stream',
      'stream-pool-id': mismatch ? pool.id : 'other-pool',
      'freeform-tags': { ...stream['freeform-tags'], run: 'other-run' }
    }
    state.streams.push(stream, other)
    if (mismatch) {
      await rejects(resources.cleanup(run), { code: 'PLT_KFK_USER', message: /ownership/ })
      ok(!calls.some(args => args[3] === 'delete'))
    } else {
      await resources.cleanup(run)
    }
    strictEqual(other['lifecycle-state'], 'ACTIVE')
  }
})

test('the local runner always cleans up after a successful or failed smoke', async () => {
  for (const fail of [false, true]) {
    const { resources, state } = fixture()
    let ran = false
    const result = resources.withResources(run, async () => {
      ran = true
      strictEqual(state.streams.length, 1)
      if (fail) {
        throw new UserError('Smoke failed.')
      }
    })
    if (fail) {
      await rejects(result, { code: 'PLT_KFK_USER', message: 'Smoke failed.' })
    } else {
      await result
    }
    ok(ran)
    ok(state.pools.every(item => item['lifecycle-state'] === 'DELETED'))
    ok(state.streams.every(item => item['lifecycle-state'] === 'DELETED'))
  }
})

test('a cleanup failure fails the local runner even when the smoke passed', async () => {
  const { resources, state } = fixture()
  await rejects(
    resources.withResources(run, async () => {
      state.stuck = true
    }),
    {
      code: 'PLT_KFK_USER',
      message: /cleanup timed out/
    }
  )
})

test('connection settings reject ambiguous pools and incomplete Kafka settings', async () => {
  const { resources, state, pool } = fixture()
  await rejects(resources.connection(run, 'tenancy/user'), { code: 'PLT_KFK_USER' })
  state.pools.push(pool, { ...pool, id: 'duplicate-pool' })
  await rejects(resources.connection(run, 'tenancy/user'), { code: 'PLT_KFK_USER' })
  state.pools.pop()
  await rejects(resources.connection(run, 'tenancy/user/'), { code: 'PLT_KFK_USER' })
  pool['kafka-settings']['bootstrap-servers'] = ''
  await rejects(resources.connection(run, 'tenancy/user'), { code: 'PLT_KFK_USER' })
})

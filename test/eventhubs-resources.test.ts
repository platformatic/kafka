import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { test } from 'node:test'
import { EventHubsResources, resourceNames, type Command } from '../scripts/eventhubs-resources.ts'
import { UserError } from '../src/errors.ts'

const config = { subscription: 'test-subscription', repository: 'platformatic/kafka', location: 'northeurope' }

function fixture () {
  const names = resourceNames(config, '123', '1')
  const group = {
    name: names.group,
    tags: { purpose: 'kafka-eventhubs-regression', repository: config.repository, run: '123', attempt: '1' },
    properties: { provisioningState: 'Succeeded' }
  }
  const calls: string[][] = []
  const state = {
    exists: false,
    fail: '',
    deletionStuck: false,
    namespaceState: 'Succeeded',
    runStatus: 'completed',
    workflow: '.github/workflows/regression.yml',
    groups: [group],
    checksAfterDelete: 0
  }
  const command: Command = async (file, args) => {
    calls.push([file, ...args])
    const operation = args
      .slice(0, args.indexOf('--resource-group') < 0 ? 3 : args.indexOf('--resource-group'))
      .join(' ')
    if (state.fail && `${file} ${args.join(' ')}`.includes(state.fail)) {
      throw new UserError('Simulated CLI failure.')
    }
    if (file === 'gh') {
      return JSON.stringify({ status: state.runStatus, path: state.workflow, head_branch: 'main' })
    }
    if (args[0] === 'group') {
      switch (args[1]) {
        case 'exists':
          if (calls.some(call => call[1] === 'group' && call[2] === 'delete')) {
            state.checksAfterDelete++
          }
          return String(state.exists)
        case 'create':
          state.exists = true
          return ''
        case 'show':
          return JSON.stringify(group)
        case 'delete':
          if (!state.deletionStuck) {
            state.exists = false
          }
          return ''
        case 'list':
          return JSON.stringify(state.groups)
      }
    }
    if (operation.startsWith('eventhubs namespace show')) {
      return state.namespaceState
    }
    if (args.includes('keys')) {
      return 'Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=smoke;SharedAccessKey=secret'
    }
    return ''
  }
  const resources = new EventHubsResources(config, command, async () => {}, 2)
  return { resources, names, state, group, calls, command }
}

test('provisions an isolated environment, obtains credentials, and verifies deletion', async () => {
  const { resources, state, names, calls } = fixture()
  deepStrictEqual(await resources.provision('123', '1'), names)
  const credentials = await resources.credentials('123', '1')
  strictEqual(credentials.EVENTHUBS_BOOTSTRAP_SERVERS, `${names.namespace}.servicebus.windows.net:9093`)
  strictEqual(credentials.EVENTHUBS_TOPIC, 'kafka-smoke')
  ok(calls.some(call => call.includes('purpose=kafka-eventhubs-regression')))
  await resources.cleanup('123', '1')
  strictEqual(state.exists, false)
  strictEqual(state.checksAfterDelete, 1)
  ok(calls.every(call => call.includes(config.subscription)))
})

test('resource names isolate repositories and rerun attempts', () => {
  const first = resourceNames(config, '123', '1')
  ok(first.group !== resourceNames(config, '123', '2').group)
  ok(first.group !== resourceNames({ ...config, repository: 'another/repo' }, '123', '1').group)
  throws(() => resourceNames(config, '../123', '1'), { code: 'PLT_KFK_USER' })
})

test('namespace creation supports Azure CLI 2.79 and gets a longer timeout than status and cleanup calls', async () => {
  const { command } = fixture()
  const invocations: { args: string[]; timeout: number | undefined }[] = []
  const resources = new EventHubsResources(
    config,
    async (file, args, timeout) => {
      invocations.push({ args, timeout })
      return command(file, args, timeout)
    },
    async () => {}
  )
  await resources.provision('123', '1')
  await resources.cleanup('123', '1')
  const creation = invocations.find(({ args }) => args.slice(0, 3).join(' ') === 'eventhubs namespace create')!
  ok(creation)
  ok(!creation.args.includes('--no-wait'))
  strictEqual(creation.args[creation.args.indexOf('--enable-kafka') + 1], 'true')
  strictEqual(creation.timeout, 600_000)
  ok(invocations.filter(call => call !== creation).every(({ timeout }) => timeout === undefined))
  const deletion = invocations.find(({ args }) => args.slice(0, 2).join(' ') === 'group delete')!
  ok(deletion.args.includes('--no-wait'))
})

test('cleanup handles provisioning failure after the group has been created', async () => {
  const { resources, state } = fixture()
  state.fail = 'namespace create'
  await rejects(resources.provision('123', '1'), { code: 'PLT_KFK_USER' })
  strictEqual(state.exists, true)
  await resources.cleanup('123', '1')
  strictEqual(state.exists, false)
})

test('existing groups are not adopted by provisioning', async () => {
  const { resources, state, calls } = fixture()
  state.exists = true
  await rejects(resources.provision('123', '1'), { code: 'PLT_KFK_USER', message: /already exists/ })
  strictEqual(calls.length, 1)
})

test('namespace readiness has a bounded wait', async () => {
  const { resources, state } = fixture()
  state.namespaceState = 'Creating'
  await rejects(resources.provision('123', '1'), { code: 'PLT_KFK_USER', message: /did not become ready/ })
  await resources.cleanup('123', '1')
})

test('cleanup is idempotent when a group was never created or is already gone', async () => {
  const { resources, calls } = fixture()
  await resources.cleanup('123', '1')
  await resources.cleanup('123', '1')
  ok(!calls.some(call => call.includes('delete')))
})

test('cleanup refuses mismatched ownership', async () => {
  const { resources, state, group, calls } = fixture()
  state.exists = true
  group.tags.repository = 'another/repo'
  await rejects(resources.cleanup('123', '1'), { code: 'PLT_KFK_USER', message: /ownership tags/ })
  ok(!calls.some(call => call.includes('delete')))
})

test('deletion failures and verification timeouts cannot report successful cleanup', async () => {
  for (const failure of ['group delete', 'group exists', 'stuck']) {
    const { resources, state } = fixture()
    state.exists = true
    state.fail = failure === 'stuck' ? '' : failure
    state.deletionStuck = failure === 'stuck'
    await rejects(resources.cleanup('123', '1'), { code: 'PLT_KFK_USER' })
    strictEqual(state.exists, true)
  }
})

test('an unexpected existence response cannot be mistaken for an absent group', async () => {
  const resources = new EventHubsResources(
    config,
    async () => '',
    async () => {},
    1
  )
  await rejects(resources.cleanup('123', '1'), { code: 'PLT_KFK_USER', message: /Cannot determine/ })
})

test('recovery removes only groups owned by completed regression runs', async () => {
  const { resources, state, calls } = fixture()
  state.exists = true
  await resources.recover()
  strictEqual(state.exists, false)
  ok(calls.some(call => call[0] === 'gh' && call.includes('repos/platformatic/kafka/actions/runs/123')))
})

test('recovery keeps active runs and ignores manual environments', async () => {
  for (const status of ['queued', 'in_progress', 'waiting']) {
    const { resources, state, calls } = fixture()
    state.exists = true
    state.runStatus = status
    await resources.recover()
    strictEqual(state.exists, true)
    ok(!calls.some(call => call.includes('delete')))
  }
  const { resources, group, calls } = fixture()
  group.tags.purpose = 'kafka-eventhubs-smoke'
  await resources.recover()
  strictEqual(calls.length, 1)
})

test('recovery fails closed when GitHub cannot confirm workflow identity or run state', async () => {
  for (const failure of ['gh api', 'wrong-workflow', 'unknown-status']) {
    const { resources, state, calls } = fixture()
    state.exists = true
    state.fail = failure === 'gh api' ? failure : ''
    if (failure === 'wrong-workflow') {
      state.workflow = '.github/workflows/other.yml'
    }
    if (failure === 'unknown-status') {
      state.runStatus = 'unknown'
    }
    await rejects(resources.recover(), { code: 'PLT_KFK_USER', message: /recovery incomplete/ })
    ok(!calls.some(call => call.includes('delete')))
  }
})

test('recovery continues cleaning eligible groups after another group fails validation', async () => {
  const { resources, state, group } = fixture()
  state.exists = true
  state.groups.unshift({ ...group, name: 'invalid-automation-name' })
  await rejects(resources.recover(), { code: 'PLT_KFK_USER', message: /invalid-automation-name/ })
  strictEqual(state.exists, false)
})

test('cleanup verifies an already pending deletion without issuing another delete', async () => {
  const { group, command, calls } = fixture()
  group.properties.provisioningState = 'Deleting'
  let checks = 0
  const resources = new EventHubsResources(config, async (file, args) => {
    if (args[0] === 'group' && args[1] === 'exists') {
      return ++checks === 1 ? 'true' : 'false'
    }
    return command(file, args)
  })
  await resources.cleanup('123', '1')
  strictEqual(checks, 2)
  ok(!calls.some(call => call.includes('delete')))
})

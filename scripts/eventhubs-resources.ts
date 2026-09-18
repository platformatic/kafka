import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import { appendFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { setTimeout } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { UserError } from '../src/errors.ts'

const execute = promisify(execFile)
const purpose = 'kafka-eventhubs-regression'

export type Command = (file: string, args: string[], timeout?: number) => Promise<string>
export interface Configuration {
  subscription: string
  repository: string
  location: string
}

interface ResourceGroup {
  name: string
  tags?: Record<string, string>
  properties?: { provisioningState?: string }
}

export function resourceNames (config: Configuration, run: string, attempt: string) {
  if (!/^\d+$/.test(run) || !/^\d+$/.test(attempt)) {
    throw new UserError('Event Hubs run ID and attempt must be numeric.')
  }
  const scope = createHash('sha256').update(`${config.subscription}/${config.repository}`).digest('hex').slice(0, 12)
  const group = `kafka-eh-${scope}-${run}-${attempt}`
  return { group, namespace: group, topic: 'kafka-smoke', policy: 'kafka-smoke' }
}

export async function command (file: string, args: string[], timeout = 60_000): Promise<string> {
  try {
    const { stdout } = await execute(file, args, { timeout, maxBuffer: 4 * 1024 * 1024 })
    return stdout.trim()
  } catch {
    // Azure CLI errors can contain credentials. Do not include command output or the original cause.
    throw new UserError(`${file} ${args.slice(0, 3).join(' ')} failed or timed out; check access and resource state.`)
  }
}

export class EventHubsResources {
  readonly config: Configuration
  private readonly runCommand: Command
  private readonly wait: () => Promise<void>
  private readonly attempts: number

  constructor (
    config: Configuration,
    runCommand: Command = command,
    wait: () => Promise<void> = () => setTimeout(15_000),
    attempts = 40
  ) {
    this.config = config
    this.runCommand = runCommand
    this.wait = wait
    this.attempts = attempts
  }

  private az (args: string[], timeout?: number) {
    return this.runCommand('az', [...args, '--subscription', this.config.subscription, '--only-show-errors'], timeout)
  }

  private async exists (group: string) {
    const result = await this.az(['group', 'exists', '--name', group, '--output', 'tsv'])
    if (result !== 'true' && result !== 'false') {
      throw new UserError(`Cannot determine whether Event Hubs resource group ${group} exists.`)
    }
    return result === 'true'
  }

  private owns (group: ResourceGroup, run: string, attempt: string) {
    return (
      group.name === resourceNames(this.config, run, attempt).group &&
      group.tags?.purpose === purpose &&
      group.tags?.repository === this.config.repository &&
      group.tags?.run === run &&
      group.tags?.attempt === attempt
    )
  }

  async provision (run: string, attempt: string) {
    const names = resourceNames(this.config, run, attempt)
    // Never adopt or overwrite an existing group, even when retrying a partially provisioned run.
    if (await this.exists(names.group)) {
      throw new UserError(`Event Hubs resource group ${names.group} already exists; clean it up before provisioning.`)
    }
    await this.az([
      'group',
      'create',
      '--name',
      names.group,
      '--location',
      this.config.location,
      '--tags',
      `purpose=${purpose}`,
      `repository=${this.config.repository}`,
      `run=${run}`,
      `attempt=${attempt}`,
      '--output',
      'none'
    ])
    // Namespace creation does not support --no-wait in Azure CLI 2.79.0 and may take several minutes.
    await this.az(
      [
        'eventhubs',
        'namespace',
        'create',
        '--resource-group',
        names.group,
        '--name',
        names.namespace,
        '--location',
        this.config.location,
        '--sku',
        'Standard',
        '--capacity',
        '1',
        '--enable-auto-inflate',
        'false',
        '--enable-kafka',
        'true',
        '--minimum-tls-version',
        '1.2',
        '--output',
        'none'
      ],
      600_000
    )
    let ready = false
    for (let index = 0; index < this.attempts; index++) {
      const state = await this.az([
        'eventhubs',
        'namespace',
        'show',
        '--resource-group',
        names.group,
        '--name',
        names.namespace,
        '--query',
        'provisioningState',
        '--output',
        'tsv'
      ])
      if (state === 'Succeeded') {
        ready = true
        break
      }
      if (state === 'Failed' || state === 'Canceled') {
        throw new UserError(`Event Hubs namespace ${names.namespace} provisioning ${state}.`)
      }
      await this.wait()
    }
    if (!ready) {
      throw new UserError(`Event Hubs namespace ${names.namespace} did not become ready in time.`)
    }
    await this.az([
      'eventhubs',
      'eventhub',
      'create',
      '--resource-group',
      names.group,
      '--namespace-name',
      names.namespace,
      '--name',
      names.topic,
      '--partition-count',
      '2',
      '--cleanup-policy',
      'Delete',
      '--retention-time-in-hours',
      '24',
      '--enable-capture',
      'false',
      '--output',
      'none'
    ])
    await this.az([
      'eventhubs',
      'namespace',
      'authorization-rule',
      'create',
      '--resource-group',
      names.group,
      '--namespace-name',
      names.namespace,
      '--name',
      names.policy,
      '--rights',
      'Send',
      'Listen',
      '--output',
      'none'
    ])
    // Allow the newly created authorization rule to propagate before testing the data plane.
    await this.wait()
    return names
  }

  async credentials (run: string, attempt: string) {
    const names = resourceNames(this.config, run, attempt)
    const password = await this.az([
      'eventhubs',
      'namespace',
      'authorization-rule',
      'keys',
      'list',
      '--resource-group',
      names.group,
      '--namespace-name',
      names.namespace,
      '--name',
      names.policy,
      '--query',
      'primaryConnectionString',
      '--output',
      'tsv'
    ])
    if (!password.startsWith('Endpoint=sb://') || /[\r\n]/.test(password)) {
      throw new UserError('Azure returned an invalid Event Hubs connection string.')
    }
    return {
      EVENTHUBS_BOOTSTRAP_SERVERS: `${names.namespace}.servicebus.windows.net:9093`,
      EVENTHUBS_TOPIC: names.topic,
      EVENTHUBS_CONNECTION_STRING: password
    }
  }

  async cleanup (run: string, attempt: string) {
    const { group } = resourceNames(this.config, run, attempt)
    console.log(`Verifying cleanup: subscription=${this.config.subscription} resource-group=${group}`)
    if (!(await this.exists(group))) {
      console.log(`Verified absent: ${group}`)
      return
    }
    const resource: ResourceGroup = JSON.parse(await this.az(['group', 'show', '--name', group, '--output', 'json']))
    if (!this.owns(resource, run, attempt)) {
      throw new UserError(`Refusing to delete resource group ${group}: ownership tags do not match.`)
    }
    if (resource.properties?.provisioningState !== 'Deleting') {
      await this.az(['group', 'delete', '--name', group, '--yes', '--no-wait'])
    }
    for (let index = 0; index < this.attempts; index++) {
      if (!(await this.exists(group))) {
        console.log(`Verified deleted: ${group}`)
        return
      }
      await this.wait()
    }
    throw new UserError(`Cleanup timed out: resource group ${group} still exists in ${this.config.subscription}.`)
  }

  async recover () {
    const groups: ResourceGroup[] = JSON.parse(
      await this.az(['group', 'list', '--tag', `purpose=${purpose}`, '--output', 'json'])
    )
    const failed: string[] = []
    for (const group of groups) {
      if (group.tags?.purpose !== purpose || group.tags?.repository !== this.config.repository) {
        continue
      }
      try {
        const { run, attempt } = group.tags
        if (!this.owns(group, run, attempt)) {
          throw new UserError(`Invalid ownership metadata on ${group.name}.`)
        }
        // Check the current run, not an old attempt: a re-run may currently be using Azure resources.
        const status: { status: string; path: string; head_branch: string } = JSON.parse(
          await this.runCommand('gh', ['api', `repos/${this.config.repository}/actions/runs/${run}`])
        )
        if (status.path !== '.github/workflows/regression.yml' || status.head_branch !== 'main') {
          throw new UserError(`Unexpected workflow identity for ${group.name}.`)
        }
        if (!['completed', 'queued', 'in_progress', 'waiting', 'pending', 'requested'].includes(status.status)) {
          throw new UserError(`Cannot determine the GitHub run status for ${group.name}.`)
        }
        if (status.status !== 'completed') {
          console.log(`Keeping ${group.name}: GitHub run is ${status.status}.`)
          continue
        }
        await this.cleanup(run, attempt)
      } catch {
        // Continue recovering other groups, but never interpret an API error as an absent or completed run.
        console.error(`Could not verify or clean ${group.name} in subscription ${this.config.subscription}.`)
        failed.push(group.name)
      }
    }
    if (failed.length > 0) {
      throw new UserError(`Event Hubs recovery incomplete: ${failed.join(', ')}.`)
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { AZURE_SUBSCRIPTION_ID: subscription, GITHUB_REPOSITORY: repository } = process.env
    if (!subscription || !repository) {
      throw new UserError('AZURE_SUBSCRIPTION_ID and GITHUB_REPOSITORY are required.')
    }
    const config = { subscription, repository, location: process.env.AZURE_LOCATION || 'northeurope' }
    const resources = new EventHubsResources(config)
    const action = process.argv[2]
    if (action === 'recover') {
      await resources.recover()
    } else {
      const run = process.env.GITHUB_RUN_ID ?? ''
      const attempt = process.env.GITHUB_RUN_ATTEMPT ?? ''
      const names = resourceNames(config, run, attempt)
      console.log(`Event Hubs resources: subscription=${subscription} resource-group=${names.group}`)
      if (action === 'provision') {
        await resources.provision(run, attempt)
      } else if (action === 'credentials') {
        if (!process.env.GITHUB_ENV || process.env.GITHUB_ACTIONS !== 'true') {
          throw new UserError(
            'The credentials command requires GitHub Actions; see docs/eventhubs.md for local access.'
          )
        }
        const credentials = await resources.credentials(run, attempt)
        console.log(`::add-mask::${credentials.EVENTHUBS_CONNECTION_STRING.replaceAll('%', '%25')}`)
        await appendFile(
          process.env.GITHUB_ENV,
          Object.entries(credentials)
            .map(([key, value]) => `${key}=${value}\n`)
            .join('')
        )
      } else if (action === 'cleanup') {
        await resources.cleanup(run, attempt)
      } else {
        throw new UserError('Usage: node scripts/eventhubs-resources.ts <provision|credentials|cleanup|recover>')
      }
    }
  } catch (error) {
    console.error(
      error instanceof UserError ? `${error.code}: ${error.message}` : 'Event Hubs automation failed unexpectedly.'
    )
    process.exitCode = 1
  }
}

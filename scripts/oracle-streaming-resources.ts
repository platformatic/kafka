import { execFile, spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import { resolve } from 'node:path'
import { setTimeout } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { UserError } from '../src/errors.ts'

const execute = promisify(execFile)
const purpose = 'kafka-oracle-streaming-local'
const repository = 'platformatic/kafka'

export type Command = (args: string[]) => Promise<string>
export interface Configuration {
  compartment: string
  region: string
}

interface Resource {
  id: string
  name: string
  'compartment-id': string
  'lifecycle-state': string
  'freeform-tags'?: Record<string, string>
  'stream-pool-id'?: string
  'kafka-settings'?: { 'bootstrap-servers'?: string }
}

export function resourceNames (config: Configuration, run: string) {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$/.test(run)) {
    throw new UserError(
      'Oracle Streaming run ID must contain 1-64 letters, digits or hyphens, starting with a letter or digit.'
    )
  }
  const scope = createHash('sha256')
    .update(`${config.compartment}/${config.region}/${repository}`)
    .digest('hex')
    .slice(0, 12)
  return { pool: `kafka-oss-${scope}-${run}`, topic: 'kafka-smoke' }
}

export async function command (args: string[]): Promise<string> {
  try {
    const { stdout } = await execute('oci', args, { timeout: 60_000, maxBuffer: 4 * 1024 * 1024 })
    return stdout.trim()
  } catch {
    // CLI diagnostics can contain credentials; never forward stderr or the original error.
    throw new UserError(`oci ${args.slice(0, 4).join(' ')} failed or timed out; check OCI access and resource state.`)
  }
}

export class OracleStreamingResources {
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
    if (!config.compartment || !config.region) {
      throw new UserError('OCI_COMPARTMENT_ID and OCI_CLI_REGION are required.')
    }
    this.config = config
    this.runCommand = runCommand
    this.wait = wait
    this.attempts = attempts
  }

  private async oci (args: string[]) {
    const output = await this.runCommand([
      'streaming',
      'admin',
      ...args,
      '--region',
      this.config.region,
      '--output',
      'json',
      '--max-retries',
      '2'
    ])
    // OCI CLI's renderer emits no JSON for an empty collection. This is valid only after a successful list.
    if (args[1] === 'list' && output.trim() === '') {
      return { data: [] }
    }
    try {
      const result = JSON.parse(output)
      if (!result || typeof result !== 'object' || Array.isArray(result)) {
        throw new UserError('OCI returned an invalid response envelope.')
      }
      return result
    } catch {
      throw new UserError('OCI returned an invalid JSON response; resource state cannot be verified.')
    }
  }

  private async list (kind: 'stream-pool' | 'stream') {
    // List by compartment even after pool deletion, and fetch every page before declaring resources absent.
    const result = await this.oci([kind, 'list', '--compartment-id', this.config.compartment, '--all'])
    if (
      !Array.isArray(result?.data) ||
      result.data.some(
        (item: Resource) =>
          !item ||
          typeof item.id !== 'string' ||
          typeof item.name !== 'string' ||
          item['compartment-id'] !== this.config.compartment ||
          !['CREATING', 'ACTIVE', 'UPDATING', 'DELETING', 'DELETED', 'FAILED'].includes(item['lifecycle-state'])
      )
    ) {
      throw new UserError(`OCI returned an invalid ${kind} listing; resource state cannot be verified.`)
    }
    return (result.data as Resource[]).filter(item => item['lifecycle-state'] !== 'DELETED')
  }

  private owns (pool: Resource, run: string) {
    return (
      pool.name === resourceNames(this.config, run).pool &&
      pool['compartment-id'] === this.config.compartment &&
      pool['freeform-tags']?.purpose === purpose &&
      pool['freeform-tags']?.repository === repository &&
      pool['freeform-tags']?.run === run
    )
  }

  private async pools (run: string) {
    const { pool } = resourceNames(this.config, run)
    return (await this.list('stream-pool')).filter(item => item.name === pool)
  }

  private async ready (kind: 'stream-pool' | 'stream', id: string) {
    const deadline = Date.now() + 600_000
    for (let index = 0; index < this.attempts && Date.now() < deadline; index++) {
      const { data } = await this.oci([kind, 'get', `--${kind}-id`, id])
      if (data?.id !== id || !['CREATING', 'UPDATING', 'ACTIVE'].includes(data['lifecycle-state'])) {
        throw new UserError(`Oracle Streaming ${kind} ${id} is not in a usable lifecycle state.`)
      }
      if (data['lifecycle-state'] === 'ACTIVE') {
        return data as Resource
      }
      await this.wait()
    }
    throw new UserError(`Oracle Streaming ${kind} ${id} did not become ready in time.`)
  }

  async provision (run: string) {
    const names = resourceNames(this.config, run)
    // Refuse reuse before entering the cleanup scope, so a collision cannot delete another invocation's pool.
    if ((await this.pools(run)).length > 0) {
      throw new UserError(`Oracle Streaming pool ${names.pool} already exists; use a fresh run ID or clean it up.`)
    }
    const tags = JSON.stringify({ purpose, repository, run })
    try {
      const { data: pool } = await this.oci([
        'stream-pool',
        'create',
        '--compartment-id',
        this.config.compartment,
        '--name',
        names.pool,
        '--freeform-tags',
        tags,
        '--kafka-settings',
        JSON.stringify({ autoCreateTopicsEnable: false })
      ])
      if (!pool?.id || !this.owns(pool, run)) {
        throw new UserError('OCI returned an unexpected stream pool after creation.')
      }
      await this.ready('stream-pool', pool.id)
      const { data: stream } = await this.oci([
        'stream',
        'create',
        '--stream-pool-id',
        pool.id,
        '--name',
        names.topic,
        '--partitions',
        '2',
        '--retention-in-hours',
        '24',
        '--freeform-tags',
        tags
      ])
      if (!stream?.id || stream['stream-pool-id'] !== pool.id) {
        throw new UserError('OCI returned an unexpected stream after creation.')
      }
      await this.ready('stream', stream.id)
      return pool.id as string
    } catch (error) {
      // Discovery by name and tags also handles a successful create whose response was lost.
      try {
        await this.cleanup(run)
      } catch {
        throw new UserError(`Oracle Streaming provisioning and cleanup failed; retry cleanup with run ID ${run}.`)
      }
      throw error instanceof UserError ? error : new UserError('Oracle Streaming provisioning failed unexpectedly.')
    }
  }

  async connection (run: string, usernamePrefix: string) {
    if (!usernamePrefix || /[\r\n]/.test(usernamePrefix) || usernamePrefix.endsWith('/')) {
      throw new UserError('ORACLE_STREAMING_USERNAME_PREFIX must be tenancy/domain/username without a trailing slash.')
    }
    const pools = await this.pools(run)
    if (pools.length !== 1 || !this.owns(pools[0], run)) {
      throw new UserError('Cannot uniquely identify the owned Oracle Streaming pool.')
    }
    const pool = await this.ready('stream-pool', pools[0].id)
    const servers = pool['kafka-settings']?.['bootstrap-servers']
    if (typeof servers !== 'string' || !servers.trim() || /[\r\n]/.test(servers)) {
      throw new UserError('OCI did not return Kafka bootstrap servers for the stream pool.')
    }
    return {
      ORACLE_STREAMING_BOOTSTRAP_SERVERS: servers,
      ORACLE_STREAMING_TOPIC: resourceNames(this.config, run).topic,
      ORACLE_STREAMING_USERNAME: `${usernamePrefix}/${pool.id}`
    }
  }

  private async deleteResource (kind: 'stream-pool' | 'stream', resource: Resource) {
    if (resource['lifecycle-state'] === 'DELETING') {
      return
    }
    await this.runCommand([
      'streaming',
      'admin',
      kind,
      'delete',
      `--${kind}-id`,
      resource.id,
      '--force',
      '--region',
      this.config.region,
      '--output',
      'json',
      '--max-retries',
      '2'
    ])
  }

  async cleanup (run: string) {
    const pools = await this.pools(run)
    // Validate the complete selection before deleting anything, including duplicate names.
    if (pools.some(pool => !this.owns(pool, run))) {
      throw new UserError('Refusing to delete Oracle Streaming pools: resource names or ownership tags do not match.')
    }
    const ids = new Set(pools.map(pool => pool.id))
    const streams = (await this.list('stream')).filter(stream => ids.has(stream['stream-pool-id'] ?? ''))
    if (
      streams.some(
        stream =>
          stream.name !== resourceNames(this.config, run).topic ||
          stream['freeform-tags']?.purpose !== purpose ||
          stream['freeform-tags']?.repository !== repository ||
          stream['freeform-tags']?.run !== run
      )
    ) {
      throw new UserError('Refusing to delete Oracle Streaming streams: resource names or ownership tags do not match.')
    }
    for (const stream of streams) {
      await this.deleteResource('stream', stream)
    }
    // OCI rejects non-empty pools. Wait for stream deletion, including already pending requests,
    // before deleting any pool. Listings can retain DELETED records, which list() excludes.
    const streamDeadline = Date.now() + 600_000
    let streamsDeleted = false
    for (let index = 0; index < this.attempts && Date.now() < streamDeadline; index++) {
      if (!(await this.list('stream')).some(stream => ids.has(stream['stream-pool-id'] ?? ''))) {
        streamsDeleted = true
        break
      }
      await this.wait()
    }
    if (!streamsDeleted) {
      throw new UserError(`Oracle Streaming stream cleanup timed out; retry cleanup with run ID ${run}.`)
    }
    for (const pool of pools) {
      await this.deleteResource('stream-pool', pool)
    }
    const deadline = Date.now() + 600_000
    for (let index = 0; index < this.attempts && Date.now() < deadline; index++) {
      const remainingPools = await this.pools(run)
      const streams = await this.list('stream')
      const remainingStreams = streams.filter(
        stream =>
          ids.has(stream['stream-pool-id'] ?? '') ||
          (stream['freeform-tags']?.purpose === purpose &&
            stream['freeform-tags']?.repository === repository &&
            stream['freeform-tags']?.run === run)
      )
      if (remainingPools.length === 0 && remainingStreams.length === 0) {
        console.error(`Verified Oracle Streaming cleanup: region=${this.config.region} run=${run}`)
        return
      }
      await this.wait()
    }
    throw new UserError(`Oracle Streaming cleanup timed out; retry cleanup with run ID ${run}.`)
  }

  async withResources (run: string, smoke: () => Promise<void>) {
    await this.provision(run)
    try {
      await smoke()
    } finally {
      await this.cleanup(run)
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const action = process.argv[2]
    const run = process.argv[3] ?? ''
    if (!['provision', 'connection', 'cleanup', 'run'].includes(action)) {
      throw new UserError(
        'Usage: node scripts/oracle-streaming-resources.ts <provision|connection|cleanup|run> <run-id>'
      )
    }
    const resources = new OracleStreamingResources({
      compartment: process.env.OCI_COMPARTMENT_ID ?? '',
      region: process.env.OCI_CLI_REGION ?? ''
    })
    const names = resourceNames(resources.config, run)
    console.error(`Oracle Streaming resources: pool=${names.pool} run=${run}`)
    if (action === 'provision') {
      console.log(await resources.provision(run))
    } else if (action === 'connection') {
      // Only non-secret connection settings are printed, as JSON rather than executable shell code.
      console.log(
        JSON.stringify(await resources.connection(run, process.env.ORACLE_STREAMING_USERNAME_PREFIX ?? ''), null, 2)
      )
    } else if (action === 'cleanup') {
      await resources.cleanup(run)
    } else {
      const prefix = process.env.ORACLE_STREAMING_USERNAME_PREFIX ?? ''
      if (!prefix || !process.env.ORACLE_STREAMING_AUTH_TOKEN) {
        throw new UserError('ORACLE_STREAMING_USERNAME_PREFIX and ORACLE_STREAMING_AUTH_TOKEN are required.')
      }
      await resources.withResources(run, async () => {
        const connection = await resources.connection(run, prefix)
        // Reuse the resources, but launch independent test processes to verify isolation from retained records.
        for (let iteration = 1; iteration <= 2; iteration++) {
          console.log(`Oracle Streaming smoke execution ${iteration}/2`)
          await new Promise<void>((resolve, reject) => {
            const child = spawn(process.execPath, ['--test', 'test/e2e/oracle-streaming.e2e-test.ts'], {
              cwd: fileURLToPath(new URL('../', import.meta.url)),
              env: { ...process.env, ...connection },
              stdio: 'inherit',
              timeout: 300_000,
              killSignal: 'SIGKILL'
            })
            child.once('error', () => reject(new UserError('Could not start the Oracle Streaming smoke test.')))
            child.once('exit', code => {
              if (code === 0) {
                resolve()
              } else {
                reject(new UserError('Oracle Streaming smoke test failed or timed out.'))
              }
            })
          })
        }
      })
    }
  } catch (error) {
    console.error(
      error instanceof UserError
        ? `${error.code}: ${error.message}`
        : `${UserError.code}: Oracle Streaming automation failed unexpectedly.`
    )
    process.exitCode = 1
  }
}

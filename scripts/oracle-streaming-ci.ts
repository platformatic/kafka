import { execFile } from 'node:child_process'
import { appendFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { UserError } from '../src/errors.ts'
import { loginSession } from './oracle-streaming-auth.ts'
import {
  OracleStreamingResources,
  resourceNames,
  type Command,
  type Configuration
} from './oracle-streaming-resources.ts'

const execute = promisify(execFile)
type Session = Awaited<ReturnType<typeof loginSession>>
type RunStatus = { status: string; path: string; head_branch: string; event: string }

export function renewingCommand (
  login: () => Promise<Session> = loginSession,
  run: (args: string[], environment: Session['environment']) => Promise<string> = async (args, environment) => {
    try {
      const { stdout } = await execute('oci', args, {
        env: { ...process.env, ...environment },
        timeout: 60_000,
        maxBuffer: 4 * 1024 * 1024
      })
      return stdout.trim()
    } catch {
      // Never expose CLI output or transport errors, which can contain session credentials.
      throw new UserError('Oracle Streaming CI resource command failed or timed out.')
    }
  },
  now: () => number = Date.now
): Command {
  let session: Session | undefined
  return async args => {
    // Refresh before each bounded operation when needed, including polling and teardown after smoke failure.
    if (!session || now() >= session.expiresAt) {
      session = await login()
      if (session.expiresAt <= now()) {
        throw new UserError('Oracle Streaming WIF session is already expired.')
      }
    }
    return run(args, session.environment)
  }
}

export async function recover (
  config: Configuration,
  command: Command,
  github: (run: string) => Promise<RunStatus>,
  wait?: () => Promise<void>,
  attempts?: number
) {
  if (!config.owner) {
    throw new UserError('Oracle Streaming recovery requires CI repository ownership.')
  }
  const discovery = new OracleStreamingResources(config, command, wait, attempts)
  // Include tagged orphan streams in discovery, so a missing pool cannot hide a billing/resource leak.
  const resources = [...(await discovery.list('stream-pool')), ...(await discovery.list('stream'))]
  const candidates = new Map<string, { run: string; attempt: string }>()
  for (const resource of resources) {
    const tags = resource['freeform-tags']
    if (tags?.purpose !== 'kafka-oracle-streaming-regression' || tags.repository !== config.owner.repository) {
      continue
    }
    if (!/^\d+$/.test(tags.run ?? '') || !/^\d+$/.test(tags.attempt ?? '')) {
      throw new UserError('Oracle Streaming recovery found invalid CI ownership tags.')
    }
    candidates.set(`${tags.run}/${tags.attempt}`, { run: tags.run, attempt: tags.attempt })
  }
  const failures: string[] = []
  for (const { run, attempt } of candidates.values()) {
    try {
      const status = await github(run)
      if (
        status.path !== '.github/workflows/regression.yml' ||
        status.head_branch !== 'main' ||
        !['completed', 'queued', 'in_progress', 'waiting', 'pending', 'requested'].includes(status.status)
      ) {
        throw new UserError('Oracle Streaming recovery could not verify the owning workflow.')
      }
      // Inspect the current run status, not an old attempt that may have been re-run since discovery.
      if (status.status !== 'completed') {
        console.log(`Keeping Oracle Streaming run ${run}/${attempt}: ${status.status}.`)
        continue
      }
      const ownedConfig = { ...config, owner: { repository: config.owner.repository, attempt } }
      const pools = resources.filter(
        resource =>
          resource['stream-pool-id'] === undefined &&
          resource['freeform-tags']?.run === run &&
          resource['freeform-tags']?.attempt === attempt &&
          resource['freeform-tags']?.purpose === 'kafka-oracle-streaming-regression' &&
          resource['freeform-tags']?.repository === config.owner!.repository
      )
      if (pools.some(pool => pool.name !== resourceNames(ownedConfig, run).pool)) {
        throw new UserError('Oracle Streaming recovery found an unexpected pool name.')
      }
      await new OracleStreamingResources(ownedConfig, command, wait, attempts).cleanup(run)
    } catch {
      failures.push(`${run}/${attempt}`)
      console.error(`Could not verify or clean Oracle Streaming run ${run}/${attempt}.`)
    }
  }
  if (failures.length > 0) {
    throw new UserError(`Oracle Streaming recovery incomplete: ${failures.join(', ')}.`)
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.env.GITHUB_ACTIONS !== 'true' || process.env.GITHUB_REF !== 'refs/heads/main') {
      throw new UserError('Oracle Streaming CI requires a trusted GitHub Actions workflow ref.')
    }
    const config: Configuration = {
      compartment: process.env.OCI_COMPARTMENT_ID ?? '',
      region: process.env.OCI_CLI_REGION ?? '',
      owner: { repository: process.env.GITHUB_REPOSITORY ?? '', attempt: process.env.GITHUB_RUN_ATTEMPT ?? '' }
    }
    const run = process.env.GITHUB_RUN_ID ?? ''
    resourceNames(config, run)
    const command = renewingCommand()
    const resources = new OracleStreamingResources(config, command)
    const action = process.argv[2]
    if (action === 'recover') {
      await recover(config, command, async id => {
        try {
          const { stdout } = await execute('gh', ['api', `repos/${config.owner!.repository}/actions/runs/${id}`], {
            timeout: 30_000,
            maxBuffer: 1024 * 1024
          })
          return JSON.parse(stdout)
        } catch {
          throw new UserError('Cannot verify the GitHub run for Oracle Streaming recovery.')
        }
      })
    } else if (action === 'provision') {
      await resources.provision(run)
    } else if (action === 'credentials') {
      if (!process.env.GITHUB_ENV) {
        throw new UserError('Oracle Streaming CI credentials require GITHUB_ENV.')
      }
      const connection = await resources.connection(run, process.env.ORACLE_STREAMING_USERNAME_PREFIX ?? '')
      await appendFile(
        process.env.GITHUB_ENV,
        Object.entries(connection)
          .map(([key, value]) => `${key}=${value}\n`)
          .join('')
      )
    } else if (action === 'cleanup') {
      await resources.cleanup(run)
    } else {
      throw new UserError('Usage: node scripts/oracle-streaming-ci.ts <provision|credentials|cleanup|recover>')
    }
  } catch (error) {
    console.error(
      error instanceof UserError
        ? `${error.code}: ${error.message}`
        : `${UserError.code}: Oracle Streaming CI failed unexpectedly.`
    )
    process.exitCode = 1
  }
}

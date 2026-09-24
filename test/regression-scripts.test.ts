import { deepStrictEqual, match, strictEqual } from 'node:assert'
import { spawnSync } from 'node:child_process'
import { chmod, copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test, type TestContext } from 'node:test'

async function fixture (t: TestContext) {
  const root = await mkdtemp(join(tmpdir(), 'kafka-regression-scripts-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const scripts = join(root, 'scripts')
  const bin = join(root, 'bin')
  const trace = join(root, 'commands.log')
  await mkdir(scripts)
  await mkdir(bin)
  await writeFile(trace, '')
  for (const name of ['run-regression-suite.sh', 'run-protocol-load-test.sh', 'collect-kafka-diagnostics.sh']) {
    const target = join(scripts, name)
    await copyFile(new URL(`../scripts/${name}`, import.meta.url), target)
    await chmod(target, 0o755)
  }

  const commands = {
    pnpm: `#!/bin/sh
printf 'pnpm %s\n' "$*" >> "$TRACE"
if [ "$2" = "$FAIL_SUITE" ]; then
  exit 7
fi
`,
    docker: `#!/bin/sh
printf 'docker %s\n' "$*" >> "$TRACE"
case "$*" in
  'compose exec -T broker-single kafka-topics --version')
    if [ "$LEGACY_KAFKA" = true ]; then
      printf 'version is not a recognized option\n' >&2
      exit 1
    fi
    printf '4.0.0\n'
    ;;
  'compose images broker-single') printf 'broker-single confluentinc/cp-kafka 4.1.0\n' ;;
  'compose ps --all') printf 'broker-single Exited (1)\n' ;;
  'compose logs --no-color') printf 'broker startup error\n' ;;
  *) exit 91 ;;
esac
`,
    taskset: `#!/bin/sh
shift 2
exec "$@"
`
  }
  for (const [name, contents] of Object.entries(commands)) {
    const target = join(bin, name)
    await writeFile(target, contents, { mode: 0o755 })
  }
  await writeFile(
    join(scripts, 'node'),
    `#!/bin/sh
printf 'node %s pin=%s artifact=%s\n' "$*" "$PROTOCOL_BENCH_PIN" "$PROTOCOL_BENCH_ARTIFACT" >> "$TRACE"
if [ "$1" = "$FAIL_SCRIPT" ]; then
  exit 9
fi
`,
    { mode: 0o755 }
  )

  return {
    root,
    commands: () => readFile(trace, 'utf8'),
    run: (script: string, args: string[], env: Record<string, string> = {}) =>
      spawnSync('bash', [join(scripts, script), ...args], {
        cwd: root,
        encoding: 'utf8',
        env: {
          ...process.env,
          PATH: `${bin}:${process.env.PATH}`,
          TRACE: trace,
          FAIL_SUITE: '',
          FAIL_SCRIPT: '',
          LEGACY_KAFKA: '',
          PROTOCOL_BENCH_PIN: '',
          PROTOCOL_BENCH_ARTIFACT: '',
          PROTOCOL_BENCH_ARTIFACT_PREFIX: '',
          ...env
        }
      })
  }
}

test('functional regression preserves suite failures and continues without managing Docker', async t => {
  const context = await fixture(t)
  const result = context.run('run-regression-suite.sh', ['modern', '8.2.0', 'modern'], {
    FAIL_SUITE: 'test:integrity'
  })
  strictEqual(result.status, 1, result.stderr)
  deepStrictEqual((await context.commands()).trim().split('\n'), ['pnpm run test:integrity', 'pnpm run test:memory'])
  const report = await readFile(join(context.root, 'regression/artifacts/modern-report.md'), 'utf8')
  match(report, /integrity.*Failed \(exit 7\)/)
  match(report, /memory.*Passed/)
})

test('legacy, smoke and performance suites use caller-owned clusters', async t => {
  for (const [mode, suite] of [
    ['legacy', 'test:compat'],
    ['redpanda', 'test:e2e:redpanda'],
    ['eventhubs', 'test:e2e:eventhubs'],
    ['performance', 'test:performance']
  ]) {
    const context = await fixture(t)
    const result = context.run('run-regression-suite.sh', [mode, 'test-version', mode])
    strictEqual(result.status, 0, result.stderr)
    strictEqual((await context.commands()).trim(), `pnpm run ${suite}`)
  }
})

test('each protocol invocation runs one sweep with its tier pinning and no cluster mutations', async t => {
  for (const tier of ['1', '2']) {
    for (const sweep of ['produce', 'consume']) {
      const context = await fixture(t)
      // The legacy CLI must reject --version, just as Kafka 1.1.0 does.
      const result = context.run('run-regression-suite.sh', ['protocol', 'test-version', 'lane', tier, sweep], {
        LEGACY_KAFKA: String(tier === '2')
      })
      strictEqual(result.status, 0, result.stderr)
      const expected = [
        'node --version pin= artifact=',
        tier === '1'
          ? 'docker compose exec -T broker-single kafka-topics --version'
          : 'docker compose images broker-single'
      ]
      if (tier === '1') {
        expected.push('node benchmarks/protocol-versions/guards.ts pin=true artifact=')
      }
      expected.push(
        `node benchmarks/protocol-versions/${sweep}-versions.ts pin=${tier === '1'} artifact=lane-tier${tier}-${sweep}`
      )
      deepStrictEqual((await context.commands()).trim().split('\n'), expected)
      if (tier === '2') {
        match(result.stdout, /confluentinc\/cp-kafka 4\.1\.0/)
      }
    }
  }
})

test('a failed modern version query is not hidden by the legacy compatibility handling', async t => {
  const context = await fixture(t)
  const result = context.run('run-regression-suite.sh', ['protocol', '8.2.0', 'version', '1', 'produce'], {
    LEGACY_KAFKA: 'true'
  })
  strictEqual(result.status, 1, result.stderr)
  match(result.stdout, /version is not a recognized option/)
  strictEqual((await context.commands()).includes('produce-versions.ts'), false)
  match(
    await readFile(join(context.root, 'regression/artifacts/version-report.md'), 'utf8'),
    /protocol-load.*Failed \(exit 1\)/
  )
})

test('a failed conversion guard prevents benchmarking and fails the lane report', async t => {
  const context = await fixture(t)
  const result = context.run('run-regression-suite.sh', ['protocol', '8.2.0', 'guard', '1', 'produce'], {
    FAIL_SCRIPT: 'benchmarks/protocol-versions/guards.ts'
  })
  strictEqual(result.status, 1, result.stderr)
  strictEqual((await context.commands()).includes('produce-versions.ts'), false)
  match(
    await readFile(join(context.root, 'regression/artifacts/guard-report.md'), 'utf8'),
    /protocol-load.*Failed \(exit 9\)/
  )
})

test('protocol runner rejects ambiguous live invocations before touching the broker', async t => {
  for (const args of [['all'], ['1'], ['2', 'all']]) {
    const context = await fixture(t)
    const result = context.run('run-protocol-load-test.sh', args)
    strictEqual(result.status, 2, result.stderr)
    strictEqual(await context.commands(), '')
  }
})

test('diagnostics preserve exited-container state and broker errors without cleanup', async t => {
  const context = await fixture(t)
  const result = context.run('collect-kafka-diagnostics.sh', ['failed-start'])
  strictEqual(result.status, 0, result.stderr)
  match(
    await readFile(join(context.root, 'regression/artifacts/failed-start-containers.log'), 'utf8'),
    /broker-single Exited \(1\)/
  )
  match(
    await readFile(join(context.root, 'regression/artifacts/failed-start-broker.log'), 'utf8'),
    /broker startup error/
  )
  deepStrictEqual((await context.commands()).trim().split('\n'), [
    'docker compose ps --all',
    'docker compose logs --no-color'
  ])
})

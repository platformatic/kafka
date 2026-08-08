# @platformatic/kafka Project Guide

## Build & Test Commands

```
# Build the project
npm run build

# Run all tests
npm test

# Run a single test file
node --test 'test/path/to/file.test.ts'

# Run a single test file with coverage (local, c8 — Node 22/24)
c8 -c test/config/c8-local.json node --test 'test/path/to/file.test.ts'

# CI uses Node's built-in coverage (works on Node 26, where c8/yargs breaks):
# node --test --experimental-test-coverage --test-coverage-include='src/**' 'test/path/to/file.test.ts'

# Lint the code
npm run lint

# Run memory tests (manual — not part of CI)
# Requires Docker with 3-broker cluster running (docker compose up -d --wait)
npm run test:memory

# Run the API version compatibility sweeps (separate CI job, not part of npm test)
npm run test:compat
```

Memory tests (`test/memory/*.memory-test.ts`) use `--expose-gc` and a 3-broker cluster
with sustained backpressure to detect heap leaks. They are excluded from CI due to resource
requirements but should be run manually when modifying the consumer stream, fetch loop, or
backpressure handling. Use the `.memory-test.ts` suffix for new memory tests.

Compatibility tests (`test/integration/*.compat-test.ts`) exercise the legacy API version codecs
against a real broker. `Base[kGetApi]` always negotiates the newest version a broker advertises, so
without pinning, every codec below the maximum is dead code: `pinApiVersions` in
`test/helpers/api-versions.ts` rewrites the negotiated range, and `forEachVersion` sweeps an API
across every version the broker still accepts, reporting the unreachable ones as diagnostics.
They run in their own CI job against the oldest and newest brokers in the matrix, because Kafka 4.0
raised the minimum accepted version of several APIs. Use the `.compat-test.ts` suffix for new ones.

The delegation token sweeps need `-f docker-compose.delegation-tokens.yml`, which only works on
Confluent 7.6.0 or later: KRaft gained delegation tokens in Apache Kafka 3.6, and a 3.5 broker
configured with a token secret key refuses to start. They skip themselves elsewhere.

`docker-compose.legacy.yml` runs the same sweeps against Apache Kafka 1.1.0, the oldest supported
broker. It is a standalone stack rather than an override because pre-KRaft brokers need ZooKeeper
and must not receive the KRaft settings, and compose overrides cannot remove keys. The whole suite
runs against it, and it is the only broker which reaches the delegation token v0 codecs.

## Code Style Guidelines

- **TypeScript**: Strict typing with explicit type imports `import type { X }`. Avoid `any` all the times. Ensure types compliance.
- **Formatting**: 2-space indentation, no semicolons, single quotes
- **Imports**: Group related imports, use explicit `.ts` extensions
- **Naming**: camelCase for variables/functions, PascalCase for classes/types
- **Errors**: Extend GenericError class with descriptive error codes prefixed with `PLT_KFK_`
- **Error Handling**: Use try/catch with specific error types.
- **API Design**: Consistent API interface with options objects and promise-based returns.
- **Testing**: Node.js test runner with deep assertions. Use `deepStrictEqual` when appropriate. Never modify the `src` folder. The test file for `src/foo/bar/baz.ts` is `test/foo/bar/baz.test.ts`

## Node Requirements

- Node.js >= 22.22.0 or >= 24.6.0

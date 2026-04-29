# @platformatic/kafka Project Guide

## Build & Test Commands

```
# Build the project
npm run build

# Run all tests
npm test

# Run a single test file
node --test 'test/path/to/file.test.ts'

# Run a single test file with coverage
c8 -c test/config/c8-local.json node --test --test 'test/path/to/file.test.ts'

# Lint the code
npm run lint

# Run memory tests (manual — not part of CI)
# Requires Docker with 3-broker cluster running (docker compose up -d --wait)
npm run test:memory
```

Memory tests (`test/memory/*.memory-test.ts`) use `--expose-gc` and a 3-broker cluster
with sustained backpressure to detect heap leaks. They are excluded from CI due to resource
requirements but should be run manually when modifying the consumer stream, fetch loop, or
backpressure handling. Use the `.memory-test.ts` suffix for new memory tests.

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

# @platformatic/kafka Project Guide

## Build & Test Commands

```
# Build the project
npm run build

# Run all tests
npm test

# Run a single test file
node --env-file=test/config/env --test 'test/path/to/file.test.ts'

# Run a single test file with coverage
c8 -c test/config/c8-local.json node --test --env-file=test/config/env --test 'test/path/to/file.test.ts'

# Lint the code
npm run lint
```

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

- Node.js >= 22.14.0

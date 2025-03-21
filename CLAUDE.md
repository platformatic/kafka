# @platformatic/kafka Project Guide

## Build & Test Commands
```
# Build the project
npm run build

# Run all tests
npm test

# Run a single test file
node --env-file=test/config/env --test 'test/path/to/file.test.ts'

# Lint the code
npm run lint

# Generate APIs from protocol specs
npm run generate:apis

# Generate error codes
npm run generate:errors
```

## Code Style Guidelines
- **TypeScript**: Strict typing with explicit type imports `import type { X }`
- **Formatting**: 2-space indentation, no semicolons, single quotes
- **Imports**: Group related imports, use explicit `.ts` extensions
- **Naming**: camelCase for variables/functions, PascalCase for classes/types
- **Errors**: Extend GenericError class with descriptive error codes prefixed with `PLT_KFK_`
- **Error Handling**: Use try/catch with specific error types, leverage `invokeAPIWithRetry` for retryable operations
- **API Design**: Consistent API interface with options objects and promise-based returns
- **Testing**: Node.js test runner with deep assertions

## Node Requirements
- Node.js >= 22.14.0
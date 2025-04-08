import { notStrictEqual, strictEqual } from 'assert'
import { afterEach, test } from 'node:test'
import { createDebugLogger, logger, loggers, setDebugLoggers, setLogger } from '../src/index.ts'

afterEach(() => {
  setDebugLoggers(process.env.NODE_DEBUG)
  setLogger(process.env.LOG_LEVEL)
})

test('logger should respect LOG_LEVEL environment variable', () => {
  strictEqual(logger.level, 'debug')

  setLogger('info')
  strictEqual(logger.level, 'info')
})

test('createDebugLogger should return null when no debug patterns match', () => {
  strictEqual(createDebugLogger('test-logger'), null)
})

test('createDebugLogger should return a logger when a debug pattern matches exactly', () => {
  setDebugLoggers('test-logger')
  const debugLogger = createDebugLogger('test-logger')

  notStrictEqual(debugLogger, null)
  strictEqual(debugLogger?.bindings().name, 'test-logger')
})

test('createDebugLogger should return a logger when a debug pattern matches with wildcard', () => {
  setDebugLoggers('test-*')
  const debugLogger = createDebugLogger('test-logger')

  notStrictEqual(debugLogger, null)
  strictEqual(debugLogger?.bindings().name, 'test-logger')
})

test('createDebugLogger should handle multiple patterns in NODE_DEBUG', () => {
  setDebugLoggers('foo,test-*,bar')

  const debugLoggerFoo = createDebugLogger('foo')
  const debugLoggerTest = createDebugLogger('test-logger')
  const debugLoggerBar = createDebugLogger('bar')
  const debugLoggerBaz = createDebugLogger('baz')

  notStrictEqual(debugLoggerFoo, null)
  notStrictEqual(debugLoggerTest, null)
  notStrictEqual(debugLoggerBar, null)
  strictEqual(debugLoggerBaz, null)
})

test('createDebugLogger should ignore empty patterns', () => {
  setDebugLoggers(',test-logger,,')

  const debugLogger = createDebugLogger('test-logger')
  notStrictEqual(debugLogger, null)
})

test('loggers should be pre-configured for known namespaces', () => {
  setDebugLoggers('plt-kafka:*')

  notStrictEqual(loggers.protocol, null)
  notStrictEqual(loggers.client, null)
  notStrictEqual(loggers.producer, null)
  notStrictEqual(loggers.consumer, null)
  notStrictEqual(loggers['consumer:heartbeat'], null)
  notStrictEqual(loggers.admin, null)
})

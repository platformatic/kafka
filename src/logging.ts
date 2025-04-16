import { type Logger, pino } from 'pino'

export function setDebugLoggers (debug: string | undefined): void {
  enabledDebugLoggers = (debug ?? '')
    .split(',')
    .map(x => {
      x = x.trim()
      return x.length ? new RegExp(`^${x.replaceAll('*', '.*')}$`) : null
    })
    .filter(x => x) as RegExp[]

  loggers = {
    protocol: createDebugLogger('plt-kafka:protocol'),
    client: createDebugLogger('plt-kafka:client'),
    producer: createDebugLogger('plt-kafka:producer'),
    consumer: createDebugLogger('plt-kafka:consumer'),
    'consumer:heartbeat': createDebugLogger('plt-kafka:consumer:heartbeat'),
    admin: createDebugLogger('plt-kafka:admin')
  }
}

export function setLogger (level: string | undefined): void {
  logger = pino({
    level: level ?? 'debug',
    transport: {
      target: 'pino-pretty'
    }
  })
}

export function createDebugLogger (name: string | undefined): Logger | null {
  name ??= ''

  if (!enabledDebugLoggers.some(r => r.test(name))) {
    return null
  }

  return logger.child({ name })
}

// These two methods are defined via functions to ensure testability
export let logger: Logger
export let enabledDebugLoggers: RegExp[]
export let loggers: Record<string, Logger | null> = {}

setLogger(process.env.LOG_LEVEL)
setDebugLoggers(process.env.NODE_DEBUG ?? '')

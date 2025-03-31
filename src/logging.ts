import { type Logger, pino } from 'pino'

const toDebug = (process.env.NODE_DEBUG ?? '')
  .split(',')
  .map(x => {
    x = x.trim()
    return x.length ? new RegExp(`^${x.replaceAll('*', '.*')}$`) : null
  })
  .filter(x => x) as RegExp[]

export const logger = pino({
  level: process.env.LOG_LEVEL ?? 'debug',
  transport: {
    target: 'pino-pretty'
  }
})

export function createDebugLogger (name: string): Logger | null {
  if (!toDebug.some(r => r.test(name))) {
    return null
  }

  return logger.child({ name })
}

export const loggers: Record<string, Logger | null> = {
  protocol: createDebugLogger('plt-kafka:protocol'),
  client: createDebugLogger('plt-kafka:client'),
  producer: createDebugLogger('plt-kafka:producer'),
  consumer: createDebugLogger('plt-kafka:consumer'),
  'consumer:heartbeat': createDebugLogger('plt-kafka:consumer:heartbeat'),
  admin: createDebugLogger('plt-kafka:admin')
}

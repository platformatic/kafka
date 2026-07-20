import type { LogEntry, Logger, logCreator } from './types.ts'
import { logLevel } from './constants.ts'

const labels = {
  [logLevel.ERROR]: 'ERROR',
  [logLevel.WARN]: 'WARN',
  [logLevel.INFO]: 'INFO',
  [logLevel.DEBUG]: 'DEBUG'
} as const

function defaultLogCreator () {
  return ({ namespace, level, label, log }: LogEntry) => {
    const prefix = namespace ? `[${namespace}] ` : ''
    const message = JSON.stringify({ level: label, ...log, message: `${prefix}${log.message}` })
    switch (level) {
      case logLevel.INFO:
        return console.info(message)
      case logLevel.ERROR:
        return console.error(message)
      case logLevel.WARN:
        return console.warn(message)
      case logLevel.DEBUG:
        return console.log(message)
    }
  }
}

export function createLogger (initialLevel: number = logLevel.INFO, creator?: logCreator): Logger {
  const evaluateLevel = (candidate?: number) => {
    const envLevel = logLevel[(process.env.KAFKAJS_LOG_LEVEL || '').toUpperCase() as keyof typeof logLevel]
    return envLevel ?? candidate
  }
  let level = evaluateLevel(initialLevel)!
  const write = (creator ?? defaultLogCreator)(level)

  function namespaced (namespace = '', namespaceLevel?: number): Logger {
    const evaluatedNamespaceLevel = evaluateLevel(namespaceLevel)
    const currentLevel = () => evaluatedNamespaceLevel ?? level
    const method = (methodLevel: keyof typeof labels) =>
      (message: string, extra: object = {}) => {
        if (methodLevel > currentLevel()) {
          return
        }
        write({
          namespace,
          level: methodLevel,
          label: labels[methodLevel],
          log: { timestamp: new Date().toISOString(), logger: 'kafkajs', message, ...extra }
        })
      }

    return {
      error: method(logLevel.ERROR),
      warn: method(logLevel.WARN),
      info: method(logLevel.INFO),
      debug: method(logLevel.DEBUG),
      namespace: namespaced,
      setLogLevel (newLevel) {
        level = newLevel
      }
    }
  }

  return namespaced()
}

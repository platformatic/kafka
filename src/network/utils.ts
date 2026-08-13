import { type Broker } from './connection.ts'

const BRACKETED_BROKER_RE = /^\[([^\]]+)](?::(\d*))?$/

export function parseBroker (broker: Broker | string, defaultPort: number = 9092): Broker {
  if (typeof broker !== 'string') {
    const host = stripBrackets(broker.host)

    if (host === broker.host) {
      return broker
    }

    return { ...broker, host }
  }

  const bracketed = BRACKETED_BROKER_RE.exec(broker)
  if (bracketed) {
    return { host: bracketed[1], port: parsePort(bracketed[2], defaultPort) }
  }

  const separator = broker.lastIndexOf(':')

  // Bare IPv6 addresses contain multiple colons and no port. The host:port form
  // is unambiguous only with a single colon, or with brackets around the host.
  if (separator === -1 || broker.indexOf(':') !== separator) {
    return { host: broker, port: defaultPort }
  }

  return { host: broker.slice(0, separator), port: parsePort(broker.slice(separator + 1), defaultPort) }
}

function parsePort (port: string | undefined, defaultPort: number): number {
  return port === undefined || port.length === 0 ? defaultPort : Number(port)
}

function stripBrackets (host: string): string {
  if (host.startsWith('[') && host.endsWith(']')) {
    return host.slice(1, -1)
  }

  return host
}

import { type Broker } from './connection.ts'

export function parseBroker (broker: Broker | string, defaultPort: number = 9092): Broker {
  if (typeof broker === 'string') {
    if (broker.includes(':')) {
      const [host, port] = broker.split(':')
      return { host, port: Number(port) }
    } else {
      return { host: broker, port: defaultPort }
    }
  }

  return broker
}

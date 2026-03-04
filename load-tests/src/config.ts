export const config = {
  kafka: {
    bootstrapBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9001').split(','),
    clientId: 'load-test',
  },
  reportIntervalMs: 5_000,
  drainTimeoutMs: 30_000,
} as const

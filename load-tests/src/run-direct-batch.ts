import { parseArgs } from 'node:util'
import { runDirectBatchLoadTest } from './direct-batch-load-generator.ts'

const { values } = parseArgs({
  options: {
    rate: { type: 'string', short: 'r', default: '1000' },
    duration: { type: 'string', short: 'd', default: '60' },
    batch: { type: 'string', short: 'b', default: '100' },
    'consumer-batch': { type: 'string', default: '50' },
    'consumer-timeout': { type: 'string', default: '200' },
  },
  strict: true,
})

await runDirectBatchLoadTest({
  rate: Number.parseInt(values.rate!, 10),
  duration: Number.parseInt(values.duration!, 10),
  batchSize: Number.parseInt(values.batch!, 10),
  consumerBatchSize: Number.parseInt(values['consumer-batch']!, 10),
  consumerBatchTimeoutMs: Number.parseInt(values['consumer-timeout']!, 10),
})

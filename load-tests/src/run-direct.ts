import { parseArgs } from 'node:util'
import { runDirectLoadTest } from './direct-load-generator.ts'

const { values } = parseArgs({
  options: {
    rate: { type: 'string', short: 'r', default: '1000' },
    duration: { type: 'string', short: 'd', default: '60' },
    batch: { type: 'string', short: 'b', default: '100' },
  },
  strict: true,
})

await runDirectLoadTest({
  rate: Number.parseInt(values.rate!, 10),
  duration: Number.parseInt(values.duration!, 10),
  batchSize: Number.parseInt(values.batch!, 10),
})

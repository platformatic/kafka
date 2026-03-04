import { parseArgs } from 'node:util'
import { runStressLoadTest } from './stress-load-generator.ts'

const { values } = parseArgs({
  options: {
    rate: { type: 'string', short: 'r', default: '5000' },
    duration: { type: 'string', short: 'd', default: '120' },
    batch: { type: 'string', short: 'b', default: '100' },
    'handler-delay': { type: 'string', default: '10' },
    'cpu-work': { type: 'string', default: '2' },
    'retry-rate': { type: 'string', default: '0.1' },
    partitions: { type: 'string', default: '6' },
    'payload-kb': { type: 'string', default: '2' },
    topics: { type: 'string', default: '4' },
    'max-wait-time': { type: 'string', default: '5000' },
    'max-bytes': { type: 'string', default: '10485760' },
  },
  strict: true,
})

await runStressLoadTest({
  rate: Number.parseInt(values.rate!, 10),
  duration: Number.parseInt(values.duration!, 10),
  batchSize: Number.parseInt(values.batch!, 10),
  handlerDelayMs: Number.parseInt(values['handler-delay']!, 10),
  cpuWorkMs: Number.parseInt(values['cpu-work']!, 10),
  retryRate: Number.parseFloat(values['retry-rate']!),
  partitions: Number.parseInt(values.partitions!, 10),
  payloadKb: Number.parseInt(values['payload-kb']!, 10),
  topics: Number.parseInt(values.topics!, 10),
  maxWaitTime: Number.parseInt(values['max-wait-time']!, 10),
  maxBytes: Number.parseInt(values['max-bytes']!, 10),
})

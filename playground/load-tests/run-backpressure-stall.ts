import { parseArgs } from 'node:util'
import { runBackpressureStallTest } from './backpressure-stall-test.ts'

const { values } = parseArgs({
  options: {
    topics: { type: 'string', short: 't', default: '15' },
    'messages-per-topic': { type: 'string', short: 'm', default: '1000' },
    'batch-size': { type: 'string', short: 'b', default: '500' },
    'handler-delay': { type: 'string', short: 'd', default: '5' },
    'high-water-mark': { type: 'string', default: '1024' },
    timeout: { type: 'string', default: '60' }
  },
  strict: true
})

await runBackpressureStallTest({
  topicCount: Number.parseInt(values.topics!, 10),
  messagesPerTopic: Number.parseInt(values['messages-per-topic']!, 10),
  batchSize: Number.parseInt(values['batch-size']!, 10),
  handlerDelayMs: Number.parseInt(values['handler-delay']!, 10),
  consumerHighWaterMark: Number.parseInt(values['high-water-mark']!, 10),
  timeoutSeconds: Number.parseInt(values.timeout!, 10)
})

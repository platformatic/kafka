import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'

export interface WorkSimulatorOptions {
  /** Async I/O delay per message in ms (simulates DB writes, API calls) */
  handlerDelayMs: number
  /** CPU-bound work per message in ms (simulates JSON serialization, schema validation) */
  cpuWorkMs: number
  /** Probability (0-1) that a handler "fails" and triggers retry logic */
  retryRate: number
}

/**
 * Simulates MQT's processing overhead per message:
 * - randomUUID() + JSON.stringify() + object spread (transaction ID, error context, message cloning)
 * - Configurable async I/O delay (DB writes, API calls)
 * - Configurable CPU-bound work (tight loop for serialization overhead)
 * - Handler retry logic with exponential backoff (1ms, 2ms, 4ms)
 */
export async function simulateWork(
  message: Record<string, unknown>,
  options: WorkSimulatorOptions,
): Promise<void> {
  // MQT overhead: transaction ID, error context serialization, parsed message cloning
  simulateMqtOverhead(message)

  // Async I/O delay (DB writes, API calls)
  if (options.handlerDelayMs > 0) {
    await setTimeout(options.handlerDelayMs)
  }

  // CPU-bound work (JSON serialization, schema validation overhead, object cloning)
  if (options.cpuWorkMs > 0) {
    simulateCpuWork(options.cpuWorkMs)
  }

  // Handler retry logic with exponential backoff
  if (options.retryRate > 0 && shouldRetry(options.retryRate)) {
    for (let attempt = 0; attempt < 3; attempt++) {
      await setTimeout(2 ** attempt)
      if (!shouldRetry(options.retryRate)) break
    }
  }
}

/**
 * Simulates MQT overhead per message:
 * randomUUID() for transaction IDs, JSON.stringify() for error context,
 * object spread for parsed message cloning
 */
function simulateMqtOverhead(message: Record<string, unknown>): void {
  const _transactionId = randomUUID()
  const _serialized = JSON.stringify(message)
  const _clone = { ...message, _transactionId }
}

/**
 * Burns CPU time with a tight loop to simulate synchronous processing overhead
 * (JSON serialization, schema validation, object cloning in hot paths)
 */
function simulateCpuWork(ms: number): void {
  const end = performance.now() + ms
  while (performance.now() < end) {
    // Tight loop to burn CPU
  }
}

function shouldRetry(retryRate: number): boolean {
  return Math.random() < retryRate
}

import { PerformanceObserver } from 'node:perf_hooks'

export interface Timing {
  /** Nanoseconds per call, median across repetitions. */
  nsPerOp: number
  /** Interquartile range of nsPerOp, as a fraction of the median. */
  spread: number
  /** Milliseconds spent in GC across the whole measurement. */
  gcMs: number
  /** Client CPU microseconds per call, median across repetitions. */
  cpuUsPerOp: number
}

export interface TimeOptions {
  /** Calls per timed block. Timing a block rather than a call keeps hrtime overhead out of the result. */
  iterations: number
  repetitions: number
  warmups: number
}

/**
 * Times a synchronous function.
 *
 * Reports the median of per-block averages rather than a single total: one block that happens to
 * collide with a major GC would otherwise set the number for the whole version.
 */
export function time (fn: () => void, options: TimeOptions): Timing {
  const { iterations, repetitions, warmups } = options

  for (let index = 0; index < warmups * iterations; index++) {
    fn()
  }

  let gcMs = 0
  const observer = new PerformanceObserver(list => {
    for (const entry of list.getEntries()) {
      gcMs += entry.duration
    }
  })

  observer.observe({ entryTypes: ['gc'] })

  const wall: number[] = []
  const cpu: number[] = []

  for (let repetition = 0; repetition < repetitions; repetition++) {
    const startCpu = process.cpuUsage()
    const start = process.hrtime.bigint()

    for (let index = 0; index < iterations; index++) {
      fn()
    }

    const elapsed = Number(process.hrtime.bigint() - start)
    const usedCpu = process.cpuUsage(startCpu)

    wall.push(elapsed / iterations)
    cpu.push((usedCpu.user + usedCpu.system) / iterations)
  }

  observer.disconnect()

  const nsPerOp = median(wall)

  return {
    nsPerOp,
    spread: nsPerOp > 0 ? iqr(wall) / nsPerOp : 0,
    gcMs,
    cpuUsPerOp: median(cpu)
  }
}

/**
 * Iterations that keep a timed block at roughly a constant amount of work, so a 10000 record block
 * is not a hundred times slower to measure than a 100 record one.
 */
export function iterationsFor (recordsPerCall: number, targetRecords = 200_000): number {
  return Math.max(5, Math.ceil(targetRecords / Math.max(1, recordsPerCall)))
}

export function median (values: number[]): number {
  if (values.length === 0) {
    return 0
  }

  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)

  return sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2
}

export function iqr (values: number[]): number {
  if (values.length < 4) {
    return 0
  }

  const sorted = [...values].sort((a, b) => a - b)

  return quantile(sorted, 0.75) - quantile(sorted, 0.25)
}

function quantile (sorted: number[], fraction: number): number {
  const position = (sorted.length - 1) * fraction
  const lower = Math.floor(position)
  const upper = Math.ceil(position)

  return sorted[lower]! + (sorted[upper]! - sorted[lower]!) * (position - lower)
}

/** Deterministic shuffle, so an interleaved run order is reproducible between invocations. */
export function shuffle<Type> (items: Type[], seed: number): Type[] {
  const result = [...items]
  let state = seed >>> 0

  for (let index = result.length - 1; index > 0; index--) {
    state = (state * 1664525 + 1013904223) >>> 0

    const target = state % (index + 1)
    const swap = result[index]!

    result[index] = result[target]!
    result[target] = swap
  }

  return result
}

export function table (headers: string[], rows: (string | number)[][]): string {
  const cells = [headers, ...rows.map(row => row.map(cell => String(cell)))]
  const widths = headers.map((_, column) => Math.max(...cells.map(row => (row[column] ?? '').length)))
  const line = (row: string[]) =>
    '  ' + row.map((cell, column) => (column === 0 ? cell.padEnd(widths[column]!) : cell.padStart(widths[column]!))).join('  ')

  return [line(headers), '  ' + widths.map(width => '-'.repeat(width)).join('  '), ...cells.slice(1).map(line)].join('\n')
}

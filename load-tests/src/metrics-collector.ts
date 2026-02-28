export class MetricsCollector {
  private producedTotal = 0
  private consumedTotal = 0
  private windowProduced = 0
  private windowConsumed = 0
  private readonly perTopicConsumed = new Map<string, number>()
  private readonly latencies: number[] = []

  private readonly startTime = Date.now()
  private lastReportTime = Date.now()

  recordProduced(count: number): void {
    this.producedTotal += count
    this.windowProduced += count
  }

  recordConsumed(topic: string, loadtestTs?: number): void {
    this.consumedTotal++
    this.windowConsumed++
    this.perTopicConsumed.set(topic, (this.perTopicConsumed.get(topic) ?? 0) + 1)

    if (loadtestTs) {
      this.latencies.push(Date.now() - loadtestTs)
    }
  }

  get backlog(): number {
    return this.producedTotal - this.consumedTotal
  }

  get totalConsumed(): number {
    return this.consumedTotal
  }

  get totalProduced(): number {
    return this.producedTotal
  }

  report(): void {
    const now = Date.now()
    const windowSec = (now - this.lastReportTime) / 1000
    const elapsedSec = Math.round((now - this.startTime) / 1000)

    const producedRate = windowSec > 0 ? Math.round(this.windowProduced / windowSec) : 0
    const consumedRate = windowSec > 0 ? Math.round(this.windowConsumed / windowSec) : 0

    const latencyStats = this.computeLatencyStats()

    const topicBreakdown = [...this.perTopicConsumed.entries()]
      .map(([t, c]) => `${t}=${c.toLocaleString()}`)
      .join(' | ')

    console.log(`
=== Load Test Report (t=${elapsedSec}s) ===
  Produced:   ${this.producedTotal.toLocaleString()} total | ${producedRate}/sec
  Consumed:   ${this.consumedTotal.toLocaleString()} total | ${consumedRate}/sec
  Backlog:    ${this.backlog.toLocaleString()} messages
  Latency:    avg=${latencyStats.avg}ms | p50=${latencyStats.p50}ms | p95=${latencyStats.p95}ms | p99=${latencyStats.p99}ms
  Per Topic:  ${topicBreakdown || 'n/a'}
================================
`)

    this.windowProduced = 0
    this.windowConsumed = 0
    this.lastReportTime = now
  }

  printFinalReport(): void {
    const elapsedSec = (Date.now() - this.startTime) / 1000

    const latencyStats = this.computeLatencyStats()

    const topicBreakdown = [...this.perTopicConsumed.entries()]
      .map(([t, c]) => `${t}=${c.toLocaleString()}`)
      .join(' | ')

    console.log(`
╔══════════════════════════════════════╗
║        FINAL LOAD TEST REPORT        ║
╠══════════════════════════════════════╣
  Duration:   ${elapsedSec.toFixed(1)}s
  Produced:   ${this.producedTotal.toLocaleString()} total | ${Math.round(this.producedTotal / elapsedSec)}/sec avg
  Consumed:   ${this.consumedTotal.toLocaleString()} total | ${Math.round(this.consumedTotal / elapsedSec)}/sec avg
  Backlog:    ${this.backlog.toLocaleString()} messages
  Latency:    avg=${latencyStats.avg}ms | p50=${latencyStats.p50}ms | p95=${latencyStats.p95}ms | p99=${latencyStats.p99}ms
  Per Topic:  ${topicBreakdown || 'n/a'}
  Samples:    ${this.latencies.length.toLocaleString()} latency measurements
╚══════════════════════════════════════╝
`)
  }

  private computeLatencyStats(): { avg: number; p50: number; p95: number; p99: number } {
    if (this.latencies.length === 0) {
      return { avg: 0, p50: 0, p95: 0, p99: 0 }
    }

    const sorted = [...this.latencies].sort((a, b) => a - b)
    const sum = sorted.reduce((s, v) => s + v, 0)

    return {
      avg: Math.round(sum / sorted.length),
      p50: sorted[Math.floor(sorted.length * 0.5)]!,
      p95: sorted[Math.floor(sorted.length * 0.95)]!,
      p99: sorted[Math.floor(sorted.length * 0.99)]!,
    }
  }
}

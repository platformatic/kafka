// Interfaces to make the package compatible with prom-client

// Unused in this package
type RegistryContentType =
  | 'application/openmetrics-text; version=1.0.0; charset=utf-8'
  | 'text/plain; version=0.0.4; charset=utf-8'

// Unused in this package
export interface Metric {
  name?: string
  get(): Promise<unknown>
  reset: () => void
}
export interface Counter extends Metric {
  inc: (value?: number) => void
}

export interface Gauge extends Metric {
  inc: (value?: number) => void
  dec: (value?: number) => void
}

export interface Histogram {}

export interface Summary {}

export interface Registry {
  getSingleMetric: (name: string) => Counter | Gauge | any

  // Unused in this package
  metrics(): Promise<string>
  clear(): void
  resetMetrics(): void
  registerMetric(metric: Metric): void
  getMetricsAsJSON(): Promise<any>
  getMetricsAsArray(): any[]
  removeSingleMetric(name: string): void
  setDefaultLabels(labels: object): void
  getSingleMetricAsString(name: string): Promise<string>
  readonly contentType: RegistryContentType
  setContentType(contentType: RegistryContentType): void
}

export interface Prometheus {
  Counter: new (options: { name: string; help: string; registers: Registry[] }) => Counter
  Gauge: new (options: { name: string; help: string; registers: Registry[] }) => Gauge
  Registry: new (contentType?: string) => Registry
}

export interface Metrics {
  registry: Registry
  client: Prometheus
}

export function ensureCounter (metrics: Metrics, name: string, help: string): Counter {
  let counter = metrics.registry.getSingleMetric(name) as Counter | undefined

  if (!counter) {
    counter = new metrics.client.Counter({ name, help, registers: [metrics.registry] })
  }

  return counter
}

export function ensureGauge (metrics: Metrics, name: string, help: string): Gauge {
  let gauge = metrics.registry.getSingleMetric(name) as Gauge

  if (!gauge) {
    gauge = new metrics.client.Gauge({ name, help, registers: [metrics.registry] })
  }

  return gauge
}

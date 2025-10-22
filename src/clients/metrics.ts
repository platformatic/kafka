// Interfaces to make the package compatible with prom-client

// Unused in this package
type RegistryContentType =
  | 'application/openmetrics-text; version=1.0.0; charset=utf-8'
  | 'text/plain; version=0.0.4; charset=utf-8'

// Unused in this package
export interface Metric {
  name?: string
  get (): Promise<unknown>
  reset: () => void
  labels (labels: any): any
}

export interface Counter extends Metric {
  inc: (value?: number) => void
}

export interface Gauge extends Metric {
  inc: (value?: number) => void
  dec: (value?: number) => void
}

export interface Histogram extends Metric {
  observe: (value: number) => void
}

export interface Summary {}

export interface Registry {
  getSingleMetric: (name: string) => Counter | Gauge | Histogram | any

  // Unused in this package
  metrics (): Promise<string>
  clear (): void
  resetMetrics (): void
  registerMetric (metric: Metric): void
  getMetricsAsJSON (): Promise<any>
  getMetricsAsArray (): any[]
  removeSingleMetric (name: string): void
  setDefaultLabels (labels: object): void
  getSingleMetricAsString (name: string): Promise<string>
  readonly contentType: RegistryContentType
  setContentType (contentType: RegistryContentType): void
}

export interface Prometheus {
  Counter: new (options: { name: string; help: string; registers: Registry[]; labelNames?: string[] }) => Counter
  Gauge: new (options: { name: string; help: string; registers: Registry[]; labelNames?: string[] }) => Gauge
  Histogram: new (options: { name: string; help: string; registers: Registry[]; labelNames?: string[] }) => Histogram
  Registry: new (contentType?: string) => Registry
}

export interface Metrics {
  registry: Registry
  client: Prometheus
  labels?: Record<string, any>
}

export function ensureMetric<MetricType extends Metric> (
  metrics: Metrics,
  type: 'Gauge' | 'Counter' | 'Histogram',
  name: string,
  help: string
): MetricType {
  let metric = metrics.registry.getSingleMetric(name) as MetricType
  const labels = Object.keys(metrics.labels ?? {})

  if (!metric) {
    metric = new metrics.client[type]({
      name,
      help,
      registers: [metrics.registry],
      labelNames: labels
    }) as unknown as MetricType
  } else {
    // @ts-expect-error Overriding internal API
    metric.labelNames = metric.sortedLabelNames = Array.from(new Set([...metric.labelNames, ...labels])).sort()
  }

  return metric.labels(metrics.labels ?? {}) as MetricType
}

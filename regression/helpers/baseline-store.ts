import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import type { BenchmarkResult } from './index.ts'

export interface RegressionBaselineDocument {
  lane: string
  commit?: string
  runner?: string
  kafkaVersion?: string
  nodeVersion: string
  updatedAt: string
  results: Record<string, BenchmarkResult>
}

export interface BaselineStore {
  read: (lane: string) => Promise<RegressionBaselineDocument | undefined>
  write: (lane: string, document: RegressionBaselineDocument) => Promise<void>
}

export class FileBaselineStore implements BaselineStore {
  readonly #root: string

  constructor (root = join('regression', 'artifacts', 'baselines')) {
    this.#root = root
  }

  async read (lane: string): Promise<RegressionBaselineDocument | undefined> {
    try {
      const contents = await readFile(this.#fileForLane(lane), 'utf-8')
      return JSON.parse(contents) as RegressionBaselineDocument
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return undefined
      }

      throw error
    }
  }

  async write (lane: string, document: RegressionBaselineDocument): Promise<void> {
    const file = this.#fileForLane(lane)
    await mkdir(dirname(file), { recursive: true })
    await writeFile(file, `${JSON.stringify(document, null, 2)}\n`)
  }

  #fileForLane (lane: string): string {
    return join(this.#root, `${lane}.json`)
  }
}

export class HttpBaselineStore implements BaselineStore {
  readonly #baseUrl: string
  readonly #headers: Record<string, string>

  constructor (baseUrl: string, token = process.env.REGRESSION_BASELINE_TOKEN) {
    this.#baseUrl = baseUrl.replace(/\/$/, '')
    this.#headers = token ? { authorization: `Bearer ${token}` } : {}
  }

  async read (lane: string): Promise<RegressionBaselineDocument | undefined> {
    const response = await fetch(this.#urlForLane(lane), { headers: this.#headers })

    if (response.status === 404) {
      return undefined
    }

    if (!response.ok) {
      throw new Error(`failed to read regression baseline: ${response.status}`)
    }

    return (await response.json()) as RegressionBaselineDocument
  }

  async write (lane: string, document: RegressionBaselineDocument): Promise<void> {
    const response = await fetch(this.#urlForLane(lane), {
      method: 'PUT',
      headers: { ...this.#headers, 'content-type': 'application/json' },
      body: JSON.stringify(document, null, 2)
    })

    if (!response.ok) {
      throw new Error(`failed to write regression baseline: ${response.status}`)
    }
  }

  #urlForLane (lane: string): string {
    return `${this.#baseUrl}/${encodeURIComponent(lane)}.json`
  }
}

export function createBaselineStore (): BaselineStore {
  if (process.env.REGRESSION_BASELINE_URL) {
    return new HttpBaselineStore(process.env.REGRESSION_BASELINE_URL)
  }

  return new FileBaselineStore(process.env.REGRESSION_BASELINE_DIR)
}

export function createBaselineDocument (
  lane: string,
  results: Record<string, BenchmarkResult>,
  options: Partial<Omit<RegressionBaselineDocument, 'lane' | 'nodeVersion' | 'updatedAt' | 'results'>> = {}
): RegressionBaselineDocument {
  return {
    lane,
    nodeVersion: process.version,
    updatedAt: new Date().toISOString(),
    results,
    ...options
  }
}

import { Writable } from 'node:stream'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { notifyCreation } from '../../diagnostic.ts'
import type { MessageToProduce } from '../../protocol/records.ts'
import { defaultProducerStreamOptions } from './options.ts'
import type { Producer } from './producer.ts'
import {
  ProducerStreamReportModes,
  type ProduceOptions,
  type ProduceResult,
  type ProducerStreamMessageReport,
  type ProducerStreamOptions,
  type ProducerStreamReport,
  type ProducerStreamReportModeValue
} from './types.ts'

let currentInstance = 0

export class ProducerStream<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> extends Writable {
  #producer: Producer<Key, Value, HeaderKey, HeaderValue>
  #batchSize: number
  #batchTime: number
  #reportMode: ProducerStreamReportModeValue
  #produceOptions: ProduceOptions<Key, Value, HeaderKey, HeaderValue>
  #buffer: MessageToProduce<Key, Value, HeaderKey, HeaderValue>[]
  #timer: NodeJS.Timeout | null
  #flushing: boolean
  #batchId: number

  instance: number

  constructor (
    producer: Producer<Key, Value, HeaderKey, HeaderValue>,
    options: ProducerStreamOptions<Key, Value, HeaderKey, HeaderValue>
  ) {
    super({
      objectMode: true,
      highWaterMark: options.highWaterMark
    })

    const { batchSize, batchTime, reportMode, highWaterMark: _highWaterMark, ...sendOptions } = options

    this.#producer = producer
    this.#batchSize = batchSize ?? defaultProducerStreamOptions.batchSize
    this.#batchTime = batchTime ?? defaultProducerStreamOptions.batchTime
    this.#reportMode = reportMode ?? defaultProducerStreamOptions.reportMode
    this.#produceOptions = sendOptions
    this.#buffer = []
    this.#timer = null
    this.#flushing = false
    this.#batchId = 0
    this.instance = currentInstance++

    notifyCreation('producer-stream', this)
  }

  get producer (): Producer<Key, Value, HeaderKey, HeaderValue> {
    return this.#producer
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this.end((error?: Error | null) => callback!(error ?? null))

    return callback[kCallbackPromise]
  }

  _write (
    message: MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
    _: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    this.#buffer.push(message)
    callback(null)

    if (this.#buffer.length >= this.#batchSize) {
      this.#clearTimer()
      this.#flush()
      return
    }

    this.#scheduleFlush()
  }

  _final (callback: (error?: Error | null) => void): void {
    this.#clearTimer()

    if (!this.#buffer.length && !this.#flushing) {
      callback(null)
      return
    }

    const self = this

    function onError (error: Error): void {
      self.removeListener('error', onError)
      self.removeListener('flush', onFlush)
      callback(error)
    }

    function onFlush (): void {
      if (self.#buffer.length || self.#flushing) {
        return
      }

      self.removeListener('error', onError)
      self.removeListener('flush', onFlush)
      callback(null)
    }

    this.on('error', onError)
    this.on('flush', onFlush)

    this.#flush()
  }

  _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
    this.#clearTimer()

    this.#buffer = []

    callback(error)
  }

  #scheduleFlush (): void {
    if (this.#timer || this.#batchTime < 0) {
      return
    }

    this.#timer = setTimeout(() => {
      this.#timer = null
      this.#flush()
    }, this.#batchTime)
  }

  #clearTimer (): void {
    if (this.#timer) {
      clearTimeout(this.#timer)
      this.#timer = null
    }
  }

  #flush (): void {
    if (this.#flushing || this.destroyed) {
      return
    }

    /* c8 ignore next 3 - Hard to test */
    if (this.#buffer.length === 0) {
      return
    }

    this.#flushing = true

    const pending = this.#buffer.splice(0, this.#batchSize)

    const batchId = this.#batchId++
    const startedAt = Date.now()

    this.#producer.send({ messages: pending, ...this.#produceOptions }, (error, result) => {
      this.#flushing = false

      if (error) {
        this.destroy(error)
        return
      }

      this.#emitReports(this.#reportMode, batchId, pending, result!)
      this.emit('flush', { batchId, count: pending.length, duration: Date.now() - startedAt, result: result! })

      if (this.#buffer.length >= this.#batchSize) {
        this.#flush()
      } else if (this.#buffer.length > 0) {
        this.#scheduleFlush()
      }
    })
  }

  #emitReports (
    mode: ProducerStreamReportModeValue,
    batchId: number,
    pending: MessageToProduce<Key, Value, HeaderKey, HeaderValue>[],
    result: ProduceResult
  ): void {
    if (mode === ProducerStreamReportModes.NONE) {
      return
    }

    if (mode === ProducerStreamReportModes.BATCH) {
      const report: ProducerStreamReport = {
        batchId,
        count: pending.length,
        result
      }

      this.emit('delivery-report', report)
      return
    }

    for (let i = 0; i < pending.length; i++) {
      const report: ProducerStreamMessageReport<Key, Value, HeaderKey, HeaderValue> = {
        batchId,
        index: i,
        message: pending[i]
      }

      this.emit('delivery-report', report)
    }
  }
}

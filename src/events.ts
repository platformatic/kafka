import { EventEmitter } from 'node:events'

export interface TypedEvents {
  error: (error: Error) => void
}

/* c8 ignore start */
export class TypedEventEmitter<EventsType extends TypedEvents = TypedEvents> extends EventEmitter {
  addListener<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  addListener (event: string | symbol, listener: (...args: any[]) => void): this
  addListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.addListener(event, listener)
  }

  on<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  on (event: string | symbol, listener: (...args: any[]) => void): this
  on (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener)
  }

  once<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  once (event: string | symbol, listener: (...args: any[]) => void): this
  once (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.once(event, listener)
  }

  prependListener<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  prependListener (event: string | symbol, listener: (...args: any[]) => void): this
  prependListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependListener(event, listener)
  }

  prependeOnceListener<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  prependeOnceListener (event: string | symbol, listener: (...args: any[]) => void): this
  prependeOnceListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependOnceListener(event, listener)
  }

  off<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  off (event: string | symbol, listener: (...args: any[]) => void): this
  off (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.off(event, listener)
  }

  removeListener<K extends keyof EventsType> (event: K, listener: EventsType[K]): this
  removeListener (event: string | symbol, listener: (...args: any[]) => void): this
  removeListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.removeListener(event, listener)
  }

  removeAllListeners<K extends keyof EventsType> (event: K): this
  removeAllListeners (event: string | symbol): this
  removeAllListeners (event: string | symbol): this {
    return super.removeAllListeners(event)
  }
}
/* c8 ignore end */

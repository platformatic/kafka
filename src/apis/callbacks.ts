import { MultipleErrors } from '../errors.ts'
import { type Callback } from './definitions.ts'

export const kCallbackPromise = Symbol('plt.kafka.callbackPromise')

// This is only meaningful for testing
export const kNoopCallbackReturnValue = Symbol('plt.kafka.noopCallbackReturnValue')

export const noopCallback: CallbackWithPromise<any> = () => {
  return Promise.resolve(kNoopCallbackReturnValue)
}

export type CallbackWithPromise<ReturnType> = Callback<ReturnType> & { [kCallbackPromise]?: Promise<ReturnType> }

export function createPromisifiedCallback<ReturnType> (): CallbackWithPromise<ReturnType> {
  const { promise, resolve, reject } = Promise.withResolvers<ReturnType>()

  function callback (error?: Error | null, payload?: ReturnType): void {
    if (error) {
      reject(error)
    } else {
      resolve(payload!)
    }
  }

  callback[kCallbackPromise] = promise

  return callback
}

export function runConcurrentCallbacks<R, V> (
  errorMessage: string,
  collection: Iterable<V>,
  operation: (item: V, cb: Callback<R>) => void,
  callback: Callback<R[]>
): void {
  let allScheduled = false
  let completed = 0
  let callbackCalled = false
  let hasErrors = false
  const errors: [number, Error][] = []
  const results: [number, R][] = []

  let i = 0

  function operationCallback (index: number, e: Error | null, result?: R): void {
    if (e) {
      hasErrors = true
      errors.push([index, e])
      results.push([index, undefined as unknown as R])
    } else {
      results.push([index, result!])
    }

    completed++

    if (allScheduled && completed === i) {
      callbackCalled = true
      const e = errors.sort((a, b) => a[0] - b[0]).map(([_, error]) => error)
      const r = results.sort((a, b) => a[0] - b[0]).map(([_, result]) => result)
      callback(hasErrors ? new MultipleErrors(errorMessage, e) : null, r)
    }
  }

  for (const item of collection) {
    operation(item, operationCallback.bind(null, i++))
  }

  if (i === 0) {
    callback(null, [])
    return
  }

  allScheduled = true

  // If all operations were synchronous, allScheduled is not set to true in time for the operation
  // callback to pick it up. In that case, call the callback here.
  if (completed === i && !callbackCalled) {
    const e = errors.sort((a, b) => a[0] - b[0]).map(([_, error]) => error)
    const r = results.sort((a, b) => a[0] - b[0]).map(([_, result]) => result)
    callback(hasErrors ? new MultipleErrors(errorMessage, e) : null, r)
  }
}

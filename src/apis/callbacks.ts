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
  let scheduled = 0
  let allScheduled = false
  let completed = 0
  let callbackCalled = false
  let hasErrors = false
  const errors: Error[] = []
  const results: R[] = []

  function operationCallback (e: Error | null, result?: R): void {
    if (e) {
      hasErrors = true
      errors.push(e)
    } else {
      results.push(result!)
    }

    completed++

    if (allScheduled && completed === scheduled) {
      callbackCalled = true
      callback(hasErrors ? new MultipleErrors(errorMessage, errors) : null, results)
    }
  }

  for (const item of collection) {
    scheduled++
    operation(item, operationCallback)
  }

  allScheduled = true

  if (allScheduled && scheduled === 0) {
    callback(null, [])
  }

  // If all operations were synchronous, allScheduled is not set to true in time for the operation
  // callback to pick it up. In that case, call the callback here.
  if (completed === scheduled && !callbackCalled) {
    callback(hasErrors ? new MultipleErrors(errorMessage, errors) : null, results)
  }
}

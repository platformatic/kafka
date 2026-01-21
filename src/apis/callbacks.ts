import { MultipleErrors } from '../errors.ts'
import { promiseWithResolvers } from '../utils.ts'
import { type Callback } from './definitions.ts'

export const kCallbackPromise = Symbol('plt.kafka.callbackPromise')

// This is only meaningful for testing
export const kNoopCallbackReturnValue = Symbol('plt.kafka.noopCallbackReturnValue')

export const noopCallback: CallbackWithPromise<any> = () => {
  return Promise.resolve(kNoopCallbackReturnValue)
}

export type CallbackWithPromise<ReturnType> = Callback<ReturnType> & { [kCallbackPromise]?: Promise<ReturnType> }

export function createPromisifiedCallback<ReturnType> (): CallbackWithPromise<ReturnType> {
  const { promise, resolve, reject } = promiseWithResolvers<ReturnType>()

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

export function runConcurrentCallbacks<ReturnType, K, V> (
  errorMessage: string,
  collection: Map<K, V>,
  operation: (item: [K, V], cb: Callback<ReturnType>) => void,
  callback: Callback<ReturnType[]>
): void
export function runConcurrentCallbacks<ReturnType, V> (
  errorMessage: string,
  collection: Set<V>,
  operation: (item: V, cb: Callback<ReturnType>) => void,
  callback: Callback<ReturnType[]>
): void
export function runConcurrentCallbacks<ReturnType, V> (
  errorMessage: string,
  collection: V[],
  operation: (item: V, cb: Callback<ReturnType>) => void,
  callback: Callback<ReturnType[]>
): void
export function runConcurrentCallbacks<ReturnType> (
  errorMessage: string,
  collection: Map<any, any> | Set<any> | any[],
  operation: (item: any, cb: Callback<ReturnType>) => void,
  callback: Callback<ReturnType[]>
): void {
  let remaining = Array.isArray(collection) ? collection.length : collection.size
  let hasErrors = false
  const errors: Error[] = Array.from(Array(remaining))
  const results: ReturnType[] = Array.from(Array(remaining))

  let i = 0

  function operationCallback (index: number, e: Error | null, result?: ReturnType): void {
    if (e) {
      hasErrors = true
      errors[index] = e
    } else {
      results[index] = result!
    }

    remaining--

    if (remaining === 0) {
      callback(hasErrors ? new MultipleErrors(errorMessage, errors) : null, results)
    }
  }

  if (remaining === 0) {
    callback(null, results)
  }

  for (const item of collection) {
    operation(item, operationCallback.bind(null, i++))
  }
}

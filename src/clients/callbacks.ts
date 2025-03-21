import { type Callback } from '../apis/definitions.ts'
import { MultipleErrors } from '../errors.ts'

export const kCallbackPromise = Symbol('plt.kafka.callbackPromise')
export const noopCallback: CallbackWithPromise<any> = () => {}

export type CallbackWithPromise<T> = Callback<T> & { [kCallbackPromise]?: Promise<T> }

export function createPromisifiedCallback<T> (): CallbackWithPromise<T> {
  const { promise, resolve, reject } = Promise.withResolvers<T>()

  function callback (error?: Error | null, payload?: T): void {
    if (error) {
      reject(error)
    } else {
      resolve(payload!)
    }
  }

  callback[kCallbackPromise] = promise

  return callback
}

export function runConcurrentCallbacks<T> (
  errorMessage: string,
  collection: unknown[] | Set<unknown> | Map<unknown, unknown>,
  operation: (item: any, cb: Callback<T>) => void,
  callback: Callback<T[]>
): void {
  let remaining = Array.isArray(collection) ? collection.length : collection.size
  let hasErrors = false
  const errors: Error[] = Array.from(Array(remaining))
  const results: T[] = Array.from(Array(remaining))

  let i = 0

  function operationCallback (index: number, e: Error | null, result: T): void {
    if (e) {
      hasErrors = true
      errors[index] = e
    } else {
      results[index] = result
    }

    remaining--

    if (remaining === 0) {
      callback(hasErrors ? new MultipleErrors(errorMessage, errors) : null, results)
    }
  }

  for (const item of collection) {
    operation(item, operationCallback.bind(null, i++))
  }
}

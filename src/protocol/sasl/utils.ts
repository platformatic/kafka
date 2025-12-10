import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/index.ts'
import { AuthenticationError } from '../../errors.ts'
import { type SASLCredentialProvider } from '../../network/connection.ts'

export function getCredential<T> (
  label: string,
  credentialOrProvider: T | SASLCredentialProvider<T>,
  callback: CallbackWithPromise<T>
): void
export function getCredential<T> (label: string, credentialOrProvider: T | SASLCredentialProvider<T>): Promise<T>
export function getCredential<T> (
  label: string,
  credentialOrProvider: T | SASLCredentialProvider<T>,
  callback?: CallbackWithPromise<T>
): void | Promise<T> {
  if (!callback) {
    callback = createPromisifiedCallback<T>()
  }

  if (typeof credentialOrProvider === 'undefined') {
    callback(new AuthenticationError(`The ${label} should be a value or a function.`), undefined as unknown as T)
    return callback[kCallbackPromise]
  } else if (typeof credentialOrProvider !== 'function') {
    callback(null, credentialOrProvider)
    return callback[kCallbackPromise]
  }

  try {
    const credential = (credentialOrProvider as SASLCredentialProvider<T>)()

    if (credential == null) {
      callback(
        new AuthenticationError(`The ${label} provider should return a string or a promise that resolves to a value.`),
        undefined as unknown as T
      )
      return callback[kCallbackPromise]
    } else if (typeof (credential as Promise<string>)?.then !== 'function') {
      callback(null, credential as T)
      return callback[kCallbackPromise]
    }

    ;(credential as Promise<T>)
      .then((result: T) => {
        if (result == null) {
          process.nextTick(
            callback,
            new AuthenticationError(`The ${label} provider should resolve to a value.`),
            undefined as unknown as string
          )

          return
        }

        process.nextTick(callback, null, result)
      })
      .catch((error: Error) => {
        process.nextTick(callback, new AuthenticationError(`The ${label} provider threw an error.`, { cause: error }))
      })
  } catch (error) {
    callback(
      new AuthenticationError(`The ${label} provider threw an error.`, { cause: error as Error }),
      undefined as unknown as T
    )
  }

  return callback[kCallbackPromise]
}

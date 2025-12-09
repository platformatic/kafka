import { type Callback } from '../../apis/index.ts'
import { AuthenticationError } from '../../errors.ts'
import { type SASLCredentialProvider } from '../../network/connection.ts'

export function getCredential<T> (
  label: string,
  credentialOrProvider: T | SASLCredentialProvider<T>,
  callback: Callback<T>
): void {
  if (typeof credentialOrProvider === 'undefined') {
    callback(new AuthenticationError(`The ${label} should be a value or a function.`), undefined as unknown as T)
    return
  } else if (typeof credentialOrProvider !== 'function') {
    callback(null, credentialOrProvider)
    return
  }

  try {
    const credential = (credentialOrProvider as SASLCredentialProvider<T>)()

    if (credential == null) {
      callback(
        new AuthenticationError(`The ${label} provider should return a string or a promise that resolves to a value.`),
        undefined as unknown as T
      )
      return
    } else if (typeof (credential as Promise<string>)?.then !== 'function') {
      callback(null, credential as T)
      return
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
}

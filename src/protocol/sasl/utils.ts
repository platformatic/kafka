import { type CallbackWithPromise, createPromisifiedCallback, kCallbackPromise } from '../../apis/index.ts'
import { AuthenticationError } from '../../errors.ts'
import { type SASLCredentialProvider } from '../../network/connection.ts'

export function getCredential (
  label: string,
  credentialOrProvider: string | SASLCredentialProvider,
  callback: CallbackWithPromise<string>
): void
export function getCredential (label: string, credentialOrProvider: string | SASLCredentialProvider): Promise<string>
export function getCredential (
  label: string,
  credentialOrProvider: string | SASLCredentialProvider,
  callback?: CallbackWithPromise<string>
): void | Promise<string> {
  if (!callback) {
    callback = createPromisifiedCallback<string>()
  }

  if (typeof credentialOrProvider === 'string') {
    callback(null, credentialOrProvider)
    return callback[kCallbackPromise]
  } else if (typeof credentialOrProvider !== 'function') {
    callback(new AuthenticationError(`The ${label} should be a string or a function.`), undefined as unknown as string)
    return callback[kCallbackPromise]
  }

  try {
    const credential = credentialOrProvider()

    if (typeof credential === 'string') {
      callback(null, credential)
      return callback[kCallbackPromise]
    } else if (typeof (credential as Promise<string>)?.then !== 'function') {
      callback(
        new AuthenticationError(`The ${label} provider should return a string or a promise that resolves to a string.`),
        undefined as unknown as string
      )

      return callback[kCallbackPromise]
    }

    credential
      .then(token => {
        if (typeof token !== 'string') {
          process.nextTick(
            callback,
            new AuthenticationError(`The ${label} provider should resolve to a string.`),
            undefined as unknown as string
          )

          return callback[kCallbackPromise]
        }

        process.nextTick(callback, null, token)
      })
      .catch(error => {
        process.nextTick(
          callback,
          new AuthenticationError(`The ${label} provider threw an error.`, { cause: error as Error })
        )
      })
  } catch (error) {
    callback(
      new AuthenticationError(`The ${label} provider threw an error.`, { cause: error as Error }),
      undefined as unknown as string
    )
  }

  return callback[kCallbackPromise]
}

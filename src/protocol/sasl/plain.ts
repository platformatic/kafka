import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { type Connection, type CredentialProvider } from '../../network/connection.ts'
import { getCredential } from './utils.ts'

export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | CredentialProvider,
  passwordProvider: string | CredentialProvider,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | CredentialProvider,
  passwordProvider: string | CredentialProvider
): Promise<SaslAuthenticateResponse>
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | CredentialProvider,
  passwordProvider: string | CredentialProvider,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  getCredential('SASL/PLAIN username', usernameProvider, (error, username) => {
    if (error) {
      callback!(error)
      return
    }

    getCredential('SASL/PLAIN password', passwordProvider, (error, password) => {
      if (error) {
        callback!(error)
        return
      }

      authenticateAPI(connection, Buffer.from(['', username, password].join('\0')), callback)
    })
  })

  return callback[kCallbackPromise]
}

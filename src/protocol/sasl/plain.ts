import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { type Connection, type SASLCredentialProvider } from '../../network/connection.ts'
import { getCredential } from './credential-provider.ts'

export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider
): Promise<SaslAuthenticateResponse>
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  getCredential('SASL/PLAIN username', usernameProvider, (error, username) => {
    if (error) {
      return callback!(error, undefined as unknown as SaslAuthenticateResponse)
    }

    getCredential('SASL/PLAIN password', passwordProvider, (error, password) => {
      if (error) {
        return callback!(error, undefined as unknown as SaslAuthenticateResponse)
      }

      authenticateAPI(connection, Buffer.from(['', username, password].join('\0')), callback)
    })
  })

  return callback[kCallbackPromise]
}

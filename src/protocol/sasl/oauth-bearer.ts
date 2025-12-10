import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { AuthenticationError } from '../../errors.ts'
import { type Connection, type SASLCredentialProvider } from '../../network/connection.ts'
import { getCredential } from './utils.ts'

export function jwtValidateAuthenticationBytes (authBytes: Buffer, callback: CallbackWithPromise<Buffer>): void {
  let authData: Record<string, string> = {}

  try {
    if (authBytes.length > 0) {
      authData = JSON.parse(authBytes.toString('utf-8'))
    }

    /* c8 ignore next 8  - Hard to test */
  } catch (e) {
    callback(
      new AuthenticationError('Invalid authBytes in SASL/OAUTHBEARER response', { authBytes }),
      undefined as unknown as Buffer
    )

    return
  }

  if (authData.status === 'invalid_token') {
    callback(new AuthenticationError('Invalid SASL/OAUTHBEARER token.', { authData }), undefined as unknown as Buffer)
  }

  callback(null, authBytes)
}

export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  tokenOrProvider: string | SASLCredentialProvider,
  extensions: Record<string, string> | SASLCredentialProvider<Record<string, string>>,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  tokenOrProvider: string | SASLCredentialProvider,
  extensions: Record<string, string> | SASLCredentialProvider<Record<string, string>>
): Promise<SaslAuthenticateResponse>
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  tokenOrProvider: string | SASLCredentialProvider,
  extensionsOrProvider: Record<string, string> | SASLCredentialProvider<Record<string, string>>,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  getCredential('SASL/OAUTHBEARER token', tokenOrProvider, (error, token) => {
    if (error) {
      callback!(error, undefined as unknown as SaslAuthenticateResponse)
      return
    }

    getCredential('SASL/OAUTHBEARER extensions', extensionsOrProvider ?? {}, (error, extensionsMap) => {
      if (error) {
        return callback!(error, undefined as unknown as SaslAuthenticateResponse)
      }

      let extensions = ''
      if (extensionsMap) {
        for (const [key, value] of Object.entries(extensionsMap)) {
          extensions += `\x01${key}=${value}`
        }
      }

      authenticateAPI(connection, Buffer.from(`n,,\x01auth=Bearer ${token}${extensions}\x01\x01`), callback!)
    })
  })

  return callback[kCallbackPromise]
}

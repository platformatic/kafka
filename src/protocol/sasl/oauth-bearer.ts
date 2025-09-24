import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { AuthenticationError } from '../../errors.ts'
import { type Connection, type SASLCredentialProvider } from '../../network/connection.ts'
import { getCredential } from './credential-provider.ts'

export function jwtValidateAuthenticationBytes (authBytes: Buffer, callback: CallbackWithPromise<Buffer>): void {
  let authData: Record<string, string>
  try {
    authData = authBytes.length > 0 ? JSON.parse(authBytes.toString('utf-8')) : {}
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
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  tokenOrProvider: string | SASLCredentialProvider
): Promise<SaslAuthenticateResponse>
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  tokenOrProvider: string | SASLCredentialProvider,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  getCredential('SASL/OAUTHBEARER token', tokenOrProvider, (error, token) => {
    if (error) {
      return callback!(error, undefined as unknown as SaslAuthenticateResponse)
    }

    authenticateAPI(connection, Buffer.from(`n,,\x01auth=Bearer ${token}\x01\x01`), callback!)
  })

  return callback[kCallbackPromise]
}

import { createRequire } from 'node:module'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { AuthenticationError } from '../../errors.ts'
import { type Connection, type SASLCredentialProvider } from '../../network/connection.ts'
import { EMPTY_BUFFER } from '../definitions.ts'
import { getCredential } from './credential-provider.ts'

function performChallenge (
  connection: Connection,
  authenticateAPI: SASLAuthenticationAPI,
  client: any,
  target: string,
  input: Buffer,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void {
  try {
    const { completed, output } = client.step(target, input)

    authenticateAPI(connection, output, (error, response) => {
      /* c8 ignore next 7 - Hard to test */
      if (error) {
        callback(
          new AuthenticationError('SASL authentication failed.', { cause: error }),
          undefined as unknown as SaslAuthenticateResponse
        )
        return
      }

      /* c8 ignore next 4 - Hard to test */
      if (!completed) {
        performChallenge(connection, authenticateAPI, client, target, response.authBytes, callback)
        return
      }

      try {
        client.unwrap(response.authBytes)
        /* c8 ignore next 6 - Hard to test */
      } catch (e) {
        callback(
          new AuthenticationError('Cannot unwrap Kerberos response.', { kerberosError: e }),
          undefined as unknown as SaslAuthenticateResponse
        )
      }

      try {
        // Byte 0: No security layer; Byte 1-3: max message size - 0=none
        const wrapped = client.wrap(Buffer.from([1, 0, 0, 0]))

        authenticateAPI(connection, Buffer.from(wrapped, 'base64'), (error, response) => {
          /* c8 ignore next 7 - Hard to test */
          if (error) {
            callback(
              new AuthenticationError('SASL authentication failed.', { cause: error }),
              undefined as unknown as SaslAuthenticateResponse
            )
            return
          }

          callback(null, response)
        })
        /* c8 ignore next 6 - Hard to test */
      } catch (e) {
        callback(
          new AuthenticationError('Cannot wrap Kerberos response.', { kerberosError: e }),
          undefined as unknown as SaslAuthenticateResponse
        )
      }
    })
    /* c8 ignore next 6 - Hard to test */
  } catch (e) {
    callback!(
      new AuthenticationError('Cannot perform Kerberos step challenge.', { kerberosError: e }),
      undefined as unknown as SaslAuthenticateResponse
    )
  }
}

export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider,
  keytabProvider: string | SASLCredentialProvider,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider,
  keytabProvider: string | SASLCredentialProvider
): Promise<SaslAuthenticateResponse>
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider,
  keytabProvider: string | SASLCredentialProvider,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  /* c8 ignore next 3 - Hard to test */
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  const require = createRequire(import.meta.url)
  const { GSSAPI } = require('../../../native/gssapi.darwin-arm64.node')

  getCredential('SASL/GSSAPI username', usernameProvider, (error, username) => {
    if (error) {
      callback!(error, undefined as unknown as SaslAuthenticateResponse)
      return
    }

    let credentialProvider: string | SASLCredentialProvider
    let credentialType: 'password' | 'keytab'

    if (passwordProvider) {
      credentialType = 'password'
      credentialProvider = passwordProvider
    } else {
      credentialType = 'keytab'
      credentialProvider = keytabProvider
    }

    getCredential(`SASL/GSSAPI ${credentialType}`, credentialProvider, (error, credential) => {
      if (error) {
        callback!(error, undefined as unknown as SaslAuthenticateResponse)
        return
      }

      const client = new GSSAPI('localhost:8000', 'EXAMPLE.COM')
      try {
        credentialType === 'password'
          ? client.authenticateWithPassword(username, credential)
          : client.authenticateWithKeytab(username, credential)
      } catch (e) {
        callback!(
          new AuthenticationError('SASL authentication failed.', { kerberosError: e }),
          undefined as unknown as SaslAuthenticateResponse
        )

        return
      }

      performChallenge(connection, authenticateAPI, client, 'broker@broker-sasl-kerberos', EMPTY_BUFFER, callback!)
    })
  })

  return callback[kCallbackPromise]
}

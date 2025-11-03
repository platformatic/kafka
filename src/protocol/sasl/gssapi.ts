import krb, { type KerberosClient } from 'kerberos'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { AuthenticationError } from '../../errors.ts'
import { type Connection, type SASLCredentialProvider } from '../../network/connection.ts'
import { EMPTY_BUFFER } from '../definitions.ts'
import { getCredential } from './credential-provider.ts'

function createKerberosAuthenticationError (message: string, kerberosError: string): AuthenticationError {
  return new AuthenticationError(message, { kerberosError })
}

/*
// We use this to verify username and password
// krb.checkPassword(username, password, 'kafka', 'EXAMPLE.COM', error => {
//   if (error) {
//     callback(
//       createKerberosAuthenticationError('Invalid credentials.', error),
//       undefined as unknown as SaslAuthenticateResponse
//     )
//     return
//   }
// })
*/

function performChallenge (
  connection: Connection,
  authenticateAPI: SASLAuthenticationAPI,
  client: KerberosClient,
  step: string,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void {
  client.step(step, async (error: string | null, challenge: string) => {
    if (error) {
      callback(
        createKerberosAuthenticationError('Cannot continue Kerberos step challenge.', error),
        undefined as unknown as SaslAuthenticateResponse
      )
      return
    }

    const challengeBuffer = challenge ? Buffer.from(challenge, 'base64') : EMPTY_BUFFER

    authenticateAPI(connection, challengeBuffer, (error, response) => {
      if (error) {
        callback(
          new AuthenticationError('SASL authentication failed.', { cause: error }),
          undefined as unknown as SaslAuthenticateResponse
        )
        return
      }

      if (response.authBytes.length === 0) {
        callback(null, response)
        return
      }

      if (client.contextComplete) {
        client.unwrap(response.authBytes.toString('base64'), (error: string | null) => {
          if (error) {
            callback(
              createKerberosAuthenticationError('Cannot unwrap Kerberose response', error),
              undefined as unknown as SaslAuthenticateResponse
            )
            return
          }

          // Byte 0: No security layer; Byte 1-3: max message size - 0=none
          client.wrap(Buffer.from([1, 0, 0, 0]).toString('base64'), {}, (error: string | null, wrapped: string) => {
            if (error) {
              callback(
                createKerberosAuthenticationError('Cannot wrap Kerberos response.', error),
                undefined as unknown as SaslAuthenticateResponse
              )
              return
            }

            // Invia la risposta wrappata a Kafka
            authenticateAPI(connection, Buffer.from(wrapped, 'base64'), (error, response) => {
              if (error) {
                callback(
                  new AuthenticationError('SASL authentication failed.', { cause: error }),
                  undefined as unknown as SaslAuthenticateResponse
                )
                return
              }

              callback(null, response)
            })
          })
        })

        return
      }

      // Altrimenti continua normalmente
      performChallenge(connection, authenticateAPI, client, response.authBytes.toString('base64'), callback)
    })
  })
}

export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  username: string,
  password: string,
  keytab: string,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  username: string,
  password: string,
  keytab: string
): Promise<SaslAuthenticateResponse>
export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  usernameProvider: string | SASLCredentialProvider,
  passwordProvider: string | SASLCredentialProvider,
  keytabProvider: string | SASLCredentialProvider,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  getCredential('SASL/GSSAPI username', usernameProvider, (error, username) => {
    if (error) {
      callback!(error, undefined as unknown as SaslAuthenticateResponse)
      return
    }

    passwordProvider ??= () => ''
    keytabProvider ??= () => ''

    getCredential('SASL/GSSAPI password', passwordProvider, (error, password) => {
      if (error) {
        callback!(error, undefined as unknown as SaslAuthenticateResponse)
        return
      }

      getCredential('SASL/GSSAPI keytab', keytabProvider, (error, password) => {
        if (error) {
          callback!(error, undefined as unknown as SaslAuthenticateResponse)
        }
      })

      /*
        Before calling this, you need to invoke kinit:

        echo $password | kinit $username
        kinit -kt $keytab $username
      */

      krb.initializeClient('broker@broker-sasl-kerberos', { principal: 'admin-keytab@EXAMPLE.COM' }, (
        error: string | null,
        client: KerberosClient
      ) => {
        if (error) {
          callback(
            createKerberosAuthenticationError('Cannot initialize Kerberos client.', error),
            undefined as unknown as SaslAuthenticateResponse
          )
          return
        }

        performChallenge(connection, authenticateAPI, client, '', callback)
      })
    })
  })

  return callback[kCallbackPromise]
}

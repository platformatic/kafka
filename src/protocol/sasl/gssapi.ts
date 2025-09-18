import krb, { type KerberosClient } from 'kerberos'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { AuthenticationError } from '../../errors.ts'
import { type Connection } from '../../network/connection.ts'

function createKerberosAuthenticationError (message: string, kerberosError: string): AuthenticationError {
  return new AuthenticationError(message, { kerberosError })
}

function performChallenge (
  client: KerberosClient,
  challenge: string,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void {
  client.step('', async (error, challenge) => {
    if (error) {
      return callback!(
        createKerberosAuthenticationError('Cannot continue Kerberos challenge.', error),
        undefined as unknown as SaslAuthenticateResponse
      )
    }

    process._rawDebug('CHALLENGE', challenge)
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
  username: string,
  password: string,
  keytab: string,
  callback?: CallbackWithPromise<SaslAuthenticateResponse>
): void | Promise<SaslAuthenticateResponse> {
  if (!callback) {
    callback = createPromisifiedCallback<SaslAuthenticateResponse>()
  }

  // krb.checkPassword(username, password, 'kafka', 'EXAMPLE.COM', error => {
  //   if (error) {
  //     return callback(
  //       createKerberosAuthenticationError('Invalid credentials.', error),
  //       undefined as unknown as SaslAuthenticateResponse
  //     )
  //   }

  krb.initializeClient('broker@broker-sasl-kerberos', { principal: 'admin-keytab@EXAMPLE.COM' }, (error, client) => {
    if (error) {
      return callback(
        createKerberosAuthenticationError('Cannot initialize Kerberos client.', error),
        undefined as unknown as SaslAuthenticateResponse
      )
    }

    performChallenge(client, '', callback)
  })
  // })

  setInterval(() => {}, 1000)
  return callback[kCallbackPromise]
}

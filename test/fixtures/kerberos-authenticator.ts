import krb, { type KerberosClient } from 'kerberos'
import { execSync } from 'node:child_process'
import { rm } from 'node:fs'
import { mkdtemp, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'
import {
  AuthenticationError,
  EMPTY_BUFFER,
  saslUtils,
  type Callback,
  type CallbackWithPromise,
  type Connection,
  type saslAuthenticateV2,
  type SASLCredentialProvider,
  type SASLCustomAuthenticator,
  type SASLMechanismValue
} from '../../src/index.ts'

type SaslAuthenticateResponse = saslAuthenticateV2.SaslAuthenticateResponse
type SASLAuthenticationAPI = saslAuthenticateV2.SASLAuthenticationAPI

function createKerberosAuthenticationError (message: string, kerberosError: string): AuthenticationError {
  return new AuthenticationError(message, { kerberosError })
}

function performChallenge (
  connection: Connection,
  authenticate: SASLAuthenticationAPI,
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

    authenticate(connection, challengeBuffer, (error, response) => {
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
            authenticate(connection, Buffer.from(wrapped, 'base64'), (error, response) => {
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

      performChallenge(connection, authenticate, client, response.authBytes.toString('base64'), callback)
    })
  })
}

function restoreEnvironment (
  callback: Callback<SaslAuthenticateResponse>,
  kerberosRoot: string,
  originalKrb5Config: string | undefined,
  originalKrbCCName: string | undefined,
  error: Error | null,
  response: SaslAuthenticateResponse
): void {
  if (typeof originalKrb5Config !== 'undefined') {
    process.env.KRB5_CONFIG = originalKrb5Config
  } else {
    delete process.env.KRB5_CONFIG
  }

  if (typeof originalKrbCCName !== 'undefined') {
    process.env.KRB5CCNAME = originalKrbCCName
  } else {
    delete process.env.KRB5CCNAME
  }

  rm(kerberosRoot, { recursive: true, force: true }, () => {
    callback(error, response)
  })
}

function authenticate (
  service: string,
  kerberosRoot: string,
  _m: SASLMechanismValue,
  connection: Connection,
  authenticate: saslAuthenticateV2.SASLAuthenticationAPI,
  usernameProvider: string | SASLCredentialProvider | undefined,
  passwordProvider: string | SASLCredentialProvider | undefined,
  _t: string | SASLCredentialProvider | undefined,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): void {
  saslUtils.getCredential('SASL/GSSAPI username', usernameProvider!, (error, username) => {
    if (error) {
      callback!(error, undefined as unknown as SaslAuthenticateResponse)
      return
    }

    saslUtils.getCredential('SASL/GSSAPI password', passwordProvider!, (error, password) => {
      if (error) {
        callback!(error, undefined as unknown as SaslAuthenticateResponse)
        return
      }

      const afterRestoreCallback = restoreEnvironment.bind(
        null,
        callback!,
        kerberosRoot,
        process.env.KRB5_CONFIG,
        process.env.KRB5CCNAME
      )
      process.env.KRB5_CONFIG = `${kerberosRoot}/krb5.conf`
      process.env.KRB5CCNAME = `${kerberosRoot}/krb5.cache`

      let args = ''
      if (password.startsWith('keytab:')) {
        args = `-kt ${password.substring('keytab:'.length)}`
      } else {
        args = `--password-file=${kerberosRoot}/password`
      }

      // Import the password via kinit
      try {
        execSync(`kinit ${args} ${username}`, { stdio: 'pipe' })
      } catch (error) {
        afterRestoreCallback(
          new AuthenticationError('Cannot execute kinit to import user Kerberos credentials', { error }),
          undefined as unknown as SaslAuthenticateResponse
        )
        return
      }

      krb.initializeClient(service, { principal: username }, (error: string | null, client: KerberosClient) => {
        if (error) {
          callback(
            createKerberosAuthenticationError('Cannot initialize Kerberos client.', error),
            undefined as unknown as SaslAuthenticateResponse
          )
          return
        }

        performChallenge(connection, authenticate, client, '', afterRestoreCallback)
      })
    })
  })
}

export async function createAuthenticator (
  _u: string,
  password: string,
  realm: string,
  kdc: string
): Promise<SASLCustomAuthenticator> {
  const tmpDir = await mkdtemp(resolve(tmpdir(), 'sasl-gssapi-'))

  await writeFile(
    `${tmpDir}/krb5.conf`,
    `
[libdefaults]
  default_realm = ${realm}
  default_ccache_name = FILE:${tmpDir}/krb5.cache

[realms]
  ${realm} = {
    kdc = ${kdc}
  }

[domain_realm]
  .${realm.toLowerCase()} = ${realm}
  ${realm.toLowerCase()} = ${realm}
`,
    'utf-8'
  )

  if (!password.startsWith('keytab:')) {
    await writeFile(`${tmpDir}/password`, password, 'utf-8')
  }

  return authenticate.bind(null, 'broker@broker-sasl-kerberos', tmpDir)
}

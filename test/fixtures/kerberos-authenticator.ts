import krb, { type KerberosClient } from 'kerberos'
import { execSync } from 'node:child_process'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { resolve as resolvePaths } from 'node:path'
import {
  AuthenticationError,
  EMPTY_BUFFER,
  saslUtils,
  type Callback,
  type CallbackWithPromise,
  type Connection,
  type CredentialProvider,
  type saslAuthenticateV2,
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
  client.step(step, async (error, challenge) => {
    if (error) {
      callback(createKerberosAuthenticationError('Cannot continue Kerberos step challenge.', error))
      return
    }

    const challengeBuffer = challenge ? Buffer.from(challenge, 'base64') : EMPTY_BUFFER

    authenticate(connection, challengeBuffer, (error, response) => {
      if (error) {
        callback(new AuthenticationError('SASL authentication failed.', { cause: error }))
        return
      }

      if (response!.authBytes.length === 0) {
        callback(null, response)
        return
      }

      if (client.contextComplete) {
        client.unwrap(response!.authBytes.toString('base64'), error => {
          if (error) {
            callback(createKerberosAuthenticationError('Cannot unwrap Kerberose response', error))
            return
          }

          // Byte 0: No security layer; Byte 1-3: max message size - 0=none
          client.wrap(Buffer.from([1, 0, 0, 0]).toString('base64'), {}, (error, wrapped) => {
            if (error) {
              callback(createKerberosAuthenticationError('Cannot wrap Kerberos response.', error))
              return
            }

            authenticate(connection, Buffer.from(wrapped, 'base64'), (error, response) => {
              if (error) {
                callback(new AuthenticationError('SASL authentication failed.', { cause: error }))
                return
              }

              callback(null, response)
            })
          })
        })

        return
      }

      performChallenge(connection, authenticate, client, response!.authBytes.toString('base64'), callback)
    })
  })
}

async function restoreEnvironment (
  callback: Callback<SaslAuthenticateResponse>,
  kerberosRoot: string,
  originalKrb5Config: string | undefined,
  originalKrbCCName: string | undefined,
  error: Error | null,
  response?: SaslAuthenticateResponse
): Promise<void> {
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

  await rm(kerberosRoot, { recursive: true, force: true })
  callback(error, response)
}

async function authenticate (
  service: string,
  kerberosRoot: string,
  _m: SASLMechanismValue,
  connection: Connection,
  authenticate: saslAuthenticateV2.SASLAuthenticationAPI,
  usernameProvider: string | CredentialProvider | undefined,
  passwordProvider: string | CredentialProvider | undefined,
  _t: string | CredentialProvider | undefined,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): Promise<void> {
  const afterRestoreCallback = restoreEnvironment.bind(
    null,
    callback,
    kerberosRoot,
    process.env.KRB5_CONFIG,
    process.env.KRB5CCNAME
  )

  try {
    const username = await saslUtils.getCredential('SASL/GSSAPI username', usernameProvider!)
    let password = await saslUtils.getCredential('SASL/GSSAPI password', passwordProvider!)

    process.env.KRB5_CONFIG = `${kerberosRoot}/krb5.conf`
    process.env.KRB5CCNAME = `${kerberosRoot}/krb5.cache`

    // Using a password
    if (!password.startsWith('keytab:')) {
      // On MIT Kerberos, kinit does not support reading password from stdin or a password file
      // so we convert it to a keytab file if needed using ktutil
      if (process.platform !== 'darwin') {
        execSync(`ktutil --keytab ${kerberosRoot}/keytab`, {
          input: `addent -password -p ${username} -k 1 -f \n${password}\nwkt ${kerberosRoot}/keytab\nquit\n`
        })

        password = `keytab:${kerberosRoot}/keytab`
      } else {
        // On MacOS, we can use a password file directly since it uses Heimdal Kerberos
        await writeFile(`${kerberosRoot}/password`, password, 'utf-8')
      }
    }

    let args = ''
    if (password.startsWith('keytab:')) {
      args = `-kt ${password.slice(7)}`
    } else {
      args = `--password-file=${kerberosRoot}/password`
    }

    // Import the password via kinit
    execSync(`kinit ${args} ${username}`, { stdio: 'pipe', env: process.env })

    krb.initializeClient(service, {}, (error, client) => {
      if (error) {
        callback(createKerberosAuthenticationError('Cannot initialize Kerberos client.', error))
        return
      }

      performChallenge(connection, authenticate, client, '', afterRestoreCallback)
    })
  } catch (error) {
    await afterRestoreCallback(error)
  }
}

export async function createAuthenticator (
  service: string,
  realm: string,
  kdc: string
): Promise<SASLCustomAuthenticator> {
  const tmpDir = await mkdtemp(resolvePaths(tmpdir(), 'sasl-gssapi-'))

  // We disable shortname qualification to avoid issues with domain-less hostnames on CI
  await writeFile(
    `${tmpDir}/krb5.conf`,
    `
[libdefaults]
  qualify_shortname = ""
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

  return authenticate.bind(null, service, tmpDir)
}

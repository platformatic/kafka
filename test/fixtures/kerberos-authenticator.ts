import krb, { type KerberosClient } from 'kerberos'
import { execSync } from 'node:child_process'
import { rmSync } from 'node:fs'
import { mkdtemp, writeFile } from 'node:fs/promises'
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

export interface KerberosCredentials {
  username: string
  password: string
}

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

/*
  KRB5_CONFIG and KRB5CCNAME are process wide, so they are only set while the Kerberos tooling needs
  them and the previous values are put back afterwards.
*/
function useKerberosEnvironment (kerberosRoot: string): () => void {
  const { KRB5_CONFIG, KRB5CCNAME } = process.env

  process.env.KRB5_CONFIG = `${kerberosRoot}/krb5.conf`
  process.env.KRB5CCNAME = `${kerberosRoot}/krb5.cache`

  return function restoreKerberosEnvironment () {
    if (typeof KRB5_CONFIG !== 'undefined') {
      process.env.KRB5_CONFIG = KRB5_CONFIG
    } else {
      delete process.env.KRB5_CONFIG
    }

    if (typeof KRB5CCNAME !== 'undefined') {
      process.env.KRB5CCNAME = KRB5CCNAME
    } else {
      delete process.env.KRB5CCNAME
    }
  }
}

/*
  Populates the credentials cache in kerberosRoot. This spawns up to two processes and performs a
  round trip to the KDC, all of it blocking the event loop, so the caller decides when to pay for it.
*/
async function acquireTicket (kerberosRoot: string, username: string, password: string): Promise<void> {
  // On MIT Kerberos, kinit does not support reading password from stdin or a password file
  // so we convert it to a keytab file if needed using ktutil
  if (!password.startsWith('keytab:')) {
    if (process.platform !== 'darwin') {
      execSync(`ktutil --keytab ${kerberosRoot}/keytab`, {
        input: `addent -password -p ${username} -k 1 -f \n${password}\nwkt ${kerberosRoot}/keytab\nquit\n`
      })

      password = `keytab:${kerberosRoot}/keytab`
      /* c8 ignore next 4 - Only executed on MacOS */
    } else {
      // On MacOS, we can use a password file directly since it uses Heimdal Kerberos
      await writeFile(`${kerberosRoot}/password`, password, 'utf-8')
    }
  }

  /* c8 ignore next - The password file branch is only executed on MacOS */
  const args = password.startsWith('keytab:')
    ? `-kt ${password.slice(7)}`
    : `--password-file=${kerberosRoot}/password`

  execSync(`kinit ${args} ${username}`, { stdio: 'pipe', env: process.env })
}

async function authenticate (
  service: string,
  kerberosRoot: string,
  hasTicket: boolean,
  _m: SASLMechanismValue,
  connection: Connection,
  authenticate: saslAuthenticateV2.SASLAuthenticationAPI,
  usernameProvider: string | CredentialProvider | undefined,
  passwordProvider: string | CredentialProvider | undefined,
  _t: string | CredentialProvider | undefined,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
): Promise<void> {
  let restoreKerberosEnvironment: (() => void) | undefined

  const afterRestoreCallback: Callback<SaslAuthenticateResponse> = (error, response) => {
    restoreKerberosEnvironment?.()
    callback(error, response)
  }

  try {
    /* c8 ignore next 6 - Only executed when the ticket was not acquired upfront */
    let username: string | undefined
    let password: string | undefined

    if (!hasTicket) {
      username = await saslUtils.getCredential('SASL/GSSAPI username', usernameProvider!)
      password = await saslUtils.getCredential('SASL/GSSAPI password', passwordProvider!)
    }

    restoreKerberosEnvironment = useKerberosEnvironment(kerberosRoot)

    /* c8 ignore next 3 - Only executed when the ticket was not acquired upfront */
    if (!hasTicket) {
      await acquireTicket(kerberosRoot, username!, password!)
    }

    krb.initializeClient(service, {}, (error, client) => {
      /* c8 ignore next 4 - Hard to test */
      if (error) {
        afterRestoreCallback(createKerberosAuthenticationError('Cannot initialize Kerberos client.', error))
        return
      }

      performChallenge(connection, authenticate, client, '', afterRestoreCallback)
    })
    /* c8 ignore next 3 - Hard to test */
  } catch (error) {
    afterRestoreCallback(error as Error)
  }
}

export async function createAuthenticator (
  service: string,
  realm: string,
  kdc: string,
  credentials?: KerberosCredentials
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

  /*
    The configuration and the credentials cache have to outlive a single authentication, since the
    same authenticator serves every connection it is attached to, so they are only removed on exit.
  */
  process.once('exit', () => {
    rmSync(tmpDir, { recursive: true, force: true })
  })

  /*
    Acquiring the ticket upfront keeps ktutil, kinit and their KDC round trip out of the connection
    handshake, which is bounded by connectTimeout. Doing it while authenticating used to push slow
    CI machines past that deadline and fail the connection with a timeout.
  */
  if (credentials) {
    const restoreKerberosEnvironment = useKerberosEnvironment(tmpDir)

    try {
      await acquireTicket(tmpDir, credentials.username, credentials.password)
    } finally {
      restoreKerberosEnvironment()
    }
  }

  return authenticate.bind(null, service, tmpDir, !!credentials)
}

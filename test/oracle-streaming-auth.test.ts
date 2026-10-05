import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import {
  audience,
  exchangeToken,
  validateConfiguration,
  writeCredentials,
  type FederationConfiguration
} from '../scripts/oracle-streaming-auth.ts'

const config: FederationConfiguration = {
  domain: 'https://idcs-test.identity.oraclecloud.com',
  clientId: 'client-id',
  clientSecret: 'secret-value',
  tenancy: 'ocid1.tenancy.oc1..test',
  region: 'us-sanjose-1',
  requestUrl: 'https://token.actions.githubusercontent.com/request?api-version=2',
  requestToken: 'github-request-secret',
  repository: 'platformatic/kafka'
}

function token (changes = {}) {
  return `header.${Buffer.from(
    JSON.stringify({
      iss: 'https://token.actions.githubusercontent.com',
      aud: audience,
      sub: 'repo:platformatic/kafka:environment:oracle',
      repository: 'platformatic/kafka',
      ref: 'refs/heads/main',
      exp: Math.floor(Date.now() / 1000) + 300,
      ...changes
    })
  ).toString('base64url')}.signature`
}

test('WIF requests the exact audience and binds the OCI token to the supplied public key', async () => {
  const jwt = token()
  const calls: { url: URL; options?: RequestInit }[] = []
  const request: typeof fetch = async (url, options) => {
    calls.push({ url: new URL(String(url)), options })
    return Response.json(calls.length === 1 ? { value: jwt } : { token: 'oci-session-token' })
  }
  strictEqual(await exchangeToken(config, 'public-key-base64', request), 'oci-session-token')
  strictEqual(calls.length, 2)
  strictEqual(calls[0].url.searchParams.get('audience'), audience)
  strictEqual(calls[0].url.searchParams.get('api-version'), '2')
  strictEqual(calls[1].url.href, `${config.domain}/oauth2/v1/token`)
  deepStrictEqual(Object.fromEntries(calls[1].options!.body as URLSearchParams), {
    grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
    requested_token_type: 'urn:oci:token-type:oci-upst',
    subject_token_type: 'jwt',
    subject_token: jwt,
    public_key: 'public-key-base64'
  })
  strictEqual(
    new Headers(calls[1].options?.headers).get('authorization'),
    `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64')}`
  )
  ok(calls.every(call => call.options?.redirect === 'error' && call.options.signal))
})

test('configuration rejects insecure URLs, missing credentials and INI injection before network requests', async () => {
  for (const change of [
    { domain: 'http://idcs-test.identity.oraclecloud.com' },
    { domain: 'https://attacker.example' },
    { domain: 'https://idcs-test.identity.oraclecloud.com/path' },
    { domain: 'https://user:password@idcs-test.identity.oraclecloud.com' },
    { requestUrl: 'http://example.com' },
    { clientSecret: '' },
    { region: 'us-sanjose-1\nkey_file=another-file' },
    { tenancy: 'not-an-ocid' },
    { clientId: 'id:secret' },
    { repository: '../repo' }
  ]) {
    throws(() => validateConfiguration({ ...config, ...change }), { code: 'PLT_KFK_USER' })
    await rejects(
      exchangeToken({ ...config, ...change }, 'key', async () => {
        throw new Error('Network must not be used.')
      }),
      { code: 'PLT_KFK_USER' }
    )
  }
})

test('tokens outside the trusted environment and main never reach OCI', async () => {
  for (const changes of [
    { iss: 'other-issuer' },
    { aud: 'other-audience' },
    { sub: 'repo:platformatic/kafka:pull_request' },
    { sub: 'repo:platformatic/kafka:environment:regression' },
    { sub: 'repo:platformatic/kafka:environment:oracle-streaming' },
    { repository: 'other/repo' },
    { ref: 'refs/heads/feature' },
    { exp: 0 },
    { exp: 'future' }
  ]) {
    let calls = 0
    await rejects(
      exchangeToken(config, 'key', async () => {
        calls++
        return Response.json({ value: token(changes) })
      }),
      { code: 'PLT_KFK_USER', message: /claims/ }
    )
    strictEqual(calls, 1)
  }
})

test('HTTP errors and malformed responses never leak credential-bearing bodies or transport errors', async () => {
  for (const stage of [1, 2]) {
    for (const failure of ['http', 'transport', 'json', 'missing-token', 'newline']) {
      let calls = 0
      await rejects(
        exchangeToken(config, 'key', async () => {
          calls++
          if (calls !== stage) {
            return Response.json({ value: token() })
          }
          if (failure === 'transport') {
            throw new Error(config.clientSecret)
          }
          if (failure === 'http') {
            return new Response(config.clientSecret, { status: 401 })
          }
          if (failure === 'json') {
            return new Response(config.clientSecret)
          }
          return Response.json(failure === 'missing-token' ? {} : { token: 'secret\nvalue', value: 'secret\nvalue' })
        }),
        (error: Error & { code: string }) => {
          strictEqual(error.code, 'PLT_KFK_USER')
          ok(!error.message.includes(config.clientSecret))
          strictEqual(error.cause, undefined)
          return true
        }
      )
    }
  }
})

test('ephemeral CLI profiles have private permissions and never store the OAuth secret', async () => {
  const directory = await mkdtemp(fileURLToPath(new URL('../.oracle-wif-test-', import.meta.url)))
  try {
    const environment = await writeCredentials(directory, config, 'private-test-key', 'oci-session-token')
    strictEqual(environment.OCI_CLI_AUTH, 'security_token')
    strictEqual(environment.OCI_CLI_PROFILE, 'DEFAULT')
    const profile = await readFile(environment.OCI_CLI_CONFIG_FILE, 'utf8')
    ok(profile.includes(`tenancy=${config.tenancy}`))
    ok(profile.includes(`region=${config.region}`))
    ok(!profile.includes(config.clientSecret))
    const [folder] = await readdir(directory)
    strictEqual((await stat(join(directory, folder))).mode & 0o777, 0o700)
    for (const file of ['config', 'private.pem', 'token']) {
      strictEqual((await stat(join(directory, folder, file))).mode & 0o777, 0o600)
    }
    const second = await writeCredentials(directory, config, 'second-key', 'second-token')
    ok(second.OCI_CLI_CONFIG_FILE !== environment.OCI_CLI_CONFIG_FILE)
    strictEqual(await readFile(join(directory, folder, 'token'), 'utf8'), 'oci-session-token')
  } finally {
    // Remove only the unique test directory created above, never the configured runner directory.
    await rm(directory, { recursive: true, force: true })
  }
})

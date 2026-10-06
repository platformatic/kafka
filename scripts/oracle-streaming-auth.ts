import { generateKeyPair } from 'node:crypto'
import { appendFile, mkdtemp, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { UserError } from '../src/errors.ts'

const generateKeys = promisify(generateKeyPair)
export const audience = 'https://cloud.oracle.com'

export interface FederationConfiguration {
  domain: string
  clientId: string
  clientSecret: string
  tenancy: string
  region: string
  requestUrl: string
  requestToken: string
  repository: string
}

export function validateConfiguration (config: FederationConfiguration) {
  if (Object.values(config).some(value => !value || /[\r\n\0]/.test(value))) {
    throw new UserError('OCI WIF configuration is missing or contains invalid characters.')
  }
  let domain: URL
  let request: URL
  try {
    domain = new URL(config.domain)
    request = new URL(config.requestUrl)
  } catch {
    throw new UserError('OCI WIF requires valid HTTPS domain and GitHub token request URLs.')
  }
  if (
    domain.protocol !== 'https:' ||
    !domain.hostname.endsWith('.identity.oraclecloud.com') ||
    domain.username ||
    domain.password ||
    domain.port ||
    domain.search ||
    domain.hash ||
    domain.pathname !== '/' ||
    request.protocol !== 'https:' ||
    request.username ||
    request.password ||
    request.hash
  ) {
    throw new UserError('OCI WIF requires an OCI Identity Domain URL and a secure GitHub token request URL.')
  }
  if (
    !/^ocid1\.tenancy\.[a-z0-9.]+$/.test(config.tenancy) ||
    !/^[a-z0-9-]+$/.test(config.region) ||
    !/^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(config.repository) ||
    config.repository.split('/').some(part => part === '.' || part === '..') ||
    config.clientId.includes(':')
  ) {
    throw new UserError('OCI WIF tenancy, region, repository or OAuth client ID is invalid.')
  }
}

export async function exchangeToken (
  config: FederationConfiguration,
  publicKey: string,
  request: typeof fetch = fetch
) {
  validateConfiguration(config)
  try {
    const url = new URL(config.requestUrl)
    url.searchParams.set('audience', audience)
    const githubResponse = await request(url, {
      headers: { authorization: `Bearer ${config.requestToken}` },
      redirect: 'error',
      signal: AbortSignal.timeout(30_000)
    })
    if (!githubResponse.ok) {
      throw new UserError(`GitHub OIDC request failed with HTTP ${githubResponse.status}.`)
    }
    const { value: jwt } = (await githubResponse.json()) as { value?: string }
    if (typeof jwt !== 'string' || jwt.split('.').length !== 3 || /[\r\n]/.test(jwt)) {
      throw new UserError('GitHub returned an invalid OIDC token.')
    }
    const claims = JSON.parse(Buffer.from(jwt.split('.')[1], 'base64url').toString())
    // This sanity check is not signature validation. OCI must validate the JWT against its configured trust.
    if (
      claims.iss !== 'https://token.actions.githubusercontent.com' ||
      claims.aud !== audience ||
      claims.sub !== `repo:${config.repository}:environment:oracle` ||
      claims.repository !== config.repository ||
      claims.ref !== 'refs/heads/main' ||
      !Number.isFinite(claims.exp) ||
      claims.exp * 1000 <= Date.now() + 60_000
    ) {
      throw new UserError('GitHub OIDC claims do not match the trusted oracle environment on main.')
    }
    // Match OCI SDK TokenExchangeSigner's UPST request, binding the session to an ephemeral RSA public key.
    const response = await request(new URL('/oauth2/v1/token', config.domain), {
      method: 'POST',
      headers: {
        authorization: `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64')}`,
        'content-type': 'application/x-www-form-urlencoded'
      },
      body: new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
        requested_token_type: 'urn:oci:token-type:oci-upst',
        subject_token_type: 'jwt',
        subject_token: jwt,
        public_key: publicKey
      }),
      redirect: 'error',
      signal: AbortSignal.timeout(30_000)
    })
    if (!response.ok) {
      throw new UserError(
        `OCI WIF token exchange failed with HTTP ${response.status}; check the OAuth client and trust.`
      )
    }
    const { token } = (await response.json()) as { token?: string }
    if (typeof token !== 'string' || !token || /[\r\n\0]/.test(token)) {
      throw new UserError('OCI WIF returned an invalid session token.')
    }
    return token
  } catch (error) {
    // HTTP bodies and transport exceptions can expose JWTs or credentials. Only emit our sanitized errors.
    throw error instanceof UserError ? error : new UserError('OCI WIF authentication failed or timed out.')
  }
}

export async function writeCredentials (
  directory: string,
  config: FederationConfiguration,
  privateKey: string,
  token: string
) {
  validateConfiguration(config)
  if (!privateKey || !token || /[\r\n\0]/.test(token)) {
    throw new UserError('OCI WIF credential material is invalid.')
  }
  // A fresh private directory avoids overwriting local profiles and prevents key/token pairs from mixing.
  const folder = await mkdtemp(join(directory, 'oracle-streaming-wif-'))
  const keyFile = join(folder, 'private.pem')
  const tokenFile = join(folder, 'token')
  const configFile = join(folder, 'config')
  await writeFile(keyFile, privateKey, { mode: 0o600, flag: 'wx' })
  await writeFile(tokenFile, token, { mode: 0o600, flag: 'wx' })
  await writeFile(
    configFile,
    [
      '[DEFAULT]',
      `tenancy=${config.tenancy}`,
      `region=${config.region}`,
      `key_file=${keyFile}`,
      `security_token_file=${tokenFile}`,
      ''
    ].join('\n'),
    { mode: 0o600, flag: 'wx' }
  )
  return { OCI_CLI_CONFIG_FILE: configFile, OCI_CLI_AUTH: 'security_token', OCI_CLI_PROFILE: 'DEFAULT' }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (
      process.env.GITHUB_ACTIONS !== 'true' ||
      process.env.GITHUB_REF !== 'refs/heads/main' ||
      !process.env.RUNNER_TEMP ||
      !process.env.GITHUB_ENV
    ) {
      throw new UserError('OCI WIF login requires GitHub Actions on main with a runner temporary directory.')
    }
    const config: FederationConfiguration = {
      domain: process.env.OCI_WIF_DOMAIN_URL ?? '',
      clientId: process.env.OCI_WIF_CLIENT_ID ?? '',
      clientSecret: process.env.OCI_WIF_CLIENT_SECRET ?? '',
      tenancy: process.env.OCI_TENANCY_ID ?? '',
      region: process.env.OCI_CLI_REGION ?? '',
      requestUrl: process.env.ACTIONS_ID_TOKEN_REQUEST_URL ?? '',
      requestToken: process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN ?? '',
      repository: process.env.GITHUB_REPOSITORY ?? ''
    }
    validateConfiguration(config)
    const { publicKey, privateKey } = await generateKeys('rsa', { modulusLength: 2048 })
    const token = await exchangeToken(config, publicKey.export({ type: 'spki', format: 'der' }).toString('base64'))
    console.log(`::add-mask::${token.replaceAll('%', '%25')}`)
    const environment = await writeCredentials(
      process.env.RUNNER_TEMP,
      config,
      privateKey.export({ type: 'pkcs1', format: 'pem' }).toString(),
      token
    )
    await appendFile(
      process.env.GITHUB_ENV,
      Object.entries(environment)
        .map(([key, value]) => `${key}=${value}\n`)
        .join('')
    )
    console.log('OCI WIF session configured; no permanent API signing key was used.')
  } catch (error) {
    console.error(
      error instanceof UserError
        ? `${error.code}: ${error.message}`
        : `${UserError.code}: OCI WIF authentication failed unexpectedly.`
    )
    process.exitCode = 1
  }
}

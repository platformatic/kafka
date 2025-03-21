import { createHash, createHmac, pbkdf2Sync, randomBytes } from 'node:crypto'
import { type SaslAuthenticateResponse, saslAuthenticateV2 } from '../../apis/security/sasl-authenticate.ts'
import { type Connection } from '../../connection/connection.ts'
import { AuthenticationError } from '../../errors.ts'

const GS2_HEADER = 'n,,'
const GS2_HEADER_BASE64 = Buffer.from(GS2_HEADER).toString('base64')
const HMAC_CLIENT_KEY = 'Client Key'
const HMAC_SERVER_KEY = 'Server Key'
const PARAMETERS_PARSER = /([^=]+)=(.+)/

export interface ScramAlgorithmDefinition {
  keyLength: number
  algorithm: string
  minIterations: number
}

export const ScramAlgorithms = {
  'SHA-256': {
    keyLength: 32,
    algorithm: 'sha256',
    minIterations: 4096
  },
  'SHA-512': {
    keyLength: 64,
    algorithm: 'sha512',
    minIterations: 4096
  }
} as const
export type ScramAlgorithm = keyof typeof ScramAlgorithms

function createNonce (): string {
  return randomBytes(16).toString('base64url')
}

function sanitizeString (str: string): string {
  return str.replaceAll('=', '=3D').replace(',', '=2C')
}

function parseParameters (data: Buffer): Record<string, string> {
  const original = data.toString('utf-8')

  return {
    __original: original,
    ...Object.fromEntries(original.split(',').map(param => param.match(PARAMETERS_PARSER)!.slice(1, 3)))
  }
}

// h, hi, hmac and xor, are defined in https://datatracker.ietf.org/doc/html/rfc5802#section-2.2

function h (definition: ScramAlgorithmDefinition, data: string | Buffer) {
  return createHash(definition.algorithm).update(data).digest()
}

function hi (definition: ScramAlgorithmDefinition, password: string, salt: Buffer, iterations: number) {
  return pbkdf2Sync(password, salt, iterations, definition.keyLength, definition.algorithm)
}

function hmac (definition: ScramAlgorithmDefinition, key: Buffer, data: string | Buffer): Buffer {
  return createHmac(definition.algorithm, key).update(data).digest()
}

function xor (a: Buffer, b: Buffer): Buffer {
  if (a.byteLength !== b.byteLength) {
    throw new AuthenticationError('Buffers must have the same length.')
  }

  const result = Buffer.allocUnsafe(a.length)

  for (let i = 0; i < a.length; i++) {
    result[i] = a[i] ^ b[i]
  }

  return result
}

// Implements https://datatracker.ietf.org/doc/html/rfc5802#section-9
export async function authenticate (
  connection: Connection,
  algorithm: ScramAlgorithm,
  username: string,
  password: string
): Promise<SaslAuthenticateResponse> {
  const definition = ScramAlgorithms[algorithm]

  if (!definition) {
    throw new AuthenticationError(`Unsupported SCRAM algorithm ${algorithm}`)
  }

  const clientNonce = createNonce()
  const clientFirstMessageBare = `n=${sanitizeString(username)},r=${clientNonce}`

  // First of all, send the first message
  const firstResponse = await saslAuthenticateV2.async(
    connection,
    Buffer.from(`${GS2_HEADER}${clientFirstMessageBare}`)
  )
  const firstData = parseParameters(firstResponse.authBytes)

  // Extract some parameters
  const salt = Buffer.from(firstData.s, 'base64')
  const iterations = parseInt(firstData.i, 10)
  const serverNonce = firstData.r
  const serverFirstMessage = firstData.__original

  // Validate response
  if (!serverNonce.startsWith(clientNonce)) {
    throw new AuthenticationError('Server nonce does not start with client nonce.')
  }

  if (definition.minIterations > iterations) {
    throw new AuthenticationError(
      `Algorithm ${algorithm} requires at least ${definition.minIterations} iterations, while ${iterations} were requested.`
    )
  }

  // SaltedPassword  := Hi(Normalize(password), salt, i)
  // ClientKey       := HMAC(SaltedPassword, "Client Key")
  // StoredKey       := H(ClientKey)
  // AuthMessage     := ClientFirstMessageBare + "," ServerFirstMessage + "," + ClientFinalMessageWithoutProof
  // ClientSignature := HMAC(StoredKey, AuthMessage)
  // ClientProof     := ClientKey XOR ClientSignature
  // ServerKey       := HMAC(SaltedPassword, "Server Key")
  // ServerSignature := HMAC(ServerKey, AuthMessage)
  const saltedPassword = hi(definition, password, salt, iterations)
  const clientKey = hmac(definition, saltedPassword, HMAC_CLIENT_KEY)
  const storedKey = h(definition, clientKey)
  const clientFinalMessageWithoutProof = `c=${GS2_HEADER_BASE64},r=${serverNonce}`
  const authMessage = `${clientFirstMessageBare},${serverFirstMessage},${clientFinalMessageWithoutProof}`
  const clientSignature = hmac(definition, storedKey, authMessage)
  const clientProof = xor(clientKey, clientSignature)
  const serverKey = hmac(definition, saltedPassword, HMAC_SERVER_KEY)
  const serverSignature = hmac(definition, serverKey, authMessage)

  // Send the last message to the server
  const lastResponse = await saslAuthenticateV2.async(
    connection,
    Buffer.from(`${clientFinalMessageWithoutProof},p=${clientProof.toString('base64')}`)
  )
  const lastData = parseParameters(lastResponse.authBytes)

  if (lastData.e) {
    throw new AuthenticationError(lastData.e)
  } else if (lastData.v !== serverSignature.toString('base64')) {
    throw new AuthenticationError('Invalid server signature.')
  }

  return lastResponse
}

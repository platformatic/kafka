import { type SASLAuthenticationAPI, type SaslAuthenticateResponse } from '../../apis/security/sasl-authenticate-v2.ts'
import { type Connection } from '../../network/connection.ts'

export function authenticate (
  authenticateAPI: SASLAuthenticationAPI,
  connection: Connection,
  username: string,
  password: string
): Promise<SaslAuthenticateResponse> {
  return authenticateAPI.async(connection, Buffer.from(['', username, password].join('\0')))
}

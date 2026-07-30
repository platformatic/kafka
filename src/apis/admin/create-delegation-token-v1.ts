import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './create-delegation-token-v0.ts'

export type {
  CreateDelegationTokenRequest,
  CreateDelegationTokenRequestRenewer,
  CreateDelegationTokenResponse
} from './create-delegation-token-v0.ts'

/*
  CreateDelegationToken Request (Version: 1) => [renewers] max_lifetime_ms
  CreateDelegationToken Response (Version: 1) => error_code principal_type principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms
*/
export { createRequest, parseResponse }
export const api = createAPI(38, 1, createRequest, parseResponse, false, false)

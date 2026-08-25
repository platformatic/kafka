import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './renew-delegation-token-v0.ts'

export type { RenewDelegationTokenRequest, RenewDelegationTokenResponse } from './renew-delegation-token-v0.ts'

/*
  RenewDelegationToken Request (Version: 1) => hmac renew_period_ms
  RenewDelegationToken Response (Version: 1) => error_code expiry_timestamp_ms throttle_time_ms
*/
export { createRequest, parseResponse }
export const api = createAPI(39, 1, createRequest, parseResponse, false, false)

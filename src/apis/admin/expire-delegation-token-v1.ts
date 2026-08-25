import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './expire-delegation-token-v0.ts'

export type { ExpireDelegationTokenRequest, ExpireDelegationTokenResponse } from './expire-delegation-token-v0.ts'

/*
  ExpireDelegationToken Request (Version: 1) => hmac expiry_time_period_ms
  ExpireDelegationToken Response (Version: 1) => error_code expiry_timestamp_ms throttle_time_ms
*/
export { createRequest, parseResponse }
export const api = createAPI(40, 1, createRequest, parseResponse, false, false)

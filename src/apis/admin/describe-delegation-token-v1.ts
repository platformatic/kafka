import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './describe-delegation-token-v0.ts'

export type {
  DescribeDelegationTokenRequest,
  DescribeDelegationTokenRequestOwner,
  DescribeDelegationTokenResponse,
  DescribeDelegationTokenResponseRenewer,
  DescribeDelegationTokenResponseToken
} from './describe-delegation-token-v0.ts'

/*
  DescribeDelegationToken Request (Version: 1) => [owners]
  DescribeDelegationToken Response (Version: 1) => error_code [tokens] throttle_time_ms
*/
export { createRequest, parseResponse }
export const api = createAPI(41, 1, createRequest, parseResponse, false, false)

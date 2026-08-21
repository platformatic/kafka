import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './end-txn-v0.ts'
import type { EndTxnRequest, EndTxnResponse } from './end-txn-v0.ts'

export { createRequest, parseResponse }
export type { EndTxnRequest, EndTxnResponse } from './end-txn-v0.ts'

/*
  EndTxn Request (Version: 1) => transactional_id producer_id producer_epoch committed
  EndTxn Response (Version: 1) => throttle_time_ms error_code
  Version 1 has the same wire schema as version 0.
*/
export const api = createAPI<EndTxnRequest, EndTxnResponse>(26, 1, createRequest, parseResponse, false, false)

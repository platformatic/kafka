import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './add-partitions-to-txn-v0.ts'
import type { AddPartitionsToTxnRequest, AddPartitionsToTxnResponse } from './add-partitions-to-txn-v0.ts'

export { createRequest, parseResponse }
export type * from './add-partitions-to-txn-v0.ts'

/*
  AddPartitionsToTxn Request (Version: 2) => transactional_id producer_id producer_epoch [topics]
  AddPartitionsToTxn Response (Version: 2) => throttle_time_ms [errors]
  Version 2 has the same wire schema as version 0.
*/
export const api = createAPI<AddPartitionsToTxnRequest, AddPartitionsToTxnResponse>(
  24,
  2,
  createRequest,
  parseResponse,
  false,
  false
)

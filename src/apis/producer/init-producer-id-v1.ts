import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './init-producer-id-v0.ts'
import type { InitProducerIdRequest, InitProducerIdResponse } from './init-producer-id-v0.ts'

export { createRequest, parseResponse }
export type { InitProducerIdRequest, InitProducerIdResponse } from './init-producer-id-v0.ts'

/*
  InitProducerId Request (Version: 1) => transactional_id transaction_timeout_ms
  InitProducerId Response (Version: 1) => throttle_time_ms error_code producer_id producer_epoch
  Version 1 has the same wire schema as version 0.
*/
export const api = createAPI<InitProducerIdRequest, InitProducerIdResponse>(
  22,
  1,
  createRequest,
  parseResponse,
  false,
  false
)

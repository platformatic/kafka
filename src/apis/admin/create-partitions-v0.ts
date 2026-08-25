import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './create-partitions-v1.ts'
export * from './create-partitions-v1.ts'
/* CreatePartitions v0 has the same classic wire layout as v1. */
export const api = createAPI(37, 0, createRequest, parseResponse, false, false)

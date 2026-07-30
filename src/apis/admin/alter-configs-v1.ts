import { createAPI } from '../definitions.ts'
import { createRequest, parseResponse } from './alter-configs-v0.ts'
export * from './alter-configs-v0.ts'
/* AlterConfigs v1 uses the v0 wire shape and adds no fields. */
export const api = createAPI(33, 1, createRequest, parseResponse, false, false)

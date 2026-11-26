import { type BaseOptions } from '../base/types.ts'

export interface ShareConsumerOptions extends BaseOptions {
  groupId: string
  leaveGroupTimeout?: number
}

export interface ShareGroupOptions {
  topics: string[]
}

export interface ShareConsumerGroupPayload {
  groupId: string
  memberId: string
  memberEpoch: number
}

export interface ShareConsumerHeartbeatErrorPayload extends ShareConsumerGroupPayload {
  error: Error
}

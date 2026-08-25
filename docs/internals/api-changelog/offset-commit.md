# OffsetCommit (API key 8)

Intended range: v0-v9.

| Version | Request                                                             | Response                                  |
| ------- | ------------------------------------------------------------------- | ----------------------------------------- |
| v9      | Supports member epochs for the consumer protocol.                   | Supports `GROUP_ID_NOT_FOUND` and `STALE_MEMBER_EPOCH`. |
| v8      | Uses flexible encoding and tagged fields.                           | Uses flexible encoding and tagged fields. |
| v7      | Adds a group instance ID.                                           | Supports `FENCED_MEMBER_EPOCH`.           |
| v6      | Adds committed leader epochs.                                       | Unchanged.                                |
| v5      | Removes retention time.                                             | Unchanged.                                |
| v4      | Unchanged.                                                          | Returns responses before quota throttling. |
| v3      | Unchanged.                                                          | Adds `throttle_time_ms`.                  |
| v2      | Adds retention time.                                                | Unchanged.                                |
| v1      | Adds generation, member, and commit timestamp fields.               | Unchanged.                                |
| v0      | Group ID and committed offsets.                                     | Per-topic partition errors.               |

# OffsetFetch (API key 9)

Intended range: v0-v9.

| Version | Request                                              | Response                                                        |
| ------- | ---------------------------------------------------- | --------------------------------------------------------------- |
| v9      | Wire: adds group member ID and member epoch.         | Wire: unchanged from v8. Semantics: the new consumer group protocol may return `STALE_MEMBER_EPOCH` or `UNKNOWN_MEMBER_ID`. |
| v8      | Supports multiple groups and uses flexible encoding. | Returns results for multiple groups and uses flexible encoding. |
| v7      | Adds `require_stable`.                               | Supports `UNSTABLE_OFFSET_COMMIT`.                              |
| v6      | Uses flexible encoding and tagged fields.            | Uses flexible encoding and tagged fields.                       |
| v5      | Unchanged.                                           | Adds committed leader epochs.                                   |
| v4      | Unchanged.                                           | Returns responses before quota throttling.                      |
| v3      | Unchanged.                                           | Adds `throttle_time_ms`.                                        |
| v2      | Allows a null topic list for all topics.             | Adds a top-level error code.                                    |
| v1      | Unchanged.                                           | Unchanged.                                                      |
| v0      | Group ID and topic partitions.                       | Committed offsets and metadata.                                 |

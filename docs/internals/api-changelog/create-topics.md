# CreateTopics (API key 19)

Intended range: v0-v7.

| Version | Request                                             | Response                                  |
| ------- | --------------------------------------------------- | ----------------------------------------- |
| v7      | Unchanged.                                          | Adds topic IDs.                           |
| v6      | Unchanged.                                          | Supports `THROTTLING_QUOTA_EXCEEDED`.     |
| v5      | Wire: uses flexible encoding and tagged fields.     | Wire: uses flexible encoding, tagged fields, and config result fields. Semantics: returns topic configs. |
| v4      | Makes partitions and replication factor optional.   | Unchanged.                                |
| v3      | Unchanged.                                          | Returns responses before quota throttling. |
| v2      | Unchanged.                                          | Adds `throttle_time_ms`.                  |
| v1      | Adds `validate_only`.                               | Adds per-topic error messages.            |
| v0      | Initial topic creation request.                     | Initial per-topic results.                |

# TxnOffsetCommit (API key 28)

Intended range: v0-v4.

| Version | Request                                                     | Response                                   |
| ------- | ----------------------------------------------------------- | ------------------------------------------ |
| v4      | Supports `TRANSACTION_ABORTABLE`.                           | Supports `TRANSACTION_ABORTABLE`.          |
| v3      | Uses flexible encoding; adds group membership fields.       | Uses flexible encoding; adds member errors. |
| v2      | Adds committed leader epoch for each partition.             | Unchanged.                                 |
| v1      | Unchanged.                                                  | Unchanged.                                 |
| v0      | Transactional ID, group ID, producer identity, and offsets. | Per-topic partition errors.                |

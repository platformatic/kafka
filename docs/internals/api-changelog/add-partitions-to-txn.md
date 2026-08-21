# AddPartitionsToTxn (API key 24)

Intended range: v0-v5.

| Version | Request                                                    | Response                                            |
| ------- | ---------------------------------------------------------- | --------------------------------------------------- |
| v5      | Supports `TRANSACTION_ABORTABLE`.                          | Supports `TRANSACTION_ABORTABLE`.                   |
| v4      | Supports multiple transactions and adds `verify_only`.     | Adds a top-level error and results per transaction. |
| v3      | Uses flexible encoding and tagged fields.                  | Uses flexible encoding and tagged fields.           |
| v2      | Supports `PRODUCER_FENCED`.                                | Supports `PRODUCER_FENCED`.                         |
| v1      | Unchanged.                                                 | Unchanged.                                          |
| v0      | Transactional ID, producer identity, and topic partitions. | Per-topic partition errors.                         |

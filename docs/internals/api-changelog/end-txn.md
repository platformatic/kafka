# EndTxn (API key 26)

Intended range: v0-v4.

| Version | Request                                                     | Response                                  |
| ------- | ----------------------------------------------------------- | ----------------------------------------- |
| v4      | Supports `TRANSACTION_ABORTABLE`.                           | Supports `TRANSACTION_ABORTABLE`.         |
| v3      | Uses flexible encoding and tagged fields.                   | Uses flexible encoding and tagged fields. |
| v2      | Supports `PRODUCER_FENCED`.                                 | Supports `PRODUCER_FENCED`.                |
| v1      | Unchanged.                                                  | Unchanged.                                |
| v0      | Transactional ID, producer identity, and commit/abort flag. | Throttle time and error code.             |

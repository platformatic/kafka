# InitProducerId (API key 22)

Intended range: v0-v5.

| Version | Request                                                                | Response                                  |
| ------- | ---------------------------------------------------------------------- | ----------------------------------------- |
| v5      | Supports `TRANSACTION_ABORTABLE`.                                      | Supports `TRANSACTION_ABORTABLE`.         |
| v4      | Supports `PRODUCER_FENCED`.                                            | Supports `PRODUCER_FENCED`.               |
| v3      | Adds producer ID and epoch for epoch bumps.                            | Unchanged.                                |
| v2      | Uses flexible encoding and tagged fields.                              | Uses flexible encoding and tagged fields. |
| v1      | Unchanged.                                                             | Unchanged.                                |
| v0      | Transactional ID and transaction timeout.                              | Producer ID and epoch.                    |

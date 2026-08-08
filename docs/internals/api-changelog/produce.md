# Produce (API key 0)

Intended range: v3-v11.

| Version | Request                                              | Response                                     |
| ------- | ---------------------------------------------------- | -------------------------------------------- |
| v11     | Unchanged.                                           | Supports `TRANSACTION_ABORTABLE`.            |
| v10     | Unchanged.                                           | Adds current leaders and node endpoints.     |
| v9      | Uses flexible encoding and tagged fields.            | Uses flexible encoding and tagged fields.    |
| v8      | Unchanged.                                           | Adds per-record errors and an error message. |
| v7      | Adds Zstandard compression support.                  | Unchanged.                                   |
| v6      | Unchanged.                                           | Unchanged.                                   |
| v5      | Unchanged.                                           | Adds `log_start_offset`.                     |
| v4      | Unchanged.                                           | Supports `KAFKA_STORAGE_ERROR`.              |
| v3      | Uses record batches and includes `transactional_id`. | Unchanged.                                   |

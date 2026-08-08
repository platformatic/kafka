# Fetch (API key 1)

Intended range: v4-v17.

| Version | Request                                   | Response                                           |
| ------- | ----------------------------------------- | -------------------------------------------------- |
| v17     | Adds replica directory IDs.               | Unchanged.                                         |
| v16     | Unchanged.                                | Adds current-leader node endpoints.                 |
| v15     | Adds replica state for follower fetches.  | Unchanged.                                         |
| v14     | Wire: unchanged from v13.                  | Wire: unchanged from v13. Semantics: may return `OFFSET_MOVED_TO_TIERED_STORAGE`. |
| v13     | Replaces topic names with topic IDs.      | Replaces topic names with topic IDs.               |
| v12     | Uses flexible encoding and adds `last_fetched_epoch`. | Uses flexible encoding and adds divergence and leader data. |
| v11     | Adds `rack_id`.                           | Unchanged.                                         |
| v10     | Supports Zstandard-compressed records.    | Supports Zstandard-compressed records.              |
| v9      | Adds `current_leader_epoch`.              | Unchanged.                                         |
| v8      | Unchanged.                                | On quota violation, sends responses before throttling. |
| v7      | Adds incremental fetch sessions.          | Adds a session ID.                                 |
| v6      | Unchanged.                                | Supports `KAFKA_STORAGE_ERROR`.                     |
| v5      | Adds `log_start_offset`.                  | Adds `log_start_offset`.                            |
| v4      | Adds `isolation_level`.                   | Adds last stable offsets and aborted transactions. |

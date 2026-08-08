# ListOffsets (API key 2)

Intended range: v0-v9.

| Version | Request                                              | Response                                                           |
| ------- | ---------------------------------------------------- | ------------------------------------------------------------------ |
| v9      | Adds last-tiered-offset lookup.                      | Adds last-tiered-offset lookup.                                    |
| v8      | Adds local-log-start-offset lookup.                  | Adds local-log-start-offset lookup.                                |
| v7      | Adds maximum-timestamp lookup.                       | Unchanged.                                                         |
| v6      | Uses flexible encoding and tagged fields.            | Uses flexible encoding and tagged fields.                          |
| v5      | Unchanged.                                           | Supports `OFFSET_NOT_AVAILABLE`.                                   |
| v4      | Adds current leader epochs.                          | Adds leader epochs.                                                |
| v3      | Unchanged.                                           | On quota violation, sends responses before throttling.             |
| v2      | Adds `isolation_level`.                              | Adds `throttle_time_ms`.                                           |
| v1      | Removes `max_num_offsets`; returns one offset.       | Replaces the offsets array with one offset and adds its timestamp. |
| v0      | Timestamp-based offset lookup.                       | Returns multiple offsets per partition.                            |

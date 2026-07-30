# SyncGroup (API key 14)

Intended range: v0-v5.

| Version | Request                                                 | Response                                  |
| ------- | ------------------------------------------------------- | ----------------------------------------- |
| v5      | Adds protocol type and name.                            | Adds protocol type and name.              |
| v4      | Uses flexible encoding and tagged fields.               | Uses flexible encoding and tagged fields. |
| v3      | Adds a group instance ID.                               | Unchanged.                                |
| v2      | Unchanged.                                              | Unchanged.                                |
| v1      | Unchanged.                                              | Adds `throttle_time_ms`.                  |
| v0      | Group assignments from the leader.                      | Member assignment and group error.        |

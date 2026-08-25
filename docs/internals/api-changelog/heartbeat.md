# Heartbeat (API key 12)

Intended range: v0-v4.

| Version | Request                                   | Response                                  |
| ------- | ----------------------------------------- | ----------------------------------------- |
| v4      | Uses flexible encoding and tagged fields. | Uses flexible encoding and tagged fields. |
| v3      | Adds a group instance ID.                 | Unchanged.                                |
| v2      | Unchanged.                                | Unchanged.                                |
| v1      | Unchanged.                                | Adds `throttle_time_ms`.                  |
| v0      | Group ID, generation ID, and member ID.   | Group error.                              |

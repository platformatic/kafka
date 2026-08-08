# LeaveGroup (API key 13)

Intended range: v0-v5.

| Version | Request                                           | Response                                  |
| ------- | ------------------------------------------------- | ----------------------------------------- |
| v5      | Unchanged.                                        | Unchanged.                                |
| v4      | Uses flexible encoding and tagged fields.         | Uses flexible encoding and tagged fields. |
| v3      | Adds a reason for leaving.                        | Unchanged.                                |
| v2      | Supports multiple members and group instance IDs. | Returns an error for each member.         |
| v1      | Unchanged.                                        | Adds `throttle_time_ms`.                  |
| v0      | Group ID and member ID.                           | Group error.                              |

# JoinGroup (API key 11)

Intended range: v0-v9.

| Version | Request                                        | Response                                  |
| ------- | ---------------------------------------------- | ----------------------------------------- |
| v9      | Adds a rebalance reason.                       | Unchanged.                                |
| v8      | Unchanged.                                     | Adds `skip_assignment`.                   |
| v7      | Unchanged.                                     | Makes protocol type and name nullable.    |
| v6      | Uses flexible encoding and tagged fields.      | Uses flexible encoding and tagged fields. |
| v5      | Unchanged.                                     | Unchanged.                                |
| v4      | Adds group instance IDs for static membership. | Adds group instance IDs for members.      |
| v3      | Unchanged.                                     | Unchanged.                                |
| v2      | Adds `rebalance_timeout_ms`.                   | Unchanged.                                |
| v1      | Unchanged.                                     | Adds `throttle_time_ms`.                  |
| v0      | Group membership protocols.                    | Generation, leader, and member metadata.  |

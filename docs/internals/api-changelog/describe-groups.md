# DescribeGroups (API key 15)

Intended range: v0-v5.

| Version | Request                                   | Response                                  |
| ------- | ----------------------------------------- | ----------------------------------------- |
| v5      | Uses flexible encoding and tagged fields. | Uses flexible encoding and tagged fields. |
| v4      | Unchanged.                                | Adds member group instance IDs.           |
| v3      | Adds `include_authorized_operations`.     | Adds authorized operations.               |
| v2      | Unchanged.                                | Returns responses before quota throttling. |
| v1      | Unchanged.                                | Adds throttle time.                       |
| v0      | Initial group-name list.                  | Initial group and member descriptions.    |

# DescribeLogDirs (API key 35)

Intended range: v0-v4.

| Version | Request                                   | Response                                            |
| ------- | ----------------------------------------- | --------------------------------------------------- |
| v4      | No changes.                               | Adds total and usable bytes for each log directory. |
| v3      | No changes.                               | Adds a top-level error code.                        |
| v2      | Uses flexible encoding and tagged fields. | Uses flexible encoding and tagged fields.           |
| v1      | No changes.                               | Adds throttle time.                                 |
| v0      | Initial broker-local request.             | Initial log directory descriptions.                 |

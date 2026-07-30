# ApiVersions (API key 18)

Intended range: v0-v4.

| Version | Request                                                                          | Response                                  |
| ------- | -------------------------------------------------------------------------------- | ----------------------------------------- |
| v4      | Unchanged.                                                                       | Allows supported-feature minimum version 0. |
| v3      | Adds client software name and version; uses flexible encoding and tagged fields. | Uses flexible encoding and tagged fields. |
| v2      | Unchanged.                                                                       | Returns responses before quota throttling. |
| v1      | Unchanged.                                                                       | Adds `throttle_time_ms`.                  |
| v0      | Empty request.                                                                   | API key and minimum/maximum version list. |

# DeleteRecords (API key 21)

Intended range: v0-v2.

| Version | Request                                   | Response                                  |
| ------- | ----------------------------------------- | ----------------------------------------- |
| v2      | Uses flexible encoding and tagged fields. | Uses flexible encoding and tagged fields. |
| v1      | Unchanged.                                | Adds throttle time.                       |
| v0      | Initial partition offset request.         | Initial low-watermark results.            |

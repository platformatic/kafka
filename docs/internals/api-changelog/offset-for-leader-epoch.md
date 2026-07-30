# OffsetForLeaderEpoch (API key 23)

Intended range: v0-v4.

| Version | Request                                   | Response                                  |
| ------- | ----------------------------------------- | ----------------------------------------- |
| v4      | Uses flexible encoding and tagged fields. | Uses flexible encoding and tagged fields. |
| v3      | Unchanged.                                | Unchanged.                                |
| v2      | Adds current leader epochs.               | Adds current leader epochs.               |
| v1      | Adds replica IDs.                         | Unchanged.                                |
| v0      | Leader epochs per topic partition.        | End offsets and errors per partition.     |

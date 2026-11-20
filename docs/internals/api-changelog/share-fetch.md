# ShareFetch (API key 78)

Intended range: v1-v2.

| Version | Request                                                                                         | Response                                                                                        |
| ------- | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| v2      | Adds `share_acquire_mode` (KIP-1206) and `is_renew_ack` (KIP-1222).                             | No wire-schema changes.                                                                         |
| v1      | Initial share session fetch with piggybacked acknowledgements, `max_records`, and `batch_size`. | Initial records, acquired record ranges with delivery counts, and the acquisition lock timeout. |

> v0 was the early access version of KIP-932 in Apache Kafka 4.0 and was removed in Apache Kafka 4.1, so v1 is the minimum supported version.

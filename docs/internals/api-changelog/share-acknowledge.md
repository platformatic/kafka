# ShareAcknowledge (API key 79)

Intended range: v1-v2.

| Version | Request                                                              | Response                                       |
| ------- | -------------------------------------------------------------------- | ---------------------------------------------- |
| v2      | Adds `is_renew_ack` for the `RENEW` acknowledgement type (KIP-1222). | Adds `acquisition_lock_timeout_ms`.            |
| v1      | Initial acknowledgement batches for a share session.                 | Initial per-partition acknowledgement results. |

> v0 was the early access version of KIP-932 in Apache Kafka 4.0 and was removed in Apache Kafka 4.1, so v1 is the minimum supported version.

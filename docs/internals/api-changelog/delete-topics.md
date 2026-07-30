# DeleteTopics (API key 20)

Intended range: v0-v6.

| Version | Request                                              | Response                                              |
| ------- | ---------------------------------------------------- | ----------------------------------------------------- |
| v6      | Reorganizes topics; adds IDs and nullable names.     | Adds topic IDs and nullable topic names.              |
| v5      | Unchanged.                                           | Adds error messages and throttling-quota errors.      |
| v4      | Uses flexible encoding and tagged fields.            | Uses flexible encoding and tagged fields.             |
| v3      | Unchanged.                                           | Supports `TOPIC_DELETION_DISABLED`.                   |
| v2      | Unchanged.                                           | On quota violation, responds before throttling.       |
| v1      | Unchanged.                                           | Adds `throttle_time_ms`.                              |
| v0      | Initial topic-name list.                             | Initial per-topic errors.                             |

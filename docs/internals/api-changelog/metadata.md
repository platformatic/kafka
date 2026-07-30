# Metadata (API key 3)

Intended range: v0-v12.

<!-- In v9-v10, cluster authorization fields are intentionally not requested, although they remain in the protocol. -->

| Version | Request                                                                                 | Response                                                                |
| ------- | -------                                                                                 | --------                                                                |
| v12     | Supports topic-ID requests without topic names.                                         | Topic names become nullable.                                            |
| v11     | Removes `include_cluster_authorized_operations`.                                        | Removes `cluster_authorized_operations`.                                |
| v10     | Adds topic IDs; brokers do not support ID-only requests until v12.                      | Adds topic IDs.                                                         |
| v9      | Uses flexible encoding and tagged fields.                                               | Uses flexible encoding and tagged fields.                               |
| v8      | Adds `include_cluster_authorized_operations` and `include_topic_authorized_operations`. | Adds `cluster_authorized_operations` and `topic_authorized_operations`. |
| v7      | Unchanged.                                                                              | Adds partition leader epochs.                                           |
| v6      | Unchanged.                                                                              | Returns responses before quota throttling.                              |
| v5      | Unchanged.                                                                              | Adds `offline_replicas`.                                                |
| v4      | Adds `allow_auto_topic_creation`.                                                       | Unchanged.                                                              |
| v3      | Unchanged.                                                                              | Adds `throttle_time_ms`.                                                |
| v2      | Unchanged.                                                                              | Adds `cluster_id`.                                                      |
| v1      | Topics become nullable.                                                                 | Adds broker racks, `controller_id`, and `is_internal`.                  |
| v0      | Initial topic-name list.                                                                | Initial broker and topic-partition metadata.                            |

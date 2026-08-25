# ConsumerGroupHeartbeat (API key 68)

Intended range: v0-v1.

| Version | Request                                                                                           | Response                                                                 |
| ------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| v1      | Adds `subscribed_topic_regex`; member IDs become client-generated (behavioral change).            | No wire-schema changes; adds `INVALID_REGULAR_EXPRESSION` (behavioral). |
| v0      | Initial consumer group member state and subscriptions; the coordinator assigns an empty member ID. | Initial member epoch, heartbeat interval, and assignment.               |

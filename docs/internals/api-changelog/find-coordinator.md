# FindCoordinator (API key 10)

Intended range: v0-v6.

| Version | Request                                                                             | Response                                                             |
| ------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| v6      | Supports share-group coordinator keys.                                              | Supports share-group coordinators.                                   |
| v5      | Unchanged.                                                                          | Supports `TRANSACTION_ABORTABLE`.                                    |
| v4      | Replaces one key with `coordinator_keys`.                                           | Returns a coordinator entry for each key.                            |
| v3      | Uses flexible encoding and tagged fields.                                           | Uses flexible encoding and tagged fields.                            |
| v2      | Unchanged.                                                                          | On quota violation, sends responses before throttling.               |
| v1      | Adds `key_type` to select group or transaction coordinators.                        | Adds `throttle_time_ms` and error messages.                          |
| v0      | Group ID only.                                                                      | Coordinator error and broker endpoint.                               |

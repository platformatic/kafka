# CreateAcls (API key 30)

Intended range: v0-v3.

| Version | Request                                              | Response                                              |
| ------- | ---------------------------------------------------- | ----------------------------------------------------- |
| v3      | Adds the user resource type.                         | Adds the user resource type.                          |
| v2      | Uses flexible encoding and tagged fields.            | Uses flexible encoding and tagged fields.             |
| v1      | Adds resource pattern types.                         | On quota violation, responds before throttling.       |
| v0      | Initial ACL creation list.                           | Initial per-ACL results.                              |

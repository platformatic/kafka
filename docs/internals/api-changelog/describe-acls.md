# DescribeAcls (API key 29)

Intended range: v0-v3.

| Version | Request                                              | Response                                              |
| ------- | ---------------------------------------------------- | ----------------------------------------------------- |
| v3      | Unchanged.                                           | Adds the user resource type.                          |
| v2      | Uses flexible encoding and tagged fields.            | Uses flexible encoding and tagged fields.             |
| v1      | Adds resource pattern filtering.                     | Adds pattern types; responds before throttling.       |
| v0      | Initial ACL filter.                                  | Initial ACL resource list.                            |

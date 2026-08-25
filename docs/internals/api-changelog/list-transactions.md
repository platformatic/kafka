# ListTransactions (API key 66)

Intended range: v0-v2.

| Version | Request                                                    | Response                                             |
| ------- | ---------------------------------------------------------- | ---------------------------------------------------- |
| v2      | Adds `transactional_id_pattern`.                           | May return `INVALID_REGULAR_EXPRESSION`.             |
| v1      | Adds `duration_filter`.                                    | Unchanged.                                           |
| v0      | Initial state and producer ID filters; flexible encoding. | Initial list; throttle time; flexible encoding.       |

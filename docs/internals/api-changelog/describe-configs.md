# DescribeConfigs (API key 32)

Intended range: v0-v4.

| Version | Request                                              | Response                                               |
| ------- | ---------------------------------------------------- | ------------------------------------------------------ |
| v4      | Wire: uses flexible encoding and tagged fields.      | Wire: uses flexible encoding and tagged fields.         |
| v3      | Wire: adds `include_documentation`.                  | Wire: adds `config_type` and documentation.             |
| v2      | Wire: unchanged.                                     | Wire: unchanged. Semantics: responses precede throttling on quota violation. |
| v1      | Wire: adds `include_synonyms`.                       | Wire: removes `is_default`; adds `config_source` and synonyms. |
| v0      | Initial resource selection.                          | Initial resource configuration descriptions.            |

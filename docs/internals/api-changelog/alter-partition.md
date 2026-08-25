# AlterPartition (API key 56)

Intended range: v0-v3.

| Version | Request                                             | Response                                  |
| ------- | --------------------------------------------------- | ----------------------------------------- |
| v3      | Replaces ISR broker IDs with broker IDs and epochs. | No changes.                               |
| v2      | Replaces topic names with UUID topic IDs.           | Replaces topic names with UUID topic IDs. |
| v1      | Adds leader recovery state.                         | Adds leader recovery state.               |
| v0      | Initial ISR and leader epoch update.                | Initial per-partition result.             |

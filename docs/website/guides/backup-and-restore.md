---
description: Back up and restore pond with .pond archives - compact, index-free snapshots with embeddings preserved. Restore is an idempotent, deduplicated merge.
---

# Backup & restore

A `.pond` archive is a compact, restorable snapshot: clean index-free Lance datasets plus a manifest, embeddings preserved.

```sh
pond copy --from @ --to backup/2026-06-16.pond     # snapshot the configured store
pond copy --from backup/2026-06-16.pond --to @     # restore into the configured store
```

`@` is the configured store; `local` is the local default dir. Restore is an idempotent merge - existing rows are deduped, never duplicated.

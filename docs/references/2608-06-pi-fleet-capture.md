# Fleet capture: pi workers with a pond sidecar (2026-08-06)

How to keep the sessions a fleet of headless [pi](https://github.com/earendil-works/pi) workers produces, and query them from one place.

Nothing here is new pond machinery - it is a deployment shape assembled from primitives pond already has: env-only configuration (`storage-configless` / `storage-env-mirror`, spec 3.9), multi-writer OCC (spec 3.5), `pond serve --with-sync --bootstrap`, and the per-host sync lock (spec 7.8). A runnable version of exactly this topology lives at [`ops/examples/pi-fleet/`](../../ops/examples/pi-fleet).

## Topology

```
  worker pod 1                worker pod N
  +---------------------+     +---------------------+
  |  pi (-p / rpc)      |     |  pi                 |
  |    writes sessions  |     |                     |
  |         |           |     |                     |
  |   [shared volume]   | ... |   [shared volume]   |
  |         |           |     |                     |
  |  pond serve         |     |  pond serve         |
  |   --with-sync       |     |   --with-sync       |
  +----------|----------+     +----------|----------+
             |                           |
             +------------+--------------+
                          v
                  s3://bucket/tenants/<t>      one Lance store
                          ^
             +------------+--------------+
             |                           |
     pond serve --transport http    pond optimize --only embed
       (central read side)             (one embedding cron)
```

Each worker pod runs pi and a pond sidecar that share a volume: pi appends to its session files, pond tails them. Writes from every worker land in one store; pond's optimistic concurrency (spec 3.5) is what makes N concurrent writers to one Lance store safe, with no coordinator and no lock service.

The read side is a separate `pond serve --transport http` on the same store URL. It writes nothing, so it can be scaled independently and restarted freely.

## Configuration is environment only

A worker image ships no config file. Every knob pond needs is an env var (spec 3.9's `storage-env-mirror`), which is what makes the image identical across tenants:

```
POND_STORAGE_PATH=s3+https://s3.example.com/bucket/tenants/acme
POND_CREDS_ACCESS_KEY_ID=...
POND_CREDS_SECRET_ACCESS_KEY=...
```

The sidecar's whole command line is then:

```
pond serve --transport http --with-sync --bootstrap pi-coding-agent
```

`--bootstrap pi-coding-agent` is the piece that removes the setup step: on a pod whose pond has NO `[adapters.*]` entries at all, serve discovers and enables the pi adapter before the sync loop starts. It never touches an existing adapter config, and a disabled entry counts as configured (spec 7.8), so a deliberately disabled adapter stays disabled across restarts.

## Per-tenant isolation is one store URL per tenant

There is no tenant column and no namespace routing to configure - hosted namespace routing stays deferred (spec 9.5). Isolation is the store URL:

```
s3://bucket/tenants/acme
s3://bucket/tenants/globex
```

Two tenants share no bytes, no manifest, and no index. A tenant's whole corpus is one prefix: to export it, copy the prefix; to delete it, delete the prefix. Credentials scoped per prefix make the isolation enforceable at the object store rather than in pond.

## Split the embedding work off the workers

Embedding is the expensive half of ingest and it does not need to happen on the worker. Run the workers fts-only and let one central cron do the vectors:

- **Workers**: turn embedding off at ingest (`[embeddings] enabled = false`, or `POND_EMBEDDINGS_ENABLED=false`). Sessions land immediately and are full-text searchable; no ~500 MB model is loaded per pod.
- **Central**: one `pond optimize --only embed` on a schedule fills the backlog and folds the semantic index.

This is the single biggest lever on fleet cost: N worker pods each holding an embedding model is N times the memory for work one process can do in batches.

## Compliance

`pond erase <session-id>` is the only deletion pond performs and it is a true byte purge, not a tombstone: delete predicate, compaction, version-history cleanup, blob purge (spec 5.4 `session-append-only-exception`). The erased key enters a denylist the ingest path consults, so a later sync from a still-present source file cannot resurrect it.

It is operator-only - CLI and HTTP, never on the MCP read surface. In this topology that means: run it against the tenant's store URL from an admin context, not from a worker.

## The honest loss window

A worker that is hard-killed loses whatever its sidecar had not yet synced, unless the volume outlives the pod.

Two things bound that window. pi appends one line per mutation, so the session file on disk is always current up to the last completed write and a torn final line is a recognized crash artifact rather than corruption. And the sidecar's sync interval (`--sync-every`, default 5 minutes) is the actual exposure: shorten it to trade object-store requests for a smaller window.

If the volume survives the pod - a PVC rather than an emptyDir - the window closes entirely: the next pod to mount it syncs what the last one left. That is the recommended shape.

## Security

`pond serve` speaks its HTTP transport unauthenticated by design. There is no user model, no token, and no per-caller scoping: pond's position is that identity belongs to the integrator (spec 2.3). Consequences for a fleet:

- Bind the read side to a private network or localhost, never a public interface. Put your own auth in front of it if it needs to be reachable.
- Anyone who can reach the read side can read every session in that tenant's store. That is the whole point of the read side, so the network boundary IS the access control.
- The store credentials are the other boundary: scope them per tenant prefix so a compromised worker cannot read a neighbor's corpus.

## Verifying a fleet

```
pond status --hosts
```

The fleet view: which worker hosts have fed this store, how many sessions each contributed, and each one's latest activity. A worker whose latest session stops advancing is the signal that its sidecar died or its volume went away - long before anyone notices missing recall.

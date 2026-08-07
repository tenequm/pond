# pi fleet capture - runnable example

One tenant's slice of the topology in [`docs/references/2608-06-pi-fleet-capture.md`](../../../docs/references/2608-06-pi-fleet-capture.md): a pi worker, a pond sidecar sharing its sessions volume, one object store, and a separate read side on the same store URL.

What it demonstrates: a headless pi worker's sessions land in a central store with **no pond config file anywhere** - env vars plus `--bootstrap` are the whole setup - and are searchable from a process that never touched the worker.

## Prerequisites

- Docker with Compose v2.
- A provider API key for pi. `ANTHROPIC_API_KEY` is the default path; any provider pi supports works if you adjust `worker-pi`'s environment.

That second one is a real prerequisite, not a formality: `worker-pi` runs pi against a model. The pond half of the example (sidecar, store, read side) works without it - see "Without an API key" below.

## Run

```
export ANTHROPIC_API_KEY=sk-ant-...
docker compose up --build -d
```

The first `up` builds a small pond image from the released Linux binary and pulls MinIO and node - allow a minute or two.

Watch the worker do its one prompt:

```
docker compose logs -f worker-pi
```

Watch the sidecar pick the session up (it syncs every minute):

```
docker compose logs -f worker-pond
```

## Query it from the read side

The read side is a different process that only ever reads the store:

```
curl -s localhost:9797/health
```

```
curl -s localhost:9797/search \
  -H 'content-type: application/json' \
  -d '{"protocol_version":1,"query":"write-ahead log","mode":"fts","limit":5}' | jq
```

`mode: "fts"` is deliberate: the workers run with `POND_EMBEDDINGS_ENABLED=false`, so nothing is embedded yet and vector search would fall back anyway. That split is the point - see the fleet doc's "Split the embedding work off the workers".

Fill in the vectors from one central place, exactly as a cron would:

```
docker compose run --rm read-side optimize --only embed
```

Then the same query works semantically:

```
curl -s localhost:9797/search \
  -H 'content-type: application/json' \
  -d '{"protocol_version":1,"query":"durability of database writes","limit":5}' | jq
```

## See the fleet view

```
docker compose run --rm read-side status --hosts
```

One row per worker host that has fed this store, with each one's session count and latest activity - the signal that tells you a worker's sidecar died long before anyone notices missing recall.

## Without an API key

Drop any pi session file (v3 or v4) onto the shared volume and the sidecar ingests it on the next tick:

```
docker compose up -d minio minio-init worker-pond read-side
docker compose cp \
  ../../../packages/pond/tests/fixtures/adapter/pi-coding-agent/sessions \
  worker-pond:/pi/.pi/agent/
```

Every query above then works against the fixture corpus.

## Clean up

```
docker compose down -v
```

`-v` removes the sessions volume and the MinIO data. Leave it off to see the loss-window property from the fleet doc: bring the stack back up and the sidecar resumes from whatever the last worker left behind.

## Adapting this

- **Another tenant**: change `tenants/demo` in `POND_STORAGE_PATH` on both pond services. Two tenants share no bytes, no manifest, and no index.
- **Real object storage**: replace the MinIO URL with `s3+https://<host>/<bucket>/<prefix>` (or `s3://`) and the two `POND_CREDS_*` values. Nothing else changes.
- **More workers**: scale `worker-pi`/`worker-pond` as a pair. Concurrent writers to one store are safe by pond's optimistic concurrency; no coordinator is involved.
- **Security**: `pond serve` is unauthenticated by design. The `9797:9797` port publish here is for the demo; in a real deployment bind it to a private network and put your own auth in front.

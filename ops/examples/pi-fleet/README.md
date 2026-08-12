# pi fleet capture - runnable example

One tenant's slice of the topology in [`docs/references/2608-06-pi-fleet-capture.md`](../../../docs/references/2608-06-pi-fleet-capture.md): a pi worker, a pond sidecar sharing its sessions volume, one object store, and a separate read side on the same store URL.

What it demonstrates: a headless pi worker's sessions land in a central store with **no pond config file anywhere** - env vars plus `--bootstrap` are the whole setup - and are searchable from a process that never touched the worker.

## Prerequisites

- Docker with Compose v2.
- Outbound HTTPS. The sidecar embeds every message as it ingests and there is no switch to turn that off, so it downloads the ~500 MB embedding model from HuggingFace on first use - see "What embedding costs here".
- A provider API key for pi, only for the `worker-pi` half. `ANTHROPIC_API_KEY` is the default path; any provider pi supports works if you adjust `worker-pi`'s environment. The pond half (sidecar, store, read side) needs no key - see "Without an API key".

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

Each tick prints one `in-serve sync complete sessions=N messages=N` line. That is only visible because the service sets `RUST_LOG=warn,pond::sync=info,lance::io::exec::count_pushdown=error`; at pond's default WARN level a sidecar ingesting nothing looks exactly like a healthy one.

## Query it from the read side

The read side is a different process that only ever reads the store. Its HTTP surface is `POST /v1/*` - there is no `GET /health`, so the liveness check is a real query:

```
curl -s localhost:9797/v1/search \
  -H 'content-type: application/json' \
  -d '{"protocol_version":1,"query":"write-ahead log","limit":5}' | jq
```

That is the vector arm (the default), and it works on a fresh store because the sidecar embedded inline as it synced. `"mode":"fts"` switches to exact whole words (BM25):

```
curl -s localhost:9797/v1/search \
  -H 'content-type: application/json' \
  -d '{"protocol_version":1,"query":"write-ahead","mode":"fts","limit":5}' | jq
```

## What embedding costs here

pond has no ingest-time embedding switch - not an env var, not a config key. `[embeddings]` carries only `model` and `dim`, and the sidecar's inline embed is unconditional, so **every worker pod in this shape holds the embedding model**. Concretely:

- First embeddable row triggers a ~500 MB download into `$HOME/.cache/huggingface`, which is the container layer, not the shared volume - a replaced pod downloads it again.
- A pod that cannot reach HuggingFace does not degrade to full-text-only; the model-load error propagates out of the write and the sync writes nothing.

The consequence for the fleet doc's "Split the embedding work off the workers": that split is not available today. The central pass still exists and is still worth scheduling, but on this topology it finds an empty backlog and exits immediately:

```
docker compose run --rm read-side optimize --only embed
```

It earns its keep for sessions that arrive unembedded by another route (`pond copy`) and for the model-swap re-embed (`--force-embed`).

## See the fleet view

```
docker compose run --rm read-side status --hosts
```

One row per worker host that has fed this store, with each one's session count and latest activity - the signal that tells you a worker's sidecar died long before anyone notices missing recall.

## Without an API key

Drop any pi session file onto the shared volume and the sidecar ingests it on the next tick:

```
docker compose up -d --build minio minio-init worker-pond read-side
docker compose cp \
  ../../../packages/pond/tests/fixtures/adapter/pi-coding-agent/sessions \
  worker-pond:/pi/.pi/agent/
```

The fixture corpus is pi conversations about MCP config and a storage rewrite, not write-ahead logs, so query it for what it contains:

```
curl -s localhost:9797/v1/search \
  -H 'content-type: application/json' \
  -d '{"protocol_version":1,"query":"adding an MCP server to the config","limit":5}' | jq
```

Expect `pond status` to report fewer sessions than there are fixture files, and expect the sidecar to keep logging a couple of `status="empty"` sessions on every tick: pond never half-ingests a format it does not understand, and 2 of the 6 fixture files are harness-v2 (v4) JSONL that only a pond built with the v4 reader decodes. On the released binary the other 4 (v3) ingest and those 2 re-read as `status="empty"` on every tick.

## Clean up

```
docker compose down -v
```

`-v` removes the sessions volume and the MinIO data. Leave it off to see the loss-window property from the fleet doc: bring the stack back up and the sidecar resumes from whatever the last worker left behind.

## Adapting this

- **Another tenant**: change `tenants/demo` in `POND_STORAGE_PATH` on both pond services. Two tenants share no bytes, no manifest, and no index.
- **Real object storage**: replace the MinIO URL with `s3+https://<host>/<bucket>/<prefix>` (or `s3://`) and the two `POND_CREDS_DEFAULT_*` values. Drop `?virtual_hosted_style_request=false` too - it is here because pond defaults S3-compatible endpoints to virtual-hosted addressing and MinIO under a DNS name (`minio:9000`) cannot serve `pond.minio:9000`.
- **More workers**: scale `worker-pi`/`worker-pond` as a pair. Concurrent writers to one store are safe by pond's optimistic concurrency; no coordinator is involved.
- **Security**: `pond serve` is unauthenticated by design. The `9797:9797` port publish here is for the demo; in a real deployment bind it to a private network and put your own auth in front.

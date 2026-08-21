# pi fleet capture - runnable example

One tenant's slice of the topology in [`docs/references/2608-06-pi-fleet-capture.md`](../../../docs/references/2608-06-pi-fleet-capture.md): a pi worker, a pond sidecar sharing its sessions volume, one object store, and a separate read side on the same store URL.

What it demonstrates: a headless pi worker's sessions land in a central store with **no pond config file anywhere** - env vars plus `--bootstrap` are the whole setup - and are searchable from a process that never touched the worker.

## Prerequisites

- Docker with Compose v2.
- Outbound HTTPS, for the object store. Nothing else phones home: semantic search is off by default, so no embedding model is downloaded - see "Turning on semantic search".
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

That is the full-text arm (BM25), the default, and it works on a fresh store with no model anywhere in the picture. `"mode":"vector"` is refused until semantic search is turned on (below).

## Turning on semantic search

Embeddings are opt-in and off here, which is why no service in `docker-compose.yml` sets them. Turning them on for the sidecar is one env var plus one volume:

```yaml
  worker-pond:
    environment:
      POND_EMBEDDINGS_ENABLED: "true"
    volumes:
      - pi-sessions:/pi/.pi
      - hf-cache:/pi/.cache/huggingface   # add `hf-cache:` to the top-level `volumes:` too
```

The cache volume is not optional in a fleet: the model lands in `$HOME/.cache/huggingface`, which is the container layer and not the shared sessions volume, so without it every replaced pod re-downloads 466 MiB. Budget the memory too - a pond process sits around 100 MiB with embeddings off and 500-900 MiB once any vector work has run - and expect the first sync to be CPU-bound, since a Linux container has neither Metal nor CUDA.

The value must be the literal `true` or `false`; `1` fails config load.

The read side needs nothing set to *read* vectors, but it does need the flag to *make* them - which is how the fleet doc's "Split the embedding work off the workers" is done here. Leave `worker-pond` at the default (off) and run the central pass on an enabled read side:

```
docker compose run --rm -e POND_EMBEDDINGS_ENABLED=true read-side optimize --only embed
```

That embeds everything the workers ingested without vectors, and it is the same pass that earns its keep for sessions arriving by another route (`pond copy`) and for the model-swap re-embed (`--force-embed`). Until it has run, `"mode":"vector"` is refused; afterwards it answers from the same store.

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

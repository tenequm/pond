#!/bin/sh
# One pi worker: install pi, run a scripted prompt in print mode, then idle so
# the sidecar keeps syncing (and so `docker compose exec` can poke at the
# volume). pi writes its session file to $HOME/.pi/agent/sessions - the volume
# the pond sidecar shares.
set -eu

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "worker: ANTHROPIC_API_KEY is empty - export it and re-run, or use the README's 'Without an API key' flow" >&2
  exit 1
fi

npm install -g --silent @earendil-works/pi-coding-agent@latest

echo "worker: running prompt"
pi -p "$PI_PROMPT" || echo "worker: pi exited non-zero (check the provider key)"

echo "worker: session files on the shared volume:"
find "$HOME/.pi/agent/sessions" -name '*.jsonl' -print

# Stay up: the sidecar syncs on an interval, and a pod that exits here would
# make the loss window look worse than it is.
tail -f /dev/null

#!/usr/bin/env bash
set -euo pipefail

# Run one collector in a container from a manifest on the host.
#
#   ./run_md_worker.sh ../serve/configs/md_worker_binance.toml
#
# Build the image first from the workspace root:
#
#   docker build -t iteralabs/aetelier-md-worker:v0.1.0 \
#                -f aetelier-sdk/serve/md_worker.Dockerfile .

WORKER_CONFIG="${1:?usage: $0 <path/to/md_worker_<venue>.toml>}"
IMAGE="${AETELIER_MD_WORKER_IMAGE:-iteralabs/aetelier-md-worker:v0.1.0}"

if [[ ! -f "$WORKER_CONFIG" ]]; then
  echo "ERROR: config not found: $WORKER_CONFIG" >&2
  exit 1
fi

docker run --rm -it \
  -v "$(cd "$(dirname "$WORKER_CONFIG")" && pwd)/$(basename "$WORKER_CONFIG"):/configs/manifest.toml:ro" \
  -v "$(pwd)/datasets:/data" \
  "$IMAGE" --config /configs/manifest.toml

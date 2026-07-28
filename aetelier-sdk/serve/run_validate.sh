#!/usr/bin/env bash
# aetelier-sdk/serve/run_validate.sh
#
# Cron-friendly wrapper that runs the `iteralabs/aetelier-validate`
# container against the same /data volume the live `md_worker` services
# are writing into. Designed to be invoked by /etc/cron.d/aetelier on an
# Ubuntu server.
#
# Behaviour:
#   * Mounts the host data dir read-only — the validator never mutates
#     the lake.
#   * Mounts a host state dir read-write so cumulative statistics
#     persist across runs.
#   * Appends every run's stdout to a rotating log under /var/log.
#   * Forwards the validator's exit code so that cron's MAILTO catches
#     failures (exit 1 = test failure, exit 2 = fatal).
#
# Override the defaults via environment variables:
#   AETELIER_DATA_DIR       (default: /var/lib/aetelier/datasets)
#   AETELIER_STATE_DIR      (default: /var/lib/aetelier/validate-state)
#   AETELIER_VALIDATE_IMAGE (default: iteralabs/aetelier-validate:v0.1.0)
#   AETELIER_FLUSH_THRESHOLD (default: 3600)
#   AETELIER_GRID_PERIOD_MS  (default: 100)
#   AETELIER_LOG_FILE        (default: /var/log/aetelier/validate.log)

set -euo pipefail

DATA_DIR="${AETELIER_DATA_DIR:-/var/lib/aetelier/datasets}"
STATE_DIR="${AETELIER_STATE_DIR:-/var/lib/aetelier/validate-state}"
IMAGE="${AETELIER_VALIDATE_IMAGE:-iteralabs/aetelier-validate:v0.1.0}"
FLUSH_THRESHOLD="${AETELIER_FLUSH_THRESHOLD:-3600}"
GRID_PERIOD_MS="${AETELIER_GRID_PERIOD_MS:-100}"
LOG_FILE="${AETELIER_LOG_FILE:-/var/log/aetelier/validate.log}"

mkdir -p "$STATE_DIR" "$(dirname "$LOG_FILE")"

# Stamp every entry so cron-driven log lines are easy to bisect later.
printf '\n────────── %s ──────────\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >> "$LOG_FILE"

set +e
docker run --rm \
    --name aetelier-validate-cron \
    -v "${DATA_DIR}:/data:ro" \
    -v "${STATE_DIR}:/state" \
    "${IMAGE}" \
    --data-dir /data \
    --state-file /state/validate_state.json \
    --report-out /state/last_report.json \
    --flush-threshold "${FLUSH_THRESHOLD}" \
    --grid-period-ms "${GRID_PERIOD_MS}" \
    --skip-seen \
    --verbose \
    >> "$LOG_FILE" 2>&1
status=$?
set -e

echo "validate.exit_code=${status}" >> "$LOG_FILE"
exit "$status"

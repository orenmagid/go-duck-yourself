#!/bin/bash
# Watchtower Ring 4 — PID-locked runner.
# Periodic truth reconciliation (weekly cadence; the ring's own gate enforces
# the interval, so the cron may fire more often without harm — an early run
# no-ops). Prevents concurrent instances. Logs to ring4.log.
#
# Usage: watchtower-ring4-runner.sh
#
# Modeled on watchtower-ring2-runner.sh. Ring 4 stage 1 makes no Claude API
# call, so it has no hard dependency on ANTHROPIC_API_KEY — env is sourced for
# parity and forward-compatibility (semantic checks would add the dependency).

set -euo pipefail

WATCHTOWER_DIR="${WATCHTOWER_DIR:-${HOME}/.claude-cabinet/watchtower}"

# Source watchtower environment (node path, and API keys if ever needed)
ENV_FILE="${WATCHTOWER_DIR}/env"
if [ -f "${ENV_FILE}" ]; then
  set -a; source "${ENV_FILE}"; set +a
fi

# Use configured node path or fall back to PATH
NODE_BIN="${WATCHTOWER_NODE_PATH:-$(command -v node || true)}"
if [ -z "${NODE_BIN}" ] || [ ! -x "${NODE_BIN}" ]; then
  echo "[watchtower] ERROR: node not found. Set WATCHTOWER_NODE_PATH in ${ENV_FILE}" >&2
  exit 1
fi

LOCK_DIR="${WATCHTOWER_DIR}/lock"
PID_FILE="${LOCK_DIR}/ring4.pid"
LOG_DIR="${WATCHTOWER_DIR}/logs"
LOG_FILE="${LOG_DIR}/ring4.log"
SCRIPT_DIR="${WATCHTOWER_DIR}/scripts"
RING4_SCRIPT="${SCRIPT_DIR}/watchtower-ring4.mjs"

# Ensure directories exist
mkdir -p "${LOCK_DIR}" "${LOG_DIR}"

# Clean up PID file on exit
cleanup() {
  rm -f "${PID_FILE}"
}
trap cleanup EXIT

# Check for already-running instance
if [ -f "${PID_FILE}" ]; then
  existing_pid=$(cat "${PID_FILE}" 2>/dev/null || echo "")
  if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Ring 4 already running (PID ${existing_pid}), skipping" >> "${LOG_FILE}"
    # Remove the trap so we don't delete the active PID file
    trap - EXIT
    exit 0
  fi
  # Stale PID file — previous run crashed
  rm -f "${PID_FILE}"
fi

# Write our PID
echo $$ > "${PID_FILE}"

# Check that the script exists
if [ ! -f "${RING4_SCRIPT}" ]; then
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ERROR: ${RING4_SCRIPT} not found" >> "${LOG_FILE}"
  exit 1
fi

# Log rotation: if >5MB, rotate (keep 1 backup)
if [ -f "${LOG_FILE}" ]; then
  LOG_SIZE=$(stat -f%z "${LOG_FILE}" 2>/dev/null || stat -c%s "${LOG_FILE}" 2>/dev/null || echo 0)
  if [ "${LOG_SIZE}" -gt 5242880 ]; then
    mv "${LOG_FILE}" "${LOG_FILE}.1"
  fi
fi

# Random jitter to avoid login-time burst
sleep $((RANDOM % 30))

# Run Ring 4
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Ring 4 starting (PID $$)" >> "${LOG_FILE}"
"${NODE_BIN}" "${RING4_SCRIPT}" >> "${LOG_FILE}" 2>&1
exit_code=$?
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Ring 4 finished (exit ${exit_code})" >> "${LOG_FILE}"

exit ${exit_code}

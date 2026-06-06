#!/bin/bash
# Watchtower Ring 1 — PID-locked runner.
# Prevents concurrent instances. Logs to ring1.log.

set -euo pipefail

WATCHTOWER_DIR="${WATCHTOWER_DIR:-${HOME}/.claude-cabinet/watchtower}"

# Source watchtower environment (API keys, node path)
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
PID_FILE="${LOCK_DIR}/ring1.pid"
LOG_DIR="${WATCHTOWER_DIR}/logs"
LOG_FILE="${LOG_DIR}/ring1.log"
SCRIPT_DIR="${WATCHTOWER_DIR}/scripts"
RING1_SCRIPT="${SCRIPT_DIR}/watchtower-ring1.mjs"

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
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Ring 1 already running (PID ${existing_pid}), skipping" >> "${LOG_FILE}"
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
if [ ! -f "${RING1_SCRIPT}" ]; then
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ERROR: ${RING1_SCRIPT} not found" >> "${LOG_FILE}"
  exit 1
fi

# Log rotation: if >5MB, rotate (keep 1 backup)
if [ -f "${LOG_FILE}" ]; then
  LOG_SIZE=$(stat -f%z "${LOG_FILE}" 2>/dev/null || stat -c%s "${LOG_FILE}" 2>/dev/null || echo 0)
  if [ "${LOG_SIZE}" -gt 5242880 ]; then
    mv "${LOG_FILE}" "${LOG_FILE}.1"
  fi
fi

# Consume ring1-trigger if present (written by Ring 2 slow tier)
TRIGGER_FILE="${WATCHTOWER_DIR}/lock/ring1-trigger"
if [ -f "${TRIGGER_FILE}" ]; then rm -f "${TRIGGER_FILE}"; fi

# Run Ring 1
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Ring 1 starting (PID $$)" >> "${LOG_FILE}"
"${NODE_BIN}" "${RING1_SCRIPT}" >> "${LOG_FILE}" 2>&1
exit_code=$?
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Ring 1 finished (exit ${exit_code})" >> "${LOG_FILE}"

exit ${exit_code}

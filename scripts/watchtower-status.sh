#!/bin/bash
# Watchtower health — quick status from any terminal. No Claude session needed.
#
# Usage: watchtower-status [--verbose|-v]

set -euo pipefail

WATCHTOWER_DIR="${WATCHTOWER_DIR:-$HOME/.claude-cabinet/watchtower}"
VERBOSE="${1:-}"

if [ ! -d "$WATCHTOWER_DIR" ]; then
  echo "Watchtower not installed ($WATCHTOWER_DIR missing)"
  exit 1
fi

# --- Helpers ---

json_field() {
  local file="$1" field="$2"
  [ -f "$file" ] || return 1
  node -p "try{JSON.parse(require('fs').readFileSync('$file','utf8'))${field}}catch{}" 2>/dev/null
}

# Read a field off a thread's CURRENT cursor (last cursor_history entry),
# falling back to a legacy single `cursor` field for un-migrated thread files.
# Mirrors currentCursor() in watchtower-lib.mjs. Returns '' when absent.
current_cursor_field() {
  local file="$1" sub="$2"
  [ -f "$file" ] || return 1
  node -p "try{const t=JSON.parse(require('fs').readFileSync('$file','utf8'));const h=t.cursor_history;const c=(Array.isArray(h)&&h.length?h[h.length-1].cursor:t.cursor)||{};c['$sub']??''}catch{}" 2>/dev/null
}

ts_to_epoch() {
  local ts="${1%%.*}"
  ts="${ts%%Z}"
  TZ=UTC date -j -f "%Y-%m-%dT%H:%M:%S" "$ts" +%s 2>/dev/null || echo 0
}

age_seconds() {
  local ts="$1"
  [ -z "$ts" ] && { echo 999999; return; }
  local now then
  now=$(date +%s)
  then=$(ts_to_epoch "$ts")
  [ "$then" -eq 0 ] && { echo 999999; return; }
  echo $((now - then))
}

age_human() {
  local diff="$1"
  if   [ "$diff" -lt 60 ];    then echo "${diff}s"
  elif [ "$diff" -lt 3600 ];  then echo "$((diff / 60))m"
  elif [ "$diff" -lt 86400 ]; then echo "$((diff / 3600))h"
  else echo "$((diff / 86400))d"
  fi
}

freshness() {
  local age_s="$1" interval_s="$2"
  if [ "$age_s" -le "$interval_s" ]; then echo "✓"
  elif [ "$age_s" -le $((interval_s * 2)) ]; then echo "✓"
  else echo "⚠"
  fi
}

ring_line() {
  local label="$1" file="$2" interval_s="$3" cadence_label="$4"
  if [ ! -f "$file" ]; then
    printf "  %-22s  —  not yet run\n" "$label"
    return
  fi
  local ts status age_s icon age_str
  ts=$(json_field "$file" ".last_run")
  status=$(json_field "$file" ".status")
  age_s=$(age_seconds "$ts")
  age_str=$(age_human "$age_s")

  if [ "$status" != "success" ]; then
    icon="✗"
  else
    icon=$(freshness "$age_s" "$interval_s")
  fi

  printf "  %-22s  %s  %s ago  (every %s)\n" "$label" "$icon" "$age_str" "$cadence_label"
}

# ═══════════════════════════════════════

echo "═══ Watchtower ═══"

# --- Inbox (most actionable → first) ---

echo ""
queue_dir="$WATCHTOWER_DIR/queue/items"
if [ -d "$queue_dir" ]; then
  pending=0; urgent=0; resolved=0; total=0
  oldest_ts=""
  for f in "$queue_dir"/*.json; do
    [ -f "$f" ] || continue
    total=$((total + 1))
    status=$(json_field "$f" ".status")
    if [ "$status" = "pending" ]; then
      pending=$((pending + 1))
      urg=$(json_field "$f" ".urgency")
      [ "$urg" = "urgent" ] && urgent=$((urgent + 1))
      filed=$(json_field "$f" ".filed_at")
      if [ -n "$filed" ] && { [ -z "$oldest_ts" ] || [[ "$filed" < "$oldest_ts" ]]; }; then
        oldest_ts="$filed"
      fi
    elif [ "$status" = "resolved" ] || [ "$status" = "dismissed" ]; then
      resolved=$((resolved + 1))
    fi
  done

  if [ $pending -gt 0 ]; then
    oldest_age=""
    if [ -n "$oldest_ts" ]; then
      oldest_s=$(age_seconds "$oldest_ts")
      oldest_age=", oldest $(age_human "$oldest_s")"
    fi
    if [ $urgent -gt 0 ]; then
      echo "  Inbox:  $pending pending — $urgent urgent${oldest_age}"
    else
      echo "  Inbox:  $pending pending${oldest_age}"
    fi
  else
    echo "  Inbox:  none"
  fi
  [ $resolved -gt 0 ] && echo "                      $resolved resolved"
else
  echo "  Inbox:  none"
fi

# --- Work state ---

threads_dir="$WATCHTOWER_DIR/state/threads"
if [ -d "$threads_dir" ]; then
  active=0; dormant=0
  for f in "$threads_dir"/*.json; do
    [ -f "$f" ] || continue
    status=$(json_field "$f" ".status")
    if [ "$status" = "active" ]; then active=$((active + 1))
    elif [ "$status" = "dormant" ]; then dormant=$((dormant + 1))
    fi
  done
  thread_str="$active active"
  [ $dormant -gt 0 ] && thread_str="$thread_str, $dormant dormant"
  echo "  Work threads:       $thread_str"
fi

processed_dir="$WATCHTOWER_DIR/ring3/processed"
if [ -d "$processed_dir" ]; then
  count=$(find "$processed_dir" -name "*.json" -not -name "*-test*" | wc -l | tr -d ' ')
  echo "  Sessions captured:  $count"
fi

# --- Background processes ---

echo ""

# Read intervals from config (with defaults)
r1_interval=300; r2f_interval=300; r2s_interval=1800
config="$WATCHTOWER_DIR/config.json"
if [ -f "$config" ]; then
  val=$(json_field "$config" ".ring1?.interval_seconds")
  [ -n "$val" ] && [ "$val" != "undefined" ] && r1_interval="$val"
  val=$(json_field "$config" ".ring2?.fast?.interval_seconds")
  [ -n "$val" ] && [ "$val" != "undefined" ] && r2f_interval="$val"
  val=$(json_field "$config" ".ring2?.slow?.interval_seconds")
  [ -n "$val" ] && [ "$val" != "undefined" ] && r2s_interval="$val"
fi

r1_label="$(( r1_interval / 60 ))m"
r2f_label="$(( r2f_interval / 60 ))m"
r2s_label="$(( r2s_interval / 60 ))m"

ring_line "Scanner"          "$WATCHTOWER_DIR/state/ring1-health.json"      "$r1_interval" "$r1_label"
ring_line "Enrichment"       "$WATCHTOWER_DIR/state/ring2-fast-health.json" "$r2f_interval" "$r2f_label"
ring_line "Synthesis"        "$WATCHTOWER_DIR/state/ring2-slow-health.json" "$r2s_interval" "$r2s_label"

# Session capture is event-driven — show last run without cadence judgment
r3_file="$WATCHTOWER_DIR/state/ring3-health.json"
if [ -f "$r3_file" ]; then
  r3_ts=$(json_field "$r3_file" ".last_run")
  r3_status=$(json_field "$r3_file" ".status")
  r3_filed=$(json_field "$r3_file" ".items_filed")
  r3_age_s=$(age_seconds "$r3_ts")
  r3_age=$(age_human "$r3_age_s")
  r3_icon="✓"; [ "$r3_status" != "success" ] && r3_icon="✗"
  printf "  %-22s  %s  %s ago  (on session end)\n" "Session capture" "$r3_icon" "$r3_age"
else
  printf "  %-22s  —  not yet run\n" "Session capture"
fi

# --- Projects ---

echo ""
if [ -f "$config" ]; then
  proj_count=$(node -p "try{Object.keys(JSON.parse(require('fs').readFileSync('$config','utf8')).projects||{}).length}catch{0}" 2>/dev/null)
  echo "  Watching $proj_count projects"
fi

# Portfolio counts (live pib-db open/flagged data) refresh each Ring 1 scan —
# surface the capability here since it's otherwise invisible outside /briefing
if grep -q "## Portfolio Pulse" "$WATCHTOWER_DIR/state/summary.md" 2>/dev/null; then
  echo "  Portfolio counts:   live (per-project pib data — see /briefing)"
fi

# ═══ Verbose mode ═══

if [ "$VERBOSE" = "--verbose" ] || [ "$VERBOSE" = "-v" ]; then

  # --- Per-project data freshness ---
  echo ""
  echo "─── Project state freshness ───"
  proj_dir="$WATCHTOWER_DIR/state/projects"
  if [ -d "$proj_dir" ]; then
    for f in "$proj_dir"/*.md; do
      [ -f "$f" ] || continue
      name=$(basename "$f" .md)
      mod=$(stat -f %m "$f" 2>/dev/null || echo 0)
      now=$(date +%s)
      age_s=$((now - mod))
      age_str=$(age_human "$age_s")
      printf "  %-24s scanned %s ago\n" "$name" "$age_str"
    done
  fi

  # --- Active thread cursors ---
  if [ -d "$threads_dir" ]; then
    echo ""
    echo "─── Active threads ───"
    for f in "$threads_dir"/*.json; do
      [ -f "$f" ] || continue
      status=$(json_field "$f" ".status")
      [ "$status" != "active" ] && continue
      slug=$(json_field "$f" ".thread")
      what=$(current_cursor_field "$f" "what" 2>/dev/null || echo "")
      [ "$what" = "undefined" ] && what=""
      left_off=$(current_cursor_field "$f" "where_left_off" 2>/dev/null || echo "")
      [ "$left_off" = "undefined" ] && left_off=""
      echo "  $slug"
      [ -n "$what" ] && echo "    $what"
      [ -n "$left_off" ] && echo "    Left off: $left_off"
    done
  fi

  # --- Ring logs ---
  echo ""
  echo "─── Ring logs ───"

  for ring_info in "Enrichment:ring2-fast" "Synthesis:ring2-slow"; do
    label="${ring_info%%:*}"
    logname="${ring_info##*:}"
    logfile="$WATCHTOWER_DIR/logs/${logname}.log"
    echo ""
    echo "  $label (last 8 lines):"
    if [ -f "$logfile" ]; then
      tail -8 "$logfile" | sed 's/^/    /'
    else
      echo "    (no log)"
    fi
  done

  echo ""
  echo "  Session capture (last session):"
  latest_log=$(ls -t "$WATCHTOWER_DIR/logs/ring3-close-"*.log 2>/dev/null | head -1 || true)
  if [ -n "$latest_log" ]; then
    tail -8 "$latest_log" | sed 's/^/    /'
  else
    echo "    (no logs)"
  fi
fi

echo ""

#!/usr/bin/env node

// Watchtower routine dispatch — the generic push mechanism for declared
// interactive routines (act:c2a55c08).
//
// Projects declare routines in config.json: { name, trigger, script, ... }.
// Watchtower's existing passes watch the triggers — Ring 1's mechanical tick
// evaluates time-of-day / interval / path-nonempty, Ring 3's close pass
// raises session-close events. When a trigger fires, the routine is
// dispatched to the desk's MAIN session exactly like a QA handoff: an inbox
// item (category 'routine') is the durable record, and a routing descriptor
// is pushed through `mux qa dispatch` — the SAME hardened path qa-handoffs
// use (ghost-skip, in-flight state, inbox fallback, gate-exit descriptor
// clearing; act:796fe6dc). Delivery is pushed, not invoked — nothing
// depends on the operator remembering a verb.
//
// The routine CONTENT is a project-authored phase file (the established CC
// convention): `script` is a path relative to the project root; the pickup
// prompt tells the receiving session to read it and run it as the
// conversation script.
//
// Structural anti-pile-up: a pending 'routine' inbox item for the same
// routine key blocks refiring; once the pending item is older than the
// routine's stale_after_hours (default 24), the next due firing SUPERSEDES
// it and dispatches fresh — yesterday's morning briefing never accumulates,
// and the supersede (a terminal exit) clears the old dispatch descriptor.
//
// Non-interactive customs are out of scope here — they belong to the ring
// consumer hooks (config.json `hooks.*`).

import { existsSync, readFileSync, readdirSync, statSync, unlinkSync } from 'fs';
import { join, isAbsolute, resolve as resolvePath } from 'path';
import { homedir } from 'os';
import { execFileSync, execSync } from 'child_process';
import {
  atomicWrite, log as _log, logError as _logError,
} from './watchtower-lib.mjs';
import { createItem, listPending, supersedeItem } from './watchtower-queue.mjs';

// Same load-time env pattern as watchtower-queue.mjs: tests point
// WATCHTOWER_DIR at a fixture dir BEFORE dynamically importing this module.
const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const STATE_PATH = join(WATCHTOWER_DIR, 'state', 'routine-state.json');
const DISPATCH_TMP_DIR = join(WATCHTOWER_DIR, 'tmp');

export const TRIGGER_TYPES = ['time-of-day', 'interval', 'path-nonempty', 'session-close'];
const DEFAULT_STALE_AFTER_HOURS = 24;
const DEFAULT_PATH_COOLDOWN_MINUTES = 60;
const MUX_TIMEOUT_MS = 15_000;

// ---------------------------------------------------------------------------
// Declaration validation — warn-and-skip, never throw on a bad declaration
// ---------------------------------------------------------------------------

/**
 * Validate one routine declaration. Returns an array of error strings
 * (empty = valid). A routine is { name, trigger: {type, ...}, script,
 * description?, urgency?, stale_after_hours?, cooldown_minutes? }.
 */
export function validateRoutine(routine) {
  const errors = [];
  if (!routine || typeof routine !== 'object') return ['not an object'];
  if (typeof routine.name !== 'string' || !/^[a-z0-9][a-z0-9-]*$/.test(routine.name)) {
    errors.push("name must be a kebab-case slug (e.g. 'morning-briefing')");
  }
  if (typeof routine.script !== 'string' || !routine.script.trim()) {
    errors.push('script is required (path to the conversation phase file, relative to the project root)');
  }
  const t = routine.trigger;
  if (!t || typeof t !== 'object' || !TRIGGER_TYPES.includes(t.type)) {
    errors.push(`trigger.type must be one of: ${TRIGGER_TYPES.join(' | ')}`);
    return errors;
  }
  if (t.type === 'time-of-day' && !/^([01]?\d|2[0-3]):[0-5]\d$/.test(t.at || '')) {
    errors.push("time-of-day trigger needs at: 'HH:MM' (local time)");
  }
  if (t.type === 'interval' && (!Number.isInteger(t.minutes) || t.minutes < 5)) {
    errors.push('interval trigger needs minutes: integer >= 5');
  }
  if (t.type === 'path-nonempty' && (typeof t.path !== 'string' || !t.path.trim())) {
    errors.push('path-nonempty trigger needs path (relative to the project root, or absolute)');
  }
  return errors;
}

export function routineKey(projectName, routine) {
  return `${projectName}/${routine.name}`;
}

// ---------------------------------------------------------------------------
// Per-routine firing state — { "<project>/<name>": { last_fired, last_item_id } }
// ---------------------------------------------------------------------------

export function loadRoutineState() {
  try {
    if (!existsSync(STATE_PATH)) return {};
    const parsed = JSON.parse(readFileSync(STATE_PATH, 'utf8'));
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function saveRoutineState(state) {
  atomicWrite(STATE_PATH, JSON.stringify(state, null, 2) + '\n');
}

// ---------------------------------------------------------------------------
// Trigger evaluation — pure given (routine, lastFired, event, now)
// ---------------------------------------------------------------------------

function pathHasContent(p) {
  try {
    const st = statSync(p);
    if (st.isDirectory()) {
      return readdirSync(p).some((n) => !n.startsWith('.'));
    }
    return st.size > 0;
  } catch {
    return false;
  }
}

/**
 * Is this routine's trigger due?
 * @param {object} routine - validated declaration
 * @param {string|null} lastFiredIso - ISO timestamp of the last firing
 * @param {object} event - { type: 'tick' } (Ring 1) or
 *   { type: 'session-close' } (Ring 3, already filtered to this project)
 * @param {Date} now
 * @param {string} projectPath - for resolving relative trigger paths
 * @returns {boolean}
 */
export function triggerDue(routine, lastFiredIso, event, now, projectPath) {
  const t = routine.trigger;
  const lastFired = lastFiredIso ? new Date(lastFiredIso) : null;

  if (t.type === 'session-close') {
    if (event.type !== 'session-close') return false;
    const cooldownMin = Number.isInteger(routine.cooldown_minutes) ? routine.cooldown_minutes : 0;
    if (lastFired && now - lastFired < cooldownMin * 60_000) return false;
    return true;
  }

  // The remaining types are mechanical-tick triggers.
  if (event.type !== 'tick') return false;

  if (t.type === 'time-of-day') {
    const [hh, mm] = t.at.split(':').map(Number);
    const todayAt = new Date(now);
    todayAt.setHours(hh, mm, 0, 0);
    if (now < todayAt) return false;
    // Fire once per day: due only if the last firing predates today's slot.
    return !lastFired || lastFired < todayAt;
  }

  if (t.type === 'interval') {
    if (!lastFired) return true;
    return now - lastFired >= t.minutes * 60_000;
  }

  if (t.type === 'path-nonempty') {
    const p = isAbsolute(t.path) ? t.path : resolvePath(projectPath, t.path);
    if (!pathHasContent(p)) return false;
    const cooldownMin = Number.isInteger(routine.cooldown_minutes)
      ? routine.cooldown_minutes : DEFAULT_PATH_COOLDOWN_MINUTES;
    if (lastFired && now - lastFired < cooldownMin * 60_000) return false;
    return true;
  }

  return false;
}

function triggerSummary(routine) {
  const t = routine.trigger;
  switch (t.type) {
    case 'time-of-day': return `daily at ${t.at}`;
    case 'interval': return `every ${t.minutes} min`;
    case 'path-nonempty': return `when ${t.path} is non-empty`;
    case 'session-close': return 'on session close';
    default: return t.type;
  }
}

// ---------------------------------------------------------------------------
// Dispatch — inbox item (durable) + mux descriptor (routing), no fork
// ---------------------------------------------------------------------------

function buildPickupPrompt({ routine, projectName, projectPath, itemId }) {
  const scriptPath = isAbsolute(routine.script)
    ? routine.script : join(projectPath, routine.script);
  return `Routine '${routine.name}' for ${projectName} fired (${triggerSummary(routine)}). `
    + `This is a declared interactive routine — read ${scriptPath} and run it as the `
    + `conversation script it contains (it defines what this routine does with the operator). `
    + `When the routine completes, resolve inbox item ${itemId} via watchtower-queue `
    + `resolveItem with resolution_type 'acted-on' and a resolution naming what it produced `
    + `— the terminal exit clears the dispatch descriptor. If the script file is missing, `
    + `dismiss the item with notes saying so.`;
}

// Find the mux binary; null when mux isn't installed (inbox-only degradation
// — `mux qa drain`'s inbox fallback picks pending routine items up later).
// WATCHTOWER_MUX_BIN overrides discovery (tests point it at a stub; a
// missing override path means "behave as if mux is absent").
function findMux() {
  if (process.env.WATCHTOWER_MUX_BIN !== undefined) {
    const o = process.env.WATCHTOWER_MUX_BIN;
    return o && existsSync(o) ? o : null;
  }
  const local = join(homedir(), '.local', 'bin', 'mux');
  if (existsSync(local)) return local;
  try {
    const p = execSync('command -v mux', { encoding: 'utf8' }).trim();
    return p || null;
  } catch {
    return null;
  }
}

/**
 * Dispatch one fired routine: file the inbox item, write a routing
 * descriptor, push it through `mux qa dispatch` (the single dispatch path).
 * Returns { status: 'dispatched' | 'inbox-only', item_id }.
 */
export function dispatchRoutine({ projectName, projectPath, routine, filedBy, desk = null }) {
  const key = routineKey(projectName, routine);
  const itemId = createItem({
    project: projectName,
    project_path: projectPath,
    category: 'routine',
    urgency: routine.urgency === 'urgent' || routine.urgency === 'low' ? routine.urgency : 'normal',
    title: `Routine: ${routine.name}`,
    summary: routine.description
      || `Declared routine '${routine.name}' fired (${triggerSummary(routine)})`,
    context_anchor: `routine ${key} — script ${routine.script}`,
    evidence: {
      source: 'routine-dispatch',
      routine_key: key,
      routine_name: routine.name,
      trigger: routine.trigger,
      script: routine.script,
    },
    filed_by: filedBy,
    desk,
  });

  const descriptor = {
    project: projectName,
    project_path: projectPath,
    item_id: itemId,
    what: `routine '${routine.name}' (${triggerSummary(routine)})`,
    pickup_prompt: buildPickupPrompt({ routine, projectName, projectPath, itemId }),
  };
  const descriptorPath = join(DISPATCH_TMP_DIR, `routine-${itemId}.json`);
  atomicWrite(descriptorPath, JSON.stringify(descriptor, null, 2) + '\n');

  let status = 'inbox-only';
  const mux = findMux();
  if (mux) {
    try {
      execFileSync(mux, ['qa', 'dispatch', descriptorPath], {
        timeout: MUX_TIMEOUT_MS, stdio: 'pipe', encoding: 'utf8',
      });
      status = 'dispatched';
    } catch {
      // mux failed (no tmux server, dead desk, …) — the inbox item is the
      // durable record and the drain's inbox fallback covers the pull path.
    }
  }
  try { unlinkSync(descriptorPath); } catch { /* best-effort tmp cleanup */ }
  return { status, item_id: itemId };
}

// ---------------------------------------------------------------------------
// The pass — evaluate every declared routine against one event
// ---------------------------------------------------------------------------

/**
 * Run one routine pass. Never throws.
 * @param {object} params
 * @param {object} params.config - watchtower config (projects[*].routines)
 * @param {object} params.event - { type: 'tick' } for Ring 1 ticks, or
 *   { type: 'session-close', project: '<config project name>' } from Ring 3
 * @param {string} params.filedBy - 'ring1' | 'ring3-close'
 * @param {Date} [params.now]
 * @returns {{fired: Array, skipped: Array, invalid: Array}}
 */
export function runRoutinePass({ config, event, filedBy, now = new Date() }) {
  const fired = [];
  const skipped = [];
  const invalid = [];
  const state = loadRoutineState();
  let stateDirty = false;

  const projects = config?.projects || {};
  for (const [projectName, entry] of Object.entries(projects)) {
    if (event.type === 'session-close' && event.project !== projectName) continue;
    const projectPath = entry?.path || entry;
    const routines = Array.isArray(entry?.routines) ? entry.routines : [];

    for (const routine of routines) {
      const errors = validateRoutine(routine);
      const key = routine?.name ? routineKey(projectName, routine) : `${projectName}/<unnamed>`;
      if (errors.length > 0) {
        invalid.push({ key, errors });
        _logError('routines', `invalid routine ${key}: ${errors.join('; ')} — skipped`);
        continue;
      }

      const lastFired = state[key]?.last_fired || null;
      if (!triggerDue(routine, lastFired, event, now, projectPath)) continue;

      // Structural dedup: an undone pending item for this routine blocks a
      // refire — unless it has gone stale, in which case the fresh firing
      // supersedes it (terminal exit → old descriptor cleared by the queue
      // lib) and dispatches anew.
      const pending = listPending({ project: projectName, category: 'routine' })
        .find((i) => i.evidence?.routine_key === key);
      if (pending) {
        const staleHours = Number.isFinite(routine.stale_after_hours)
          ? routine.stale_after_hours : DEFAULT_STALE_AFTER_HOURS;
        const ageMs = now - new Date(pending.filed_at);
        if (ageMs < staleHours * 3_600_000) {
          skipped.push({ key, reason: 'pending-exists', item_id: pending.id });
          continue;
        }
        try {
          supersedeItem(pending.id, {
            reason: `superseded by a newer firing of routine ${key} (stale after ${staleHours}h)`,
          });
        } catch (e) {
          _logError('routines', `could not supersede stale ${pending.id} for ${key}: ${e.message}`);
          skipped.push({ key, reason: 'supersede-failed', item_id: pending.id });
          continue;
        }
      }

      try {
        const result = dispatchRoutine({ projectName, projectPath, routine, filedBy, desk: event.desk || null });
        state[key] = { last_fired: now.toISOString(), last_item_id: result.item_id };
        stateDirty = true;
        fired.push({ key, ...result });
        _log('routines', `fired ${key} → ${result.item_id} (${result.status})`);
      } catch (e) {
        _logError('routines', `dispatch failed for ${key}: ${e.message}`);
      }
    }
  }

  if (stateDirty) {
    try { saveRoutineState(state); } catch (e) {
      _logError('routines', `could not persist routine state: ${e.message}`);
    }
  }
  return { fired, skipped, invalid };
}

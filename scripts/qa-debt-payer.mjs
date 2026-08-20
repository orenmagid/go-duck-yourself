#!/usr/bin/env node
/**
 * qa-debt-payer — the predicates behind `/qa-drain` Step 4.
 *
 * Piece 3 of `.claude/plans/qa-debt-payer-build.md`. Carriers: act:df8e90e7
 * (the payer) and act:d439fd2d (the fired-trigger queue it reads FIRST).
 *
 * WHY THIS IS CODE AND NOT PROSE IN A SKILL. Step 4 is the artifact a previous
 * session claimed to have BUILT while the skill file had no Step 4 at all, and
 * this repo's recurring failure is a prose feature shipping green tests that
 * all exercise one mechanical guard. So selection, reaping and the ledger state
 * are exported functions with fixtures; the skill narrates and calls.
 *
 * JURISDICTION, STATED ONCE (§0). An action `in-progress` WITH this payer's
 * spawn marker belongs to the reaper. An action `in-progress` WITHOUT one — 45
 * such items exist, oldest ~65 days — belongs to the human review passes and is
 * never reset here. A released-trigger item is OFFERED regardless of status;
 * offering is not resetting, so the two rules cannot collide.
 */

import { existsSync } from 'node:fs';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { homedir } from 'node:os';
import { execFileSync } from 'node:child_process';

export const SPAWN_MARKER = '<!-- qa-payer-lane';
export const FIRED_QUEUE_CAP = 2;
export const FIX_BATCH_CAP = 2;
export const RUN_BATCH_CAP = 3;
export const LANE_STALE_HOURS = 24;
export const PAYER_STALE_DAYS = 14;

const stateDir = () => join(process.env.WATCHTOWER_DIR || join(homedir(), '.claude-cabinet', 'watchtower'), 'state');
const statePath = () => join(stateDir(), 'qa-payer-state.json');

// ---------------------------------------------------------------------------
// §1 — the spawn marker and the reaper
// ---------------------------------------------------------------------------

/**
 * The marker is the reaper's ONLY clock. pib actions have no `updated_at`, so
 * without a timestamp of its own nothing can bound "is this lane still alive" —
 * an abandoned dirty worktree would read as live forever (mux only auto-removes
 * CLEAN ones).
 */
export function buildSpawnMarker({ branch, attempt, at }) {
  return `${SPAWN_MARKER} branch=${branch} attempt=${attempt} at=${at} -->`;
}

export function parseSpawnMarker(notes) {
  if (typeof notes !== 'string') return null;
  const i = notes.lastIndexOf(SPAWN_MARKER);
  if (i === -1) return null;
  const end = notes.indexOf('-->', i);
  if (end === -1) return null;
  const body = notes.slice(i + SPAWN_MARKER.length, end);
  const get = (k) => (body.match(new RegExp(`${k}=(\\S+)`)) || [])[1] || null;
  const attempt = parseInt(get('attempt'), 10);
  return { branch: get('branch'), attempt: Number.isFinite(attempt) ? attempt : 1, at: get('at') };
}

/**
 * Classify one marked lane.
 *
 * `landed` is decided by searching main's commit messages for the action's fid,
 * NOT by branch ancestry: a squash-merged-then-deleted branch is
 * indistinguishable from "branch gone" by ancestry, and treating it as dead
 * would reset LANDED work to open and spawn it a second time.
 */
export function classifyLane({ marker, worktreeExists, fidInMainLog, branchExists, now = Date.now() }) {
  if (!marker) return { state: 'unmarked', action: 'leave' };
  if (fidInMainLog) return { state: 'landed', action: 'flag' };
  if (worktreeExists) {
    const ageH = marker.at ? (now - Date.parse(marker.at)) / 3600000 : null;
    if (ageH == null || !Number.isFinite(ageH)) return { state: 'live', action: 'skip' };
    return ageH > LANE_STALE_HOURS
      ? { state: 'stale', action: 'flag', age_hours: Math.round(ageH) }
      : { state: 'live', action: 'skip', age_hours: Math.round(ageH) };
  }
  return { state: 'dead', action: 'reset', next_attempt: marker.attempt + 1, branchExists: !!branchExists };
}

/** Lane name: unique per ATTEMPT, not per action. 17 chars — inside mux's 30-char slug truncation. */
export function laneName(fid, attempt = 1) {
  const short = String(fid).replace(/^act:/, '').slice(0, 8);
  return `qa-fix-${short}-${attempt}`;
}

/** A branch at the computed name must REFUSE the spawn — mux would check it out and resume dead commits. */
export function branchExists(repoPath, name) {
  try {
    execFileSync('git', ['-C', repoPath, 'show-ref', '--verify', '--quiet', `refs/heads/mux/${name}`]);
    return true;
  } catch { return false; }
}

/** Did work for this fid already land on main? Searches commit messages, not ancestry. */
export function fidLandedOnMain(repoPath, fid) {
  try {
    const out = execFileSync('git', ['-C', repoPath, 'log', '--oneline', '-50', `--grep=${fid}`, 'main'],
      { encoding: 'utf8', timeout: 5000 });
    return out.trim().length > 0;
  } catch { return false; }
}

// ---------------------------------------------------------------------------
// §2/§4 — selection
// ---------------------------------------------------------------------------

/**
 * Pick ONE batch of ONE kind, project-scoped, oldest-first.
 *
 * EXCLUSIONS use the tracker's own derived answers (`waiting_on_trigger`,
 * derived blockedness), never the raw presence of a trigger condition. The raw
 * test is WIDER than the background evaluator's own collect predicate — that
 * evaluator only looks at deferred rows — so excluding on it opens a permanent
 * dead zone: 31 measured items that neither side would ever touch, growing every
 * time the payer marks something, because nothing can clear a trigger condition.
 */
export function selectBatch({ ledgerEntries, project, trackerRows = new Map(), firedQueue = [] }) {
  const fired = firedQueue.slice(0, FIRED_QUEUE_CAP);

  const candidates = ledgerEntries
    .filter((e) => e.bucket === 'open' && e.project === project && e.kind)
    .map((e) => ({ ...e, row: trackerRows.get(e.fid) || {} }))
    .filter((e) => {
      if (e.row.waiting_on_trigger) return false;      // still waiting — the evaluator's job
      if (e.row.has_unmet_blocker) return false;        // edge-blocked — derived, reversible
      if (e.row.in_progress_marked) return false;       // a live lane of ours
      if (e.kind === 'event-gated') return false;       // never selectable by design
      return true;
    })
    .sort((a, b) => (b.age_days || 0) - (a.age_days || 0));

  // The residue nobody watches: a trigger-carrying row in a status the
  // evaluator never collects can never receive a check, so its verdict stays
  // null forever and it is excluded forever. Reported, never silently dropped.
  const strandedTriggers = ledgerEntries.filter((e) => {
    const r = trackerRows.get(e.fid);
    return e.bucket === 'open' && e.project === project
      && r && r.has_trigger && r.status !== 'deferred';
  }).map((e) => e.fid);

  const byKind = (k) => candidates.filter((c) => c.kind === k);
  const runBatch = byKind('qa-verify').slice(0, RUN_BATCH_CAP);
  const fixBatch = byKind('code-fix').slice(0, FIX_BATCH_CAP);
  const askItem = [...byKind('decision'), ...byKind('operator')][0] || null;

  // One mode per drain. Fired work is offered ALONGSIDE, capped, and never
  // starves the pile: measured, one project carries 24 released items against a
  // 1-2 bite, so a fired-first-until-empty rule would take a dozen drains to
  // read its own pile once.
  let batch = null;
  if (runBatch.length) batch = { kind: 'qa-verify', items: runBatch, mode: 'in-session' };
  else if (fixBatch.length) batch = { kind: 'code-fix', items: fixBatch, mode: 'spawn-lanes-on-approval' };
  else if (askItem) batch = { kind: askItem.kind, items: [askItem], mode: 'surface-one' };

  return {
    fired,
    fired_total: firedQueue.length,
    batch,
    candidates_total: candidates.length,
    stranded_triggers: strandedTriggers,
    event_gated: ledgerEntries.filter((e) => e.bucket === 'open' && e.project === project && e.kind === 'event-gated').length,
  };
}

// ---------------------------------------------------------------------------
// §3 — the ledger line's state sidecar
// ---------------------------------------------------------------------------

/**
 * Written on EVERY Step 4 execution, including a no-op.
 *
 * A drain that selects nothing writes no marker and completes no action, so
 * without this there is no evidence the payer ran at all — and net movement
 * cannot distinguish "nothing to pay" from "the payer never fires". That
 * distinction is the whole no-drains detector.
 */
export async function recordPayerRun({ project, open, paid = 0, now = new Date().toISOString() }) {
  const p = statePath();
  let state = {};
  try { state = JSON.parse(await readFile(p, 'utf8')); } catch { /* first run */ }
  state[project] = { last_run: now, open_at_last_run: open, paid_at_last_run: paid };
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, JSON.stringify(state, null, 2));
  return state[project];
}

export async function readPayerState() {
  try { return JSON.parse(await readFile(statePath(), 'utf8')); } catch { return {}; }
}

/** The no-drains detector: a non-empty pile the payer has not touched in 14 days. */
export function payerIsStale(entry, { open, now = Date.now() } = {}) {
  if (!open) return false;
  if (!entry || !entry.last_run) return true;
  return (now - Date.parse(entry.last_run)) / 86400000 > PAYER_STALE_DAYS;
}

export { statePath as payerStatePath };

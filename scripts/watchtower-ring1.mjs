#!/usr/bin/env node

// Watchtower Ring 1 — Mechanical heartbeat.
// Zero Claude API calls. Runs on a cron (default 300s / 5min).
//
// For each tracked project:
//   - Git state (branch, last commit, branches ahead of main)
//   - pib-db queries (open actions, flagged, stale, completion candidates)
//   - Active sessions (recently-modified .jsonl files)
//   - Deployment detection (railway.toml, fly.toml, vercel.json)
//
// Assembles state/summary.md (hard cap 30 lines, atomic write)
// and per-project state/projects/<slug>.md files.
// Writes ring1-health.json with last-run timestamp.
//
// Also delivers the global CC feedback outbox (~/.claude/
// cc-feedback-outbox.json) to the CC repo's feedback/ dir every tick —
// deterministic file IO belongs at the mechanical layer (act:6c3a4763).
//
// Consumer hooks: reads config.json hooks.ring1-post-collect, spawns
// each with 30s timeout, passes project state JSON on stdin.

import {
  readFileSync, readdirSync, existsSync, statSync,
  mkdirSync, realpathSync,
} from 'fs';
import { pathToFileURL } from 'url';
import { join, basename } from 'path';
import { execSync, execFileSync } from 'child_process';
import { homedir } from 'os';
import {
  atomicWrite, loadConfig, slugify, log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending, loadBetterSqlite3,
  writeProjectStatePreservingRing3, flushFeedbackOutbox, resolveCcSourceRepo,
  authoredClaudeDirs, claudeChurnIsDisposable, checkMemoryReachability,
  autoReconcileItem,
} from './watchtower-lib.mjs';
import { runRoutinePass } from './watchtower-routines.mjs';
import { analyze, resolveRoots } from './watchtower-sync.mjs';

const WATCHTOWER_DIR = getWatchtowerDir();

const CLAUDE_HOME = join(homedir(), '.claude');
const STALE_DAYS = 14;
const ACTIVE_SESSION_THRESHOLD_MS = 30 * 60 * 1000; // 30 minutes

// The flagged-actions query (act:b1b21a15). Exported so the hermetic test
// asserts the SAME SQL collectPibState runs — one source for the filter, no
// divergent copy. Scope: active work only (open/in-progress/blocked),
// deliberately excluding flagged *deferred* actions (the documented narrowing
// from orient's `completed = 0`) and excluding soft-deleted rows.
export const FLAGGED_ACTIONS_SQL =
  "SELECT fid, text FROM actions WHERE flagged = 1 " +
  "AND status IN ('open', 'in-progress', 'blocked') AND deleted_at IS NULL";
const DEPLOY_MARKERS = ['railway.toml', 'fly.toml', 'vercel.json'];
const HOOK_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function log(msg) { _log('ring1', msg); }
function logError(msg) { _logError('ring1', msg); }

function ago(ms) {
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function safeExec(cmd, opts = {}) {
  try {
    // stderr is ignored, not inherited — otherwise expected failures
    // (e.g. non-git projects) bleed "fatal:" noise into the launchd log
    return execSync(cmd, {
      encoding: 'utf8', timeout: 10_000,
      stdio: ['ignore', 'pipe', 'ignore'],
      ...opts,
    }).trim();
  } catch {
    return null;
  }
}

// Non-shell sibling of safeExec for arguments that are DATA, not commands —
// filenames and other strings a repo's author controls. execFileSync passes
// args verbatim (no shell parse), so a file literally named `$(cmd)` is just
// a filename, never an execution (CP3 security finding on isMainShadow).
function safeExecFile(file, args, opts = {}) {
  try {
    return execFileSync(file, args, {
      encoding: 'utf8', timeout: 10_000,
      stdio: ['ignore', 'pipe', 'ignore'],
      ...opts,
    }).trim();
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Default-branch / origin comparison helpers
// ---------------------------------------------------------------------------
//
// The unmerged-branch ahead-check (act:6f36cbe2) compares against
// origin/<main>, NEVER the stale local <main> ref. A worktree is often cut
// from a non-main HEAD, and the local main ref can lag origin/main by many
// commits — so `git log main..<branch>` reported the branch as carrying all
// of main's drift as its own work (the recurring "2 commits look like 80
// files of baggage" trap that produced false MERGE-OR-LOSE / N-commits-ahead
// banners). Comparing against origin/<main> is the WHAT fix; build-context's
// re-verify-at-read is the WHEN fix (act:a136b362).
//
// resolveMainRef takes an injectable exec (the inline call sites pass a
// safeExec bound to a cwd) so the logic is hermetically testable against a
// temp git repo. It:
//   - resolves the default branch's *name* from `origin/HEAD` (fall back to
//     'main', then 'master'),
//   - fetches origin first so origin/<main> is current — but TOLERATES fetch
//     failure (Ring 1's launchd cron PATH/network is constrained) and falls
//     back to whatever origin/<main> ref already exists locally,
//   - picks compareRef = origin/<main> when that remote-tracking ref exists,
//     else the local <main> (a repo with no remote still works), and
//   - flags localLagsRemote when the local <main> is a strict ancestor of
//     origin/<main> (a stale local main is itself worth surfacing).
function resolveMainRef(exec, { fetch = true } = {}) {
  if (fetch) {
    // Best-effort refresh; never let a failed/blocked fetch error the check.
    exec('git fetch origin --quiet');
  }

  // Default branch name from origin/HEAD, e.g. "origin/main" → "main".
  let mainName = null;
  const headRef = exec('git rev-parse --abbrev-ref origin/HEAD');
  if (headRef) mainName = headRef.replace(/^origin\//, '').trim() || null;
  if (!mainName) {
    // No origin/HEAD (no remote, or never set) — probe local refs.
    if (exec('git rev-parse --verify --quiet main')) mainName = 'main';
    else if (exec('git rev-parse --verify --quiet master')) mainName = 'master';
    else mainName = 'main';
  }

  const remoteRef = `origin/${mainName}`;
  const remoteSha = exec(`git rev-parse --verify --quiet ${remoteRef}`);
  const localSha = exec(`git rev-parse --verify --quiet ${mainName}`);

  // Prefer the remote-tracking ref; fall back to local main when there is no
  // remote (a brand-new repo, or origin unreachable and never fetched).
  const compareRef = remoteSha ? remoteRef : mainName;

  // localLagsRemote: local main is behind its remote (strict ancestor, not
  // equal). Only meaningful when both refs resolve and we're comparing
  // against the remote. This is the stale-local-main signal worth a NOTE.
  let localLagsRemote = false;
  if (remoteSha && localSha && remoteSha !== localSha) {
    localLagsRemote = exec(`git merge-base --is-ancestor ${mainName} ${remoteRef}`) !== null;
  }

  return { mainName, compareRef, localLagsRemote, hasRemote: !!remoteSha };
}

// Commits on `branch` not yet in `compareRef` (origin/<main>). Uses
// `git log <branch> --not <compareRef>` — the work range that is correct for
// both pre- and post-merge states (see qa-handoff Step 1). Returns a count.
function aheadCount(exec, branch, compareRef) {
  const out = exec(`git log --oneline ${branch} --not ${compareRef}`);
  return out ? out.split('\n').filter(l => l.trim()).length : 0;
}

// True when `branch`'s tip is already an ancestor of `compareRef` (merged).
function isMergedInto(exec, branch, compareRef) {
  return exec(`git merge-base --is-ancestor ${branch} ${compareRef}`) !== null;
}

// True when `branch` carries content not already present in `compareRef`.
// SQUASH-ROBUST and direction-aware, unlike the ancestry/ahead-count gates it
// replaced (act:a152cf6c): a squash-merge leaves `branch` a NON-ancestor of
// main with different commit hashes but an IDENTICAL tree, so `isMergedInto`
// (ancestry) and `aheadCount` both report phantom unmerged work — permanently,
// since main never gains `branch` as an ancestor. ~40 of 48 maginnis worktree
// false positives were squash-merged branches that no ahead-check could clear.
//
// A two-dot `compareRef..branch` diff is NOT a valid content test: it is a
// symmetric tip-vs-tip comparison, so once `compareRef` advances past the
// squash point (true for every aging item) it reports compareRef's OWN newer
// files as differences (verified empirically). The correct test is a trial
// in-memory merge: if merging `branch` into `compareRef` changes nothing
// (result tree == compareRef's tree), `branch` contributes no unmerged
// content. `git merge-tree --write-tree` (git >= 2.38) touches no working tree
// or index. It exits non-zero on a merge conflict (→ null here) — a conflict
// means real divergent content, so we fail toward flagging. Likewise a missing
// ref, an old git without --write-tree, or any git error fails toward flagging,
// so real work is never silently suppressed.
function hasUnmergedContent(exec, branch, compareRef) {
  // Fast path: a true ancestor is unambiguously merged (and cheaper to test).
  if (isMergedInto(exec, branch, compareRef)) return false;
  const baseTree = exec(`git rev-parse --verify --quiet ${compareRef}^{tree}`);
  const mergedOut = exec(`git merge-tree --write-tree ${compareRef} ${branch}`);
  if (baseTree === null || mergedOut === null) return true; // error/conflict → flag
  const mergedTree = mergedOut.split('\n')[0].trim();
  return mergedTree !== baseTree;
}

// ---------------------------------------------------------------------------
// Git state collection
// ---------------------------------------------------------------------------

function collectGitState(projectPath) {
  if (!existsSync(join(projectPath, '.git'))) {
    return null;
  }

  const cwd = projectPath;
  const exec = (cmd) => safeExec(cmd, { cwd });
  const branch = safeExec('git rev-parse --abbrev-ref HEAD', { cwd });
  const lastCommitRaw = safeExec(
    'git log -1 --format="%H%x00%s%x00%aI"',
    { cwd }
  );

  let lastCommit = null;
  if (lastCommitRaw) {
    const [hash, message, timestamp] = lastCommitRaw.split('\0');
    lastCommit = { hash, message, timestamp };
  }

  // Branches ahead of origin/<main> — compared against the remote-tracking
  // ref, not the local <main> ref (act:6f36cbe2). A branch counts as "ahead"
  // only when it carries CONTENT not yet in origin/<main> — squash-merged
  // branches (non-ancestor, different hashes, identical tree) are NOT ahead
  // (act:a152cf6c). hasUnmergedContent short-circuits on the ancestry fast
  // path, so this is no more expensive than the old isMergedInto check for the
  // common already-merged case.
  const { mainName, compareRef, localLagsRemote } = resolveMainRef(exec);
  const branchesAhead = [];
  // `branches` distinguishes listing FAILURE (null — never treat a branch as
  // absent) from a successful listing (array, possibly empty). The
  // branch-diverged reconciler's gone-branch retraction keys on membership in
  // a VERIFIED listing, never on a failed per-ref probe — safeExec collapses
  // "ref absent" and "git errored" into the same null, and reading an error
  // as "branch gone" would auto-close items over real unmerged work.
  let branches = null;
  const branchList = safeExec(`git for-each-ref --format='%(refname:short)' refs/heads/`, { cwd });
  if (branchList !== null) {
    branches = branchList.split('\n')
      .map(line => line.trim().replace(/^\* /, ''))
      .filter(b => b);
    for (const b of branches) {
      if (b === mainName) continue;
      // Git ref rules allow '$', ';', '(' — legal-but-shell-hostile names
      // (e.g. from a cloned third-party repo) never reach the interpolated
      // content check. Skipped loudly; such a branch is not flagged.
      if (!isSafeRefName(b)) {
        logError(`collectGitState ${cwd}: skipping unsafe branch name ${JSON.stringify(b)}`);
        continue;
      }
      if (hasUnmergedContent(exec, b, compareRef)) branchesAhead.push(b);
    }
  }

  return { branch, lastCommit, branchesAhead, branches, mainBranch: mainName, compareRef, localLagsRemote };
}

// ---------------------------------------------------------------------------
// pib-db queries
// ---------------------------------------------------------------------------

function collectPibState(projectPath) {
  const dbPath = join(projectPath, 'pib.db');
  if (!existsSync(dbPath)) {
    return null;
  }

  const Database = loadBetterSqlite3(projectPath);
  if (!Database) {
    // No silent failures: this error also renders in the state file and
    // aggregates into summary attention — a portfolio-wide "No pib-db
    // data" hid a broken loader for weeks (act:b9414039).
    logError(`collectPibState ${projectPath}: better-sqlite3 not available from any candidate`);
    return { error: 'better-sqlite3 not available' };
  }

  let db;
  try {
    db = new Database(dbPath, { readonly: true });
  } catch (e) {
    const msg = String(e.message).split('\n')[0];
    logError(`collectPibState ${projectPath}: cannot open pib.db: ${msg}`);
    return { error: `Cannot open pib.db: ${msg}` };
  }

  try {
    // Non-fatal per-query warnings (surfaced in the state file + logged) — a
    // bare catch used to swallow a genuinely-broken query (the dead updated_at
    // stale predicate) for weeks (data-integrity-0001).
    const pibWarnings = [];

    // Open action count. Use pib-db-lib's CANONICAL predicate
    // (deleted_at IS NULL AND completed = 0 — see listActions) so Ring 1's
    // count matches orient/work-tracker exactly. The old status-only filter
    // counted soft-deleted rows and undercounted deferred, drifting 197 vs the
    // canonical 186 (data-integrity-0001).
    const openActions = db.prepare(
      'SELECT COUNT(*) as count FROM actions WHERE deleted_at IS NULL AND completed = 0'
    ).get();

    // Flagged actions (user-prioritized, still-open work). The LIST — not
    // just a count — so the per-project state file surfaces WHICH actions are
    // flagged, at parity with orient's work-scan (act:b1b21a15). flaggedCount
    // is derived from the list: one query, one source (mirrors overdueActions
    // below). Scope is Ring 1's standard "active work" set
    // (open/in-progress/blocked) — deliberately NARROWER than orient's
    // `completed = 0`, which also surfaced flagged *deferred* actions; that
    // narrowing is recorded in the orient→watchtower coverage ledger.
    let flaggedActions = [];
    try {
      flaggedActions = db.prepare(FLAGGED_ACTIONS_SQL).all();
    } catch {
      // flagged column may not exist in older schemas
    }
    const flaggedCount = flaggedActions.length;

    // Deferred actions waiting on triggers — standing state, not attention.
    // Orient's deferred-check evaluates these every session; reported in the
    // per-project deep file, never in the summary's attention list.
    let deferredTriggerCount = 0;
    try {
      deferredTriggerCount = db.prepare(
        "SELECT COUNT(*) as count FROM actions WHERE status = 'deferred' AND trigger_condition IS NOT NULL AND deleted_at IS NULL"
      ).get().count;
    } catch {
      // trigger_condition column may not exist in older schemas
    }

    // Stale projects: an active project that still has OPEN work but has
    // COMPLETED nothing in STALE_DAYS. Mirrors orient's canonical definition
    // (MAX(completed_at) older than the threshold, or none) using columns that
    // actually exist. The prior predicate filtered on `a.updated_at`, a column
    // absent from every schema version and the live DB — so db.prepare() threw
    // every single tick, the bare catch swallowed it, and staleProjects stayed
    // [] permanently (data-integrity-0001: the /briefing backlog-hygiene nudge
    // could never fire).
    const staleThreshold = new Date(Date.now() - STALE_DAYS * 86400000).toISOString().slice(0, 10);
    let staleProjects = [];
    try {
      staleProjects = db.prepare(
        `SELECT p.fid, p.name FROM projects p
         WHERE p.status = 'active' AND p.deleted_at IS NULL
           AND EXISTS (
             SELECT 1 FROM actions a
             WHERE a.project_fid = p.fid AND a.deleted_at IS NULL AND a.completed = 0
           )
           AND NOT EXISTS (
             SELECT 1 FROM actions a
             WHERE a.project_fid = p.fid AND a.deleted_at IS NULL
               AND a.completed_at IS NOT NULL AND a.completed_at > ?
           )`
      ).all(staleThreshold);
    } catch (e) {
      // SURFACE it — the swallowed error here masked a dead query for weeks
      // (data-integrity-0001). Now that the predicate uses only stable columns,
      // a throw means the query is genuinely broken, not a known-absent column.
      const m = String(e && e.message).split('\n')[0];
      logError(`collectPibState ${projectPath}: stale-project query failed: ${m}`);
      pibWarnings.push(`stale-project query failed: ${m}`);
    }

    // Completion candidates (all actions done)
    let completionCandidates = [];
    try {
      completionCandidates = db.prepare(
        `SELECT p.fid, p.name FROM projects p
         WHERE p.status = 'active' AND p.deleted_at IS NULL
         AND EXISTS (SELECT 1 FROM actions a WHERE a.project_fid = p.fid AND a.deleted_at IS NULL)
         AND NOT EXISTS (
           SELECT 1 FROM actions a
           WHERE a.project_fid = p.fid
           AND a.deleted_at IS NULL
           AND a.status != 'done'
         )`
      ).all();
    } catch {
      // best-effort
    }

    // Per-project breakdown for portfolio pulse
    let projectBreakdown = [];
    try {
      // open_count uses the same canonical predicate as the total (completed = 0)
      // so per-project counts sum consistently with it; the JOIN filters
      // soft-deleted rows so tombstoned actions never inflate the pulse
      // (data-integrity-0001).
      projectBreakdown = db.prepare(
        `SELECT p.fid, p.name,
           SUM(CASE WHEN a.completed = 0 THEN 1 ELSE 0 END) as open_count,
           SUM(CASE WHEN a.completed = 0 AND a.status = 'blocked' THEN 1 ELSE 0 END) as blocked_count
         FROM projects p
         LEFT JOIN actions a ON a.project_fid = p.fid AND a.deleted_at IS NULL
         GROUP BY p.fid, p.name`
      ).all();
    } catch {
      // best-effort
    }

    // Overdue actions (due date is in the past). The `due` column is
    // unconstrained free text (TEXT, no CHECK), so a malformed value like
    // "06/18/2026" would string-compare as overdue — the GLOB guard skips
    // anything that isn't a well-formed YYYY-MM-DD. `<= date('now')` (not
    // `<`) matches pib-db-lib.mjs's existing overdue convention, the single
    // source of truth; due-today counts as overdue.
    let overdueActions = [];
    try {
      overdueActions = db.prepare(
        `SELECT fid, text, due FROM actions
         WHERE status IN ('open','in-progress','blocked')
           AND due GLOB '????-??-??' AND due <= date('now')
           AND deleted_at IS NULL`
      ).all();
    } catch {
      // due column may not exist in older schemas
    }

    return {
      openActions: openActions.count,
      flaggedCount,
      flaggedActions,
      deferredTriggerCount,
      staleProjects,
      completionCandidates,
      overdueActions,
      overdueCount: overdueActions.length,
      projectBreakdown,
      pibWarnings,
    };
  } finally {
    db.close();
  }
}

// ---------------------------------------------------------------------------
// Active session detection
// ---------------------------------------------------------------------------

function checkMemoryIntegrity(projectPath) {
  const encoded = projectPath.replace(/\//g, '-');
  const memDir = join(CLAUDE_HOME, 'projects', encoded, 'memory');
  // Reachability, not substring: a file is an orphan only when neither a
  // direct index line (MEMORY.md or MEMORY-archive.md) nor a region
  // pointer's glob covers it — the same rule scripts/validate-memory.mjs
  // enforces. The old filename-substring scan predated region pointers and
  // the archive index and flagged 203 false orphans while the validator
  // passed green (act:49cb1c27).
  return checkMemoryReachability(memDir);
}

function detectActiveSessions(projectPath) {
  // Claude Code stores session transcripts as .jsonl files under
  // ~/.claude/projects/<encoded-path>/
  const encoded = projectPath.replace(/\//g, '-');
  const sessionsDir = join(CLAUDE_HOME, 'projects', encoded);

  if (!existsSync(sessionsDir)) return [];

  const now = Date.now();
  const active = [];

  try {
    const entries = readdirSync(sessionsDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.endsWith('.jsonl')) continue;
      const filePath = join(sessionsDir, entry.name);
      try {
        const st = statSync(filePath);
        if (now - st.mtimeMs < ACTIVE_SESSION_THRESHOLD_MS) {
          active.push({
            file: entry.name,
            lastModified: st.mtime.toISOString(),
            agoMs: now - st.mtimeMs,
          });
        }
      } catch {
        // stat failure, skip
      }
    }
  } catch {
    // directory read failure, skip
  }

  return active;
}

// ---------------------------------------------------------------------------
// Deployment detection (cached per config cycle)
// ---------------------------------------------------------------------------

// CC-repo feedback-arrival check (act:b08efbc2). Only the CC SOURCE repo
// (package.json name === 'create-claude-cabinet') has a feedback/ root and a
// proposals/ dir; everywhere else this returns null at near-zero cost.
// feedback/ root files are untriaged BY DEFINITION (triage-at-arrival moves
// them to feedback/resolved/), and proposals/ holds pending extraction
// proposals. Any count > 0 raises a LOUD attention line in summary.md —
// recomputed every tick, so it cannot expire while the files remain. The
// blocking triage rule itself lives in the CC repo's always-loaded
// instructions (CLAUDE.md), not here: Ring 1 surfaces, the session acts.
function checkCcFeedbackArrival(projectPath) {
  try {
    const pkgPath = join(projectPath, 'package.json');
    if (!existsSync(pkgPath)) return null;
    const pkg = JSON.parse(readFileSync(pkgPath, 'utf8'));
    if (pkg.name !== 'create-claude-cabinet') return null;
    const countMd = (dir) => {
      try {
        return readdirSync(join(projectPath, dir), { withFileTypes: true })
          .filter((e) => e.isFile() && e.name.endsWith('.md') && !e.name.startsWith('.'))
          .length;
      } catch {
        return 0;
      }
    };
    const feedbackCount = countMd('feedback');
    const proposalCount = countMd('proposals');
    if (feedbackCount === 0 && proposalCount === 0) return null;
    return { feedbackCount, proposalCount };
  } catch {
    return null;
  }
}

function detectDeployment(projectPath) {
  const found = [];
  for (const marker of DEPLOY_MARKERS) {
    if (existsSync(join(projectPath, marker))) {
      found.push(marker);
    }
  }
  return found;
}

// ---------------------------------------------------------------------------
// Consumer hooks
// ---------------------------------------------------------------------------

function runConsumerHooksSync(hooks, projectState) {
  if (!hooks || !Array.isArray(hooks) || hooks.length === 0) return [];

  const results = [];
  for (const hookCmd of hooks) {
    try {
      const output = execSync(hookCmd, {
        encoding: 'utf8',
        timeout: HOOK_TIMEOUT_MS,
        input: JSON.stringify(projectState),
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      try {
        const parsed = JSON.parse(output);
        if (parsed && parsed.additional_checks) {
          results.push(...parsed.additional_checks);
        }
      } catch {
        // non-JSON output, ignore
      }
    } catch (e) {
      // Hook failure — log, never abort
      results.push({
        hook: hookCmd,
        error: e.message,
        status: 'hook-failed',
      });
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Queue item creation + retraction for branch divergence
// ---------------------------------------------------------------------------

// Long-lived branches that diverge from main BY DESIGN — filing an inbox item
// for them is pure noise, and because dedup checks only PENDING items, every
// human dismissal reopens the door on the next tick (the 2026-07-12 backup/*
// refile loop: dismissed at ~20:15 UTC, refiled 21:19 UTC). Entries without a
// '*' are EXACT names (a branch named "staging-fix" files normally); a '*' is
// a glob. Consumers override via config defaults.long_lived_branches (the
// consumer-default risk — a real work branch literally named "staging" never
// files — is documented in watchtower-contracts.md "Detector Symmetry").
const DEFAULT_LONG_LIVED_BRANCHES = ['staging', 'production', 'backup/*'];

function branchExclusionMatcher(config) {
  const list = Array.isArray(config?.defaults?.long_lived_branches)
    ? config.defaults.long_lived_branches
    : DEFAULT_LONG_LIVED_BRANCHES;
  const rules = [];
  for (const entry of list) {
    // A silently-dropped or whitespace-padded entry means the exclusion the
    // operator configured never fires and the refile loop continues — log
    // the malformed shape, and trim before compiling (git ref names never
    // carry surrounding whitespace, so an untrimmed exact-match can't hit).
    if (typeof entry !== 'string' || !entry.trim()) {
      logError(`ignoring malformed defaults.long_lived_branches entry: ${JSON.stringify(entry)}`);
      continue;
    }
    const name = entry.trim();
    if (!name.includes('*')) {
      rules.push((b) => b === name);
    } else {
      const re = new RegExp(
        '^' + name.split('*').map(s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$'
      );
      rules.push((b) => re.test(b));
    }
  }
  return (branch) => rules.some(rule => rule(branch));
}

// Git ref names a ring may safely interpolate into a shell line. Queue-item
// evidence is stored JSON — a branch name is not trusted just because a past
// tick wrote it. Anything outside this conservative set (or containing '..')
// is skipped loudly and NEVER passed to safeExec or auto-resolved.
const SAFE_REF_NAME = /^[A-Za-z0-9][A-Za-z0-9._\/-]*$/;
function isSafeRefName(name) {
  return typeof name === 'string' && SAFE_REF_NAME.test(name) && !name.includes('..');
}

// The exclusion gates FILING ONLY (here), never the attention surfaces:
// main() pushes every diverged branch into ps.divergedBranches BEFORE calling
// this, and the attention line, git-attention sidecar, summary, and Standing
// Issues all render from ps.divergedBranches — an excluded branch stays
// visible everywhere a human looks; it just never becomes an inbox item to
// dismiss. Gating any earlier (at the ps push) would silently blind all four
// surfaces (the coupling at the main() detection loop).
function createBranchDivergedItem(projectName, projectPath, branch, isExcluded) {
  try {
    if (isExcluded && isExcluded(branch)) return;

    // Check for existing branch-diverged item for this branch (dedup)
    const existingItems = listPending({ category: 'branch-diverged' });
    const isDuplicate = existingItems.some(item =>
      item.evidence &&
      item.evidence.branch === branch &&
      item.project_path === projectPath
    );

    if (isDuplicate) return;

    createItem({
      project: projectName,
      project_path: projectPath,
      filed_by: 'ring1',
      category: 'branch-diverged',
      urgency: 'normal',
      title: `Branch "${branch}" diverged from main`,
      summary: `Branch "${branch}" in ${projectName} is ahead of origin/main with no active session on it. Consider merging or cleaning up.`,
      context_anchor: `git log ${branch} --not origin/main in ${projectPath}`,
      evidence: { branch, project_path: projectPath },
    });
  } catch (e) {
    logError(`Failed to create branch-diverged queue item: ${e.message}`);
  }
}

// Auto-resolve pending branch-diverged items whose alarm no longer holds —
// the same one-way-queue cure autoResolveWorktreeItems applies to
// worktree-unmerged (dedup suppresses refiling but nothing retracts the
// original; stale alarms train the operator to ignore real ones — 35 of 39
// pending items were stale in the 2026-07-12 drain). Retraction conditions:
//   - branch verified ABSENT from a successful for-each-ref listing
//     (git.branches; a null listing = command failure = skip, never resolve)
//   - branch carries no unmerged content vs origin/<main> (squash-aware
//     hasUnmergedContent, which fails toward flagging on any git error)
//   - branch is on the long-lived exclusion list (retract-if-pending half of
//     the filing exclusion above)
// No fetch here: `git` state (compareRef, branches) comes from this tick's
// collectGitState, which already fetched — a fourth per-project fetch per
// tick would push a network-partitioned tick past the cron interval.
function autoResolveBranchDivergedItems(projectPath, git, isExcluded, pendingItems) {
  const mine = (pendingItems || []).filter(i => i.project_path === projectPath);
  if (mine.length === 0) return;
  if (!git || !git.compareRef) return; // no git state this tick — leave items alone

  const exec = (cmd) => safeExec(cmd, { cwd: projectPath });

  for (const item of mine) {
    const ev = item.evidence || {};
    if (!ev.branch) continue;
    if (!isSafeRefName(ev.branch)) {
      logError(`branch-diverged reconciler: item ${item.id} carries unsafe branch name ${JSON.stringify(ev.branch)} — skipping, not resolving`);
      continue;
    }

    let resolution = null;
    let staleReason = null;
    if (isExcluded && isExcluded(ev.branch)) {
      resolution = 'excluded-by-design';
      staleReason = `branch "${ev.branch}" is on the long-lived branch exclusion list (defaults.long_lived_branches)`;
    } else if (Array.isArray(git.branches) && !git.branches.includes(ev.branch)) {
      resolution = 'branch-gone';
      staleReason = `branch "${ev.branch}" no longer exists (verified against the branch listing)`;
    } else if (Array.isArray(git.branches) && !hasUnmergedContent(exec, ev.branch, git.compareRef)) {
      resolution = 'merged';
      staleReason = `branch "${ev.branch}" has no unmerged content vs ${git.compareRef} (squash/merge-aware)`;
    }
    // branches not a verified array (null listing failure, or absent field):
    // fall through — only a successful listing is absence evidence.

    if (!staleReason) continue;

    try {
      const res = autoReconcileItem(item.id, {
        resolution,
        notes: `Auto-resolved by Ring 1: ${staleReason}.`,
        actor: 'ring1',
        evidence: { branch: ev.branch },
      });
      if (res) log(`Auto-resolved stale branch-diverged item ${item.id} (${ev.branch}: ${resolution})`);
    } catch (e) {
      logError(`Failed to auto-resolve branch-diverged item ${item.id}: ${e.message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Completion-review reconciliation — retract items whose action closed
// ---------------------------------------------------------------------------
//
// A completion-review item filed while its pib-db action was still open goes
// permanently stale the moment normal work closes the action — nothing
// re-checked (28 of 66 pending items in the 2026-07-12 drain, ~42% of the
// pile; act:9eebbac4). Each tick, every pending item's referenced action is
// looked up in ITS OWN project's pib.db (resolved from the watchtower config
// entry — never the item's stored path, which the attribution bug filed
// wrongly) and the item auto-resolves only when the action is verifiably
// closed.
//
// FAIL-TOWARD-KEEPING, like every reconciler here:
//   - project not in config, db missing/unreadable/ABI-broken → skip loudly,
//     never resolve (one log line per project per tick, matching
//     collectPibState's error discipline)
//   - fid row NOT FOUND → keep pending. This is load-bearing for flow, whose
//     pib.db is readable but VESTIGIAL (its real tracker is flow.db,
//     act:dad1e3f3) — "not found → retract" would mass-resolve flow's items
//     against the wrong database. Misattributed items (act:29001b07's
//     disease) are equally protected: their fid lives in a different
//     project's db, so the wrong-project lookup misses and keeps them.
//   - fid row soft-deleted → keep pending (deletion is not completion; a
//     human disposes the review).
//
// Foreign dbs are opened READ-ONLY behind an existsSync guard — reconciler
// writes land only in the watchtower queue, never in a project's pib.db.
// Cross-project db access deliberately reuses ring1's own join(projectPath,
// 'pib.db') pattern: the runtime tier ships only watchtower-* scripts, so
// importing the repo's pib-db-path resolver here would die with
// ERR_MODULE_NOT_FOUND under cron while passing every repo-side test.
function autoReconcileCompletionReviews({ items, config, openDb }) {
  // raced: the item left pending state between the tick's snapshot and this
  // pass (a human resolved it) — nothing to do, but the counters must still
  // sum to the number of items processed.
  const summary = { resolved: 0, kept: 0, skipped: 0, raced: 0 };
  const pending = (items || []).filter(i => i.category === 'completion-review');
  if (pending.length === 0) return summary;

  const projects = config?.projects || {};
  const byProject = new Map();
  for (const item of pending) {
    const list = byProject.get(item.project) || [];
    list.push(item);
    byProject.set(item.project, list);
  }

  const defaultOpenDb = (projectPath) => {
    const dbPath = join(projectPath, 'pib.db');
    if (!existsSync(dbPath)) return { error: 'no pib.db' };
    const Database = loadBetterSqlite3(projectPath);
    if (!Database) return { error: 'better-sqlite3 not available' };
    try {
      return { db: new Database(dbPath, { readonly: true }) };
    } catch (e) {
      return { error: `cannot open pib.db: ${String(e.message).split('\n')[0]}` };
    }
  };
  const open = openDb || defaultOpenDb;

  for (const [projectName, projectItems] of byProject) {
    if (!projectName || typeof projectName !== 'string') {
      // Malformed items (no project field) can never be reconciled here and
      // have no mechanical disposal path — one bounded line per tick, and
      // fail toward keeping.
      logError(`completion-review reconciler: ${projectItems.length} item(s) missing a project field (${projectItems.map(i => i.id).join(', ')}) — keeping pending`);
      summary.skipped += projectItems.length;
      continue;
    }
    const entry = projects[projectName];
    const projectPath = entry && (entry.path || entry);
    if (!projectPath || typeof projectPath !== 'string') {
      logError(`completion-review reconciler: project "${projectName}" not in watchtower config — keeping ${projectItems.length} item(s) pending`);
      summary.skipped += projectItems.length;
      continue;
    }

    const opened = open(projectPath);
    if (!opened || !opened.db) {
      logError(`completion-review reconciler: ${projectName}: ${opened?.error || 'db open failed'} — keeping ${projectItems.length} item(s) pending`);
      summary.skipped += projectItems.length;
      continue;
    }

    const { db } = opened;
    try {
      let stmt;
      try {
        stmt = db.prepare('SELECT status, completed, deleted_at FROM actions WHERE fid = ?');
      } catch (e) {
        logError(`completion-review reconciler: ${projectName}: cannot query actions table (${String(e.message).split('\n')[0]}) — keeping ${projectItems.length} item(s) pending`);
        summary.skipped += projectItems.length;
        continue;
      }

      let kept = 0;
      let resolvedHere = 0;
      for (const item of projectItems) {
        // Newer items carry the promoted top-level plan_fid; pre-promotion
        // items only carry evidence.fid — checking one shape would silently
        // exempt the other half of the pile.
        const fid = item.plan_fid || item.evidence?.fid;
        if (!fid) { kept++; summary.kept++; continue; }

        let row;
        try {
          row = stmt.get(fid);
        } catch (e) {
          logError(`completion-review reconciler: ${projectName}: lookup failed for ${fid} (${String(e.message).split('\n')[0]}) — keeping item`);
          kept++; summary.kept++;
          continue;
        }
        const isClosed = row && !row.deleted_at
          && (row.status === 'done' || row.completed === 1);
        if (!isClosed) { kept++; summary.kept++; continue; }

        try {
          const res = autoReconcileItem(item.id, {
            resolution: 'action-closed',
            notes: `Auto-resolved by Ring 1: action ${fid} is already closed in ${projectName}'s work tracker — completion confirmed by normal work.`,
            actor: 'ring1',
            evidence: { action_fid: fid },
          });
          if (res) {
            summary.resolved++;
            resolvedHere++;
            log(`Auto-resolved stale completion-review item ${item.id} (${fid} closed)`);
          } else {
            summary.raced++; // no longer pending — a human got there first
          }
        } catch (e) {
          logError(`completion-review reconciler: failed to auto-resolve ${item.id}: ${e.message}`);
          kept++; summary.kept++;
        }
      }
      // Throttled on THIS project's own activity (not global state, which
      // would make the line iteration-order-dependent): kept-detail logs
      // only on ticks where this project's reconciliation did something.
      if (kept > 0 && resolvedHere > 0) {
        log(`completion-review reconciler: ${projectName}: ${kept} item(s) kept pending (action open, missing, or deleted)`);
      }
    } finally {
      try { db.close(); } catch { /* readonly close failure is inert */ }
    }
  }

  return summary;
}

// ---------------------------------------------------------------------------
// Worktree scan — find orphaned worktrees with unmerged work
// ---------------------------------------------------------------------------

// Count uncommitted changes in a worktree, excluding CC/mux session
// artifacts (.claude/, .mcp.json), node_modules, and package-manager
// lockfiles — all of which churn in mux worktrees without representing
// work to lose. node_modules is untracked in every worktree; lockfiles
// regenerate with worktree-relative `file:` dependency paths (a worktree
// sits deeper than the main repo, so any install rewrites resolved tarball
// paths, e.g. ../../ → ../../../../). Both produced false "unmerged work"
// alarms for fully-merged branches. A genuine dependency change also
// dirties package.json, which is still counted, so real work still alarms.
// safeExec trims output, which can shift porcelain column offsets —
// match artifact patterns anywhere in the line instead.
function countRealUncommitted(wtPath) {
  const uncommitted = safeExec('git status --porcelain', { cwd: wtPath });
  if (!uncommitted) return 0;
  // Authored .claude/ subtrees (plans, methodology, rules, …) are real work,
  // not disposable mux infra — re-included per the canonical exclusion
  // contract (act:e91fdfcf, claudeChurnIsDisposable in watchtower-lib).
  const authoredDirs = authoredClaudeDirs(wtPath, safeExec);
  return uncommitted.split('\n').filter(l => {
    if (!l.trim()) return false;
    // Generated-state exclusions (untracked verification/advisories/
    // checklist/verify-progress residue) live in the lib's
    // GENERATED_STATE_PATTERNS — the SSOT claudeChurnIsDisposable now checks
    // FIRST, so ring3's Phase 2a filter inherits the same names with zero
    // edits (act:c008862c; never fork the list). Tracked files of those
    // names still count: the patterns are anchored to `??` lines.
    if (claudeChurnIsDisposable(l, authoredDirs)) return false;
    if (/\s\.mcp\.json$/.test(l)) return false;
    if (/\snode_modules$/.test(l) || /\snode_modules\//.test(l)) return false;
    if (/(?:^|[\s/])package-lock\.json$/.test(l)) return false;
    if (/(?:^|[\s/])npm-shrinkwrap\.json$/.test(l)) return false;
    if (/(?:^|[\s/])yarn\.lock$/.test(l)) return false;
    if (/(?:^|[\s/])pnpm-lock\.yaml$/.test(l)) return false;
    if (/(?:^|[\s/])bun\.lockb$/.test(l)) return false;
    if (isMainShadow(l, wtPath)) return false;
    return true;
  }).length;
}

// A worktree whose HEAD predates a file committed to main shows that file as
// untracked ("?? path") even though main already owns it — a stale-worktree
// shadow, not work to lose. (The mux/devex false-alarm: scripts/skill-usage.mjs
// was added to main after devex branched, so it counted as a "real" uncommitted
// change and blocked the merged branch from ever auto-resolving.) Exclude an
// untracked file only when main holds the SAME path with byte-identical content
// — a genuinely new untracked file, or a modified shadow, still counts, so real
// work-to-lose still alarms.
function isMainShadow(porcelainLine, wtPath) {
  const m = porcelainLine.match(/^\?\?\s+(.+)$/);
  if (!m) return false;
  let p = m[1].trim();
  if (p.startsWith('"') && p.endsWith('"')) p = p.slice(1, -1);
  // Untracked FILENAMES are repo-author-controlled data — passed as
  // execFile args (no shell), never interpolated: double quotes don't stop
  // $()/backtick expansion, and porcelain doesn't C-quote those characters.
  const mainBlob = safeExecFile('git', ['rev-parse', '--verify', '--quiet', `main:${p}`], { cwd: wtPath });
  if (!mainBlob) return false;
  const wtBlob = safeExecFile('git', ['hash-object', '--', p], { cwd: wtPath });
  return !!wtBlob && wtBlob === mainBlob;
}

// Does a worktree-unmerged item belong to this project? Can't trust
// item.project_path alone — Ring 3 historically attributed items to the
// worktree path rather than the project root. Resolve ownership through
// git itself: a worktree's --git-common-dir points at the main repo.
function itemBelongsToProject(item, projectPath) {
  if (item.project_path === projectPath) return true;
  const ev = item.evidence || {};
  if (ev.worktree_path && existsSync(ev.worktree_path)) {
    const commonDir = safeExec(
      'git rev-parse --path-format=absolute --git-common-dir',
      { cwd: ev.worktree_path }
    );
    return !!commonDir && commonDir === join(projectPath, '.git');
  }
  // Worktree gone — claim only if the branch lives in this repo
  return !!(ev.branch && safeExec(`git rev-parse --verify ${ev.branch}`, { cwd: projectPath }));
}

// Auto-resolve pending worktree-unmerged items whose branch is now clean.
// Mechanical truth-checking, not judgment: if the branch has 0 commits
// ahead of origin/<main> and no real uncommitted changes, the alarm is
// stale — and it will never self-heal otherwise, because dedup suppresses
// refiling but nothing retracts the original. Stale alarms train the
// operator to ignore real ones.
function autoResolveWorktreeItems(projectName, projectPath) {
  let pending;
  try {
    pending = listPending({ category: 'worktree-unmerged' });
  } catch {
    return;
  }

  const exec = (cmd) => safeExec(cmd, { cwd: projectPath });
  const { compareRef } = resolveMainRef(exec);
  // Verified branch listing for the gone-branch test (fail-toward-keeping,
  // same discipline as autoResolveBranchDivergedItems): safeExec collapses
  // "ref absent" and "git errored" into one null, so a per-ref probe reads
  // any transient git failure as "branch gone" and auto-closes an item over
  // real work. Absence is only ever concluded from membership in a
  // SUCCESSFUL listing; a failed listing retracts nothing via absence.
  const listing = safeExec(`git for-each-ref --format='%(refname:short)' refs/heads/`, { cwd: projectPath });
  const localBranches = listing === null
    ? null
    : listing.split('\n').map(l => l.trim().replace(/^\* /, '')).filter(Boolean);

  for (const item of pending) {
    const ev = item.evidence || {};
    // Stored queue JSON is not a trusted shell input (the same
    // isSafeRefName gate as the branch-diverged reconciler) — checked
    // BEFORE itemBelongsToProject, which interpolates ev.branch.
    if (ev.branch && !isSafeRefName(ev.branch)) {
      logError(`worktree reconciler: item ${item.id} carries unsafe branch name ${JSON.stringify(ev.branch)} — skipping, not resolving`);
      continue;
    }
    if (!itemBelongsToProject(item, projectPath)) continue;
    if (!ev.branch) continue;

    let staleReason = null;
    if (Array.isArray(localBranches) && !localBranches.includes(ev.branch)) {
      // Branch is verified gone — merged or deliberately deleted;
      // either way there is nothing left to lose.
      staleReason = `branch "${ev.branch}" no longer exists (verified against the branch listing)`;
    } else if (Array.isArray(localBranches)) {
      // Content-based, squash-aware retraction: an item whose branch carries no
      // unmerged content vs origin/<main> and no real uncommitted work is a
      // stale alarm — this is what finally retracts the ~40 squash-merged
      // false positives that the old ahead===0 test never cleared
      // (act:a152cf6c).
      const unmerged = hasUnmergedContent(exec, ev.branch, compareRef);
      const uncommittedCount = (ev.worktree_path && existsSync(ev.worktree_path))
        ? countRealUncommitted(ev.worktree_path)
        : 0;
      if (!unmerged && uncommittedCount === 0) {
        staleReason = `no unmerged content vs ${compareRef} (squash/merge-aware), no uncommitted changes (session artifacts excluded)`;
      }
    }

    if (!staleReason) continue;

    try {
      // Routed through autoReconcileItem (watchtower-lib) like every ring
      // retraction, so machine resolutions carry the typed
      // 'auto-reconciled' + evidence.actor stamp — an untyped resolution
      // here would count cron activity as operator engagement in the
      // cross-ring reader's disposition mix.
      const res = autoReconcileItem(item.id, {
        resolution: 'merged',
        notes: `Auto-resolved by Ring 1: ${staleReason}.`,
        actor: 'ring1',
        evidence: { branch: ev.branch },
      });
      if (res) log(`Auto-resolved stale worktree-unmerged item ${item.id} (${ev.branch})`);
    } catch (e) {
      logError(`Failed to auto-resolve ${item.id}: ${e.message}`);
    }
  }
}

function scanWorktrees(projectName, projectPath) {
  if (!existsSync(join(projectPath, '.git'))) return [];

  // Retract stale alarms before looking for new ones
  autoResolveWorktreeItems(projectName, projectPath);

  const wtList = safeExec('git worktree list --porcelain', { cwd: projectPath });
  if (!wtList) return [];

  const worktrees = [];
  let current = {};
  for (const line of wtList.split('\n')) {
    if (line.startsWith('worktree ')) {
      if (current.path) worktrees.push(current);
      current = { path: line.slice(9) };
    } else if (line.startsWith('branch ')) {
      current.branch = line.slice(7).replace('refs/heads/', '');
    } else if (line === 'bare') {
      current.bare = true;
    } else if (line.trim() === '') {
      if (current.path) worktrees.push(current);
      current = {};
    }
  }
  if (current.path) worktrees.push(current);

  // Filter to mux worktrees (under ~/.mux/worktrees/) that aren't the main worktree
  const muxDir = join(homedir(), '.mux', 'worktrees');
  const orphaned = [];

  // Resolve the comparison ref once for this project — origin/<main>, never
  // the stale local main (act:6f36cbe2).
  const exec = (cmd) => safeExec(cmd, { cwd: projectPath });
  const { compareRef, hasRemote } = resolveMainRef(exec);
  if (!hasRemote && !safeExec(`git rev-parse --verify --quiet ${compareRef}`, { cwd: projectPath })) {
    return []; // No comparison ref at all — nothing to measure against.
  }

  for (const wt of worktrees) {
    if (wt.bare || !wt.path.startsWith(muxDir)) continue;
    if (wt.path === projectPath) continue;
    // Detached / branch-less worktree — no branch to content-compare. Skip
    // rather than let the merge-tree call fail toward flagging an
    // "undefined"-branch worktree (act:a152cf6c).
    if (!wt.branch) continue;
    // worktree-list output is git-controlled but the NAME is repo-author
    // data — same interpolation gate as every other branch-name path.
    if (!isSafeRefName(wt.branch)) {
      logError(`scanWorktrees ${projectPath}: skipping unsafe branch name ${JSON.stringify(wt.branch)}`);
      continue;
    }

    // Flag on CONTENT, not ahead-count: a squash-merged branch is a
    // non-ancestor with phantom "ahead" commits but no real unmerged content
    // (act:a152cf6c). aheadCount is retained only for the human-readable
    // "N unmerged commit(s)" detail line, which is shown only when we flag.
    const unmerged = hasUnmergedContent(exec, wt.branch, compareRef);
    const ahead = aheadCount(exec, wt.branch, compareRef);

    // Also check for uncommitted changes in the worktree
    // (artifact exclusions live in countRealUncommitted)
    const uncommittedCount = countRealUncommitted(wt.path);

    if (!unmerged && uncommittedCount === 0) continue;

    // Check if there's an active tmux window for this worktree
    const windowName = basename(wt.path).replace(/^[^-]+-/, '');
    const tmuxWindows = safeExec('tmux list-windows -a -F "#{window_name}" 2>/dev/null');
    const hasWindow = tmuxWindows && tmuxWindows.split('\n').some(w => w.trim() === windowName);

    if (hasWindow) continue; // Active window exists, not orphaned

    orphaned.push({
      path: wt.path,
      branch: wt.branch,
      ahead,
      uncommitted: uncommittedCount,
      // Drives the wording split at every render site: real unmerged
      // commits earn the data-loss register ("MERGE OR LOSE"); a fully
      // merged branch with only authored uncommitted files is a review
      // nudge, never a data-loss alarm (act:c008862c).
      unmerged,
    });
  }

  // Create inbox items for orphaned worktrees
  for (const wt of orphaned) {
    fileOrphanedWorktreeItem(projectName, projectPath, wt);
  }

  return orphaned;
}

// Files (or re-registers) the inbox item for one orphaned worktree.
// Exported for hermetic tests — the register logic below is not reachable
// through scanWorktrees without a live tmux server.
//
// The dedup is REGISTER-AWARE (CP2 finding): before the urgency/wording
// split both worktree states produced identical items, so branch+path dedup
// was lossless; with the split, a pending item filed under the OTHER
// register would mask a real state transition — an escalation (fully-merged
// worktree later gains real unmerged commits, but the queue keeps the soft
// "review or commit" item) or a stale data-loss alarm (branch merged,
// authored files remain: the reconciler can't retract while uncommitted>0,
// and dedup blocked the correct 'normal' refile). On a register change the
// stale item is auto-resolved (typed, machine-stamped) and a fresh item
// files under the current register. Items filed before the split carry no
// evidence.unmerged — treated as the ALARMING register, matching the
// render-site fail-toward-alarm default.
function fileOrphanedWorktreeItem(projectName, projectPath, wt) {
  try {
    const fresh = wt.unmerged !== false;
    const existing = listPending({ category: 'worktree-unmerged' });
    const match = existing.find(item =>
      item.evidence?.branch === wt.branch &&
      item.evidence?.worktree_path === wt.path
    );
    if (match) {
      const filedRegister = match.evidence?.unmerged !== false;
      if (filedRegister === fresh) return; // duplicate, same register
      // Escalation damping (CP3): hasUnmergedContent fails toward TRUE on
      // transient git errors, so a soft→urgent flip is trusted only when
      // corroborated by real commits ahead (a genuine escalation has them);
      // an uncorroborated flip keeps the existing soft item — the attention
      // surfaces still render the alarming register from the fresh scan, so
      // nothing is hidden, and the item stops flapping (supersede+refile
      // twice per git blip). De-escalation needs no damping: git errors
      // cannot produce unmerged=false.
      if (fresh && !filedRegister && !(wt.ahead > 0)) return;
      try {
        autoReconcileItem(match.id, {
          resolution: 'register-changed',
          notes: `Auto-resolved by Ring 1: worktree "${wt.branch}" changed state (${filedRegister ? 'unmerged work' : 'uncommitted files only'} → ${fresh ? 'unmerged work' : 'uncommitted files only'}) — refiled under the current register.`,
          actor: 'ring1',
          evidence: { branch: wt.branch },
        });
      } catch (e) {
        // Keep the old item rather than risk double-filing.
        logError(`Failed to supersede worktree item ${match.id} on register change: ${e.message}`);
        return;
      }
    }

    const detail = [];
    if (wt.ahead > 0) detail.push(`${wt.ahead} unmerged commit(s)`);
    if (wt.uncommitted > 0) detail.push(`${wt.uncommitted} uncommitted change(s)`);

    createItem({
      project: projectName,
      project_path: projectPath,
      filed_by: 'ring1',
      category: 'worktree-unmerged',
      // Data-loss urgency is earned by unmerged COMMITS; a fully merged
      // branch with uncommitted files is a normal review nudge.
      urgency: fresh ? 'urgent' : 'normal',
      title: fresh
        ? `Orphaned worktree "${wt.branch}" has unmerged work`
        : `Orphaned worktree "${wt.branch}" has uncommitted files`,
      summary: fresh
        ? `Worktree at ${wt.path} has ${detail.join(' and ')} with no active tmux window. Merge to main or the work may be lost.`
        : `Worktree at ${wt.path} has ${detail.join(' and ')} with no active tmux window. The branch itself is fully merged — review or commit the files.`,
      context_anchor: `git log ${wt.branch} --not origin/main in ${wt.path}`,
      evidence: { branch: wt.branch, worktree_path: wt.path, ahead: wt.ahead, uncommitted: wt.uncommitted, unmerged: fresh },
      options: [
        { key: 'merge', label: 'Merge to main now' },
        { key: 'keep', label: 'Keep branch for later' },
        { key: 'dismiss', label: 'Dismiss (already handled)' },
      ],
    });
  } catch (e) {
    logError(`Failed to create worktree-unmerged item: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Summary assembly (30-line hard cap)
// ---------------------------------------------------------------------------

// Structured, re-verifiable form of the git-derived attention facts (the
// MERGE-OR-LOSE worktree lines and the diverged-branch lines). The build
// context re-checks each entry against live git before relaying the matching
// summary line, so a stale cached banner is dropped/stamped rather than
// asserted as a live fact (act:a136b362). Each fact carries everything needed
// to re-run the same origin/<main> ahead-check at read time.
function buildGitAttentionSidecar(projectStates) {
  const generatedAt = new Date().toISOString();
  const facts = [];
  for (const ps of projectStates) {
    const compareRef = ps.git?.compareRef || 'origin/main';
    if (ps.orphanedWorktrees) {
      for (const wt of ps.orphanedWorktrees) {
        facts.push({
          kind: 'worktree-unmerged',
          project: ps.name,
          project_path: ps.path,
          branch: wt.branch,
          worktree_path: wt.path,
          compare_ref: compareRef,
          // The cached banner — what build-context relays only if re-verified.
          // MERGE OR LOSE is reserved for real unmerged commits; a missing
          // flag (older cached shape) fails toward the alarming register.
          line: wt.unmerged !== false
            ? `⚠ ${ps.name}: worktree "${wt.branch}" has unmerged work — MERGE OR LOSE`
            : `${ps.name}: worktree "${wt.branch}" has uncommitted files (branch fully merged)`,
        });
      }
    }
    if (ps.divergedBranches) {
      for (const b of ps.divergedBranches) {
        facts.push({
          kind: 'diverged-branch',
          project: ps.name,
          project_path: ps.path,
          branch: b,
          compare_ref: compareRef,
          line: `${ps.name}: branch "${b}" diverged from main`,
        });
      }
    }
  }
  return { generated_at: generatedAt, facts };
}

// Runtime-script drift check (act:e81fe82f). The watchtower runtime that
// launchd/cron executes lives at ~/.claude-cabinet/watchtower/; a one-file
// fix is durable only when that runtime matches the CC source templates.
// Surfaces a stale runtime as an ambient attention line so the operator
// doesn't have to run `/watchtower status` to notice it. READ-ONLY from a
// cron context — never auto-heals. Stays SILENT (skipped) whenever there is
// no authoritative source to compare against (no CC repo in cc-registry, no
// templates/tracked tier) — a false "drift" nag would be the worse failure,
// exactly the cry-wolf decay act:a136b362 just removed.
function checkRuntimeScriptDrift(opts = {}) {
  const registryPath = opts.registryPath
    || join(homedir(), '.claude', 'cc-registry.json');
  const runtimeDir = opts.runtimeDir || WATCHTOWER_DIR;
  try {
    const ccRepo = resolveCcSourceRepo(registryPath);
    if (!ccRepo) return { skipped: true, reason: 'no-cc-source' };
    const roots = resolveRoots({ cwd: ccRepo, runtime: runtimeDir });
    // No authoritative tier (no templates/ and no tracked scripts/) or no
    // runtime on disk → nothing meaningful to diff. Stay silent.
    if (!roots.template && !roots.tracked) return { skipped: true, reason: 'no-source-tier' };
    if (!roots.runtime) return { skipped: true, reason: 'no-runtime' };
    const analysis = analyze(roots);
    // Count only files whose RUNTIME copy needs a heal (drift/missing) plus
    // runtime-resident orphans — the precise "live runtime is stale" signal.
    let driftCount = 0;
    let orphanCount = 0;
    for (const f of analysis.files) {
      if (f.healTargets?.some((h) => h.tier === 'runtime')) driftCount++;
      else if (f.status === 'orphan' && f.tiers?.runtime?.present) orphanCount++;
    }
    return { skipped: false, driftCount, orphanCount, total: analysis.summary.total };
  } catch (e) {
    return { skipped: true, reason: `error: ${e.message}` };
  }
}

// Build the global runtime-drift attention line, or null when in sync/skipped.
function runtimeDriftAttentionLine(drift) {
  if (!drift || drift.skipped) return null;
  if (!drift.driftCount && !drift.orphanCount) return null;
  const parts = [];
  if (drift.driftCount) parts.push(`${drift.driftCount} script(s) differ from the CC source templates`);
  if (drift.orphanCount) parts.push(`${drift.orphanCount} runtime orphan(s)`);
  return `⚠ watchtower runtime drift: ${parts.join(' + ')} — run \`/watchtower sync\``;
}

function assembleSummary(projectStates, config, extraAttention = []) {
  const now = new Date().toISOString();
  const lines = [];

  lines.push(`# Watchtower — ${now}`);
  lines.push('');

  // --- Where You Left Off ---
  lines.push('## Where You Left Off');
  let mostRecent = null;
  let mostRecentTime = 0;
  for (const ps of projectStates) {
    if (ps.git && ps.git.lastCommit) {
      const commitTime = new Date(ps.git.lastCommit.timestamp).getTime();
      if (commitTime > mostRecentTime) {
        mostRecentTime = commitTime;
        mostRecent = ps;
      }
    }
    // Also check active sessions
    if (ps.activeSessions && ps.activeSessions.length > 0) {
      for (const s of ps.activeSessions) {
        const t = new Date(s.lastModified).getTime();
        if (t > mostRecentTime) {
          mostRecentTime = t;
          mostRecent = ps;
        }
      }
    }
  }
  if (mostRecent) {
    const desc = mostRecent.git?.lastCommit?.message || 'unknown activity';
    lines.push(`${mostRecent.name}: ${desc} (${ago(Date.now() - mostRecentTime)})`);
  } else {
    lines.push('No recent activity detected.');
  }
  lines.push('');

  // --- What Needs Attention ---
  lines.push('## What Needs Attention');
  const attention = [...extraAttention];
  for (const ps of projectStates) {
    if (ps.pib && ps.pib.flaggedCount > 0) {
      attention.push(`${ps.name}: ${ps.pib.flaggedCount} flagged action(s)`);
    }
    if (ps.pib && ps.pib.overdueCount > 0) {
      attention.push(`${ps.name}: ${ps.pib.overdueCount} overdue action(s)`);
    }
    if (ps.pib && ps.pib.staleProjects && ps.pib.staleProjects.length > 0) {
      for (const sp of ps.pib.staleProjects) {
        attention.push(`${ps.name}/${sp.name}: stale (no activity in ${STALE_DAYS}d)`);
      }
    }
    if (ps.divergedBranches && ps.divergedBranches.length > 0) {
      for (const b of ps.divergedBranches) {
        attention.push(`${ps.name}: branch "${b}" diverged from main`);
      }
    }
    if (ps.git && ps.git.localLagsRemote) {
      attention.push(`${ps.name}: local ${ps.git.mainBranch} lags origin/${ps.git.mainBranch} — run \`git fetch && git merge\` (comparisons use origin/${ps.git.mainBranch})`);
    }
    if (ps.pib && ps.pib.completionCandidates && ps.pib.completionCandidates.length > 0) {
      for (const c of ps.pib.completionCandidates) {
        attention.push(`${ps.name}/${c.name}: all actions done — close?`);
      }
    }
    if (ps.orphanedWorktrees && ps.orphanedWorktrees.length > 0) {
      for (const wt of ps.orphanedWorktrees) {
        attention.unshift(wt.unmerged !== false
          ? `⚠ ${ps.name}: worktree "${wt.branch}" has unmerged work — MERGE OR LOSE`
          : `${ps.name}: worktree "${wt.branch}" has uncommitted files (branch fully merged)`);
      }
    }
    if (ps.ccFeedbackArrival) {
      const { feedbackCount, proposalCount } = ps.ccFeedbackArrival;
      const parts = [];
      if (feedbackCount > 0) parts.push(`${feedbackCount} untriaged feedback file(s) in feedback/ root`);
      if (proposalCount > 0) parts.push(`${proposalCount} pending extraction proposal(s) in proposals/`);
      attention.unshift(`⚠ ${ps.name}: ${parts.join(' + ')} — TRIAGE AT ARRIVAL (file to pib-db or decline, then stamp + move to feedback/resolved/)`);
    }
    if (ps.memoryIntegrity) {
      const mi = ps.memoryIntegrity;
      if (mi.orphans.length > 0) {
        attention.push(`${ps.name}: ${mi.orphans.length} orphaned memory file(s) — not reachable from MEMORY.md (no index line or region pointer)`);
      }
      if (mi.missing.length > 0) {
        attention.push(`${ps.name}: ${mi.missing.length} broken memory reference(s) — indexed but file missing`);
      }
    }
  }
  // pib collection errors aggregate to ONE line: they are almost always a
  // single system-level fault (loader/ABI breakage hits every project at
  // once), and N identical per-project lines would crowd the attention cap.
  const pibErrored = projectStates.filter(p => p.pib && p.pib.error);
  if (pibErrored.length > 0) {
    attention.unshift(`⚠ pib-db collection failing for ${pibErrored.length} project(s) — ${pibErrored[0].pib.error} (see per-project state files)`);
  }
  if (attention.length === 0) {
    lines.push('Nothing urgent.');
  } else {
    // Cap at 8 items (urgent first via natural ordering, then newest)
    const maxAttention = 8;
    const shown = attention.slice(0, maxAttention);
    for (const a of shown) {
      lines.push(`- ${a}`);
    }
    if (attention.length > maxAttention) {
      lines.push(`... and ${attention.length - maxAttention} more`);
    }
  }
  lines.push('');

  // --- Portfolio Pulse ---
  lines.push('## Portfolio Pulse');
  const pulseLines = [];
  for (const ps of projectStates) {
    const openCount = ps.pib?.openActions ?? '?';
    const hasActive = ps.activeSessions && ps.activeSessions.length > 0;
    const isBlocked = ps.pib?.projectBreakdown?.some(p => p.blocked_count > 0);
    const status = hasActive ? 'hot' : (isBlocked ? 'blocked' : 'quiet');
    pulseLines.push(`- ${ps.name}: ${openCount} open, ${status}`);
  }
  // Sort: hot first, then blocked, then quiet
  const order = { hot: 0, blocked: 1, quiet: 2 };
  pulseLines.sort((a, b) => {
    const sa = a.includes('hot') ? 0 : a.includes('blocked') ? 1 : 2;
    const sb = b.includes('hot') ? 0 : b.includes('blocked') ? 1 : 2;
    return sa - sb;
  });
  for (const pl of pulseLines) lines.push(pl);
  lines.push('');

  // --- Inbox ---
  lines.push('## Inbox');
  const pendingItems = listPending();
  if (pendingItems.length === 0) {
    lines.push('No pending items.');
  } else {
    const oldest = pendingItems.reduce((a, b) =>
      new Date(a.filed_at) < new Date(b.filed_at) ? a : b
    );
    lines.push(`${pendingItems.length} pending (oldest: ${ago(Date.now() - new Date(oldest.filed_at).getTime())})`);
  }
  lines.push('');

  // --- Health ---
  lines.push('## Health');
  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring1-health.json');
  if (existsSync(healthPath)) {
    try {
      const health = JSON.parse(readFileSync(healthPath, 'utf8'));
      const lastRun = health.last_run ? ago(Date.now() - new Date(health.last_run).getTime()) : 'never';
      lines.push(`Ring 1: last ran ${lastRun}, ${health.status || 'unknown'}`);
    } catch {
      lines.push('Ring 1: health file corrupt');
    }
  } else {
    lines.push('Ring 1: first run');
  }
  lines.push('');
  lines.push('Deep files: ~/.claude-cabinet/watchtower/state/projects/');

  // --- Truncation to 30 lines ---
  if (lines.length > 30) {
    // Truncation order: Health detail first, then quiet Portfolio projects
    // Find Health section and reduce
    const healthIdx = lines.findIndex(l => l === '## Health');
    if (healthIdx >= 0) {
      // Replace health content with one-line summary
      const nextSection = lines.findIndex((l, i) => i > healthIdx && l.startsWith('## '));
      const end = nextSection > 0 ? nextSection : lines.length;
      const healthContent = lines.slice(healthIdx + 1, end).filter(l => l.trim()).join('; ');
      lines.splice(healthIdx + 1, end - healthIdx - 1, healthContent || 'OK');
    }

    // If still too long, remove quiet projects from Portfolio Pulse
    if (lines.length > 30) {
      const pulseIdx = lines.findIndex(l => l === '## Portfolio Pulse');
      if (pulseIdx >= 0) {
        const nextSection = lines.findIndex((l, i) => i > pulseIdx && l.startsWith('## '));
        const end = nextSection > 0 ? nextSection : lines.length;
        const kept = [];
        for (let i = pulseIdx + 1; i < end; i++) {
          if (!lines[i].includes('quiet')) kept.push(lines[i]);
        }
        lines.splice(pulseIdx + 1, end - pulseIdx - 1, ...kept);
      }
    }

    // Final hard truncation
    if (lines.length > 30) {
      lines.length = 29;
      lines.push('(truncated to 30 lines)');
    }
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Per-project state file
// ---------------------------------------------------------------------------

// Cap on how many flagged actions are listed by fid in Standing Issues. Every
// other Standing Issues entry is count-only; an uncapped list would dump every
// flagged row into the file on each 5-min tick. The full count is always shown
// in the header line, so nothing is hidden — only the per-action detail is
// bounded (the same discipline as the inline-capped missed-routine section).
export const FLAGGED_RENDER_CAP = 5;

// Render the flagged-action Standing Issues entry — the count header plus a
// capped sub-list of WHICH actions are flagged (fid + one-line text), at
// parity with orient's work-scan (act:b1b21a15). Pass the FULL flagged list;
// the count is derived from it. Returns null when nothing is flagged so the
// caller can fall back / skip. Pure (no I/O) for hermetic testing.
export function renderFlaggedEntry(flaggedActions, cap = FLAGGED_RENDER_CAP) {
  const list = Array.isArray(flaggedActions) ? flaggedActions : [];
  const count = list.length;
  if (count === 0) return null;
  const out = [`${count} flagged action(s)`];
  for (const a of list.slice(0, cap)) {
    const text = String(a && a.text != null ? a.text : '').replace(/\s+/g, ' ').trim();
    const shown = text.length > 80 ? text.slice(0, 79) + '…' : text;
    const fid = a && a.fid ? a.fid : '(no fid)';
    out.push(`    - ${fid}${shown ? `: ${shown}` : ''}`);
  }
  if (count > cap) out.push(`    - …and ${count - cap} more`);
  return out.join('\n');
}

// readRecallCanary — load the Ring 2 slow over-suppression canary sidecar
// (M5, act:6354a9db). Returns the per-project map (or {} if absent/corrupt).
// Ring 1 renders an alerting project's entry into its Standing Issues so the
// signal reaches /briefing — the canary is Ring 2's, the render is Ring 1's
// (Ring 1 owns the per-project state file).
function readRecallCanary() {
  const path = join(WATCHTOWER_DIR, 'state', 'recall-canary.json');
  if (!existsSync(path)) return {};
  try {
    return JSON.parse(readFileSync(path, 'utf8')).projects || {};
  } catch { return {}; }
}

function assembleProjectState(ps) {
  const now = new Date().toISOString();
  const lines = [];

  lines.push(`# ${ps.name} — ${now}`);
  lines.push('');

  // Active Plans
  lines.push('## Active Plans');
  if (ps.pib && ps.pib.openActions != null) {
    if (ps.pib.openActions > 0) {
      lines.push(`${ps.pib.openActions} open action(s)`);
    } else {
      lines.push('No open actions.');
    }
  } else if (ps.pib && ps.pib.error) {
    // A collection error is not "no data" — the two rendered identically
    // for weeks while an ABI-poisoned better-sqlite3 broke every project's
    // collection (act:b9414039).
    lines.push(`pib-db error: ${ps.pib.error}`);
  } else {
    lines.push('No pib-db data.');
  }
  lines.push('');

  // Last Session
  lines.push('## Last Session');
  if (ps.activeSessions && ps.activeSessions.length > 0) {
    const latest = ps.activeSessions.reduce((a, b) =>
      new Date(a.lastModified) > new Date(b.lastModified) ? a : b
    );
    lines.push(`Active: ${ago(latest.agoMs)}`);
  } else if (ps.git && ps.git.lastCommit) {
    lines.push(`Last commit: ${ps.git.lastCommit.message} (${ps.git.lastCommit.timestamp})`);
  } else {
    lines.push('No session data.');
  }
  lines.push('');

  // Standing Issues
  lines.push('## Standing Issues');
  const issues = [];
  if (ps.pib && ps.pib.flaggedCount > 0) {
    // Surface WHICH actions are flagged (capped), not just the count — orient
    // parity (act:b1b21a15). Falls back to the count line if the list is
    // somehow absent (older state shape).
    issues.push(
      renderFlaggedEntry(ps.pib.flaggedActions) ||
        `${ps.pib.flaggedCount} flagged action(s)`
    );
  }
  if (ps.pib && ps.pib.overdueCount > 0) {
    issues.push(`${ps.pib.overdueCount} overdue action(s)`);
  }
  if (ps.pib && ps.pib.deferredTriggerCount > 0) {
    issues.push(`${ps.pib.deferredTriggerCount} deferred action(s) waiting on triggers`);
  }
  // Stale + completion-candidate counts are the data feed for /briefing's
  // backlog-hygiene nudge (act:5e8a9e89) — surfaced here in the per-project
  // deep file so the nudge reads one source instead of re-querying pib.
  if (ps.pib && ps.pib.staleProjects && ps.pib.staleProjects.length > 0) {
    issues.push(`${ps.pib.staleProjects.length} stale project(s)`);
  }
  if (ps.pib && ps.pib.completionCandidates && ps.pib.completionCandidates.length > 0) {
    issues.push(`${ps.pib.completionCandidates.length} completion candidate(s)`);
  }
  // Non-fatal pib-query warnings — a genuinely-broken query surfaces here
  // instead of being swallowed by a bare catch (data-integrity-0001).
  if (ps.pib && ps.pib.pibWarnings && ps.pib.pibWarnings.length > 0) {
    for (const w of ps.pib.pibWarnings) issues.push(`pib-db query warning: ${w}`);
  }
  // Recall over-suppression canary (M5, act:6354a9db) — render only on alert
  // (the data feed for /briefing's State-file-flags reader). Over-suppression
  // only; the operator eyeballs the sample in recall-canary.json.
  if (ps.recall && ps.recall.alert) {
    const pct = (n) => `${Math.round((n || 0) * 100)}%`;
    issues.push(
      `recall canary: ${ps.recall.suppressed} dedup suppression(s), rate ` +
      `${pct(ps.recall.rate)} vs baseline ${pct(ps.recall.baseline)} — review the ` +
      `sample (over-suppression only; recall-canary.json / see /briefing)`
    );
  }
  if (ps.divergedBranches && ps.divergedBranches.length > 0) {
    issues.push(`Diverged branches: ${ps.divergedBranches.join(', ')}`);
  }
  if (ps.git && ps.git.localLagsRemote) {
    issues.push(`Local ${ps.git.mainBranch} lags origin/${ps.git.mainBranch} (comparisons use origin/${ps.git.mainBranch})`);
  }
  if (issues.length === 0) {
    lines.push('None.');
  } else {
    for (const i of issues) lines.push(`- ${i}`);
  }
  lines.push('');

  // Tech Stack
  lines.push('## Tech Stack');
  const stack = [];
  if (ps.deployment && ps.deployment.length > 0) {
    stack.push(`Deploy: ${ps.deployment.join(', ')}`);
  }
  if (ps.git) {
    stack.push(`Branch: ${ps.git.branch}`);
  }
  if (stack.length === 0) {
    lines.push('Not detected.');
  } else {
    for (const s of stack) lines.push(`- ${s}`);
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
  const startTime = Date.now();
  let status = 'success';
  let errorMessage = null;

  try {
    const config = loadConfig();
    const projects = config.projects || {};
    const projectNames = Object.keys(projects);

    if (projectNames.length === 0) {
      log('no projects configured. Run /watchtower install to add projects.');
      status = 'no-projects';
    }

    // Feedback outbox delivery — global duty, not per-project. Failure
    // must not kill the state-collection pass.
    try {
      const flush = flushFeedbackOutbox();
      if (flush.delivered || flush.skipped) {
        log(`feedback outbox: ${flush.delivered} delivered, ${flush.skipped} already-present → ${flush.destination}`);
      }
      if (flush.status === 'no-destination') {
        logError(`feedback outbox has ${flush.kept} item(s) but the CC repo could not be resolved from cc-registry — items kept`);
      } else if (flush.status === 'malformed-reset') {
        logError('feedback outbox was malformed JSON — reset to []');
      } else if (flush.status === 'partial') {
        logError(`feedback outbox: ${flush.kept} item(s) failed delivery and were kept`);
      }
    } catch (e) {
      logError(`feedback outbox flush failed: ${e.message}`);
    }

    // Routine tick — evaluate declared interactive routines' mechanical
    // triggers (time-of-day / interval / path-nonempty) and dispatch any
    // that fire to their desk's main session (act:c2a55c08). The engine
    // never throws, but belt-and-suspenders: a routine failure must not
    // kill the state-collection pass.
    if (config.defaults?.routine_dispatch !== false) {
      try {
        const pass = runRoutinePass({ config, event: { type: 'tick' }, filedBy: 'ring1' });
        if (pass.fired.length > 0) {
          log(`routines: fired ${pass.fired.map((f) => `${f.key} (${f.status})`).join(', ')}`);
        }
      } catch (e) {
        logError(`routine tick failed: ${e.message}`);
      }
    }

    const projectStates = [];
    // Ring 2 slow writes the recall-canary sidecar; Ring 1 renders an alerting
    // project's entry into its Standing Issues (the canary's named reader).
    const recallCanaryProjects = readRecallCanary();

    // Branch-diverged reconciliation inputs, resolved ONCE per tick: the
    // exclusion matcher (config-driven) and one pending-items snapshot —
    // listPending reads every file in the queue dir, so per-project
    // per-category rescans grow linearly with lifetime item count.
    const branchExcluded = branchExclusionMatcher(config);
    let pendingBranchDiverged = [];
    try {
      pendingBranchDiverged = listPending({ category: 'branch-diverged' });
    } catch (e) {
      logError(`could not list pending branch-diverged items: ${e.message}`);
    }

    for (const name of projectNames) {
      const projectPath = projects[name].path || projects[name];
      if (!existsSync(projectPath)) {
        logError(`project path ${projectPath} not found, skipping ${name}`);
        continue;
      }

      const ps = {
        name,
        path: projectPath,
        git: collectGitState(projectPath),
        pib: collectPibState(projectPath),
        activeSessions: detectActiveSessions(projectPath),
        deployment: detectDeployment(projectPath),
        ccFeedbackArrival: checkCcFeedbackArrival(projectPath),
        memoryIntegrity: checkMemoryIntegrity(projectPath),
        recall: recallCanaryProjects[name] || null,
        divergedBranches: [],
        hookResults: [],
      };

      // Retract stale branch-diverged alarms before detecting new ones
      // (same retract-then-scan order as scanWorktrees). Runs regardless of
      // the detection flag: items filed before an operator disabled
      // detection would otherwise rot pending forever.
      autoResolveBranchDivergedItems(projectPath, ps.git, branchExcluded, pendingBranchDiverged);

      // Branch divergence detection (feature-flagged). The exclusion list
      // gates FILING only (inside createBranchDivergedItem) — every diverged
      // branch, excluded or not, is pushed to ps.divergedBranches, which
      // feeds the attention line, the git-attention sidecar, the summary,
      // and Standing Issues. Excluded branches stay visible; they just never
      // become dismissable inbox noise.
      if (config.defaults?.branch_orphan_detection !== false && ps.git && ps.git.branchesAhead) {
        for (const branch of ps.git.branchesAhead) {
          // Check if there's an active session on this branch
          const hasActiveSession = ps.activeSessions.length > 0 &&
            ps.git.branch === branch;
          if (!hasActiveSession) {
            ps.divergedBranches.push(branch);
            createBranchDivergedItem(name, projectPath, branch, branchExcluded);
          }
        }
      }

      // Worktree scan — find orphaned worktrees with unmerged work
      ps.orphanedWorktrees = scanWorktrees(name, projectPath);

      // Consumer hooks
      const hooks = config.hooks?.['ring1-post-collect'] || [];
      ps.hookResults = runConsumerHooksSync(hooks, ps);

      projectStates.push(ps);
    }

    // Completion-review reconciliation — one global pass per tick (items
    // reference their OWN project's db via the config, so this is not a
    // per-project concern). Failure must not kill the state-collection pass.
    try {
      const pendingReviews = listPending({ category: 'completion-review' });
      const rec = autoReconcileCompletionReviews({ items: pendingReviews, config });
      if (rec.resolved > 0) {
        log(`completion-review reconciler: ${rec.resolved} resolved, ${rec.kept} kept, ${rec.skipped} skipped`);
      }
    } catch (e) {
      logError(`completion-review reconciliation failed: ${e.message}`);
    }

    // Ensure output directories exist
    const stateDir = join(WATCHTOWER_DIR, 'state');
    const projectsDir = join(stateDir, 'projects');
    mkdirSync(projectsDir, { recursive: true });

    // Runtime-script drift — a global (portfolio-level) check that the live
    // runtime matches the CC source templates (act:e81fe82f). Read-only;
    // surfaced as one ambient attention line. Failure must not kill the pass.
    const extraAttention = [];
    if (config.defaults?.script_sync_check !== false) {
      try {
        const drift = checkRuntimeScriptDrift();
        const line = runtimeDriftAttentionLine(drift);
        if (line) extraAttention.push(line);
      } catch (e) {
        logError(`runtime script-drift check failed: ${e.message}`);
      }
    }

    // Write summary.md
    const summary = assembleSummary(projectStates, config, extraAttention);
    atomicWrite(join(stateDir, 'summary.md'), summary);

    // Write the git-attention sidecar — the structured, re-verifiable form of
    // the git-derived attention lines (worktree-unmerged + diverged-branch).
    // Ring 1's summary is a cached snapshot; the SessionStart context builder
    // re-verifies each fact against current git reality before relaying it,
    // so a banner can't assert "MERGE OR LOSE" about a branch that has since
    // merged (act:a136b362). Prose lines are the human surface; this sidecar
    // is the machine-verifiable join key.
    atomicWrite(
      join(stateDir, 'git-attention.json'),
      JSON.stringify(buildGitAttentionSidecar(projectStates), null, 2)
    );

    // Write per-project state files.
    // Section ownership (watchtower-contracts.md §Project State Section
    // Ownership): Ring 3 owns "## Last Session" once it has authored a rich
    // session summary there (marked by its `_<date> (<session-id>)_`
    // attribution line). Ring 1 rebuilds every OTHER section from scratch,
    // but must carry a Ring 3-authored Last Session forward verbatim —
    // otherwise this rebuild deterministically clobbers Ring 3's summary
    // within one cron tick. The rebuild goes through the lib's re-read
    // check-and-retry helper so a Ring 3 write landing mid-merge is
    // re-merged instead of silently dropped.
    for (const ps of projectStates) {
      const slug = slugify(ps.name);
      const statePath = join(projectsDir, `${slug}.md`);
      const projectMd = assembleProjectState(ps);
      try {
        const res = writeProjectStatePreservingRing3(statePath, projectMd);
        if (res.exhausted) {
          log(`state merge for ${slug} exhausted retries — merged against freshest snapshot`);
        }
      } catch (e) {
        // Best-effort: a failed merge/write for one project must not kill
        // the whole Ring 1 pass.
        logError(`could not write state for ${slug}: ${e.message} — writing fresh`);
        atomicWrite(statePath, projectMd);
      }
    }

    log(`collected state for ${projectStates.length} project(s)`);
  } catch (e) {
    status = 'error';
    errorMessage = e.message;
    logError(`${e.message}`);
  }

  // Write ring health
  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring1-health.json');
  mkdirSync(join(WATCHTOWER_DIR, 'state'), { recursive: true });
  const health = {
    schema_version: 1,
    last_run: new Date().toISOString(),
    duration_ms: Date.now() - startTime,
    status,
    error: errorMessage,
  };
  atomicWrite(healthPath, JSON.stringify(health, null, 2));
}

// Exports for hermetic tests (and re-use by the context builder). These are
// pure given an injectable `exec` — the inline call sites bind safeExec to a
// cwd; the tests bind a runner against a temp git repo (act:6f36cbe2,
// act:a136b362).
export { resolveMainRef, aheadCount, isMergedInto, hasUnmergedContent, countRealUncommitted, buildGitAttentionSidecar, checkRuntimeScriptDrift, runtimeDriftAttentionLine, assembleProjectState, assembleSummary, collectPibState, branchExclusionMatcher, DEFAULT_LONG_LIVED_BRANCHES, createBranchDivergedItem, autoResolveBranchDivergedItems, autoResolveWorktreeItems, autoReconcileCompletionReviews, fileOrphanedWorktreeItem, isSafeRefName };

// Entry guard so tests (and other modules) can import this file's pure
// helpers without executing main(). realpathSync matters: node
// realpath-resolves the main module for import.meta.url while argv[1]
// keeps the given path — a symlinked cron invocation would otherwise
// make main() silently never run. Matches watchtower-ring2.mjs. (act:141a1c2b)
const isMain = (() => {
  try { return process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
})();
if (isMain) {
  try {
    main();
  } catch (e) {
    logError(`fatal: ${e.message}`);
    process.exit(1);
  }
}

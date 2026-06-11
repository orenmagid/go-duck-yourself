#!/usr/bin/env node

// Watchtower shared library.
// Canonical implementations of utilities used across all ring scripts.
// Ring scripts import from here instead of maintaining local copies.

import {
  readFileSync, writeFileSync, existsSync,
  mkdirSync, renameSync, realpathSync,
} from 'fs';
import { join, dirname, basename, resolve as resolvePath } from 'path';
import { homedir } from 'os';
import { createRequire } from 'module';
import { execFileSync } from 'child_process';
import { pathToFileURL } from 'url';

const require = createRequire(import.meta.url);

// Re-export queue operations so ring scripts have a single import source.
export { createItem, listPending, getItem, resolveItem } from './watchtower-queue.mjs';

// ---------------------------------------------------------------------------
// getWatchtowerDir — resolve watchtower root directory
// ---------------------------------------------------------------------------

export function getWatchtowerDir() {
  return process.env.WATCHTOWER_DIR
    || join(homedir(), '.claude-cabinet', 'watchtower');
}

// ---------------------------------------------------------------------------
// atomicWrite — mkdir -p dirname, write to .tmp, rename
// ---------------------------------------------------------------------------

export function atomicWrite(filePath, content) {
  const dir = dirname(filePath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const tmp = filePath + '.tmp';
  const data = typeof content === 'string'
    ? content
    : JSON.stringify(content, null, 2);
  writeFileSync(tmp, data);
  renameSync(tmp, filePath);
}

// ---------------------------------------------------------------------------
// loadConfig — read and validate config.json with schema_version check
// ---------------------------------------------------------------------------

export function loadConfig(watchtowerDir) {
  const dir = watchtowerDir || getWatchtowerDir();
  const configPath = join(dir, 'config.json');
  if (!existsSync(configPath)) {
    throw new Error('Watchtower config.json not found. Run /watchtower install first.');
  }
  const config = JSON.parse(readFileSync(configPath, 'utf8'));
  if (config.schema_version !== 1) {
    throw new Error(`Unsupported config schema_version ${config.schema_version} (expected 1)`);
  }
  return config;
}

// ---------------------------------------------------------------------------
// slugify — lowercase, replace non-alphanum with dash, trim dashes
// ---------------------------------------------------------------------------

export function slugify(text) {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

// ---------------------------------------------------------------------------
// Thread cursor history (schema v2)
// ---------------------------------------------------------------------------
// A thread's cursor is no longer a single overwritten object; it is an
// append-only `cursor_history` array of point-in-time snapshots, one per
// session that advanced the thread. Each entry is
//   { date, session_id, cursor: { what, why, where_left_off, open_questions,
//     next_steps } }
// The "current" cursor is always the last entry. Overwriting threw away the
// journey (symptom → diagnosis → solution → abstraction); the history keeps it.
// `cursor_history` is the thread's first sibling timeline — the QA-handoff
// protocol (.claude/plans/qa-handoff-protocol.md) adds a parallel
// point-in-time event sibling rather than nesting inside the cursor.

// migrateThreadCursor — convert a legacy (schema v1) thread object that has a
// single `cursor` field into the v2 `cursor_history` shape, in place.
// Idempotent: a thread already carrying `cursor_history` is left alone (beyond
// stripping any stale `cursor` field and bumping the version). The single
// migrated entry inherits the date/session_id of the thread's most recent
// session, since the legacy cursor reflected the latest understanding.
export function migrateThreadCursor(threadData) {
  if (!threadData || typeof threadData !== 'object') return threadData;
  if (threadData.schema_version === undefined) threadData.schema_version = 1;

  if (Array.isArray(threadData.cursor_history)) {
    delete threadData.cursor; // drop any stale legacy field
    if (threadData.schema_version < 2) threadData.schema_version = 2;
    return threadData;
  }

  const legacy = threadData.cursor;
  const sessions = Array.isArray(threadData.sessions) ? threadData.sessions : [];
  const last = sessions.length ? sessions[sessions.length - 1] : null;
  threadData.cursor_history = legacy
    ? [{
        date: last?.date || (threadData.last_updated || '').slice(0, 10) || null,
        session_id: last?.id || null,
        cursor: legacy,
      }]
    : [];
  delete threadData.cursor;
  threadData.schema_version = 2;
  return threadData;
}

// currentCursor — the most recent cursor snapshot for a thread. Reads the last
// `cursor_history` entry, falling back to a legacy `cursor` field so consumers
// stay correct against any un-migrated thread file. Always returns an object.
export function currentCursor(thread) {
  const hist = thread?.cursor_history;
  if (Array.isArray(hist) && hist.length) return hist[hist.length - 1].cursor || {};
  return thread?.cursor || {};
}

// updateThreadFile — disk-wins thread file update (Ring 3 thread capture).
//
// DISK WINS OVER MODEL: if the thread file exists on disk, this ALWAYS
// appends to it — the model's `is_new` claim is advisory naming metadata
// and is deliberately not consulted here. cursor_history is append-only by
// design; one hallucinated is_new:true must never wipe a thread's history,
// sessions[], or related_fids (watchtower-contracts.md §Project State
// Section Ownership / Thread File Durability).
//
// Corrupt-file handling: an existing file that fails JSON.parse is NEVER
// silently replaced. It is backed up aside as `<file>.corrupt-<ts>` and a
// fresh file is written; the returned outcome string reports it so the
// caller can log loudly.
//
// Returns an outcome string: 'created' | 'updated' | 'recovered — …'.
export function updateThreadFile(threadPath, threadSlug, modelThread, cursorEntry, sessionRecord, now) {
  let threadData = null;
  let outcome = 'created';

  if (existsSync(threadPath)) {
    try {
      threadData = JSON.parse(readFileSync(threadPath, 'utf8'));
      outcome = 'updated';
    } catch (e) {
      const backupPath = `${threadPath}.corrupt-${Date.now()}`;
      renameSync(threadPath, backupPath);
      threadData = null;
      outcome = `recovered — corrupt JSON backed up to ${basename(backupPath)} (${e.message})`;
    }
  }

  if (threadData) {
    // Heal pre-versioning + legacy single-cursor files into cursor_history
    // (watchtower-contracts.md §Schema Versioning), then append.
    migrateThreadCursor(threadData);
    if (!Array.isArray(threadData.cursor_history)) threadData.cursor_history = [];
    threadData.cursor_history.push(cursorEntry);
    if (modelThread.display_name) threadData.display_name = modelThread.display_name;
    threadData.last_updated = now;
    if (!Array.isArray(threadData.sessions)) threadData.sessions = [];
    threadData.sessions.push(sessionRecord);
  } else {
    threadData = {
      schema_version: 2,
      thread: threadSlug,
      display_name: modelThread.display_name || threadSlug,
      cursor_history: [cursorEntry],
      sessions: [sessionRecord],
      related_fids: [],
      last_updated: now,
      status: 'active',
    };
  }

  atomicWrite(threadPath, JSON.stringify(threadData, null, 2));
  return outcome;
}

// ---------------------------------------------------------------------------
// preserveRing3LastSession — project-state section ownership merge
// ---------------------------------------------------------------------------
// state/projects/<slug>.md is written by two rings. Ring 1 rebuilds the file
// from scratch every run; Ring 3 owns the "## Last Session" section once it
// has authored a rich session summary there. The Ring 3 attribution line
// (`_<date> (<session-id>)_`, written by ring3-close's sessionSummary) IS the
// ownership marker: when the existing file carries it, Ring 1's rebuild must
// carry the existing section forward verbatim instead of clobbering it with
// its own fallback ("Active: …" / last-commit line). Full ownership table:
// watchtower-contracts.md §Project State Section Ownership.
export const PROJECT_STATE_LAST_SESSION_HEADER = '## Last Session';
const RING3_LAST_SESSION_ATTRIBUTION = /^_\d{4}-\d{2}-\d{2} \(.+\)_$/;

// Line-anchored header search: the header must start a line and be the
// entire line (modulo trailing whitespace). A bare indexOf would match
// inside '### Last Session' or a mid-line mention of the header text.
function headerLineIndex(content, header) {
  const escaped = header.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const m = new RegExp(`(^|\\n)${escaped}[ \\t]*(?=\\n|$)`).exec(content);
  return m ? m.index + m[1].length : -1;
}

function extractSection(content, header) {
  const idx = headerLineIndex(content, header);
  if (idx < 0) return null;
  const next = content.indexOf('\n## ', idx + header.length);
  return content.slice(idx, next > 0 ? next : content.length);
}

export function preserveRing3LastSession(freshContent, existingContent) {
  if (!existingContent) return freshContent;
  const existing = extractSection(existingContent, PROJECT_STATE_LAST_SESSION_HEADER);
  if (!existing) return freshContent;

  // Only Ring 3-authored sections are preserved; Ring 1's own ephemeral
  // fallback content is rebuilt fresh every run.
  const firstBodyLine = existing
    .slice(PROJECT_STATE_LAST_SESSION_HEADER.length)
    .split('\n')
    .map((l) => l.trim())
    .find((l) => l.length > 0);
  if (!firstBodyLine || !RING3_LAST_SESSION_ATTRIBUTION.test(firstBodyLine)) {
    return freshContent;
  }

  const preserved = existing.trimEnd();
  const freshIdx = headerLineIndex(freshContent, PROJECT_STATE_LAST_SESSION_HEADER);
  if (freshIdx < 0) {
    return `${freshContent.trimEnd()}\n\n${preserved}\n`;
  }
  const next = freshContent.indexOf('\n## ', freshIdx + PROJECT_STATE_LAST_SESSION_HEADER.length);
  const end = next > 0 ? next : freshContent.length;
  return freshContent.slice(0, freshIdx) + preserved + '\n' + freshContent.slice(end);
}

// ---------------------------------------------------------------------------
// resolveProjectIdentity — THE canonical "which project is this?" answer
// ---------------------------------------------------------------------------
// Every prior keying bug (Ring 3 phantom projects, mux desk-name mismatches)
// came from call sites re-deriving identity from a path basename. This is the
// one shared resolution: realpath → worktree→main-repo via git-common-dir →
// match against the registries. Identity layering: ~/.claude/cc-registry.json
// is the canonical project list; watchtower config.projects is a projection of
// it whose KEYS are what /inbox and the rings group by, so a config-key match
// names the result. This lib placement is a way-station — the audit's Move 1
// (a mux-level registry+resolver) is the end state.
//
// Returns:
//   { name, slug, path, registered: true }  — path belongs to a watchtower-
//     configured project; `name` is the config key (the inbox grouping key)
//   { name, slug, path, registered: false } — resolved to a real main repo
//     that no registry tracks (BENIGN: an untracked project working as
//     intended; callers file under this name and must NOT warn)
//   null — could not resolve at all (missing path / no repo root derivable):
//     ANOMALOUS; callers must fail loud (file with project_unresolved + warn),
//     never silently basename.
//
// opts.registryPath overrides ~/.claude/cc-registry.json (for tests).
// `config` may be null (registry-only resolution).

function gitMainRoot(p) {
  try {
    const common = execFileSync(
      'git', ['-C', p, 'rev-parse', '--git-common-dir'],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }
    ).trim();
    // --git-common-dir is the MAIN repo's .git even from inside a worktree;
    // it may be relative (".git" from the main root itself). Match the exact
    // component, not a suffix — a BARE repo named foo.git returns itself as
    // the common dir, and endsWith would wrongly take its parent.
    const abs = resolvePath(p, common);
    return basename(abs) === '.git' ? dirname(abs) : abs;
  } catch {
    return null;
  }
}

function safeRealpath(p) {
  try {
    return realpathSync(p);
  } catch {
    return null;
  }
}

export function resolveProjectIdentity(cwdOrPath, config, opts = {}) {
  if (!cwdOrPath) return null;
  const real = safeRealpath(cwdOrPath);
  if (!real) return null;

  const configEntries = Object.entries(config?.projects || {})
    .map(([key, proj]) => ({ key, real: proj?.path && safeRealpath(proj.path) }))
    .filter(e => e.real);
  const registered = (e, path) =>
    ({ name: e.key, slug: slugify(e.key), path, registered: true });

  const mainRoot = gitMainRoot(real);
  const mainReal = mainRoot ? (safeRealpath(mainRoot) || mainRoot) : null;
  // A mux worktree's repo root lies OUTSIDE the worktree path (the worktree
  // dir is not under the main repo). That's the case the basename fallback
  // used to break on: identity belongs to the main repo, and a configured
  // ancestor of the WORKTREE path (if any) must not win over it.
  const isWorktree = !!mainReal
    && real !== mainReal && !real.startsWith(mainReal + '/');

  if (isWorktree) {
    const exact = configEntries.find(e => e.real === mainReal);
    if (exact) return registered(exact, mainReal);
  } else {
    // Inside the path itself (or no git at all): a configured project may be
    // a plain directory (no .git) or a monorepo subdirectory — the matchers
    // this resolver replaced accepted both, so the resolver must not narrow
    // that. Exact match wins; otherwise the longest configured ancestor.
    let best = null;
    for (const e of configEntries) {
      if (e.real === real) { best = e; break; }
      if (real.startsWith(e.real + '/') && (!best || e.real.length > best.real.length)) {
        best = e;
      }
    }
    if (best) return registered(best, best.real);
    if (mainReal) {
      const exact = configEntries.find(e => e.real === mainReal);
      if (exact) return registered(exact, mainReal);
    }
  }

  // No repo root derivable and no configured match — without a root there is
  // no stable identity to name. Anomalous.
  if (!mainReal) return null;

  // cc-registry.json — the canonical project list (covers projects watchtower
  // doesn't track yet).
  const registryPath = opts.registryPath
    || join(homedir(), '.claude', 'cc-registry.json');
  try {
    const reg = JSON.parse(readFileSync(registryPath, 'utf8'));
    for (const proj of reg.projects || []) {
      const projReal = proj?.path && safeRealpath(proj.path);
      if (projReal && projReal === mainReal) {
        const name = proj.name || basename(mainReal);
        return { name, slug: slugify(name), path: mainReal, registered: false };
      }
    }
  } catch {
    // no registry — fall through to the benign untracked shape
  }

  // A real repo no registry tracks: benign, name it by its main root.
  const name = basename(mainReal);
  return { name, slug: slugify(name), path: mainReal, registered: false };
}

// ---------------------------------------------------------------------------
// loadBetterSqlite3 — resolve better-sqlite3 from wherever it actually lives
// ---------------------------------------------------------------------------
// The ring scripts run from ~/.claude-cabinet/watchtower/scripts/ with no
// node_modules nearby. NODE_PATH resolution only sees TOP-LEVEL packages in
// each NODE_PATH entry — it does not descend into a package's own
// node_modules, so better-sqlite3 nested under the globally-installed CC
// package is invisible to a bare require(). Resolution candidates, in order:
//   1. Normal resolution from this script's location (works if a top-level
//      install exists)
//   2. The project's own node_modules — any project with a pib.db has
//      better-sqlite3 installed, since pib-db depends on it
//   3. Nested under create-claude-cabinet in each NODE_PATH entry
// Returns the Database constructor, or null if no candidate resolves.

export function loadBetterSqlite3(projectPath) {
  const candidates = [() => require('better-sqlite3')];
  if (projectPath) {
    candidates.push(
      () => createRequire(join(projectPath, 'package.json'))('better-sqlite3')
    );
  }
  for (const entry of (process.env.NODE_PATH || '').split(':').filter(Boolean)) {
    candidates.push(
      () => createRequire(join(entry, 'create-claude-cabinet', 'package.json'))('better-sqlite3')
    );
  }
  for (const load of candidates) {
    try {
      return load();
    } catch {
      // try the next candidate
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// log / logError — structured logging with ring name and ISO timestamp
// ---------------------------------------------------------------------------

export function log(ring, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ring} ${ts}] ${msg}`);
}

export function logError(ring, msg) {
  const ts = new Date().toISOString();
  console.error(`[${ring} ${ts}] ERROR: ${msg}`);
}

// ---------------------------------------------------------------------------
// CLI — `node watchtower-lib.mjs resolve <path>` for bash callers
// ---------------------------------------------------------------------------
// Prints the resolveProjectIdentity JSON on stdout. Exit codes: 0 resolved
// (registered or benign), 2 unresolvable (callers must treat as anomalous),
// 1 usage error. Config is best-effort: a missing config.json degrades to
// registry-only resolution rather than erroring, so the CLI works on machines
// where watchtower isn't installed.

// realpath argv[1] so invocation through a symlink still matches
// import.meta.url (which node resolves to the real path).
if (process.argv[1] && import.meta.url === pathToFileURL(safeRealpath(process.argv[1]) || process.argv[1]).href) {
  const [cmd, target] = process.argv.slice(2);
  if (cmd === 'resolve' && target) {
    let config = null;
    try { config = loadConfig(); } catch { /* registry-only */ }
    const identity = resolveProjectIdentity(target, config);
    if (identity) {
      console.log(JSON.stringify(identity));
      process.exit(0);
    }
    console.error(`unresolvable: ${target}`);
    process.exit(2);
  } else if (cmd) {
    console.error('usage: watchtower-lib.mjs resolve <path>');
    process.exit(1);
  }
}

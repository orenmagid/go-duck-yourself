#!/usr/bin/env node

// Watchtower shared library.
// Canonical implementations of utilities used across all ring scripts.
// Ring scripts import from here instead of maintaining local copies.

import {
  readFileSync, writeFileSync, existsSync, appendFileSync,
  mkdirSync, renameSync, realpathSync, readdirSync,
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
// Suppression ledger — structured sidecar of every Ring 3 dedup suppression
// ---------------------------------------------------------------------------
//
// `recordSuppression` appends one structured JSON line per suppression so the
// Ring 2 slow over-suppression canary (M5) can read what was killed and what
// it matched — a STRUCTURED record, never the human `log()` prose lines
// (format-coupling = silent failure; a reworded log line would silently break
// the canary, and per-session logs rotate away). Shape is the shared contract
// documented in `watchtower-contracts.md` ("Suppression Ledger").
//
// FAIL-OPEN is load-bearing: this ledger is observability, not the filing
// decision. A failed append logs one line and continues — a dropped ledger
// record must NEVER block an extraction from filing. The record shape is
// additive: new fields ride alongside; readers default missing fields.
//
// Append-only and atomic-enough: a single short line via appendFileSync is a
// POSIX atomic append on local fs. Growth is bounded by the canary, which
// prunes the ledger to its window on each run (not on the hot path here).

export function suppressionLedgerPath(watchtowerDir) {
  return join(watchtowerDir || getWatchtowerDir(),
    'state', 'suppression-ledger.jsonl');
}

// ---------------------------------------------------------------------------
// recentSlice — the most-recent `n` chars of a (preprocessed) transcript
// ---------------------------------------------------------------------------
//
// The M2 recall fix (act:edd79e15). `preprocessTranscript` already keeps the
// most-recent ~80% of an oversized session, but every extraction lens then
// threw that away with `compressed.slice(0, n)` — the OLDEST n chars — so a
// lesson set up in the back half of a session was silently dropped. recentSlice
// keeps the TAIL instead. When `n >= text.length` the whole string is returned
// (most sessions: one full-transcript call, zero extra cost). The leading
// partial line is trimmed (same as preprocessTranscript's tail-truncation) so a
// call never starts mid-JSON-line. The SINGLE definition every lens routes
// through — no more copy-pasted front-slices.
export function recentSlice(text, n) {
  if (typeof text !== 'string' || text.length === 0) return '';
  // Fail-open (recall-favoring, invariant #9): a non-positive or non-finite
  // budget is a misconfiguration — keep the WHOLE transcript rather than
  // silently feeding the model an empty string (an empty transcript = zero
  // extraction = the exact knowledge-drop this program exists to prevent).
  if (!Number.isFinite(n) || n <= 0) return text;
  if (text.length <= n) return text;
  let tail = text.slice(text.length - n);
  const nl = tail.indexOf('\n');
  if (nl >= 0) tail = tail.slice(nl + 1);
  return tail;
}

export function recordSuppression(record = {}, { watchtowerDir, ts } = {}) {
  try {
    const path = suppressionLedgerPath(watchtowerDir);
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const line = JSON.stringify({
      ts: ts || record.ts || new Date().toISOString(),
      project: record.project ?? null,
      corpus: record.corpus ?? null,
      suppressed_title: record.suppressed_title ?? null,
      matched_against: record.matched_against ?? null,
      session_id: record.session_id ?? null,
    }) + '\n';
    appendFileSync(path, line);
  } catch (e) {
    // Fail-open: never throw out of a suppression site.
    try { logError('suppression-ledger', `append failed (${e.message}) — continuing`); }
    catch { /* logging itself must never throw the caller */ }
  }
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
// Authored-.claude re-inclusion for worktree dirty detection (act:e91fdfcf)
// ---------------------------------------------------------------------------
// The JS-side ring dirty counts (Ring 1 countRealUncommitted, Ring 3 close)
// historically BLANKET-excluded every `.claude/` porcelain line as mux infra
// churn — which silently under-reported real authored work in worktrees
// (.claude/plans, .claude/methodology, .claude/rules: authored PROJECT RECORD
// per .claude/rules/artifacts-of-thought.md). This mirrors the canonical
// shell EXCLUSION CONTRACT in templates/mux/config/worktree-dirty-check.sh
// (point 1 EXCEPTION): a top-level `.claude/` entry that holds TRACKED files
// is authored, so churn under it counts; only disposable infra is ignored.
// The shell and JS sides remain separate implementations (one bash, one JS) —
// this is the JS single source the two rings delegate to.
const AUTHORED_CLAUDE_FLOOR = ['plans', 'methodology'];

// The set of top-level `.claude/` dirs that hold tracked files in this
// worktree's index (authored project record). Derived from `git ls-files`
// at run time; the static plans/methodology floor is ALWAYS included so a
// failed/partial index read can never drop authored work (fail-DIRTY).
export function authoredClaudeDirs(cwd, execFn) {
  const dirs = new Set(AUTHORED_CLAUDE_FLOOR);
  let out;
  try {
    out = execFn('git ls-files .claude/', { cwd });
  } catch {
    return dirs;
  }
  if (!out) return dirs;
  for (const line of out.split('\n')) {
    const m = line.trim().match(/^\.claude\/([^/]+)\//);
    if (m) dirs.add(m[1]);
  }
  return dirs;
}

// Is this porcelain line DISPOSABLE `.claude/` infra churn (safe to ignore)?
// A `.claude/` path under an authored top-level dir is real work → false
// (count it). A non-`.claude/` line is not our concern here → false. Only
// `.claude/` churn outside the authored subtrees (untracked infra, top-level
// config files) is disposable → true.
export function claudeChurnIsDisposable(porcelainLine, authoredDirs) {
  if (!/\s\.claude(?:\/|$)/.test(porcelainLine)) return false;
  const m = porcelainLine.match(/\.claude\/([^/\s]+)/);
  if (m && authoredDirs.has(m[1])) return false; // authored subtree → count it
  return true; // disposable .claude/ infra
}

// ---------------------------------------------------------------------------
// flushFeedbackOutbox — deliver the GLOBAL cc-feedback outbox to the CC repo
// ---------------------------------------------------------------------------
// Ring 1 mechanical duty (act:6c3a4763). Ports orient's delivery algorithm:
// read ~/.claude/cc-feedback-outbox.json, resolve the CC source repo via
// cc-registry (the entry whose package.json name is create-claude-cabinet),
// write each undelivered item's body to <cc>/feedback/{date}-{slug}.md with
// a skip-if-exists guard against feedback/ AND feedback/resolved/ — a name
// containing the slug, or a delivery-scheme name whose slug is a whole-token
// prefix/subset of the new one, means a prior session already delivered or
// resolved it — then atomically rewrite the outbox — [] on a clean pass, only the
// failed items otherwise. Fail-safe: if the destination cannot be resolved,
// the outbox is left untouched — an item is never dropped without a file
// existing for it. Ring 1 is now the SOLE feedback-delivery owner: orient's
// duplicate flush (incompatible {date}-{slug}-{seq}.md scheme) was retired
// (act:d53ff509) once this duty was verified in the live runtime.

const CC_PACKAGE_NAME = 'create-claude-cabinet';

// Resolve the CC source repo ROOT via cc-registry — the single registered
// project whose package.json name is create-claude-cabinet. The one place
// "which checkout is the CC source?" is answered, so callers (feedback
// delivery, the Ring 1 runtime-drift check) never re-derive it.
export function resolveCcSourceRepo(registryPath) {
  if (!existsSync(registryPath)) return null;
  let registry;
  try {
    registry = JSON.parse(readFileSync(registryPath, 'utf8'));
  } catch {
    return null;
  }
  for (const proj of registry.projects || []) {
    const root = proj.path || '';
    const pkgPath = join(root, 'package.json');
    if (!existsSync(pkgPath)) continue;
    try {
      const pkg = JSON.parse(readFileSync(pkgPath, 'utf8'));
      if (pkg.name === CC_PACKAGE_NAME) return root;
    } catch { /* unreadable package.json — not the CC repo */ }
  }
  return null;
}

function resolveCcFeedbackDir(registryPath) {
  const repo = resolveCcSourceRepo(registryPath);
  return repo ? join(repo, 'feedback') : null;
}

function feedbackFileExists(feedbackDir, slug) {
  for (const dir of [feedbackDir, join(feedbackDir, 'resolved')]) {
    if (!existsSync(dir)) continue;
    for (const name of readdirSync(dir)) {
      if (name.includes(slug)) return true;
      // Reverse direction (act:ca0aee25): a re-report under a LONGER title
      // must still match its already-delivered shorter twin. Deliberately
      // narrow — only delivery-scheme names ({date}-{slug}[-{seq}].md)
      // carry slug semantics, and the existing slug must sit on whole
      // dash-token boundaries inside the new one. Silent suppression of
      // genuinely new feedback is worse than a duplicate delivery, so no
      // substring fuzz and no participation by hand-named files.
      const m = /^\d{4}-\d{2}-\d{2}-(.+?)(?:-\d+)?\.md$/.exec(name);
      if (m && `-${slug}-`.includes(`-${m[1]}-`)) return true;
    }
  }
  return false;
}

export function flushFeedbackOutbox(opts = {}) {
  const outboxPath = opts.outboxPath
    || join(homedir(), '.claude', 'cc-feedback-outbox.json');
  const registryPath = opts.registryPath
    || join(homedir(), '.claude', 'cc-registry.json');

  const result = { status: 'ok', delivered: 0, skipped: 0, kept: 0, destination: null };

  if (!existsSync(outboxPath)) {
    result.status = 'no-outbox';
    return result;
  }

  let outbox;
  try {
    outbox = JSON.parse(readFileSync(outboxPath, 'utf8'));
  } catch {
    // Malformed outbox: per orient's contract, warn and reset to [].
    atomicWrite(outboxPath, '[]\n');
    result.status = 'malformed-reset';
    return result;
  }

  if (!Array.isArray(outbox) || outbox.length === 0) {
    result.status = 'empty';
    return result;
  }

  const undelivered = outbox.filter((item) => item && item.delivered !== true);
  if (undelivered.length === 0) {
    // Only stale delivered:true markers remain — don't accumulate them.
    atomicWrite(outboxPath, '[]\n');
    result.status = 'markers-cleared';
    return result;
  }

  const feedbackDir = opts.destination || resolveCcFeedbackDir(registryPath);
  if (!feedbackDir) {
    // Cannot resolve where feedback lives. Leave the outbox untouched —
    // never mark/drop an item without a delivered file existing for it.
    result.status = 'no-destination';
    result.kept = outbox.length;
    return result;
  }
  result.destination = feedbackDir;

  const failed = [];
  for (const item of undelivered) {
    try {
      const slug = slugify(item.title || 'untitled') || 'untitled';
      if (feedbackFileExists(feedbackDir, slug)) {
        result.skipped++;
        continue;
      }
      const date = item.date || new Date().toISOString().slice(0, 10);
      if (!existsSync(feedbackDir)) mkdirSync(feedbackDir, { recursive: true });
      writeFileSync(join(feedbackDir, `${date}-${slug}.md`), item.body || item.title || '');
      result.delivered++;
    } catch {
      failed.push(item);
    }
  }

  // Clean pass resets to []; otherwise keep only the failed items.
  atomicWrite(outboxPath, JSON.stringify(failed, null, 2) + '\n');
  result.kept = failed.length;
  if (failed.length > 0) result.status = 'partial';
  return result;
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

// ---------------------------------------------------------------------------
// Shared thread-file reading (single source of truth)
// ---------------------------------------------------------------------------
// Reading `state/threads/*.json`, skipping corrupt files, keeping only active
// threads, and deciding which threads "belong to" a given item/project was
// independently implemented twice (Ring 2's loadActiveThreads/threadsForItem
// for enrichment context, Ring 3-close's threadCursorLines for the dedup prose
// corpus). Two copies of one concept drift — and they did: Ring 3's membership
// FALLBACK matched on slug-substring containment (slugify(thread).includes(
// projectSlug)), so a short project slug ("flow") over-matched any thread whose
// slug merely contained it ("…workflow…"), pulling FOREIGN cursor prose into a
// project's suppression corpus where it could silently suppress legitimate new
// extractions. The shared membership rule below has NO slug-substring fallback:
// a thread belongs to a project only by EXPLICIT thread_ids membership OR EXACT
// sessions[].project equality (the key Ring 3 itself writes). Both rings now
// consume these helpers.

// loadActiveThreads — read all active thread files from threadsDir. Corrupt
// JSON and dormant/non-active threads are skipped; a missing/empty dir yields
// []. Never throws.
export function loadActiveThreads(threadsDir) {
  if (!existsSync(threadsDir)) return [];
  const threads = [];
  try {
    for (const entry of readdirSync(threadsDir, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
      try {
        const thread = JSON.parse(readFileSync(join(threadsDir, entry.name), 'utf8'));
        if (thread && thread.status === 'active') threads.push(thread);
      } catch { /* skip corrupt */ }
    }
  } catch { /* skip */ }
  return threads;
}

// threadMatchesProject — THE single membership predicate. A thread belongs to a
// project iff (a) its slug is in `explicitIds` (explicit thread_ids membership)
// OR (b) any of its sessions records that exact project slug. Deliberately NO
// slug-substring fallback — that over-matched foreign threads. `projectSlug`
// must already be slugified; `explicitIds` defaults to none. Returns
// { match: bool, explicit: bool } so selectors can rank explicit matches first.
export function threadMatchesProject(thread, projectSlug, explicitIds = []) {
  const explicit = Array.isArray(explicitIds) && explicitIds.includes(thread?.thread);
  const byProject = !!projectSlug
    && Array.isArray(thread?.sessions)
    && thread.sessions.some(s => s && s.project === projectSlug);
  return { match: explicit || byProject, explicit };
}

// Threads matched to an item, capped, ranked explicit-first then by recency.
const THREAD_MATCH_CAP = 3;

// threadsForItem — match active threads to a queue item: explicit
// item.thread_ids membership OR exact sessions[].project equality (slugify(
// item.project)). Explicit matches rank first, then most recent last_updated;
// capped at THREAD_MATCH_CAP. Built on threadMatchesProject so the membership
// rule lives in one place.
export function threadsForItem(item, threads) {
  const explicitIds = Array.isArray(item?.thread_ids) ? item.thread_ids : [];
  const projectSlug = item?.project ? slugify(item.project) : '';
  const matches = [];
  for (const thread of threads) {
    const { match, explicit } = threadMatchesProject(thread, projectSlug, explicitIds);
    if (match) matches.push({ thread, explicit });
  }
  matches.sort((a, b) => {
    if (a.explicit !== b.explicit) return a.explicit ? -1 : 1;
    return new Date(b.thread.last_updated || 0) - new Date(a.thread.last_updated || 0);
  });
  return matches.slice(0, THREAD_MATCH_CAP).map(m => m.thread);
}

// projectThreadCursorLines — the dedup PROSE corpus from a project's active
// thread cursors. Thread cursors (display_name / what / where_left_off /
// open_questions) are exactly "what the system already knows the user is
// working on"; an extraction that restates them is noise. Membership is the
// shared threadMatchesProject rule (exact sessions[].project equality — NO
// slug-substring over-match). Only the CURRENT cursor counts (currentCursor),
// not history. Returned lines are lowercased, ready for the memoryLines
// containment pass in Ring 3's isDuplicate. Never throws.
export function projectThreadCursorLines(threadsDir, projectSlug) {
  if (!projectSlug) return [];
  const lines = [];
  const push = (v) => {
    if (typeof v === 'string' && v.trim()) lines.push(v.toLowerCase());
  };
  for (const thread of loadActiveThreads(threadsDir)) {
    if (!threadMatchesProject(thread, projectSlug).match) continue;
    const cursor = currentCursor(thread);
    push(thread.display_name);
    push(cursor.what);
    push(cursor.where_left_off);
    if (Array.isArray(cursor.open_questions)) cursor.open_questions.forEach(push);
  }
  return lines;
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

// Shared UI-path heuristic for the verify-coverage feature. The verify-backfill
// scan (Ring 2) and the verify-coverage lens (Ring 3) must agree on what counts
// as "UI-touching" — single source of truth so the two rings never drift.
// (act:1be47d42)
export const VERIFY_UI_PATHS = ['webapp/frontend/', 'components/', 'pages/', 'app/'];
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

// buildLastSessionBlock — THE single source of the "## Last Session" block
// format. Ring 3's sessionSummary builds the per-session file body and this
// inline block from the SAME `bullets` string, so the inline section can never
// be a lossy subset of the per-session record (act:ac119994). The
// `_<date> (<sessionId>)_` attribution line is the ownership marker
// preserveRing3LastSession keys on — emit it here so a Ring 3-authored section
// is always recognized as Ring 3's. `bullets` is the COMPLETE model bullet set;
// it is never truncated or sliced for the inline block.
export function buildLastSessionBlock({ date, sessionId, bullets }) {
  return `${PROJECT_STATE_LAST_SESSION_HEADER}\n_${date} (${sessionId})_\n${String(bullets).trim()}\n`;
}

// upsertLastSessionSection — splice a freshly-built "## Last Session" block into
// the project-state file, replacing an existing section in place or appending
// one when absent. Reuses the line-anchored headerLineIndex so a "### Last
// Session" or a mid-line mention of the header text is never matched (the same
// machinery preserveRing3LastSession reads with — one splice convention, not
// two). `block` must already end with a trailing newline (buildLastSessionBlock
// guarantees this). When the section is not the file's last, a blank line is
// inserted before the following "## " so the markdown structure stays valid.
export function upsertLastSessionSection(existingContent, block) {
  const base = existingContent || '';
  const idx = headerLineIndex(base, PROJECT_STATE_LAST_SESSION_HEADER);
  if (idx < 0) {
    if (!base.trim()) return block;
    return `${base.trimEnd()}\n\n${block}`;
  }
  const next = base.indexOf(
    '\n## ', idx + PROJECT_STATE_LAST_SESSION_HEADER.length);
  if (next > 0) {
    // Replacing a non-terminal section: keep a blank line before the next "## ".
    return base.slice(0, idx) + block.trimEnd() + '\n\n' + base.slice(next + 1);
  }
  return base.slice(0, idx) + block;
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
// writeProjectStatePreservingRing3 — race-safe project-state rebuild write
// ---------------------------------------------------------------------------
// preserveRing3LastSession is a pure merge; a raw read→merge→atomicWrite
// around it has a read-then-write race: if Ring 3 writes its fresh Last
// Session between Ring 1's snapshot read and the rename, the rebuild is
// computed from a stale snapshot and silently drops the just-authored
// summary until the NEXT session close. This helper closes that window
// with a re-read check-and-retry: merge against a snapshot, re-read to
// verify nothing changed underneath us, and only then write; on change,
// re-merge against the fresh read (up to maxAttempts). If attempts are
// exhausted (pathological churn), it merges against the FRESHEST read and
// writes anyway — the rebuild is never skipped.
//
// Residual window: a write landing between the verify read and the rename
// inside atomicWrite is still theoretically possible — microseconds wide,
// down from the full merge-computation window. If it ever bites in
// practice, the structural fix is the Ring 3 sidecar-file design (option c
// in .claude/plans/watchtower-ring1-race-fix.md).
//
// opts._beforeVerifyHook is a TEST-ONLY injection seam, fired between the
// merge and the verify re-read so tests can deterministically simulate a
// concurrent Ring 3 write. Production callers must not pass it.
//
// Returns { written: true, attempts, exhausted? }.
export function writeProjectStatePreservingRing3(statePath, freshContent, opts = {}) {
  const maxAttempts = opts.maxAttempts ?? 3;
  const read = () => (existsSync(statePath) ? readFileSync(statePath, 'utf8') : null);

  let snapshot = read();
  for (let i = 0; i < maxAttempts; i++) {
    const merged = preserveRing3LastSession(freshContent, snapshot);
    opts._beforeVerifyHook?.();
    const verify = read();
    if (verify !== snapshot) {
      // Someone (Ring 3) wrote since our read — re-merge against it.
      snapshot = verify;
      continue;
    }
    atomicWrite(statePath, merged);
    return { written: true, attempts: i + 1 };
  }

  // Attempts exhausted (pathological churn): merge against the freshest
  // read and write anyway — never skip the rebuild.
  atomicWrite(statePath, preserveRing3LastSession(freshContent, read()));
  return { written: true, attempts: maxAttempts, exhausted: true };
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

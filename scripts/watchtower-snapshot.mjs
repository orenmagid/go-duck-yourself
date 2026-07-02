#!/usr/bin/env node

// Watchtower mid-session snapshot + delta.
//
// The SessionStart hook injects watchtower state exactly ONCE, at session
// start. A long session then drifts from that snapshot: other sessions
// advance threads, Ring 2 enriches and files inbox items, Ring 1 detects new
// git state, deferred actions come due. This module is the primitive behind
// the /catch-up skill — it answers "what changed in watchtower since this
// session started?" without a restart.
//
// It is deliberately a SIBLING of watchtower-build-context.mjs, not folded
// into it: that builder's single job is "assemble the start-of-session
// injection string and print it," and it runs on every SessionStart. Snapshot
// capture + diff + plain-English rendering is a separate concern with its own
// lifecycle. Both the SessionStart hook (baseline capture) and the /catch-up
// skill (current capture + diff) import the same functions here — single
// source of truth, builder untouched.
//
// State sources are the SAME files the injection summarizes, read at item
// granularity (NOT through the builder's 9500-char truncation path, so a
// budget-dropped section can never read as a delta):
//   - inbox  : listPending() from watchtower-queue.mjs (full items, with ids)
//   - threads: loadActiveThreads()/currentCursor() from watchtower-lib.mjs
//   - git    : state/git-attention.json facts (the structured git signal)
//   - identity: resolveProjectIdentity() (git-aware; a worktree resolves to
//     its main repo's project, so a worktree /catch-up diffs the right state)
//
// CLI modes (additive; this file never alters the SessionStart injection):
//   --emit-snapshot <path> --project-path <cwd> [--session-id <id>]
//       Build the current snapshot, write it atomically to <path>, then prune
//       snapshots older than 7 days from <path>'s directory. The SessionStart
//       hook calls this to capture the per-session baseline.
//   --snapshot --project-path <cwd>
//       Print the current snapshot JSON to stdout (used in tests / debugging).
//   --diff <baselinePath> --project-path <cwd>
//       Read the baseline, build the current snapshot, diff, and print the
//       plain-English delta. The /catch-up skill relays this.
//
// Never crashes — readers degrade to empty, the diff tolerates missing/partial
// snapshots, and a corrupt baseline prints an honest "no baseline" line.

import { readFileSync, existsSync, readdirSync, statSync, unlinkSync, realpathSync } from 'fs';
import { join, dirname } from 'path';
import { pathToFileURL } from 'url';
import {
  atomicWrite,
  currentCursor,
  loadActiveThreads,
  threadMatchesProject,
  resolveProjectIdentity,
} from './watchtower-lib.mjs';
import { listPending } from './watchtower-queue.mjs';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

export const SNAPSHOT_SCHEMA_VERSION = 1;
const PRUNE_DAYS = 7;
const PRUNE_MS = PRUNE_DAYS * 24 * 60 * 60 * 1000;

// --- Safe readers ---

function safeReadJSON(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

// --- Snapshot capture ---------------------------------------------------------

// buildSnapshot — read the current pollable watchtower state into a structured
// object. Scoped to the project the cwd resolves to (the operator's "what's new
// in my project" frame); if the project can't be resolved, falls back to the
// whole portfolio rather than going blank. Pure-ish: reads state files via the
// shared helpers, returns a plain object, writes nothing.
//
// SILENT-EXCLUSION NOTE (decided, not missed): inbox capture is listPending()
// only (status === 'pending'). An item that is filed AND resolved entirely
// between two snapshots appears in neither set, so it is invisible to the
// delta. That is correct for a "what's actionable now" surface — the operator
// is shown what is still pending — but it is a deliberate exclusion.
export function buildSnapshot({ projectPath } = {}) {
  const config = safeReadJSON(join(WATCHTOWER_DIR, 'config.json'));

  let projectName = null;
  let projectSlug = null;
  if (config && config.projects && projectPath) {
    try {
      const identity = resolveProjectIdentity(projectPath, config);
      if (identity && identity.registered) {
        projectName = identity.name;
        projectSlug = identity.slug;
      }
    } catch { /* unresolved → portfolio-wide fallback below */ }
  }

  // Inbox — full pending items, scoped to the project when resolved.
  let pending = [];
  try {
    pending = listPending(projectName ? { project: projectName } : {});
  } catch { pending = []; }
  const items = pending
    .map((i) => ({ id: i.id, category: i.category, title: i.title, urgency: i.urgency }))
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const byCategory = {};
  let urgent = 0;
  for (const i of items) {
    byCategory[i.category] = (byCategory[i.category] || 0) + 1;
    if (i.urgency === 'urgent') urgent++;
  }

  // Threads — active threads touching this project (or all active when
  // unresolved), at cursor granularity.
  const allThreads = loadActiveThreads(join(WATCHTOWER_DIR, 'state', 'threads'));
  const scoped = projectSlug
    ? allThreads.filter((t) => threadMatchesProject(t, projectSlug).match)
    : allThreads;
  const threads = scoped
    .map((t) => ({
      thread: t.thread,
      display_name: t.display_name || t.thread,
      last_updated: t.last_updated || null,
      what: currentCursor(t).what || '',
    }))
    .sort((a, b) => (a.thread < b.thread ? -1 : a.thread > b.thread ? 1 : 0));

  // Git attention — the structured sidecar Ring 1 writes (MERGE-OR-LOSE /
  // diverged-branch lines). The line strings are the comparable signal.
  const sidecar = safeReadJSON(join(WATCHTOWER_DIR, 'state', 'git-attention.json'));
  const gitAttention = Array.isArray(sidecar && sidecar.facts)
    ? sidecar.facts.map((f) => f && f.line).filter(Boolean).sort()
    : [];

  return {
    schema_version: SNAPSHOT_SCHEMA_VERSION,
    captured_at: new Date().toISOString(),
    session_id: null,
    project: projectName,
    project_slug: projectSlug,
    inbox: { total: items.length, urgent, byCategory, items },
    threads,
    gitAttention,
  };
}

// --- Delta --------------------------------------------------------------------

// A thread "advanced" when its last_updated moved forward. Missing/equal/
// unparseable timestamps on either side => NOT advanced (a decided default, not
// an accidental one): new Date(undefined) is Invalid Date and every comparison
// against it is false, which we make explicit here.
function advancedSince(beforeTs, afterTs) {
  const a = new Date(beforeTs || 0).getTime();
  const b = new Date(afterTs || 0).getTime();
  if (Number.isNaN(a) || Number.isNaN(b)) return false;
  return b > a;
}

// diffSnapshots — structured delta of two snapshots. Tolerates null/partial
// snapshots (a corrupt or missing field reads as empty). Inbox membership is by
// item-id SET, not by count, so a file-and-resolve that nets zero total still
// surfaces the genuinely new item. Threads diff by slug: present-both with a
// later timestamp => advanced; after-only => new; before-only => went quiet.
export function diffSnapshots(before, after) {
  const bInbox = (before && before.inbox) || {};
  const aInbox = (after && after.inbox) || {};
  const bItems = Array.isArray(bInbox.items) ? bInbox.items : [];
  const aItems = Array.isArray(aInbox.items) ? aInbox.items : [];
  const bIds = new Set(bItems.map((i) => i.id));
  const aIds = new Set(aItems.map((i) => i.id));

  const newItems = aItems.filter((i) => !bIds.has(i.id));
  const resolvedItems = bItems.filter((i) => !aIds.has(i.id));
  const newByCategory = {};
  for (const i of newItems) newByCategory[i.category] = (newByCategory[i.category] || 0) + 1;
  const newDeferredTriggers = newItems.filter((i) => i.category === 'deferred-trigger');

  const bThreads = Array.isArray(before && before.threads) ? before.threads : [];
  const aThreads = Array.isArray(after && after.threads) ? after.threads : [];
  const bByThread = new Map(bThreads.map((t) => [t.thread, t]));
  const aByThread = new Map(aThreads.map((t) => [t.thread, t]));

  const advanced = [];
  const newThreads = [];
  for (const t of aThreads) {
    const prev = bByThread.get(t.thread);
    if (!prev) {
      newThreads.push(t);
    } else if (advancedSince(prev.last_updated, t.last_updated)) {
      advanced.push(t);
    }
  }
  const closedThreads = bThreads.filter((t) => !aByThread.has(t.thread));

  const bGit = new Set(Array.isArray(before && before.gitAttention) ? before.gitAttention : []);
  const aGit = new Set(Array.isArray(after && after.gitAttention) ? after.gitAttention : []);
  const gitAdded = [...aGit].filter((l) => !bGit.has(l));
  const gitRemoved = [...bGit].filter((l) => !aGit.has(l));

  const hasChanges = newItems.length > 0 || resolvedItems.length > 0
    || advanced.length > 0 || newThreads.length > 0 || closedThreads.length > 0
    || gitAdded.length > 0 || gitRemoved.length > 0;

  return {
    capturedAt: { before: (before && before.captured_at) || null, after: (after && after.captured_at) || null },
    inbox: { newItems, resolvedItems, newByCategory, newDeferredTriggers },
    threads: { advanced, new: newThreads, closed: closedThreads },
    git: { added: gitAdded, removed: gitRemoved },
    hasChanges,
  };
}

// --- Plain-English rendering --------------------------------------------------

// Concept-first labels for inbox categories. The item TITLE carries the
// specifics; this label keeps the sentence in plain words rather than the raw
// category slug. Unknown categories fall back to a de-slugified phrasing.
const CATEGORY_LABEL = {
  'deferred-trigger': 'a deferred task came due',
  'knowledge-extraction': 'knowledge to review',
  'completion-review': 'a finished item to confirm',
  'completion-candidate': 'a finished item to confirm',
  'qa-handoff': 'a QA handoff',
  'pattern-promotion': 'a recurring-pattern note',
  'advisor-finding': 'an advisor finding',
  'methodology-capture': 'a design note captured',
  'doc-drift': 'a docs-vs-reality mismatch',
  'upstream-friction': 'upstream friction reported',
  'routing-decision': 'a routing decision',
  'coverage-warning': 'a coverage gap',
  'verify-backfill': 'a verification backfill',
  'routine': 'a scheduled routine',
  'worktree-unmerged': 'an unmerged worktree branch',
  'branch-diverged': 'a diverged branch',
  'watchtower-health': 'a watchtower health note',
  'project-completion': 'a project nearing completion',
};

function categoryLabel(category) {
  if (CATEGORY_LABEL[category]) return CATEGORY_LABEL[category];
  return String(category || 'an item').replace(/[-_]/g, ' ');
}

// Per-section caps keep /catch-up scannable when a burst lands (e.g. Ring 2
// files many pattern-promotion items at once, or a portfolio gains several
// diverged branches). Never silently drop — append an explicit "…and N more".
const ITEM_CAP = 10;
const THREAD_CAP = 5;
const GIT_CAP = 8;
const WHAT_MAX = 160;

function truncate(s, max) {
  if (!s) return '';
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

// Push up to `cap` rendered lines for `arr`, then a "…and N more" line if any
// were held back. `render` returns the bullet string for one element.
function pushCapped(lines, arr, cap, render) {
  for (const el of arr.slice(0, cap)) lines.push(render(el));
  if (arr.length > cap) lines.push(`  …and ${arr.length - cap} more`);
}

// "since HH:MM" / "since session start" framing for the header. Best-effort —
// a missing timestamp degrades to the generic phrasing.
function sinceClause(beforeIso) {
  if (!beforeIso) return 'since this session started';
  const d = new Date(beforeIso);
  if (Number.isNaN(d.getTime())) return 'since this session started';
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  return `since this session started (baseline ${hh}:${mm})`;
}

// renderDelta — plain-English, concept-first briefing of the delta. Phrased as
// PROJECT-level change ("changed in <project> since your session started"), not
// personal attribution: in a worktree, a concurrent session's Ring 3 close may
// have advanced a thread or filed an item, and that legitimately appears here —
// it is "the project moved," not "you did this."
export function renderDelta(delta, { projectName } = {}) {
  const where = projectName ? `watchtower for ${projectName}` : 'watchtower';
  if (!delta || !delta.hasChanges) {
    return `Nothing new in ${where} ${sinceClause(delta && delta.capturedAt && delta.capturedAt.before)}.`;
  }

  const lines = [];
  lines.push(`What's changed in ${where} ${sinceClause(delta.capturedAt && delta.capturedAt.before)}:`);

  const { newItems, resolvedItems, newDeferredTriggers } = delta.inbox;

  // Deferred tasks that just came due — called out first; they are the most
  // time-sensitive "act now" signal.
  if (newDeferredTriggers.length > 0) {
    lines.push('');
    const n = newDeferredTriggers.length;
    lines.push(`⏰ ${n} deferred task${n === 1 ? '' : 's'} just came due:`);
    pushCapped(lines, newDeferredTriggers, ITEM_CAP, (i) => `  - ${i.title || '(untitled)'} (${i.id})`);
  }

  // Other new inbox items (excluding the deferred triggers already shown).
  const otherNew = newItems.filter((i) => i.category !== 'deferred-trigger');
  if (otherNew.length > 0) {
    lines.push('');
    const n = otherNew.length;
    lines.push(`📥 ${n} new inbox item${n === 1 ? '' : 's'} filed:`);
    pushCapped(lines, otherNew, ITEM_CAP,
      (i) => `  - ${i.title || '(untitled)'} — ${categoryLabel(i.category)} (${i.id})`);
  }

  if (resolvedItems.length > 0) {
    const n = resolvedItems.length;
    lines.push('');
    lines.push(`✅ ${n} inbox item${n === 1 ? '' : 's'} resolved since you started.`);
  }

  // Threads.
  const { advanced, new: newThreads, closed } = delta.threads;
  if (newThreads.length > 0) {
    lines.push('');
    pushCapped(lines, newThreads, THREAD_CAP,
      (t) => `🧵 New work thread "${t.display_name || t.thread}"${t.what ? ` — ${truncate(t.what, WHAT_MAX)}` : ''}.`);
  }
  if (advanced.length > 0) {
    lines.push('');
    pushCapped(lines, advanced, THREAD_CAP,
      (t) => `🧵 Thread "${t.display_name || t.thread}" advanced${t.what ? `: ${truncate(t.what, WHAT_MAX)}` : ''}.`);
  }
  if (closed.length > 0) {
    lines.push('');
    pushCapped(lines, closed, THREAD_CAP,
      (t) => `🧵 Thread "${t.display_name || t.thread}" went quiet (no longer active).`);
  }

  // Git/repo attention.
  if (delta.git.added.length > 0) {
    lines.push('');
    lines.push('🔧 New repo attention:');
    pushCapped(lines, delta.git.added, GIT_CAP, (l) => `  - ${l}`);
  }
  if (delta.git.removed.length > 0) {
    lines.push('');
    lines.push('🔧 Cleared since you started:');
    pushCapped(lines, delta.git.removed, GIT_CAP, (l) => `  - ${l}`);
  }

  return lines.join('\n');
}

// --- Snapshot directory pruning ----------------------------------------------

// pruneSnapshots — best-effort removal of session snapshots older than 7 days
// from `dir`, by file mtime. Never throws; tolerates a file vanishing mid-prune
// (a concurrent session pruning the same dir). Keeps state/session-snapshots/
// bounded without any external cleaner. Returns the count removed.
export function pruneSnapshots(dir) {
  let removed = 0;
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return 0;
  }
  const cutoff = Date.now() - PRUNE_MS;
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
    const fp = join(dir, entry.name);
    try {
      if (statSync(fp).mtimeMs < cutoff) {
        unlinkSync(fp);
        removed++;
      }
    } catch {
      // file gone / unreadable — tolerate and continue
    }
  }
  return removed;
}

// --- CLI ----------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = { mode: null, path: null, projectPath: null, sessionId: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--emit-snapshot' && args[i + 1]) { out.mode = 'emit'; out.path = args[++i]; }
    else if (a === '--diff' && args[i + 1]) { out.mode = 'diff'; out.path = args[++i]; }
    else if (a === '--snapshot') { out.mode = 'snapshot'; }
    else if (a === '--project-path' && args[i + 1]) { out.projectPath = args[++i]; }
    else if (a === '--session-id' && args[i + 1]) { out.sessionId = args[++i]; }
  }
  return out;
}

function main() {
  const { mode, path, projectPath, sessionId } = parseArgs(process.argv);

  if (mode === 'emit') {
    const snap = buildSnapshot({ projectPath });
    if (sessionId) snap.session_id = sessionId;
    atomicWrite(path, JSON.stringify(snap, null, 2) + '\n');
    pruneSnapshots(dirname(path));
    return;
  }

  if (mode === 'snapshot') {
    process.stdout.write(JSON.stringify(buildSnapshot({ projectPath }), null, 2) + '\n');
    return;
  }

  if (mode === 'diff') {
    const baseline = safeReadJSON(path);
    if (!baseline) {
      process.stdout.write(
        'No valid session-start baseline was found, so there is nothing to compare against yet.\n'
      );
      return;
    }
    const current = buildSnapshot({ projectPath });
    const delta = diffSnapshots(baseline, current);
    process.stdout.write(renderDelta(delta, { projectName: current.project }) + '\n');
    return;
  }

  // No recognized mode — print usage to stderr, exit 0 (never crash a hook).
  process.stderr.write(
    'usage: watchtower-snapshot.mjs (--emit-snapshot <path> | --snapshot | --diff <path>) --project-path <cwd> [--session-id <id>]\n'
  );
}

// Entry guard — importing this module must NOT run the CLI (mirrors the ring
// scripts so tests can import the pure functions).
const isMain = (() => {
  try {
    return process.argv[1]
      && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
  } catch {
    return false;
  }
})();

if (isMain) {
  try {
    main();
  } catch {
    process.exit(0);
  }
}

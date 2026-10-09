#!/usr/bin/env node

// Watchtower retrieval instrument — is memory REACHED? Measured, not assumed.
// (Simplification Project 1, prj:a4c6052b, plan B1, act:8b206268; the plan's
// decision (e): measure first, no pass bar at launch.)
//
// Three numbers per registered project per rolling 30 days, written to
// state/retrieval-instrument.json — Ring 1 renders them into the per-project
// state file's Standing Issues (the recall-canary relationship: the
// measurement is Ring 2's, the render is Ring 1's) and /briefing 3b reads the
// same file:
//
//   1. memory files read WITHOUT the recall verb — a Read tool_use on a
//      non-index file under the project's memory dir, over main AND subagent
//      transcripts, split by whether the file had a MEMORY.md line at compute
//      time (indexed_files_read / unindexed_files_read);
//   2. /recall invocations — lines the verb itself appends to
//      state/recall-log.jsonl (never transcript parsing for this half);
//   3. /recall hit rate — invocations with hits > 0 over invocations. Read it
//      as "reached the index": the match query is OR-joined tokens, so almost
//      any question returns SOMETHING (act:d727fe16).
//   4. /recall results read — invocations after which the SAME session Read
//      one of the paths the search returned (joined by session_id, then by
//      timestamp window). The honest quality proxy; `unjoined` counts the
//      lines no session could be found for. Lines logged by `caller: cli`
//      or `caller: probe` are excluded from every recall number (operator
//      ruling 2026-10-04): only a session choosing to ask counts.
//   5. the recall cue's numbers (recall-cue plan §B4, decision (e),
//      act:c0a878ec) — read from the cue's OWN log, state/recall-cue-log.jsonl,
//      over the same window, and reported SEPARATELY so a cue fire can never
//      inflate "a session chose to ask": cue_prompts (every line except
//      `unregistered`), cue_fires (`fired`), cue_injected (`injected: true`),
//      cue_followed (an injected line whose top path the SAME session Read
//      AFTER the line's ts — joined by the hook payload's session_id only,
//      never the timestamp fallback), cue_shadow_read (a would-fire line NOT
//      injected whose top path the session Read anyway after ts — the natural
//      rate the cue's effect is read against, Decision Default 30), and
//      cue_blind (busy + no-index + error).
//
// Metadata-only stub transcripts (a handful of cost-state / ai-title / mode
// records, no user or assistant turn, no cwd) are NOT sessions: they are
// counted as `stub_transcripts` and never inflate `sessions_scanned`
// (act:fb56c003).
//
// THE PREDICATE, PINNED (RETRIEVAL_PREDICATE below is the same text the test
// asserts): a session counts as a retrieval if it has at least one `tool_use`
// of `Read` whose `file_path` is under the memory dir and is not `MEMORY.md` /
// `MEMORY-archive.md`, EXCLUDING any session that has a `Write` or `Edit`
// tool_use on any path under the memory dir (a session reading back what it
// wrote is not recall). Excluded sessions are COUNTED
// (sessions_excluded_for_writes): under the one-writer decision they are the
// sessions that captured something, and the operator must see how much the
// exclusion hides when thresholds are set after month one.
//
// THE WALK. Sessions write transcripts under their own cwd slug, so
// ~/.claude/projects/ holds hundreds of `-…--mux-worktrees-*` dirs beside the
// main-checkout dirs, and the dispatch lanes most likely to invoke /recall live
// there. So EVERY ~/.claude/projects/*/ dir is enumerated; each transcript is
// attributed by its first cwd-bearing record through resolveProjectIdentity,
// falling back to resolveProjectFromTranscriptSlug for a cleaned-up worktree
// (the pair ring3-close's resolveSessionProject uses); main transcripts
// (<dir>/<sid>.jsonl) and subagent transcripts (<dir>/<sid>/subagents/
// agent-*.jsonl) are both walked, each subagent file attributed to its parent
// session so the write-exclusion sees the whole session. The memory dir is
// resolved ONCE per registered project through project-context.cjs over the
// registered path (the platform derives auto memory from the git repo, so
// worktrees share one memory dir; a worktree's own memory/ dir is residue).
// `file_path` is realpath-normalized before the under-memory-dir test.
//
// THE CACHE AND THE PRUNE. Re-walking every transcript each slow tick would
// parse gigabytes a day for a number that moves a few times a day, and the
// platform deletes transcripts at cleanupPeriodDays (default 30) — a stateless
// window silently shrinks to what survived. Per-transcript verdicts are cached
// in state/retrieval-instrument-cache.json keyed by (path, size, mtime); an
// entry whose file has been pruned is KEPT while its session is inside the
// window. The walk runs once a day (runRetrievalInstrument no-ops until the
// interval has passed since computed_at; config.defaults
// .retrieval_instrument_interval_hours, template default 24). No model call.
//
// AUTO-DREAM (plan B6, decision (g)). The platform's own background
// consolidation writes into the memory folder it manages; its signature is a
// memory file carrying a `modified:` frontmatter stamp with no session Write
// on that path in the window. Reported per project as
// autodream_modified_files, beside the sessions' numbers — nobody has
// measured whether what it writes is read, the same gap as ours.
//
// No `alert` field: thresholds are set AFTER the first month, by the operator,
// against the recorded baselines (plan §Measurement).

import {
  readFileSync, readdirSync, existsSync, statSync, realpathSync,
} from 'fs';
import { join, basename, dirname, resolve as resolvePath, relative, isAbsolute } from 'path';
import { homedir } from 'os';
import { createRequire } from 'module';
import { pathToFileURL } from 'url';
import {
  atomicWrite, loadConfig, slugify, getWatchtowerDir,
  resolveProjectIdentity, resolveProjectFromTranscriptSlug,
  log as _log, logError as _logError,
} from './watchtower-lib.mjs';
// The cue log's name is the cue's to define — one constant, referenced.
import { CUE_LOG_FILE } from './watchtower-recall-cue.mjs';

const require = createRequire(import.meta.url);

function log(msg) { _log('retrieval-instrument', msg); }
function logError(msg) { _logError('retrieval-instrument', msg); }

export const WINDOW_DAYS = 30;
export const DEFAULT_INTERVAL_HOURS = 24;
export const INDEX_FILES = ['MEMORY.md', 'MEMORY-archive.md'];
export const OUTPUT_FILE = 'retrieval-instrument.json';
export const CACHE_FILE = 'retrieval-instrument-cache.json';
export const RECALL_LOG_FILE = 'recall-log.jsonl';
const CACHE_VERSION = 3; // 2: per-transcript `messages` count + state-root reads (act:fb56c003, act:d727fe16); 3: `read_last_ts` per read path (act:c0a878ec)
const DAY_MS = 24 * 60 * 60 * 1000;

// Verbatim from the plan's decision (e). The test asserts this text; the
// code below implements exactly it and nothing broader.
export const RETRIEVAL_PREDICATE =
  'A session counts as a retrieval if it has at least one `tool_use` of `Read` '
  + 'whose `file_path` is under the memory dir and is not `MEMORY.md` / '
  + '`MEMORY-archive.md`, EXCLUDING any session that has a `Write` or `Edit` '
  + 'tool_use on any path under the memory dir (a session reading back what it '
  + 'wrote is not recall).';

const READ_TOOLS = new Set(['Read']);
const WRITE_TOOLS = new Set(['Write', 'Edit']);

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

function safeRealpath(p) {
  try { return realpathSync(p); } catch { return null; }
}

// realpath-normalize a path that may no longer exist: resolve the longest
// existing ancestor through realpath and re-append the remainder, so a
// symlinked parent (/var → /private/var, a mux-shared .claude/) compares
// equal to the memory dir's own realpath even after the file was deleted.
export function normalizePath(p) {
  if (typeof p !== 'string' || !p) return null;
  const abs = isAbsolute(p) ? p : resolvePath(p);
  const direct = safeRealpath(abs);
  if (direct) return direct;
  let cur = abs;
  const tail = [];
  while (cur !== dirname(cur)) {
    tail.unshift(basename(cur));
    cur = dirname(cur);
    const real = safeRealpath(cur);
    if (real) return join(real, ...tail);
  }
  return abs;
}

export function isUnder(path, dir) {
  return typeof path === 'string' && typeof dir === 'string' && path.startsWith(dir + '/');
}

export function isIndexFile(relPath) {
  return INDEX_FILES.includes(relPath);
}

// ---------------------------------------------------------------------------
// Memory dir resolution — project-context.cjs over the registered path
// ---------------------------------------------------------------------------
// The memory module ships scripts/project-context.cjs to every consumer, and
// the source tree carries it beside this file; the global runtime tier does
// NOT (watchtower-setup globs watchtower-*.mjs only). So: the consumer's own
// copy first, then the sibling; neither → the project is reported with
// memory_dir null and status 'memory-dir-unresolved', never a silent zero.
// The platform's rule is not re-derived here — one resolver, referenced.
export function loadProjectContext(projectPath) {
  const candidates = [
    projectPath ? join(projectPath, 'scripts', 'project-context.cjs') : null,
    join(dirname(new URL(import.meta.url).pathname), 'project-context.cjs'),
  ].filter(Boolean);
  for (const c of candidates) {
    if (!existsSync(c)) continue;
    try { return require(c); } catch (e) { logError(`project-context.cjs at ${c} failed to load: ${e.message}`); }
  }
  return null;
}

export function resolveMemoryDirFor(projectPath, { homeDir, deps = {} } = {}) {
  const pc = deps.projectContext || loadProjectContext(projectPath);
  if (!pc || typeof pc.resolveMemoryDir !== 'function') return null;
  try {
    const dir = pc.resolveMemoryDir({ cwd: projectPath, homeDir });
    return normalizePath(dir);
  } catch (e) {
    logError(`memory dir for ${projectPath}: ${e.message}`);
    return null;
  }
}

// The files MEMORY.md links to (`](name.md)`), relative to the memory dir —
// "had a MEMORY.md line at compute time" is membership in this set.
export function indexedFileSet(memoryDir) {
  const set = new Set();
  const indexPath = join(memoryDir, 'MEMORY.md');
  if (!existsSync(indexPath)) return set;
  let content;
  try { content = readFileSync(indexPath, 'utf8'); } catch { return set; }
  const re = /\]\(([^)\s]+\.md)\)/g;
  let m;
  while ((m = re.exec(content)) !== null) {
    const target = m[1].replace(/^\.\//, '');
    if (/^[a-z]+:\/\//i.test(target) || isAbsolute(target)) continue;
    set.add(target);
  }
  return set;
}

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

// Every transcript file under <projectsRoot>, each with its session key —
// <dir>/<sid> for both the main file and its subagent files, so a session's
// verdict is the union of its parts.
export function walkTranscriptFiles(projectsRoot) {
  const out = [];
  if (!existsSync(projectsRoot)) return out;
  let dirs;
  try { dirs = readdirSync(projectsRoot, { withFileTypes: true }); } catch { return out; }
  for (const d of dirs) {
    if (!d.isDirectory()) continue;
    const dirPath = join(projectsRoot, d.name);
    let entries;
    try { entries = readdirSync(dirPath, { withFileTypes: true }); } catch { continue; }
    for (const e of entries) {
      if (e.isFile() && e.name.endsWith('.jsonl')) {
        const sid = e.name.slice(0, -'.jsonl'.length);
        out.push({ path: join(dirPath, e.name), dir_slug: d.name, session_id: sid,
          session_key: join(dirPath, sid), is_subagent: false });
      } else if (e.isDirectory()) {
        const subDir = join(dirPath, e.name, 'subagents');
        if (!existsSync(subDir)) continue;
        let subs;
        try { subs = readdirSync(subDir); } catch { continue; }
        for (const s of subs) {
          if (!/^agent-.*\.jsonl$/.test(s)) continue;
          out.push({ path: join(subDir, s), dir_slug: d.name, session_id: e.name,
            session_key: join(dirPath, e.name), is_subagent: true });
        }
      }
    }
  }
  return out;
}

// scanTranscript — ONE transcript file → its verdict, project-agnostic: the
// cwd of its first cwd-bearing record, first/last timestamps, and every Read /
// Write / Edit file_path that lands under ANY of the given memory dirs
// (realpath-normalized). Only memory-dir paths are kept so the cache stays
// small; the per-project split happens at count time. `read_last_ts` keeps
// the LATEST record timestamp each path was Read at: "was it Read after
// the cue line's ts" is exactly "is its latest Read later than ts", and one
// stamp per path keeps the cache flat (act:c0a878ec).
export function scanTranscript(path, memoryDirs) {
  const verdict = { cwd: null, first_ts: null, last_ts: null, reads: [], writes: [], read_last_ts: {}, messages: 0 };
  let raw;
  try { raw = readFileSync(path, 'utf8'); } catch { return verdict; }
  const reads = new Set();
  const writes = new Set();
  const readLast = {};
  const underAny = (p) => memoryDirs.some((d) => isUnder(p, d));
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t) continue;
    let o;
    try { o = JSON.parse(t); } catch { continue; }
    if (!verdict.cwd && typeof o.cwd === 'string' && o.cwd) verdict.cwd = o.cwd;
    const ts = o.timestamp || o.time || o.ts;
    if (typeof ts === 'string' && ts) {
      if (!verdict.first_ts) verdict.first_ts = ts;
      verdict.last_ts = ts;
    }
    const role = o.role || o.type;
    if (role === 'user' || role === 'assistant') verdict.messages += 1;
    if (role !== 'assistant') continue;
    const content = o.message?.content ?? o.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (!block || block.type !== 'tool_use') continue;
      const fp = block.input?.file_path;
      if (typeof fp !== 'string' || !fp) continue;
      if (!READ_TOOLS.has(block.name) && !WRITE_TOOLS.has(block.name)) continue;
      const norm = normalizePath(fp);
      if (!underAny(norm)) continue;
      if (READ_TOOLS.has(block.name)) {
        reads.add(norm);
        const ms = typeof ts === 'string' ? Date.parse(ts) : NaN;
        if (!Number.isNaN(ms) && !(readLast[norm] && Date.parse(readLast[norm]) >= ms)) readLast[norm] = ts;
      } else {
        writes.add(norm);
      }
    }
  }
  verdict.reads = [...reads];
  verdict.writes = [...writes];
  verdict.read_last_ts = readLast;
  return verdict;
}

// ---------------------------------------------------------------------------
// The recall log (written by the verb itself — never transcript parsing)
// ---------------------------------------------------------------------------

export function loadRecallLog(path, { nowMs, windowDays = WINDOW_DAYS } = {}) {
  if (!existsSync(path)) return [];
  let raw;
  try { raw = readFileSync(path, 'utf8'); } catch { return []; }
  const out = [];
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t) continue;
    let o;
    try { o = JSON.parse(t); } catch { continue; }
    const ts = Date.parse(o.ts);
    if (Number.isNaN(ts)) continue;
    if (nowMs - ts > windowDays * DAY_MS) continue;
    out.push(o);
  }
  return out;
}

// Lines the verb logs under these callers are not a session choosing to ask
// (operator ruling 2026-10-04, act:d727fe16): `cli` is a human or a script at
// a shell, `probe` is the launch probe's own harness.
export const EXCLUDED_RECALL_CALLERS = new Set(['cli', 'probe']);
// Timestamp-window join slack: a log line's ts sits inside the session's
// [first, last] record timestamps, give or take a few minutes of clock skew.
export const RECALL_JOIN_SLACK_MS = 5 * 60 * 1000;

const rate3 = (num, den) => (den > 0 ? Math.round((num / den) * 1000) / 1000 : null);

function recallNumbers(lines, projectName, projectSessions = []) {
  const slug = slugify(projectName);
  const all = lines.filter((l) => l.project === projectName || l.project === slug);
  const isExcluded = (l) => EXCLUDED_RECALL_CALLERS.has(String(l.caller || ''));
  const mine = all.filter((l) => !isExcluded(l));
  const invocations = mine.length;
  const hitLines = mine.filter((l) => Number(l.hits) > 0);
  const bySource = {};
  for (const l of hitLines) {
    for (const [src, n] of Object.entries(l.hits_by_source || {})) {
      bySource[src] = (bySource[src] || 0) + (Number(n) || 0);
    }
  }
  // The quality proxy: did the session then READ a path the search returned?
  // Join by session_id (the skill passes --session-id; the verb logs it),
  // falling back to the ONE session whose record-timestamp window holds the
  // line's ts; two candidates is ambiguous and counts as unjoined, never a
  // guess.
  const byId = new Map();
  for (const s of projectSessions) if (s.session_id) byId.set(String(s.session_id), s);
  let resultsRead = 0;
  let unjoined = 0;
  for (const l of mine) {
    let s = l.session_id ? byId.get(String(l.session_id)) : null;
    if (!s) {
      const t = Date.parse(l.ts);
      const cands = projectSessions.filter((c) => c.first_ms && c.last_ms
        && t >= c.first_ms - RECALL_JOIN_SLACK_MS && t <= c.last_ms + RECALL_JOIN_SLACK_MS);
      s = cands.length === 1 ? cands[0] : null;
    }
    if (!s) { unjoined += 1; continue; }
    const tops = (Array.isArray(l.top_paths) ? l.top_paths : []).map((p) => normalizePath(String(p)));
    if (tops.some((p) => s.reads.has(p))) resultsRead += 1;
  }
  return {
    recall_invocations: invocations,
    recall_hits: hitLines.length,
    recall_hit_rate: rate3(hitLines.length, invocations),
    recall_hits_by_source: bySource,
    recall_results_read: resultsRead,
    recall_results_read_rate: rate3(resultsRead, invocations),
    recall_unjoined: unjoined,
    recall_excluded_lines: all.length - mine.length,
  };
}

// ---------------------------------------------------------------------------
// The cue log (written by the cue hook — its OWN log, never recall-log.jsonl)
// ---------------------------------------------------------------------------

export const CUE_BLIND_OUTCOMES = new Set(['busy', 'no-index', 'error']);

// cueNumbers — the recall cue's six numbers for one project (recall-cue plan
// §B4). The follow join is by session_id ONLY: every cue line carries the
// hook payload's real session_id, so a line that joins nothing is a pruned
// or unscanned transcript, not an ambiguity a timestamp window should guess
// at. "Read after the line's ts" reads the session's latest Read stamp for
// the named path (scanTranscript's read_last_ts).
function cueNumbers(lines, projectName, projectSessions = []) {
  const slug = slugify(projectName);
  const mine = lines.filter((l) => l.outcome !== 'unregistered'
    && (l.project === projectName || l.project === slug));
  const byId = new Map();
  for (const s of projectSessions) if (s.session_id) byId.set(String(s.session_id), s);
  const readAfter = (l) => {
    const s = l.session_id ? byId.get(String(l.session_id)) : null;
    if (!s) return false;
    const top = Array.isArray(l.top3) && l.top3[0] ? normalizePath(String(l.top3[0].path || '')) : null;
    if (!top) return false;
    const lastRead = s.read_last_ms.get(top);
    const lineMs = Date.parse(l.ts);
    return typeof lastRead === 'number' && !Number.isNaN(lineMs) && lastRead > lineMs;
  };
  let fires = 0;
  let injected = 0;
  let followed = 0;
  let shadowRead = 0;
  let blind = 0;
  for (const l of mine) {
    if (CUE_BLIND_OUTCOMES.has(l.outcome)) blind += 1;
    if (l.outcome !== 'fired') continue;
    fires += 1;
    if (l.injected === true) {
      injected += 1;
      if (readAfter(l)) followed += 1;
    } else if (readAfter(l)) {
      shadowRead += 1;
    }
  }
  return {
    cue_prompts: mine.length,
    cue_fires: fires,
    cue_shadow_read: shadowRead,
    cue_injected: injected,
    cue_followed: followed,
    cue_blind: blind,
  };
}

// ---------------------------------------------------------------------------
// Auto-dream's signature
// ---------------------------------------------------------------------------

const MODIFIED_RE = /^\s*modified:\s*(\S+)/m;

export function modifiedStamp(content) {
  if (typeof content !== 'string' || !content.startsWith('---')) return null;
  const end = content.indexOf('\n---', 3);
  if (end === -1) return null;
  const fm = content.slice(3, end);
  const m = MODIFIED_RE.exec(fm);
  if (!m) return null;
  const t = Date.parse(m[1]);
  return Number.isNaN(t) ? null : t;
}

function walkMd(dir, out = []) {
  let entries;
  try { entries = readdirSync(dir, { withFileTypes: true }); } catch { return out; }
  for (const e of entries) {
    const p = join(dir, e.name);
    if (e.isDirectory()) walkMd(p, out);
    else if (e.isFile() && e.name.endsWith('.md')) out.push(p);
  }
  return out;
}

// Files under the memory dir with a `modified:` stamp inside the window and
// no session Write/Edit on that path in the window. Returns the count and the
// all-time stamped count (the ledger's reconciliation number).
export function autodreamFiles(memoryDir, { nowMs, windowDays = WINDOW_DAYS, sessionWrites = new Set() } = {}) {
  let stampedTotal = 0;
  let inWindowNoWrite = 0;
  for (const p of walkMd(memoryDir)) {
    let content;
    try { content = readFileSync(p, 'utf8'); } catch { continue; }
    const stamp = modifiedStamp(content);
    if (stamp == null) continue;
    stampedTotal += 1;
    if (nowMs - stamp > windowDays * DAY_MS) continue;
    if (sessionWrites.has(normalizePath(p))) continue;
    inWindowNoWrite += 1;
  }
  return { autodream_modified_files: inWindowNoWrite, stamped_total: stampedTotal };
}

// ---------------------------------------------------------------------------
// Attribution — the ring3-close resolver pair
// ---------------------------------------------------------------------------

function attribute({ cwd, dir_slug }, config, resolveOpts, deps) {
  const resolveIdentity = deps.resolveIdentity || resolveProjectIdentity;
  const resolveSlug = deps.resolveSlug || resolveProjectFromTranscriptSlug;
  if (cwd) {
    const id = resolveIdentity(cwd, config, resolveOpts);
    if (id && id.registered) return id;
  }
  if (dir_slug) {
    try {
      const fromSlug = resolveSlug(dir_slug, config, resolveOpts);
      if (fromSlug && fromSlug.registered) return fromSlug;
    } catch (e) {
      logError(`slug resolution failed for ${dir_slug}: ${e.message}`);
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// computeRetrievalInstrument — PURE apart from reading the fixture tree.
// Returns { report, cache }; the caller decides where they land.
// ---------------------------------------------------------------------------

export function computeRetrievalInstrument({
  config, homeDir = homedir(), watchtowerDir = getWatchtowerDir(),
  nowMs = Date.now(), windowDays = WINDOW_DAYS, cache = null, deps = {},
} = {}) {
  const scan = deps.scanTranscript || scanTranscript;
  const projectsRoot = join(homeDir, '.claude', 'projects');
  const resolveOpts = {
    muxProjectsPath: join(homeDir, '.config', 'mux', 'projects.json'),
    registryPath: join(homeDir, '.claude', 'cc-registry.json'),
    homeDir, // the slug resolver's anchor (act:5ebf992e)
  };
  const windowMs = windowDays * DAY_MS;
  const inWindow = (ms) => typeof ms === 'number' && !Number.isNaN(ms) && nowMs - ms <= windowMs;

  // 1. Registered projects → memory dirs (once each).
  const projects = {};
  for (const [name, entry] of Object.entries(config?.projects || {})) {
    const projectPath = entry?.path || entry;
    if (typeof projectPath !== 'string') continue;
    const memoryDir = resolveMemoryDirFor(projectPath, { homeDir, deps });
    projects[name] = {
      name, path: projectPath, memoryDir,
      indexed: memoryDir ? indexedFileSet(memoryDir) : new Set(),
      sessions: new Map(), // session_key → { parts: [], last_ms }
    };
  }
  const memoryDirs = Object.values(projects).map((p) => p.memoryDir).filter(Boolean);
  // Reads are kept for the memory dirs AND the watchtower state root — the
  // recall corpus (session records, archived drafts, retained transcripts)
  // lives there, and the results-read join needs those paths (act:d727fe16).
  // Counting still keys on the project's memory dir alone.
  const scanRoots = [...memoryDirs, join(watchtowerDir, 'state')];
  const memoryDirsKey = [...scanRoots].sort().join('\n');
  // A stub's transcript dir names its project when cwd is absent: the dir
  // slug is the encoded main-checkout path (act:fb56c003).
  const slugToProject = new Map(Object.values(projects)
    .map((p) => [String(p.path).replace(/[^a-zA-Z0-9]/g, '-'), p.name]));

  // 2. The cache: reused only when it was built against the same memory-dir
  // set (a newly registered project must not inherit verdicts that never
  // looked at its dir).
  const prior = (cache && cache.version === CACHE_VERSION && cache.memory_dirs_key === memoryDirsKey)
    ? (cache.entries || {}) : {};
  const entries = {};

  // 3. Walk every transcript dir; scan what changed; keep pruned entries.
  const seen = new Set();
  let scanned = 0;
  for (const f of walkTranscriptFiles(projectsRoot)) {
    seen.add(f.path);
    let st;
    try { st = statSync(f.path); } catch { continue; }
    const key = { size: st.size, mtime: st.mtimeMs };
    const old = prior[f.path];
    if (old && old.size === key.size && old.mtime === key.mtime) {
      entries[f.path] = old;
      continue;
    }
    // A file whose mtime is already outside the window cannot hold an
    // in-window session — skip the parse, record the stat so it is not
    // re-stat'd into a scan next time.
    if (!inWindow(st.mtimeMs)) {
      entries[f.path] = { ...key, dir_slug: f.dir_slug, session_key: f.session_key,
        is_subagent: f.is_subagent, cwd: null, first_ts: null, last_ts: null,
        reads: [], writes: [], out_of_window: true };
      continue;
    }
    const v = scan(f.path, scanRoots);
    scanned += 1;
    entries[f.path] = { ...key, dir_slug: f.dir_slug, session_key: f.session_key,
      is_subagent: f.is_subagent, ...v };
  }
  // Pruned transcripts: keep the verdict while its session is in the window.
  for (const [path, old] of Object.entries(prior)) {
    if (seen.has(path)) continue;
    const lastMs = Date.parse(old.last_ts || '') || old.mtime;
    if (inWindow(lastMs)) entries[path] = old;
  }

  // 4. Group by session and attribute.
  const sessions = new Map();
  for (const [path, e] of Object.entries(entries)) {
    if (e.out_of_window) continue;
    const s = sessions.get(e.session_key) || {
      key: e.session_key, session_id: basename(e.session_key), dir_slug: e.dir_slug, cwd: null,
      first_ms: 0, last_ms: 0, messages: 0, reads: new Set(), writes: new Set(), read_last_ms: new Map(), parts: 0,
    };
    if (!e.is_subagent && e.cwd) s.cwd = e.cwd;
    else if (!s.cwd && e.cwd) s.cwd = e.cwd;
    const lastMs = Date.parse(e.last_ts || '') || e.mtime || 0;
    if (lastMs > s.last_ms) s.last_ms = lastMs;
    const firstMs = Date.parse(e.first_ts || '') || 0;
    if (firstMs && (!s.first_ms || firstMs < s.first_ms)) s.first_ms = firstMs;
    s.messages += e.messages || 0;
    for (const r of e.reads || []) s.reads.add(r);
    for (const w of e.writes || []) s.writes.add(w);
    // The latest Read stamp per path across the session's parts (main +
    // subagents) — the cue's "Read after ts" test (act:c0a878ec).
    for (const [p, stamp] of Object.entries(e.read_last_ts || {})) {
      const ms = Date.parse(stamp);
      if (Number.isNaN(ms)) continue;
      if (!(s.read_last_ms.get(p) >= ms)) s.read_last_ms.set(p, ms);
    }
    s.parts += 1;
    sessions.set(e.session_key, s);
    void path;
  }

  let unattributed = 0;
  let stubs = 0;
  const byProject = {};
  const projectSessions = {};
  for (const name of Object.keys(projects)) {
    byProject[name] = { scanned: 0, excluded: 0, reading: 0, stubs: 0, indexed: new Set(), unindexed: new Set(), writes: new Set() };
    projectSessions[name] = [];
  }
  for (const s of sessions.values()) {
    if (!inWindow(s.last_ms)) continue;
    const id = attribute({ cwd: s.cwd, dir_slug: s.dir_slug }, config, resolveOpts, deps);
    // A metadata-only stub is not a session: count it, attribute it by its
    // transcript dir when nothing else can, and never let it into the
    // denominator (act:fb56c003).
    if ((s.messages || 0) === 0) {
      stubs += 1;
      const owner = (id && byProject[id.name]) ? id.name : slugToProject.get(s.dir_slug);
      if (owner && byProject[owner]) byProject[owner].stubs += 1;
      continue;
    }
    if (!id || !byProject[id.name]) { unattributed += 1; continue; }
    const proj = projects[id.name];
    const acc = byProject[id.name];
    projectSessions[id.name].push(s);
    acc.scanned += 1;
    if (!proj.memoryDir) continue;
    const memWrites = [...s.writes].filter((w) => isUnder(w, proj.memoryDir));
    if (memWrites.length > 0) {
      acc.excluded += 1;
      for (const w of memWrites) acc.writes.add(w);
      continue; // the whole session is excluded — the pinned predicate
    }
    let counted = false;
    for (const r of s.reads) {
      if (!isUnder(r, proj.memoryDir)) continue;
      const rel = relative(proj.memoryDir, r);
      if (isIndexFile(rel)) continue;
      counted = true;
      if (proj.indexed.has(rel)) acc.indexed.add(rel);
      else acc.unindexed.add(rel);
    }
    if (counted) acc.reading += 1;
  }

  // 5. The recall log, once — and the cue log, once, as a SEPARATE list
  // that never feeds the recall numbers (decision (e), act:c0a878ec).
  const recallLines = loadRecallLog(join(watchtowerDir, 'state', RECALL_LOG_FILE), { nowMs, windowDays });
  const cueLines = loadRecallLog(join(watchtowerDir, 'state', CUE_LOG_FILE), { nowMs, windowDays });

  // 6. Assemble.
  const computedAt = new Date(nowMs).toISOString();
  const report = {
    computed_at: computedAt,
    window_days: windowDays,
    predicate: RETRIEVAL_PREDICATE,
    transcripts_scanned_this_run: scanned,
    transcripts_cached: Object.keys(entries).length,
    unattributed_sessions: unattributed,
    stub_transcripts: stubs,
    projects: {},
  };
  for (const [name, proj] of Object.entries(projects)) {
    const acc = byProject[name];
    const dream = proj.memoryDir
      ? autodreamFiles(proj.memoryDir, { nowMs, windowDays, sessionWrites: acc.writes })
      : { autodream_modified_files: 0, stamped_total: 0 };
    report.projects[name] = {
      window_days: windowDays,
      status: proj.memoryDir ? 'measured' : 'memory-dir-unresolved',
      memory_dir: proj.memoryDir,
      indexed_files_read: acc.indexed.size,
      unindexed_files_read: acc.unindexed.size,
      sessions_reading: acc.reading,
      sessions_scanned: acc.scanned,
      sessions_excluded_for_writes: acc.excluded,
      stub_transcripts: acc.stubs,
      ...recallNumbers(recallLines, name, projectSessions[name]),
      ...cueNumbers(cueLines, name, projectSessions[name]),
      autodream_modified_files: dream.autodream_modified_files,
      autodream_stamped_files_total: dream.stamped_total,
      computed_at: computedAt,
    };
  }

  return {
    report,
    cache: { version: CACHE_VERSION, memory_dirs_key: memoryDirsKey, computed_at: computedAt, entries },
  };
}

// ---------------------------------------------------------------------------
// runRetrievalInstrument — the Ring 2 slow I/O wrapper with the daily gate
// ---------------------------------------------------------------------------

export function readReport(watchtowerDir = getWatchtowerDir()) {
  const p = join(watchtowerDir, 'state', OUTPUT_FILE);
  if (!existsSync(p)) return null;
  try { return JSON.parse(readFileSync(p, 'utf8')); } catch { return null; }
}

function readCache(watchtowerDir) {
  const p = join(watchtowerDir, 'state', CACHE_FILE);
  if (!existsSync(p)) return null;
  try { return JSON.parse(readFileSync(p, 'utf8')); } catch { return null; }
}

export function runRetrievalInstrument(config, {
  nowMs = Date.now(), homeDir = homedir(), watchtowerDir = getWatchtowerDir(),
  force = false, deps = {},
} = {}) {
  const projectCount = Object.keys(config?.projects || {}).length;
  if (projectCount === 0) {
    return { skipped: true, reason: 'no registered projects' };
  }
  const intervalHours = Number(config?.defaults?.retrieval_instrument_interval_hours);
  const interval = (Number.isFinite(intervalHours) && intervalHours > 0 ? intervalHours : DEFAULT_INTERVAL_HOURS) * 60 * 60 * 1000;
  const prev = readReport(watchtowerDir);
  if (!force && prev && prev.computed_at) {
    const age = nowMs - Date.parse(prev.computed_at);
    if (!Number.isNaN(age) && age >= 0 && age < interval) {
      return { skipped: true, reason: `computed ${Math.round(age / 60000)}m ago (interval ${interval / 3600000}h)`, computed_at: prev.computed_at };
    }
  }
  const started = Date.now();
  const { report, cache } = computeRetrievalInstrument({
    config, homeDir, watchtowerDir, nowMs, cache: readCache(watchtowerDir), deps,
  });
  atomicWrite(join(watchtowerDir, 'state', CACHE_FILE), JSON.stringify(cache) + '\n');
  atomicWrite(join(watchtowerDir, 'state', OUTPUT_FILE), JSON.stringify(report, null, 2) + '\n');
  const summary = Object.entries(report.projects)
    .map(([n, p]) => `${n}: ${p.indexed_files_read}+${p.unindexed_files_read} files read by ${p.sessions_reading}/${p.sessions_scanned} sessions (${p.sessions_excluded_for_writes} excl, ${p.stub_transcripts} stubs), recall ${p.recall_invocations} inv, ${p.recall_results_read} read a result, ${p.recall_unjoined} unjoined, cue ${p.cue_prompts} prompts/${p.cue_fires} would-fire/${p.cue_injected} injected/${p.cue_followed} followed/${p.cue_shadow_read} read unprompted/${p.cue_blind} blind`)
    .join('; ');
  log(`computed over ${report.transcripts_cached} transcript(s), ${report.transcripts_scanned_this_run} scanned, in ${Date.now() - started}ms — ${summary}`);
  return report;
}

// ---------------------------------------------------------------------------
// CLI (maintainer-run; the ring calls runRetrievalInstrument directly)
//   node watchtower-retrieval-instrument.mjs [--force] [--watchtower-dir D] [--home H] [--json]
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const out = { force: false, json: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--force') out.force = true;
    else if (a === '--json') out.json = true;
    else if (a === '--watchtower-dir') out.watchtowerDir = argv[++i];
    else if (a === '--home') out.homeDir = argv[++i];
    else if (a === '--config') out.configPath = argv[++i];
  }
  return out;
}

const isMain = (() => {
  try { return process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
})();
if (isMain) {
  const args = parseArgs(process.argv.slice(2));
  const watchtowerDir = args.watchtowerDir || getWatchtowerDir();
  const config = args.configPath
    ? JSON.parse(readFileSync(args.configPath, 'utf8'))
    : loadConfig(watchtowerDir);
  const result = runRetrievalInstrument(config, {
    force: args.force, watchtowerDir, homeDir: args.homeDir || homedir(),
  });
  if (args.json) process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  else if (result.skipped) console.log(`skipped: ${result.reason}`);
  else console.log(`wrote ${join(watchtowerDir, 'state', OUTPUT_FILE)}`);
}

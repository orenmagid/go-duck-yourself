#!/usr/bin/env node

// Watchtower recall — the retriever behind `/recall` and `/memory`.
// (Simplification Project 1, prj:a4c6052b, plan B2, act:79be8765; the plan's
// decision (d): the retriever is the build's centerpiece, keyword first,
// embeddings held back until the instrument shows keyword recall short.)
//
// ONE SQLite FTS5 index per registered project, in the runtime tier beside
// the per-session records it indexes:
//   ~/.claude-cabinet/watchtower/state/recall/<slug>.sqlite
// (never .claude/state/ — worktrees copy .claude/ and share memory by project
// identity, so a project-level index in the runtime tier avoids N stale copies
// and never enters git.)
//
// THE CORPUS, four sources, each tagged in the `source` column:
//   sessions     state/projects/<slug>/sessions/*.md        (Ring 3 Phase 2b)
//   memory       every *.md under the project's memory dir EXCEPT the ONE
//                exclusion list below — MEMORY.md, MEMORY-archive.md and any
//                MEMORY-pre-trim-*.md copy (front of mind is already loaded;
//                an index, or a copy of one, would dominate every hit list).
//                MEMORY_INDEX_EXCLUSIONS is defined HERE and referenced by the
//                index-trim script (plan C2) — one list, two readers.
//   archive      state/recall/<slug>/archive/extraction-drafts/*.md (plan C1;
//                runtime state, OUTSIDE the memory dir so the platform's own
//                consolidation never reads the drafts as memory — decision (g))
//   transcripts  state/projects/<slug>/transcripts/*.txt (plan B5 retention)
// A source dir that does not exist yet is ZERO docs for that source, never an
// error — the archive is written by a later lane, the transcripts by the first
// close after the reinstall; the next refresh picks them up.
//
// SCHEMA (decision default 7): docs(path PRIMARY KEY, source, mtime, size,
// title); chunks = fts5(path UNINDEXED, source UNINDEXED, heading, body,
// tokenize='porter unicode61'). Each file is split into ~1,500-char chunks at
// paragraph boundaries with the nearest preceding heading carried into
// `heading`, so a hit lands on the right paragraph of a long record, not on
// the file. Ranking: bm25(chunks, 0, 0, 2.0, 1.0) — heading weighted 2:1 over
// body. Result rows are the best chunk per path.
//
// RESULT MIXING (decision default 25): best-chunk-per-path top-k over one
// table lets a 100-chunk transcript hold 100 lottery tickets against a memory
// file's one, and the archived drafts are known near-duplicates — so the
// default result set is SOURCE-INTERLEAVED: per-source cap 3, merged by score,
// top-k (default 8), snippet <= 400 chars. `--source` narrows on request.
//
// REFRESH (default 5): refreshIndex() stat-walks the corpus and re-chunks
// only files whose (mtime, size) changed, deleting rows for files gone from
// disk. It runs at the start of EVERY `search` (a recall never reads a stale
// index) and on Ring 1's tick (plan B3, so the first recall of a session is
// fast). A missing index is BUILT, never treated as empty. If the corpus
// resolves to no files at all the script says so ({ status: 'empty-corpus',
// reason }) rather than returning []. Two refreshers racing on one index:
// busy_timeout 5s; the refresh is keyed by (path, mtime, size) so
// last-writer-wins yields the same rows; a SQLITE_BUSY past the timeout logs
// and the search runs on the stale-but-valid index rather than failing.
//
// REFUSALS (default 10), all loud: an unregistered cwd exits 2 naming
// `/watchtower configure`; better-sqlite3 unresolvable → `sqlite-unavailable`
// (exit 3); a build without FTS5 → `fts5-unavailable` (exit 3); never a LIKE
// fallback, never [] with exit 0 on a corpus that has files. better-sqlite3
// is resolved ONLY through watchtower-lib's loadBetterSqlite3(projectPath) —
// a second resolver would re-create the act:b9414039 shadow-copy bug.
//
// THE LOG (default 11, 26): every `search` appends ONE line to
// state/recall-log.jsonl — { ts, project, query, k, hits, hits_by_source,
// top_paths, session_id, caller } — `hits` = result count (hits: 0 is the
// empty outcome the retrieval instrument counts), `project` = the REGISTERED
// project name, `session_id` from --session-id (the /recall skill passes the
// id the session-start acknowledgment line carries; CLAUDE_SESSION_ID is NOT
// set in the Bash tool's environment), `caller` skill | probe | cli.
//
// CLI:
//   node watchtower-recall.mjs search --cwd <dir> --query "<q>" [--k 8]
//        [--source sessions|memory|archive|transcripts] [--session-id <id>]
//        [--caller skill|probe|cli] [--json]
//   node watchtower-recall.mjs build  --cwd <dir> [--json]
//   node watchtower-recall.mjs status --cwd <dir> [--json]
// Honours WATCHTOWER_DIR like its siblings; --home redirects the memory-dir /
// registry resolution for fixtures.

import {
  readFileSync, readdirSync, existsSync, statSync, mkdirSync, appendFileSync,
  realpathSync,
} from 'fs';
import { join, basename, dirname, resolve as resolvePath } from 'path';
import { homedir } from 'os';
import { pathToFileURL } from 'url';
import {
  loadConfig, slugify, getWatchtowerDir, resolveProjectIdentity, loadBetterSqlite3,
  log as _log, logError as _logError,
} from './watchtower-lib.mjs';
import { resolveMemoryDirFor } from './watchtower-retrieval-instrument.mjs';

function log(msg) { _log('recall', msg); }
function logError(msg) { _logError('recall', msg); }

// ---------------------------------------------------------------------------
// Constants — the decision defaults, in code
// ---------------------------------------------------------------------------

export const SOURCES = ['sessions', 'memory', 'archive', 'transcripts'];
export const DEFAULT_K = 8;
export const PER_SOURCE_CAP = 3;
export const CHUNK_TARGET_CHARS = 1500;
export const SNIPPET_MAX_CHARS = 400;
export const BUSY_TIMEOUT_MS = 5000;
export const RECALL_LOG_FILE = 'recall-log.jsonl';
export const CALLERS = ['skill', 'probe', 'cli'];
const CANDIDATE_LIMIT = 400;
const SCHEMA_VERSION = 1;

// The ONE exclusion list for the memory dir. Exact names plus the pre-trim
// copy shape plan C2 writes (outside the memory dir by design, but excluded by
// name too so a stray copy can never dominate a hit list).
export const MEMORY_INDEX_EXCLUSIONS = ['MEMORY.md', 'MEMORY-archive.md', 'MEMORY-pre-trim-*.md'];

export function isExcludedMemoryFile(name) {
  const base = basename(name);
  for (const pattern of MEMORY_INDEX_EXCLUSIONS) {
    if (pattern.includes('*')) {
      const [prefix, suffix] = pattern.split('*');
      if (base.startsWith(prefix) && base.endsWith(suffix) && base.length >= prefix.length + suffix.length) return true;
    } else if (base === pattern) {
      return true;
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

export function indexPathFor(slug, watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', 'recall', `${slug}.sqlite`);
}

export function recallLogPath(watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', RECALL_LOG_FILE);
}

export function sourceDirs(slug, { watchtowerDir = getWatchtowerDir(), memoryDir = null } = {}) {
  return {
    sessions: join(watchtowerDir, 'state', 'projects', slug, 'sessions'),
    memory: memoryDir,
    archive: join(watchtowerDir, 'state', 'recall', slug, 'archive', 'extraction-drafts'),
    transcripts: join(watchtowerDir, 'state', 'projects', slug, 'transcripts'),
  };
}

function walkFiles(dir, accept, out = []) {
  let entries;
  try { entries = readdirSync(dir, { withFileTypes: true }); } catch { return out; }
  for (const e of entries) {
    const p = join(dir, e.name);
    if (e.isDirectory()) walkFiles(p, accept, out);
    else if (e.isFile() && accept(e.name)) out.push(p);
  }
  return out;
}

// Every file in the corpus with its source tag. A missing dir is simply
// absent from the walk — zero docs for that source.
export function listCorpus(slug, opts = {}) {
  const dirs = sourceDirs(slug, opts);
  const out = [];
  const add = (source, dir, accept) => {
    if (!dir || !existsSync(dir)) return;
    for (const p of walkFiles(dir, accept)) out.push({ path: p, source });
  };
  add('sessions', dirs.sessions, (n) => n.endsWith('.md'));
  add('memory', dirs.memory, (n) => n.endsWith('.md') && !isExcludedMemoryFile(n));
  add('archive', dirs.archive, (n) => n.endsWith('.md'));
  add('transcripts', dirs.transcripts, (n) => n.endsWith('.txt'));
  return out;
}

// ---------------------------------------------------------------------------
// Chunking — pure
// ---------------------------------------------------------------------------

const HEADING_RE = /^#{1,6}\s+(.+?)\s*#*\s*$/;
// Retained transcripts carry no markdown headings; their turn markers are the
// nearest thing, so a hit on a transcript says who was speaking.
const TURN_RE = /^\[(user|assistant)\]$/;

function splitOversized(text, target) {
  const parts = [];
  let rest = text;
  while (rest.length > target) {
    let cut = rest.lastIndexOf('\n', target);
    if (cut < target / 2) cut = rest.lastIndexOf(' ', target);
    if (cut < target / 2) cut = target;
    parts.push(rest.slice(0, cut).trim());
    rest = rest.slice(cut).trim();
  }
  if (rest) parts.push(rest);
  return parts.filter(Boolean);
}

// Line scanner: a heading line closes the current chunk and names the next;
// a blank line ends a paragraph; paragraphs accumulate into a chunk until the
// next one would push it past the target. A single paragraph longer than the
// target is split (at a newline, else a space), never dropped.
export function chunkDocument(text, { target = CHUNK_TARGET_CHARS } = {}) {
  const chunks = [];
  let heading = '';
  let buf = [];
  let bufLen = 0;
  let para = [];
  const flush = () => {
    if (buf.length === 0) return;
    const body = buf.join('\n\n').trim();
    if (body) chunks.push({ heading, body });
    buf = []; bufLen = 0;
  };
  const endPara = () => {
    if (para.length === 0) return;
    const p = para.join('\n').trim();
    para = [];
    if (!p) return;
    const pieces = p.length > target ? splitOversized(p, target) : [p];
    for (const piece of pieces) {
      if (bufLen > 0 && bufLen + piece.length + 2 > target) flush();
      buf.push(piece);
      bufLen += piece.length + 2;
    }
  };
  for (const line of String(text || '').replace(/\r\n/g, '\n').split('\n')) {
    const h = HEADING_RE.exec(line);
    const turn = TURN_RE.exec(line.trim());
    if (h || turn) {
      endPara();
      flush();
      heading = h ? h[1].trim() : turn[1];
      continue;
    }
    if (line.trim() === '') { endPara(); continue; }
    para.push(line);
  }
  endPara();
  flush();
  return chunks;
}

export function titleOf(text, path) {
  const m = /^#\s+(.+?)\s*$/m.exec(text || '');
  if (m) return m[1].trim();
  const fm = /^---\n([\s\S]*?)\n---/.exec(text || '');
  if (fm) {
    const d = /^(?:description|name):\s*(.+)$/m.exec(fm[1]);
    if (d) return d[1].trim().replace(/^["']|["']$/g, '');
  }
  return basename(path).replace(/\.(md|txt)$/, '');
}

// ---------------------------------------------------------------------------
// The FTS5 match query — each whitespace token as a phrase, OR-joined, so a
// fid like act:79be8765 (which unicode61 splits at the colon) still matches
// as an adjacent pair, and a question's words rank by bm25 rather than
// requiring every one of them in a single chunk.
// ---------------------------------------------------------------------------

// Stop-words are dropped before the OR-join: with OR semantics a bare "the"
// would match every chunk and make `hits > 0` — the instrument's hit — mean
// nothing. A hit therefore means at least one content-bearing term matched;
// bm25 ranks chunks carrying more of them higher.
export const STOP_WORDS = new Set(('a an the of to in on for and or is are was were be been it its this that these those '
  + 'with as at by from what why how when where who whom which do does did done we i you your our my me not no into '
  + 'about than then there here has have had will would should could can if so but also any all some such up out over '
  + 'under again more most very just only same other each both own off per via').split(' '));

export function buildMatchQuery(query) {
  const tokens = String(query || '')
    .replace(/"/g, ' ')
    .split(/\s+/)
    .map((t) => t.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, ''))
    .filter((t) => t.length >= 2 && !STOP_WORDS.has(t.toLowerCase()));
  if (tokens.length === 0) return '';
  return tokens.map((t) => `"${t}"`).join(' OR ');
}

// ---------------------------------------------------------------------------
// Opening the index — loadBetterSqlite3 only, plus the FTS5 probe
// ---------------------------------------------------------------------------

function hasFts5(Database) {
  try {
    const db = new Database(':memory:');
    try {
      db.exec("CREATE VIRTUAL TABLE t USING fts5(x, tokenize='porter unicode61')");
      return true;
    } finally { db.close(); }
  } catch { return false; }
}

export function loadSqlite(projectPath) {
  const Database = loadBetterSqlite3(projectPath);
  if (!Database) return { error: 'sqlite-unavailable', message: 'better-sqlite3 could not be resolved (loadBetterSqlite3 returned null)' };
  if (!hasFts5(Database)) return { error: 'fts5-unavailable', message: 'the resolved better-sqlite3 build has no FTS5' };
  return { Database };
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS docs (
      path TEXT PRIMARY KEY, source TEXT NOT NULL, mtime REAL NOT NULL,
      size INTEGER NOT NULL, title TEXT
    );
    CREATE VIRTUAL TABLE IF NOT EXISTS chunks USING fts5(
      path UNINDEXED, source UNINDEXED, heading, body, tokenize='porter unicode61'
    );
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
  `);
  db.prepare('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)').run('schema_version', String(SCHEMA_VERSION));
}

export function openIndex({ projectPath, indexPath }) {
  const loaded = loadSqlite(projectPath);
  if (loaded.error) return loaded;
  mkdirSync(dirname(indexPath), { recursive: true });
  const existed = existsSync(indexPath);
  const db = new loaded.Database(indexPath);
  db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);
  try { db.pragma('journal_mode = WAL'); } catch { /* a locked db keeps its mode */ }
  ensureSchema(db);
  return { db, built: !existed };
}

// ---------------------------------------------------------------------------
// Refresh — incremental by (mtime, size); build when missing
// ---------------------------------------------------------------------------

export function refreshOpenIndex(db, corpus) {
  const existing = new Map(db.prepare('SELECT path, source, mtime, size FROM docs').all().map((r) => [r.path, r]));
  const onDisk = new Map();
  for (const f of corpus) {
    let st;
    try { st = statSync(f.path); } catch { continue; }
    onDisk.set(f.path, { ...f, mtime: st.mtimeMs, size: st.size });
  }
  const changed = [];
  for (const [path, f] of onDisk) {
    const old = existing.get(path);
    if (!old || old.mtime !== f.mtime || old.size !== f.size || old.source !== f.source) changed.push(f);
  }
  const removed = [...existing.keys()].filter((p) => !onDisk.has(p));

  const delChunks = db.prepare('DELETE FROM chunks WHERE path = ?');
  const delDoc = db.prepare('DELETE FROM docs WHERE path = ?');
  const putDoc = db.prepare('INSERT OR REPLACE INTO docs(path, source, mtime, size, title) VALUES (?, ?, ?, ?, ?)');
  const putChunk = db.prepare('INSERT INTO chunks(path, source, heading, body) VALUES (?, ?, ?, ?)');
  const setMeta = db.prepare('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)');

  let added = 0, updated = 0;
  const apply = db.transaction(() => {
    for (const p of removed) { delChunks.run(p); delDoc.run(p); }
    for (const f of changed) {
      let text;
      try { text = readFileSync(f.path, 'utf8'); } catch { continue; }
      delChunks.run(f.path);
      putDoc.run(f.path, f.source, f.mtime, f.size, titleOf(text, f.path));
      for (const c of chunkDocument(text)) putChunk.run(f.path, f.source, c.heading, c.body);
      if (existing.has(f.path)) updated += 1; else added += 1;
    }
    setMeta.run('last_refresh', new Date().toISOString());
  });
  try {
    apply.immediate();
  } catch (e) {
    if (e && (e.code === 'SQLITE_BUSY' || /database is locked/i.test(e.message))) {
      logError(`index busy past ${BUSY_TIMEOUT_MS}ms — serving the stale-but-valid index (${e.message})`);
      return { added: 0, updated: 0, removed: 0, pending: changed.length + removed.length, stale: true };
    }
    throw e;
  }
  return { added, updated, removed: removed.length, stale: false };
}

export function indexStats(db, indexPath) {
  const bySource = Object.fromEntries(SOURCES.map((s) => [s, 0]));
  for (const r of db.prepare('SELECT source, COUNT(*) AS n FROM docs GROUP BY source').all()) bySource[r.source] = r.n;
  const chunks = db.prepare('SELECT COUNT(*) AS n FROM chunks').get().n;
  const last = db.prepare("SELECT value FROM meta WHERE key = 'last_refresh'").get();
  let bytes = 0;
  try { bytes = statSync(indexPath).size; } catch { /* no file yet */ }
  return { docs_by_source: bySource, docs: Object.values(bySource).reduce((a, b) => a + b, 0), chunks, bytes, last_refresh: last ? last.value : null };
}

// ---------------------------------------------------------------------------
// Project resolution — resolveProjectIdentity (worktree-aware), config-
// registered paths only; the memory dir through project-context.cjs
// ---------------------------------------------------------------------------

export function resolveProject({ cwd, config, homeDir = homedir() }) {
  const opts = {
    registryPath: join(homeDir, '.claude', 'cc-registry.json'),
    muxProjectsPath: join(homeDir, '.config', 'mux', 'projects.json'),
  };
  const id = resolveProjectIdentity(cwd, config, opts);
  if (!id || !id.registered) return null;
  return id;
}

export function prepareProject({ name, projectPath, config, watchtowerDir = getWatchtowerDir(), homeDir = homedir() }) {
  const slug = slugify(name);
  const memoryDir = resolveMemoryDirFor(projectPath, { homeDir });
  const indexPath = indexPathFor(slug, watchtowerDir);
  const corpus = listCorpus(slug, { watchtowerDir, memoryDir });
  return { name, slug, projectPath, memoryDir, indexPath, corpus, config };
}

// refreshIndex(projectName, opts) — the Ring 1 entry point (plan B3) and the
// shared first step of every search/build. Opens (building if missing),
// refreshes, returns the summary. Throws on sqlite/fts5 unavailability so a
// caller that wraps per project (Ring 1) logs and continues.
export function refreshIndex(name, { projectPath, config = null, watchtowerDir = getWatchtowerDir(), homeDir = homedir() } = {}) {
  const proj = prepareProject({ name, projectPath, config, watchtowerDir, homeDir });
  const opened = openIndex({ projectPath, indexPath: proj.indexPath });
  if (opened.error) throw new Error(`${opened.error}: ${opened.message}`);
  let refresh, stats;
  try {
    refresh = refreshOpenIndex(opened.db, proj.corpus);
    stats = indexStats(opened.db, proj.indexPath);
  } finally {
    opened.db.close();
  }
  // Size after close: WAL pages checkpoint into the main file on close, so a
  // mid-transaction stat under-reports a freshly built index.
  try { stats.bytes = statSync(proj.indexPath).size; } catch { /* keep the pre-close figure */ }
  return { project: name, slug: proj.slug, index_path: proj.indexPath, built: opened.built, refresh, ...stats };
}

// ---------------------------------------------------------------------------
// Search — best chunk per path, source-interleaved, top-k
// ---------------------------------------------------------------------------

export function interleaveBySource(rows, { k = DEFAULT_K, perSourceCap = PER_SOURCE_CAP, source = null } = {}) {
  // rows arrive best-first; keep the first (= best) chunk per path.
  const seen = new Set();
  const best = [];
  for (const r of rows) {
    if (seen.has(r.path)) continue;
    seen.add(r.path);
    best.push(r);
  }
  const filtered = source ? best.filter((r) => r.source === source) : best;
  if (source) return filtered.slice(0, k);
  const perSource = {};
  const kept = [];
  for (const r of filtered) {
    perSource[r.source] = (perSource[r.source] || 0) + 1;
    if (perSource[r.source] <= perSourceCap) kept.push(r);
  }
  kept.sort((a, b) => a.score - b.score);
  return kept.slice(0, k);
}

export function searchOpenIndex(db, query, { k = DEFAULT_K, source = null, perSourceCap = PER_SOURCE_CAP } = {}) {
  const match = buildMatchQuery(query);
  if (!match) return [];
  const sql = `
    SELECT c.path AS path, c.source AS source, c.heading AS heading, d.title AS title,
           snippet(chunks, 3, '', '', '…', 48) AS snippet,
           bm25(chunks, 0, 0, 2.0, 1.0) AS score
    FROM chunks c JOIN docs d ON d.path = c.path
    WHERE chunks MATCH ?
    ORDER BY score
    LIMIT ?`;
  const rows = db.prepare(sql).all(match, CANDIDATE_LIMIT);
  const trimmed = rows.map((r) => ({
    path: r.path, source: r.source, title: r.title, heading: r.heading || '',
    snippet: String(r.snippet || '').replace(/\s+/g, ' ').trim().slice(0, SNIPPET_MAX_CHARS),
    score: Math.round(r.score * 10000) / 10000,
  }));
  return interleaveBySource(trimmed, { k, perSourceCap, source });
}

export function hitsBySource(results) {
  const out = {};
  for (const r of results) out[r.source] = (out[r.source] || 0) + 1;
  return out;
}

// Fail-open: a dropped log line must never fail a recall — but it is logged,
// because the instrument's invocation count reads this file.
export function appendRecallLog(entry, watchtowerDir = getWatchtowerDir()) {
  try {
    const p = recallLogPath(watchtowerDir);
    mkdirSync(dirname(p), { recursive: true });
    appendFileSync(p, JSON.stringify(entry) + '\n');
    return true;
  } catch (e) {
    logError(`recall-log append failed (${e.message}) — continuing`);
    return false;
  }
}

export function search(proj, query, { k = DEFAULT_K, source = null, sessionId = null, caller = 'cli', watchtowerDir = getWatchtowerDir() } = {}) {
  const opened = openIndex({ projectPath: proj.projectPath, indexPath: proj.indexPath });
  if (opened.error) return { status: opened.error, message: opened.message };
  try {
    const refresh = refreshOpenIndex(opened.db, proj.corpus);
    const stats = indexStats(opened.db, proj.indexPath);
    const base = {
      project: proj.name, slug: proj.slug, index_path: proj.indexPath, memory_dir: proj.memoryDir,
      query, k, source, built: opened.built, refresh, docs_by_source: stats.docs_by_source,
    };
    if (proj.corpus.length === 0 && stats.docs === 0) {
      const dirs = sourceDirs(proj.slug, { watchtowerDir, memoryDir: proj.memoryDir });
      return {
        status: 'empty-corpus',
        reason: `no files in any source for ${proj.name}: ${SOURCES.map((s) => `${s}=${dirs[s] || '(unresolved)'}`).join(', ')}`,
        ...base, hits: 0, hits_by_source: {}, results: [],
      };
    }
    const results = searchOpenIndex(opened.db, query, { k, source });
    const out = { status: 'ok', ...base, hits: results.length, hits_by_source: hitsBySource(results), results };
    appendRecallLog({
      ts: new Date().toISOString(), project: proj.name, query, k,
      hits: results.length, hits_by_source: out.hits_by_source,
      top_paths: results.map((r) => r.path), session_id: sessionId || null,
      caller: CALLERS.includes(caller) ? caller : 'cli',
    }, watchtowerDir);
    return out;
  } finally {
    opened.db.close();
  }
}

export function status(proj) {
  if (!existsSync(proj.indexPath)) {
    return { status: 'no-index', project: proj.name, slug: proj.slug, index_path: proj.indexPath, memory_dir: proj.memoryDir, corpus_files: proj.corpus.length };
  }
  const opened = openIndex({ projectPath: proj.projectPath, indexPath: proj.indexPath });
  if (opened.error) return { status: opened.error, message: opened.message };
  try {
    const stats = indexStats(opened.db, proj.indexPath);
    return { status: 'ok', project: proj.name, slug: proj.slug, index_path: proj.indexPath, memory_dir: proj.memoryDir, corpus_files: proj.corpus.length, ...stats };
  } finally {
    opened.db.close();
  }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const out = { cmd: argv[0], json: false, k: DEFAULT_K, source: null, sessionId: null, caller: 'cli' };
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--json') out.json = true;
    else if (a === '--cwd') out.cwd = argv[++i];
    else if (a === '--query') out.query = argv[++i];
    else if (a === '--k') out.k = Math.max(1, parseInt(argv[++i], 10) || DEFAULT_K);
    else if (a === '--source') out.source = argv[++i];
    else if (a === '--session-id') out.sessionId = argv[++i];
    else if (a === '--caller') out.caller = argv[++i];
    else if (a === '--home') out.homeDir = argv[++i];
    else if (a === '--watchtower-dir') out.watchtowerDir = argv[++i];
    else if (a === '--config') out.configPath = argv[++i];
    else { console.error(`unknown argument: ${a}`); process.exit(1); }
  }
  return out;
}

const USAGE = [
  'usage: watchtower-recall.mjs <search|build|status> --cwd <dir> [--query "<q>"] [--k 8]',
  '       [--source sessions|memory|archive|transcripts] [--session-id <id>] [--caller skill|probe|cli] [--json]',
].join('\n');

function emit(obj, json) {
  if (json) { process.stdout.write(JSON.stringify(obj, null, 2) + '\n'); return; }
  if (obj.status === 'ok' && Array.isArray(obj.results)) {
    if (obj.results.length === 0) { console.log(`no results for "${obj.query}" in ${obj.project}`); return; }
    for (const r of obj.results) {
      console.log(`[${r.source}] ${r.title}${r.heading ? ` › ${r.heading}` : ''}  (${r.score})\n  ${r.path}\n  ${r.snippet}\n`);
    }
    return;
  }
  console.log(JSON.stringify(obj, null, 2));
}

const isMain = (() => {
  try { return process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
})();

if (isMain) {
  const args = parseArgs(process.argv.slice(2));
  if (!['search', 'build', 'status'].includes(args.cmd) || !args.cwd) {
    console.error(USAGE);
    process.exit(1);
  }
  if (args.cmd === 'search' && !args.query) {
    console.error('search requires --query "<question>"\n' + USAGE);
    process.exit(1);
  }
  if (args.source && !SOURCES.includes(args.source)) {
    console.error(`--source must be one of ${SOURCES.join('|')}`);
    process.exit(1);
  }
  const watchtowerDir = args.watchtowerDir || getWatchtowerDir();
  const homeDir = args.homeDir || homedir();
  let config;
  try {
    config = args.configPath ? JSON.parse(readFileSync(args.configPath, 'utf8')) : loadConfig(watchtowerDir);
  } catch (e) {
    console.error(`recall: ${e.message} — run /watchtower configure to register this project.`);
    process.exit(2);
  }
  const cwd = resolvePath(args.cwd);
  const id = resolveProject({ cwd, config, homeDir });
  if (!id) {
    console.error(`recall: ${cwd} is not a registered watchtower project — run /watchtower configure to add it (exit 2).`);
    process.exit(2);
  }
  const proj = prepareProject({ name: id.name, projectPath: id.path, config, watchtowerDir, homeDir });

  let out;
  if (args.cmd === 'search') {
    out = search(proj, args.query, { k: args.k, source: args.source, sessionId: args.sessionId, caller: args.caller, watchtowerDir });
  } else if (args.cmd === 'build') {
    try {
      out = { status: 'ok', ...refreshIndex(id.name, { projectPath: id.path, config, watchtowerDir, homeDir }) };
    } catch (e) {
      const m = /^(sqlite-unavailable|fts5-unavailable): (.*)$/.exec(e.message);
      if (!m) throw e;
      out = { status: m[1], message: m[2] };
    }
  } else {
    out = status(proj);
  }
  if (out.status === 'sqlite-unavailable' || out.status === 'fts5-unavailable') {
    console.error(`recall: ${out.status} — ${out.message} (exit 3)`);
    if (args.json) process.stdout.write(JSON.stringify(out) + '\n');
    process.exit(3);
  }
  emit(out, args.json);
  // The human log line goes to stderr so --json stdout stays parseable.
  if (args.cmd === 'search' && out.status === 'ok') {
    console.error(`[recall] ${proj.name}: "${args.query}" → ${out.hits} hit(s) ${JSON.stringify(out.hits_by_source)} (refresh +${out.refresh.added}/~${out.refresh.updated}/-${out.refresh.removed}${out.refresh.stale ? ', STALE' : ''})`);
  }
}

#!/usr/bin/env node
// The recall cue — a pointer at the moment of need, shadow first.
// (Simplification Project 1, plan .claude/plans/simplification-1-recall-cue.md,
// act:e46ce733.)
//
// A `UserPromptSubmit` hook: on every prompt, ask the project's recall index
// (the same FTS5 index `/recall` reads) whether the record already holds a
// strong, distinct answer, and — ONLY when the project is switched `on` —
// hand the session ONE line naming that file by title and path. Never a
// snippet: pasted record text is an old fact sitting in the conversation
// looking current (decision (b)).
//
// Shadow by default (decision (f)): every project logs what the cue WOULD
// have said and injects nothing until `mode --set on` flips it, one project
// at a time, after the review (plan C4). `off` is the kill switch and logs
// nothing. The config key is `projects.<name>.recall_cue`; absent or
// unrecognised ⇒ `shadow` — a typo can never switch injection on.
//
// Read-only by construction (decision (c)): the index is opened
// `{ readonly: true, fileMustExist: true }` with a 200ms busy timeout; the
// cue never refreshes, never builds, never writes index CONTENT (SQLite may
// create the -wal/-shm sidecars on a read-only open; that is not a write to
// the index), and never appends to recall-log.jsonl — "a session chose to
// ask" must never be inflated by cue fires (decision (e)). The pure pieces
// of watchtower-recall.mjs are reused; its `search`/`openIndex`/refresh
// entry points are never imported here (AC6 greps for them).
//
// Fail-open, silent: every failure exits 0 with no stdout and is recorded in
// the cue's OWN log as its own outcome — `busy`, `no-index`, `unregistered`,
// `error`, `skipped` — so a quietly broken cue is visible at review rather
// than mistaken for "found nothing". It can never block or alter a prompt:
// never exit 2, never `decision: block`.
//
// The bar (fixed input from the 2026-10-03 measurement, retunable at the
// review): fire when the top bm25 score ≤ CUE_SCORE_BAR AND the lead over
// the runner-up ≥ CUE_LEAD_BAR. Scores are negative; more negative is
// stronger. With fewer than two rows the lead clause passes (Decision
// Default 6).
//
// Subcommands: `hook` (what templates/hooks/watchtower-recall-cue.sh pipes
// the payload to), `review --since <date>` (the blind review sheet, reads
// only), `mode --project <name> --set shadow|on|off [--reason "<why>"]` (the
// loud flip of decision (g): a flip away from `on` refuses without a reason
// and is appended to `recall_cue_history`, which Ring 1 surfaces for 7 days).

import {
  readFileSync, existsSync, mkdirSync, appendFileSync, realpathSync,
} from 'fs';
import { join, dirname } from 'path';
import { homedir } from 'os';
import { pathToFileURL } from 'url';
import { performance } from 'perf_hooks';
import { searchOpenIndex, resolveProject, indexPathFor, loadSqlite } from './watchtower-recall.mjs';
import { loadConfig, slugify, getWatchtowerDir, atomicWrite } from './watchtower-lib.mjs';

// ---------------------------------------------------------------------------
// Constants — exported so the review can retune them with data
// ---------------------------------------------------------------------------

export const CUE_SCORE_BAR = -20;
export const CUE_LEAD_BAR = 5;
export const CUE_BUSY_MS = 200;
export const CUE_PROMPT_HEAD = 200;
export const CUE_K = 8;
export const CUE_LONG_WORDS = 20;
export const CUE_LOG_FILE = 'recall-cue-log.jsonl';
export const CUE_MODES = ['shadow', 'on', 'off'];
export const CUE_DEFAULT_MODE = 'shadow';
export const CUE_HISTORY_DAYS = 7;

// The rubric the review judges against — written before shadow started
// (plan C4) and printed verbatim on every sheet.
export const CUE_RUBRIC = [
  '**right** = the named file holds a fact the session then used, re-derived, or got wrong;',
  '**harmless** = on-topic but not needed;',
  '**wrong** = off-topic, or would mislead.',
].join(' ');

// ---------------------------------------------------------------------------
// Pure pieces — the test seams
// ---------------------------------------------------------------------------

export function cueLogPath(watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', CUE_LOG_FILE);
}

// Exactly: a read-only, must-exist open with the short busy timeout. The
// seam exists so the suite can assert `db.readonly === true` directly — a
// read-write reader answers just as fast, so nothing behavioural would
// catch "opened read-write" (QA critique).
export function openCueDb(Database, indexPath) {
  const db = new Database(indexPath, { readonly: true, fileMustExist: true });
  db.pragma(`busy_timeout = ${CUE_BUSY_MS}`);
  return db;
}

export function decide(rows) {
  const list = Array.isArray(rows) ? rows : [];
  const top_score = list[0]?.score ?? null;
  const lead = list.length >= 2 ? Math.round((list[1].score - list[0].score) * 10000) / 10000 : null;
  const fired = list.length >= 1
    && top_score <= CUE_SCORE_BAR
    && (list.length < 2 || lead >= CUE_LEAD_BAR);
  return { top_score, lead, fired };
}

// Decision (b), verbatim. A pointer — title, heading, path — never content.
export function cueLine(row) {
  const heading = row.heading ? ` § ${row.heading}` : '';
  return `The project record may already answer this: "${row.title}"${heading} — ${row.path}. `
    + 'Read it if it\'s relevant; /recall searches further.';
}

export function readMode(config, projectName) {
  const v = config?.projects?.[projectName]?.recall_cue;
  return CUE_MODES.includes(v) ? v : CUE_DEFAULT_MODE;
}

export function promptWords(prompt) {
  const t = String(prompt || '').trim();
  return t ? t.split(/\s+/).length : 0;
}

export function isBusyError(e) {
  return e && (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED');
}

function errorDetail(e) {
  if (!e) return null;
  return e.code ? `${e.code}: ${e.message}` : String(e.message || e);
}

function top3Of(rows) {
  return rows.slice(0, 3).map((r) => ({
    path: r.path, source: r.source, title: r.title, heading: r.heading || '', score: r.score,
  }));
}

// runCue — steps 3–9 and 11 of the hook (plan B1): resolve, read the mode,
// skip, open, query, decide. Returns `{ entry, stdout }`: `entry` is the
// log line to append (null ⇒ nothing is logged: mode `off`), `stdout` the
// hookSpecificOutput payload string, or null. Appending and printing are the
// caller's (the hook subcommand's) — the pure core writes nothing.
export function runCue(payload, {
  config, watchtowerDir = getWatchtowerDir(), homeDir = homedir(), now = () => new Date(),
} = {}) {
  const base = () => ({
    ts: now().toISOString(),
    elapsed_ms: Math.round(performance.now()),
    bar: { score: CUE_SCORE_BAR, lead: CUE_LEAD_BAR },
  });
  const sessionId = payload?.session_id ?? null;
  const transcriptPath = payload?.transcript_path ?? null;

  // 3. Resolve the project. Unregistered ⇒ outcome only, no prompt text.
  const id = resolveProject({ cwd: payload?.cwd, config, homeDir });
  if (!id) {
    return {
      entry: { ...base(), project: null, session_id: sessionId, mode: null, outcome: 'unregistered', detail: null, transcript_path: transcriptPath },
      stdout: null,
    };
  }

  // 4. Mode. `off` ⇒ nothing runs, nothing is logged.
  const mode = readMode(config, id.name);
  if (mode === 'off') return { entry: null, stdout: null };

  const prompt = typeof payload?.prompt === 'string' ? payload.prompt : '';
  const trimmed = prompt.trim();
  const line = (outcome, extra = {}) => ({
    ...base(),
    project: id.name, session_id: sessionId, mode, outcome,
    prompt_head: prompt.slice(0, CUE_PROMPT_HEAD), prompt_words: promptWords(prompt),
    top3: [], top_score: null, lead: null, would_inject: null, injected: false,
    detail: null, transcript_path: transcriptPath,
    ...extra,
  });

  // 5. Skip: empty, or the session is already asking the record.
  if (!trimmed || trimmed.startsWith('/recall')) return { entry: line('skipped'), stdout: null };

  // 6. The sqlite loader (ABI / FTS5 availability).
  const loaded = loadSqlite(id.path);
  if (loaded.error) return { entry: line('error', { detail: loaded.error }), stdout: null };

  // 7. The index must already exist — the cue never builds one.
  const indexPath = indexPathFor(slugify(id.name), watchtowerDir);
  if (!existsSync(indexPath)) return { entry: line('no-index'), stdout: null };

  // 8. Open read-only, query, always close. Busy is its own outcome.
  let rows;
  let db = null;
  try {
    db = openCueDb(loaded.Database, indexPath);
    rows = searchOpenIndex(db, prompt, { k: CUE_K });
  } catch (e) {
    if (isBusyError(e)) return { entry: line('busy', { detail: errorDetail(e) }), stdout: null };
    return { entry: line('error', { detail: errorDetail(e) }), stdout: null };
  } finally {
    try { if (db) db.close(); } catch { /* a close failure changes nothing */ }
  }

  // 9. Decide. top3 is recorded on quiet lines too (Decision Default 5).
  const { top_score, lead, fired } = decide(rows);
  const would_inject = fired ? cueLine(rows[0]) : null;
  const injected = mode === 'on' && fired;
  const entry = line(fired ? 'fired' : 'quiet', { top3: top3Of(rows), top_score, lead, would_inject, injected });
  // 11. Only when injected: the hookSpecificOutput payload.
  const stdout = injected
    ? JSON.stringify({ hookSpecificOutput: { hookEventName: 'UserPromptSubmit', additionalContext: would_inject } })
    : null;
  return { entry, stdout };
}

// Fail-open append: a dropped cue line never fails a prompt.
export function appendCueLog(entry, watchtowerDir = getWatchtowerDir()) {
  if (!entry) return false;
  try {
    const p = cueLogPath(watchtowerDir);
    mkdirSync(dirname(p), { recursive: true });
    // elapsed_ms is stamped at log time (Decision Default 19).
    appendFileSync(p, JSON.stringify({ ...entry, elapsed_ms: Math.round(performance.now()) }) + '\n');
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// hook — steps 1–12. Exit 0 in every path.
// ---------------------------------------------------------------------------

function readStdin() {
  try { return readFileSync(0, 'utf8'); } catch { return ''; }
}

export function hookMain({ stdin = readStdin(), watchtowerDir = getWatchtowerDir(), homeDir = homedir(), write = (s) => process.stdout.write(s) } = {}) {
  try {
    // 1. Parse. Malformed ⇒ an error line with no project and no prompt text.
    let payload = null;
    try { payload = JSON.parse(stdin); } catch { payload = null; }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      appendCueLog({
        ts: new Date().toISOString(), outcome: 'error', detail: 'malformed-payload',
        bar: { score: CUE_SCORE_BAR, lead: CUE_LEAD_BAR },
      }, watchtowerDir);
      return 0;
    }
    // 2. Config. Missing or unreadable ⇒ nothing, not even a log line
    // (Decision Default 17: the module ships dormant until /watchtower install).
    let config;
    try { config = loadConfig(watchtowerDir); } catch { return 0; }

    const { entry, stdout } = runCue(payload, { config, watchtowerDir, homeDir });
    // 10 then 11 (Decision Default 21): append, then print. A failed append
    // never suppresses the injection.
    appendCueLog(entry, watchtowerDir);
    if (stdout) { try { write(stdout); } catch { /* reader hung up */ } }
    return 0;
  } catch (e) {
    // 12. Best-effort error line; still exit 0.
    appendCueLog({
      ts: new Date().toISOString(), outcome: 'error', detail: errorDetail(e),
      bar: { score: CUE_SCORE_BAR, lead: CUE_LEAD_BAR },
    }, watchtowerDir);
    return 0;
  }
}

// ---------------------------------------------------------------------------
// mode — the loud flip (decision (g))
// ---------------------------------------------------------------------------

export function readConfigRaw(watchtowerDir) {
  const p = join(watchtowerDir, 'config.json');
  return JSON.parse(readFileSync(p, 'utf8'));
}

// Returns { ok, error, stored, history_entry } and writes only on ok. A flip
// AWAY from `on` without a non-empty reason is refused: a kill is never
// silent. Every flip appends `{ ts, from, to, reason }` to
// `projects.<name>.recall_cue_history` (created on first flip).
export function setMode({ project, set, reason = null, watchtowerDir = getWatchtowerDir(), now = () => new Date() }) {
  let config;
  try { config = readConfigRaw(watchtowerDir); } catch (e) {
    return { ok: false, error: `config.json unreadable at ${watchtowerDir}: ${e.message}` };
  }
  const projects = config.projects || {};
  if (!project || !Object.prototype.hasOwnProperty.call(projects, project)) {
    return { ok: false, error: `unknown project "${project}" — registered: ${Object.keys(projects).join(', ') || '(none)'}` };
  }
  if (!CUE_MODES.includes(set)) {
    return { ok: false, error: `invalid mode "${set}" — valid: ${CUE_MODES.join('|')}` };
  }
  const from = readMode(config, project);
  const trimmedReason = typeof reason === 'string' ? reason.trim() : '';
  if (from === 'on' && set !== 'on' && !trimmedReason) {
    return { ok: false, error: `refusing to flip ${project} ${from}→${set} without --reason "<why>" — a kill is never silent (decision (g))` };
  }
  const history_entry = { ts: now().toISOString(), from, to: set, reason: trimmedReason || null };
  projects[project].recall_cue = set;
  projects[project].recall_cue_history = [...(projects[project].recall_cue_history || []), history_entry];
  config.projects = projects;
  atomicWrite(join(watchtowerDir, 'config.json'), JSON.stringify(config, null, 2) + '\n');
  // Read back: a write announced from the call's success is not a confirmed write.
  const stored = readMode(readConfigRaw(watchtowerDir), project);
  return { ok: true, project, stored, history_entry };
}

// renderRecallCueFlipEntries — the Ring 1 summary-attention line(s) for every
// flip AWAY from `on` within the last CUE_HISTORY_DAYS days, read from each
// project's `recall_cue_history`. Pure (no I/O) for the hermetic suite;
// Ring 1 calls it with `config.projects`. Flips toward `on` and older flips
// render nothing.
export function renderRecallCueFlipEntries(projects, now = new Date()) {
  const lines = [];
  const cutoff = now.getTime() - CUE_HISTORY_DAYS * 24 * 60 * 60 * 1000;
  for (const [name, entry] of Object.entries(projects || {})) {
    for (const h of (entry?.recall_cue_history || [])) {
      if (!h || h.from !== 'on' || h.to === 'on') continue;
      const t = Date.parse(h.ts);
      if (!Number.isFinite(t) || t < cutoff || t > now.getTime() + 60_000) continue;
      const date = h.ts.slice(0, 10);
      lines.push(`recall cue: ${name} switched ${h.from}→${h.to} on ${date} — ${h.reason || '(no reason recorded)'}`);
    }
  }
  return lines;
}

// ---------------------------------------------------------------------------
// review — the blind sheet (plan B1 `review`, Decision Defaults 3, 11, 28, 29)
// ---------------------------------------------------------------------------

export function readCueLog(watchtowerDir = getWatchtowerDir()) {
  const p = cueLogPath(watchtowerDir);
  if (!existsSync(p)) return [];
  const out = [];
  for (const raw of readFileSync(p, 'utf8').split('\n')) {
    const s = raw.trim();
    if (!s) continue;
    try { out.push(JSON.parse(s)); } catch { /* a torn line is skipped */ }
  }
  return out;
}

// mulberry32 — a seeded PRNG so the same seed yields the same shuffle.
export function seededRandom(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6D2B79F5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function seedFromString(s) {
  let h = 2166136261;
  for (const ch of String(s)) { h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619); }
  return h >>> 0;
}

export function seededShuffle(items, seed) {
  const rnd = seededRandom(seed);
  const a = items.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// 95% Wilson score interval for k successes of n, as a string.
export function wilson(k, n) {
  if (!n) return 'n=0';
  const z = 1.96;
  const p = k / n;
  const denom = 1 + z * z / n;
  const centre = (p + z * z / (2 * n)) / denom;
  const half = z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom;
  const pct = (x) => `${Math.round(Math.max(0, Math.min(1, x)) * 100)}%`;
  return `${k}/${n} = ${pct(p)} [${pct(centre - half)}–${pct(centre + half)}]`;
}

export function percentile(values, q) {
  const v = values.filter((x) => typeof x === 'number' && Number.isFinite(x)).sort((a, b) => a - b);
  if (v.length === 0) return null;
  const idx = Math.min(v.length - 1, Math.max(0, Math.ceil(q * v.length) - 1));
  return v[idx];
}

// What the session did next: the first ~300 chars of each of the next two
// assistant turns after the prompt, and the files Read in them. A missing or
// unreadable transcript is stated, never skipped (Decision Default 29).
export function nextTurnsFromTranscript(transcriptPath, entry, { maxChars = 300 } = {}) {
  if (!transcriptPath) return { note: 'no transcript_path on the log line', turns: [] };
  let text;
  try { text = readFileSync(transcriptPath, 'utf8'); } catch { return { note: `transcript unreadable: ${transcriptPath}`, turns: [] }; }
  const records = [];
  for (const raw of text.split('\n')) {
    const s = raw.trim();
    if (!s) continue;
    try { records.push(JSON.parse(s)); } catch { /* skip */ }
  }
  const entryTs = Date.parse(entry.ts);
  const head = String(entry.prompt_head || '');
  // Anchor on the user record carrying this prompt: by text, else by time.
  let anchor = -1;
  for (let i = 0; i < records.length; i++) {
    const r = records[i];
    if (r.type !== 'user') continue;
    const c = r.message?.content;
    const textOf = typeof c === 'string' ? c : (Array.isArray(c) ? c.filter((b) => b?.type === 'text').map((b) => b.text).join('\n') : '');
    if (head && textOf.startsWith(head)) { anchor = i; break; }
  }
  if (anchor < 0 && Number.isFinite(entryTs)) {
    for (let i = 0; i < records.length; i++) {
      const r = records[i];
      if (r.type !== 'user') continue;
      const t = Date.parse(r.timestamp || r.time || r.ts || '');
      if (Number.isFinite(t) && t >= entryTs - 5000) { anchor = i; break; }
    }
  }
  if (anchor < 0) return { note: 'prompt not found in the transcript', turns: [] };
  const turns = [];
  for (let i = anchor + 1; i < records.length && turns.length < 2; i++) {
    const r = records[i];
    if (r.type !== 'assistant') continue;
    const c = r.message?.content;
    const blocks = Array.isArray(c) ? c : (typeof c === 'string' ? [{ type: 'text', text: c }] : []);
    const textParts = blocks.filter((b) => b?.type === 'text').map((b) => String(b.text || ''));
    const reads = blocks.filter((b) => b?.type === 'tool_use' && b.name === 'Read' && b.input?.file_path).map((b) => b.input.file_path);
    const joined = textParts.join(' ').replace(/\s+/g, ' ').trim();
    turns.push({ text: joined.slice(0, maxChars) + (joined.length > maxChars ? '…' : ''), reads });
  }
  return { note: null, turns };
}

export function buildReview(log, { since, project = null, sample = 30, seed = null, until = null } = {}) {
  const sinceTs = Date.parse(since);
  const untilTs = until ? Date.parse(until) : Infinity;
  const inWindow = log.filter((l) => {
    const t = Date.parse(l.ts);
    return Number.isFinite(t) && t >= sinceTs && t <= untilTs && (!project || l.project === project);
  });
  const theSeed = seed == null ? seedFromString(`${since}:${project || 'all'}`) : (Number.isFinite(Number(seed)) ? Number(seed) >>> 0 : seedFromString(String(seed)));
  const byOutcome = {};
  for (const l of inWindow) byOutcome[l.outcome] = (byOutcome[l.outcome] || 0) + 1;
  const fired = inWindow.filter((l) => l.outcome === 'fired');
  const quiet = inWindow.filter((l) => l.outcome === 'quiet');
  const isLong = (l) => (l.prompt_words || 0) >= CUE_LONG_WORDS;
  const group = (xs) => ({ short: xs.filter((l) => !isLong(l)).length, long: xs.filter(isLong).length });
  const elapsed = inWindow.map((l) => l.elapsed_ms);
  const modes = [...new Set(inWindow.map((l) => l.mode).filter(Boolean))];
  const bars = [...new Set(inWindow.map((l) => l.bar ? `${l.bar.score}/${l.bar.lead}` : null).filter(Boolean))];

  // Per-project quiet sample, seeded.
  const projectsSeen = [...new Set(quiet.map((l) => l.project))];
  let quietSample = [];
  for (const p of projectsSeen) {
    const pool = quiet.filter((l) => l.project === p);
    quietSample = quietSample.concat(seededShuffle(pool, theSeed ^ seedFromString(p)).slice(0, sample));
  }
  const list = seededShuffle([...fired, ...quietSample], theSeed);

  const out = [];
  out.push(`# Recall cue review — ${project || 'all projects'} since ${since}`);
  out.push('');
  out.push(`Window: ${since} → ${until || 'now'}. Project(s): ${project || projectsSeen.join(', ') || '(none)'}. Seed: ${theSeed}. Mode(s) in force: ${modes.join(', ') || '(none)'}. Bar(s): ${bars.join(', ') || '(none)'}.`);
  out.push('');
  out.push('## Counts');
  out.push('');
  out.push(`Lines in window: ${inWindow.length}. By outcome: ${Object.entries(byOutcome).map(([k, v]) => `${k} ${v}`).join(', ') || '(none)'}.`);
  const gf = group(fired); const gq = group(quiet);
  out.push(`Fired: SHORT (< ${CUE_LONG_WORDS} words) ${gf.short}, LONG (≥ ${CUE_LONG_WORDS}) ${gf.long}. Quiet: SHORT ${gq.short}, LONG ${gq.long}.`);
  const fs_ = fired.length + quiet.length;
  out.push(`Fire rate among fired+quiet: ${wilson(fired.length, fs_)}.`);
  out.push(`elapsed_ms: p50 ${percentile(elapsed, 0.5) ?? 'n/a'}, p95 ${percentile(elapsed, 0.95) ?? 'n/a'}, max ${percentile(elapsed, 1) ?? 'n/a'}.`);
  out.push('');
  out.push('After judging (fill in; the interval is Wilson 95%, computed as k/n — inputs beside each blank):');
  out.push(`- right file at rank 1: ____ of ${fired.length} fired (n=${fired.length}); of ${quietSample.length} sampled quiet (n=${quietSample.length})`);
  out.push(`- right file in top 3: ____ of ${fired.length} fired (n=${fired.length}); of ${quietSample.length} sampled quiet (n=${quietSample.length})`);
  out.push(`- wrong at rank 1: ____ of ${fired.length} fired (n=${fired.length})`);
  out.push('');
  out.push('## Rubric');
  out.push('');
  out.push(CUE_RUBRIC);
  out.push('');
  out.push('## Section 1 — judge blind (would-fire lines and a sample of the rest, shuffled; the key is at the end)');
  out.push('');
  list.forEach((l, i) => {
    const n = i + 1;
    out.push(`### ${n}. ${l.ts} · session ${l.session_id || '?'} · ${isLong(l) ? 'LONG' : 'SHORT'} (${l.prompt_words || 0} words)`);
    out.push(`transcript: ${l.transcript_path || '(none)'}`);
    out.push('');
    out.push(`> ${String(l.prompt_head || '').replace(/\n/g, ' ')}`);
    out.push('');
    const cands = Array.isArray(l.top3) ? l.top3 : [];
    if (cands.length === 0) out.push('(no candidates)');
    cands.forEach((c, j) => {
      out.push(`- candidate ${j + 1}: "${c.title}"${c.heading ? ` § ${c.heading}` : ''} — ${c.path}  [ ] right [ ] harmless [ ] wrong`);
    });
    const next = nextTurnsFromTranscript(l.transcript_path, l);
    out.push('');
    if (next.note) out.push(`what the session did next: ${next.note}`);
    else if (next.turns.length === 0) out.push('what the session did next: no assistant turn followed');
    next.turns.forEach((t, j) => {
      out.push(`what the session did next (turn ${j + 1}): ${t.text || '(no text)'}`);
      if (t.reads.length) out.push(`  Read: ${t.reads.join(', ')}`);
    });
    out.push('');
  });
  out.push('## Section 2 — blind lines (busy / no-index / error), each with its detail');
  out.push('');
  const blind = inWindow.filter((l) => ['busy', 'no-index', 'error'].includes(l.outcome));
  if (blind.length === 0) out.push('(none)');
  for (const l of blind) out.push(`- ${l.ts} ${l.project || '(no project)'} ${l.outcome}: ${l.detail || '(no detail)'}`);
  out.push('');
  out.push('## Section 3 — per-session repeats (a path that would fire more than once in one session)');
  out.push('');
  const repeats = new Map();
  for (const l of fired) {
    const p = l.top3?.[0]?.path;
    if (!p || !l.session_id) continue;
    const key = `${l.session_id}|${p}`;
    repeats.set(key, (repeats.get(key) || 0) + 1);
  }
  const rep = [...repeats.entries()].filter(([, c]) => c > 1);
  if (rep.length === 0) out.push('(none)');
  for (const [key, c] of rep) { const [sid, p] = key.split('|'); out.push(`- session ${sid}: ${p} ×${c}`); }
  out.push('');
  out.push('## Section 4 — the key');
  out.push('');
  list.forEach((l, i) => {
    out.push(`- ${i + 1}: ${l.outcome}, top score ${l.top_score ?? 'n/a'}, lead ${l.lead ?? 'n/a'}${l.would_inject ? ` — would inject: ${l.would_inject}` : ''}`);
  });
  out.push('');
  return out.join('\n');
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const out = { cmd: argv[0] };
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--since') out.since = argv[++i];
    else if (a === '--until') out.until = argv[++i];
    else if (a === '--project') out.project = argv[++i];
    else if (a === '--sample') out.sample = Number(argv[++i]);
    else if (a === '--seed') out.seed = argv[++i];
    else if (a === '--set') out.set = argv[++i];
    else if (a === '--reason') out.reason = argv[++i];
    else if (a === '--watchtower-dir') out.watchtowerDir = argv[++i];
    else if (a === '--home') out.homeDir = argv[++i];
    else { out.unknown = a; }
  }
  return out;
}

const USAGE = [
  'usage: watchtower-recall-cue.mjs hook                              (stdin: the UserPromptSubmit payload)',
  '       watchtower-recall-cue.mjs review --since <YYYY-MM-DD> [--project <name>] [--sample N] [--seed S]',
  '       watchtower-recall-cue.mjs mode --project <name> --set shadow|on|off [--reason "<why>"]',
].join('\n');

const isMain = (() => {
  try { return process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
})();

if (isMain) {
  const args = parseArgs(process.argv.slice(2));
  const watchtowerDir = args.watchtowerDir || getWatchtowerDir();
  if (args.cmd === 'hook') {
    // Exit 0 in every path, even an unparseable argv — never fail a prompt.
    process.exitCode = hookMain({ watchtowerDir, homeDir: args.homeDir || homedir() });
  } else if (args.cmd === 'mode') {
    if (args.unknown || !args.project || !args.set) { console.error(USAGE); process.exit(1); }
    const r = setMode({ project: args.project, set: args.set, reason: args.reason ?? null, watchtowerDir });
    if (!r.ok) { console.error(`recall-cue mode: ${r.error}`); process.exit(1); }
    console.log(JSON.stringify({ project: r.project, recall_cue: r.stored, history_entry: r.history_entry }, null, 2));
  } else if (args.cmd === 'review') {
    if (args.unknown || !args.since) { console.error(USAGE); process.exit(1); }
    const log = readCueLog(watchtowerDir);
    process.stdout.write(buildReview(log, {
      since: args.since, until: args.until || null, project: args.project || null,
      sample: Number.isFinite(args.sample) ? args.sample : 30, seed: args.seed ?? null,
    }));
  } else {
    console.error(USAGE);
    process.exit(1);
  }
}

#!/usr/bin/env node

// Watchtower recall probe — "findable when asked", measured at launch.
// (Simplification Project 1, prj:a4c6052b, plan B4, act:79be8765.)
// Maintainer-run, never scheduled: one headless `claude -p` session per
// sampled fact, so a full run is ~N non-interactive sessions per project —
// a cost decision, which is why `--project <name>` or `--all` is REQUIRED
// and the default is to refuse.
//
// WHAT IT MEASURES. Samples N facts per project (default 20) ACROSS the
// corpus — one third each from (a) per-session records older than 7 days
// (so the fact is not in the Last Session block), (b) memory files with NO
// MEMORY.md line (the tier only /recall reaches), (c) the archived
// extraction drafts — so every corpus the lament is about is exercised, not
// only the one the plan already trusts. A third with no candidates is
// reported `unavailable` (the archive before plan C1 runs) and the other
// thirds are sampled as they are; N is not redistributed.
//
// For each fact, ONE model call derives the cue question ("write the
// question a session would ask that this note answers, without quoting it")
// and names the fact's KEY TOKEN — one distinctive word or identifier copied
// verbatim from the note, which any correct answer must carry. Then:
//
//   claude -p "<cue>" --output-format stream-json --verbose
//          --no-session-persistence --settings '{"disableAllHooks": true}'
//          --allowedTools "Bash(node:*)" Read Grep Glob
//
// in the project's cwd with CLAUDE_CODE_DISABLE_AUTO_MEMORY=1 in the
// environment. `--settings '{"disableAllHooks": true}'` is the stock kill
// switch (a partial --settings override would NOT remove the $HOME-layer
// watchtower hooks: hook arrays merge across scopes); the env var keeps a
// probe session from writing memory through the native path; Bash must
// stay allowed because the recall verb IS a Bash call, so --disallowedTools
// cannot do that job. No env-guard is added to the shipped hooks.
//
// THE COUNTING RULE, PINNED (scoreProbeRun, mutation-tested in
// __tests__/recall-probe-count.test.mjs): a run is FOUND when its stream
// shows a `Bash` tool_use invoking `watchtower-recall.mjs` OR a `Read` of
// the fact's source file, AND the final text carries the key token.
// Reached-but-no-token is `found-but-wrong-answer`; never reached is
// `not-found` even when the text happens to carry the token (a guess is not
// a find). The probe measures "findable when asked"; it cannot measure
// "knew to ask" — that is the invocation count in the retrieval instrument.
//
// OUTPUT: <out>/recall-probe-<date>-<project slug>.json (default
// <WATCHTOWER_DIR>/state/; the slug is ALWAYS in the name — per-project runs
// on the same day used to overwrite each other, act:d727fe16;
// `--out <dir>` lets a smoke run write outside the runtime state) carrying
// every run (source, path, fact, cue, key token, verdict, timings) and a
// ready-to-append tuning-ledger entry; `--ledger <file>` appends it.
//
// CLI:
//   node watchtower-recall-probe.mjs --project <name> [--n 20] [--out <dir>]
//        [--seed 1] [--timeout-ms 300000] [--ledger <file>] [--dry-run]
//   node watchtower-recall-probe.mjs --all ...   (every registered project)
// `--dry-run` samples facts and derives cues (model calls) but runs no
// headless session; `--no-model` additionally derives cues mechanically.

import {
  readFileSync, readdirSync, existsSync, statSync, mkdirSync, appendFileSync,
  realpathSync,
} from 'fs';
import { join, basename, relative } from 'path';
import { homedir } from 'os';
import { spawnSync } from 'child_process';
import { pathToFileURL } from 'url';
import {
  loadConfig, slugify, getWatchtowerDir, atomicWrite, recordApiUsage,
  loadAnthropicClient, WATCHTOWER_CLAUDE_MODEL,
  log as _log, logError as _logError,
} from './watchtower-lib.mjs';
import { prepareProject, sourceDirs, isExcludedMemoryFile, STOP_WORDS } from './watchtower-recall.mjs';
import { indexedFileSet, normalizePath } from './watchtower-retrieval-instrument.mjs';

// Progress goes to STDERR: `--json` stdout must stay parseable (measured on
// the smoke run — the lib's log() writes stdout and corrupted the JSON).
function log(msg) { console.error(`[recall-probe ${new Date().toISOString()}] ${msg}`); }
function logError(msg) { _logError('recall-probe', msg); }
void _log;

export const DEFAULT_N = 20;
export const SESSION_RECORD_MIN_AGE_DAYS = 7;
export const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;
export const MIN_FACT_CHARS = 60;
export const THIRDS = ['sessions', 'memory', 'archive'];
const DAY_MS = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Sampling — pure given the file tree
// ---------------------------------------------------------------------------

// Deterministic PRNG so a re-run with the same seed samples the same facts.
export function makeRng(seed = 1) {
  let s = (seed >>> 0) || 1;
  return () => {
    s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0;
    return s / 0x100000000;
  };
}

function pick(arr, n, rng) {
  const copy = arr.slice();
  const out = [];
  while (copy.length && out.length < n) {
    const i = Math.floor(rng() * copy.length);
    out.push(copy.splice(i, 1)[0]);
  }
  return out;
}

export function splitThirds(n) {
  const a = Math.ceil(n / 3);
  const b = Math.ceil((n - a) / 2);
  return { sessions: a, memory: b, archive: n - a - b };
}

function stripFrontmatter(text) {
  if (!text.startsWith('---')) return text;
  const end = text.indexOf('\n---', 3);
  return end === -1 ? text : text.slice(end + 4);
}

// Candidate facts in a file: top-level bullets and plain paragraphs of at
// least MIN_FACT_CHARS, minus headings, link-only lines and the session
// record's trailer.
export function factsIn(text) {
  const body = stripFrontmatter(text);
  const out = [];
  for (const para of body.replace(/\r\n/g, '\n').split(/\n[ \t]*\n/)) {
    for (const rawLine of para.split('\n')) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#') || line.startsWith('_Full record') || line.startsWith('Date:')) continue;
      const fact = line.replace(/^[-*]\s+/, '').trim();
      if (fact.length < MIN_FACT_CHARS) continue;
      if (/^\[.*\]\(.*\)$/.test(fact)) continue;
      out.push(fact);
    }
  }
  return out;
}

function mdFiles(dir, accept = () => true) {
  if (!dir || !existsSync(dir)) return [];
  const out = [];
  const walk = (d) => {
    let entries;
    try { entries = readdirSync(d, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      const p = join(d, e.name);
      if (e.isDirectory()) walk(p);
      else if (e.isFile() && e.name.endsWith('.md') && accept(e.name, p)) out.push(p);
    }
  };
  walk(dir);
  return out;
}

export function sampleFacts({ slug, memoryDir, watchtowerDir, n = DEFAULT_N, nowMs = Date.now(), rng = makeRng(1) }) {
  const want = splitThirds(n);
  const dirs = sourceDirs(slug, { watchtowerDir, memoryDir });
  const thirds = {};
  const facts = [];

  const sampleFrom = (third, files) => {
    const candidates = [];
    for (const path of files) {
      let text;
      try { text = readFileSync(path, 'utf8'); } catch { continue; }
      for (const fact of factsIn(text)) candidates.push({ source: third, path, fact });
    }
    if (candidates.length === 0) {
      thirds[third] = { requested: want[third], sampled: 0, status: 'unavailable', reason: files.length === 0 ? `no files under ${dirs[third] || '(unresolved)'}` : 'no candidate facts of ≥ 60 chars' };
      return;
    }
    // One fact per file where possible: sample files first, then a fact each.
    const byFile = new Map();
    for (const c of candidates) { if (!byFile.has(c.path)) byFile.set(c.path, []); byFile.get(c.path).push(c); }
    const chosenFiles = pick([...byFile.keys()], want[third], rng);
    const chosen = chosenFiles.map((p) => pick(byFile.get(p), 1, rng)[0]);
    thirds[third] = { requested: want[third], sampled: chosen.length, status: chosen.length < want[third] ? 'short' : 'ok', files_available: byFile.size };
    facts.push(...chosen);
  };

  // (a) session records older than 7 days
  const cutoff = nowMs - SESSION_RECORD_MIN_AGE_DAYS * DAY_MS;
  sampleFrom('sessions', mdFiles(dirs.sessions, (name, p) => {
    const m = /^(\d{4}-\d{2}-\d{2})/.exec(name);
    const dated = m ? Date.parse(m[1]) : NaN;
    const stamp = Number.isNaN(dated) ? (() => { try { return statSync(p).mtimeMs; } catch { return nowMs; } })() : dated;
    return stamp < cutoff;
  }));
  // (b) memory files with no index line
  const indexed = memoryDir ? indexedFileSet(memoryDir) : new Set();
  sampleFrom('memory', mdFiles(memoryDir, (name, p) => !isExcludedMemoryFile(name) && !indexed.has(relative(memoryDir, p))));
  // (c) archived drafts
  sampleFrom('archive', mdFiles(dirs.archive));

  return { thirds, facts };
}

// ---------------------------------------------------------------------------
// Cue derivation — one model call per fact
// ---------------------------------------------------------------------------

export function fallbackKeyToken(fact) {
  const backtick = /`([^`\s]{4,})`/.exec(fact);
  if (backtick) return backtick[1];
  const fid = /\b(?:act|prj|grp):[0-9a-f]{6,}\b/.exec(fact);
  if (fid) return fid[0];
  const words = fact.split(/[^\p{L}\p{N}_.-]+/u).filter((w) => w.length >= 6 && !STOP_WORDS.has(w.toLowerCase()));
  words.sort((a, b) => b.length - a.length);
  return words[0] || fact.split(/\s+/)[0];
}

const CUE_SYSTEM = [
  'You turn one note from a project record into the question a working session would ask that the note answers.',
  'Reply with JSON only: {"question": "...", "key_token": "..."}.',
  'The question must NOT quote the note and must not name the answer; it should read like something a developer would type mid-task.',
  'key_token is ONE distinctive word or identifier copied VERBATIM from the note (a name, a fid, a file, a number) that any correct answer would contain.',
].join(' ');

export async function deriveCue(fact, { client, model = WATCHTOWER_CLAUDE_MODEL, watchtowerDir } = {}) {
  if (!client) {
    return { question: `What does the record say about: ${fallbackKeyToken(fact)}?`, key_token: fallbackKeyToken(fact), derived_by: 'fallback' };
  }
  const response = await client.messages.create({
    model, max_tokens: 300, system: CUE_SYSTEM,
    messages: [{ role: 'user', content: `Note:\n${fact}` }],
  });
  recordApiUsage({ ring: 'recall-probe', model, usage: response.usage }, { watchtowerDir });
  const text = (response.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('\n');
  let parsed = null;
  try { parsed = JSON.parse(text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1)); } catch { parsed = null; }
  const question = parsed && typeof parsed.question === 'string' && parsed.question.trim() ? parsed.question.trim() : null;
  let keyToken = parsed && typeof parsed.key_token === 'string' ? parsed.key_token.trim() : '';
  if (!keyToken || !fact.toLowerCase().includes(keyToken.toLowerCase())) keyToken = fallbackKeyToken(fact);
  if (!question) return { question: `What does the record say about ${keyToken}?`, key_token: keyToken, derived_by: 'fallback-question' };
  return { question, key_token: keyToken, derived_by: 'model' };
}

// ---------------------------------------------------------------------------
// The headless run and the counting rule
// ---------------------------------------------------------------------------

export function headlessArgs(cue) {
  return [
    '-p', cue,
    '--output-format', 'stream-json', '--verbose',
    '--no-session-persistence',
    '--settings', '{"disableAllHooks": true}',
    '--allowedTools', 'Bash(node:*)', 'Read', 'Grep', 'Glob',
  ];
}

export function runHeadless(cue, { cwd, timeoutMs = DEFAULT_TIMEOUT_MS, env = {}, claudeBin = 'claude' } = {}) {
  const started = Date.now();
  const r = spawnSync(claudeBin, headlessArgs(cue), {
    cwd, encoding: 'utf8', timeout: timeoutMs, maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, CLAUDE_CODE_DISABLE_AUTO_MEMORY: '1', ...env },
  });
  return {
    stdout: r.stdout || '', stderr: r.stderr || '', status: r.status, signal: r.signal,
    timed_out: r.error?.code === 'ETIMEDOUT' || r.signal === 'SIGTERM',
    error: r.error ? r.error.message : null, duration_ms: Date.now() - started,
  };
}

export function scoreProbeRun(streamText, { sourcePath, keyToken }) {
  let invokedRecall = false;
  let readSource = false;
  let resultText = null;
  const assistantText = [];
  const want = normalizePath(sourcePath);
  for (const line of String(streamText || '').split('\n')) {
    const t = line.trim();
    if (!t) continue;
    let o;
    try { o = JSON.parse(t); } catch { continue; }
    if (o.type === 'result' && typeof o.result === 'string') { resultText = o.result; continue; }
    if (o.type !== 'assistant') continue;
    const content = o.message?.content;
    if (!Array.isArray(content)) continue;
    for (const b of content) {
      if (!b) continue;
      if (b.type === 'text' && typeof b.text === 'string') assistantText.push(b.text);
      if (b.type !== 'tool_use') continue;
      if (b.name === 'Bash' && typeof b.input?.command === 'string' && b.input.command.includes('watchtower-recall.mjs')) invokedRecall = true;
      if (b.name === 'Read' && typeof b.input?.file_path === 'string' && normalizePath(b.input.file_path) === want) readSource = true;
    }
  }
  const finalText = resultText != null ? resultText : assistantText.join('\n');
  const answered = !!keyToken && finalText.toLowerCase().includes(String(keyToken).toLowerCase());
  const reached = invokedRecall || readSource;
  const verdict = reached ? (answered ? 'found' : 'found-but-wrong-answer') : 'not-found';
  return { verdict, invoked_recall: invokedRecall, read_source: readSource, answered, final_text: finalText.slice(0, 2000) };
}

// ---------------------------------------------------------------------------
// The ledger entry — Measured filled in; the judgment fields are the
// maintainer's
// ---------------------------------------------------------------------------

export function ledgerEntry(report) {
  const date = report.date;
  const t = report.thirds;
  const third = (k) => t[k].status === 'unavailable' ? `${k} unavailable (${t[k].reason})` : `${k} ${t[k].sampled} of ${t[k].requested}`;
  const misses = report.runs.filter((r) => r.verdict !== 'found').map((r) => `${r.verdict} — "${r.cue}" (${r.source}, ${basename(r.path)}, token \`${r.key_token}\`)`);
  return [
    `## ${date} — Launch probe, ${report.project}: ${report.found} of ${report.n_sampled} facts found when asked`,
    '',
    `**Measured.** N=${report.n_sampled} (requested ${report.n_requested}; ${THIRDS.map(third).join('; ')}), one headless \`claude -p\` per fact with hooks disabled and native memory off, counted by the pinned rule (reached the record via the recall verb or a Read of the source file, AND the final text carries the key token): found ${report.found}, found-but-wrong-answer ${report.found_but_wrong_answer}, not-found ${report.not_found}${report.errors ? `, errored/timed out ${report.errors}` : ''}; verb invoked in ${report.runs.filter((r) => r.invoked_recall).length} run(s), source file read directly in ${report.runs.filter((r) => r.read_source).length}. ${misses.length ? `Misses by cue: ${misses.join('; ')}.` : 'No misses.'}`,
    '',
    '**What it means.** (maintainer)',
    '',
    '**Decision.** (maintainer)',
    '',
    `**Tracked as** act:79be8765 (lane B-retriever of prj:a4c6052b). **Expect** (maintainer; the probe runs again after the first month — plan AC14).`,
    '',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// runProbe — one project
// ---------------------------------------------------------------------------

export async function runProbe({
  name, projectPath, config, watchtowerDir = getWatchtowerDir(), homeDir = homedir(),
  n = DEFAULT_N, seed = 1, nowMs = Date.now(), timeoutMs = DEFAULT_TIMEOUT_MS,
  dryRun = false, useModel = true, deps = {},
}) {
  const proj = prepareProject({ name, projectPath, config, watchtowerDir, homeDir });
  const { thirds, facts } = sampleFacts({ slug: proj.slug, memoryDir: proj.memoryDir, watchtowerDir, n, nowMs, rng: makeRng(seed) });
  log(`${name}: sampled ${facts.length} fact(s) — ${THIRDS.map((k) => `${k} ${thirds[k].sampled}${thirds[k].status === 'unavailable' ? ' (unavailable)' : ''}`).join(', ')}`);

  let client = null;
  if (useModel && facts.length > 0) {
    client = deps.client || loadAnthropicClient();
  }
  const runs = [];
  for (const f of facts) {
    const cue = await deriveCue(f.fact, { client, watchtowerDir });
    const run = { source: f.source, path: f.path, fact: f.fact, cue: cue.question, key_token: cue.key_token, cue_derived_by: cue.derived_by };
    if (dryRun) { run.verdict = 'dry-run'; runs.push(run); continue; }
    log(`${name}: probing "${cue.question}" (token ${cue.key_token})`);
    const h = (deps.runHeadless || runHeadless)(cue.question, { cwd: projectPath, timeoutMs, env: { WATCHTOWER_DIR: watchtowerDir } });
    const score = scoreProbeRun(h.stdout, { sourcePath: f.path, keyToken: cue.key_token });
    Object.assign(run, score, { exit: h.status, timed_out: h.timed_out, error: h.error, duration_ms: h.duration_ms });
    if (h.error || (h.status !== 0 && !h.stdout)) run.verdict = 'error';
    log(`${name}: ${run.verdict} in ${h.duration_ms}ms (invoked=${score.invoked_recall} read=${score.read_source} answered=${score.answered})`);
    runs.push(run);
  }
  const count = (v) => runs.filter((r) => r.verdict === v).length;
  const report = {
    date: new Date(nowMs).toISOString().slice(0, 10), computed_at: new Date().toISOString(),
    project: name, slug: proj.slug, project_path: projectPath, memory_dir: proj.memoryDir,
    n_requested: n, n_sampled: facts.length, seed, dry_run: dryRun, thirds,
    found: count('found'), found_but_wrong_answer: count('found-but-wrong-answer'), not_found: count('not-found'), errors: count('error'),
    rule: 'found = a Bash tool_use invoking watchtower-recall.mjs OR a Read of the source file, AND the final text carries the key token',
    runs,
  };
  report.ledger_entry = ledgerEntry(report);
  return report;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const out = { projects: [], all: false, n: DEFAULT_N, seed: 1, dryRun: false, useModel: true, timeoutMs: DEFAULT_TIMEOUT_MS, json: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--project') out.projects.push(argv[++i]);
    else if (a === '--all') out.all = true;
    else if (a === '--n') out.n = Math.max(1, parseInt(argv[++i], 10) || DEFAULT_N);
    else if (a === '--seed') out.seed = parseInt(argv[++i], 10) || 1;
    else if (a === '--out') out.outDir = argv[++i];
    else if (a === '--ledger') out.ledger = argv[++i];
    else if (a === '--timeout-ms') out.timeoutMs = parseInt(argv[++i], 10) || DEFAULT_TIMEOUT_MS;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--no-model') out.useModel = false;
    else if (a === '--json') out.json = true;
    else if (a === '--home') out.homeDir = argv[++i];
    else if (a === '--watchtower-dir') out.watchtowerDir = argv[++i];
    else if (a === '--config') out.configPath = argv[++i];
    else { console.error(`unknown argument: ${a}`); process.exit(1); }
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
  const homeDir = args.homeDir || homedir();
  const config = args.configPath ? JSON.parse(readFileSync(args.configPath, 'utf8')) : loadConfig(watchtowerDir);
  const registered = Object.entries(config.projects || {}).map(([name, e]) => ({ name, path: e?.path || e }));
  let targets;
  if (args.all) targets = registered;
  else if (args.projects.length) {
    targets = args.projects.map((p) => registered.find((r) => r.name === p || slugify(r.name) === p));
    const missing = args.projects.filter((p, i) => !targets[i]);
    if (missing.length) { console.error(`not registered: ${missing.join(', ')} (registered: ${registered.map((r) => r.name).join(', ')})`); process.exit(2); }
  } else {
    console.error(`recall-probe: name the project(s) with --project <name> or pass --all — a full run is ~${args.n} headless sessions PER project (${registered.length} registered). Refusing to guess.`);
    process.exit(1);
  }
  const outDir = args.outDir || join(watchtowerDir, 'state');
  mkdirSync(outDir, { recursive: true });
  const reports = [];
  for (const t of targets) {
    const report = await runProbe({
      name: t.name, projectPath: t.path, config, watchtowerDir, homeDir,
      n: args.n, seed: args.seed, timeoutMs: args.timeoutMs, dryRun: args.dryRun, useModel: args.useModel,
    });
    const outPath = join(outDir, `recall-probe-${report.date}-${report.slug}.json`);
    atomicWrite(outPath, JSON.stringify(report, null, 2) + '\n');
    report.out_path = outPath;
    reports.push(report);
    if (args.ledger && !args.dryRun) appendFileSync(args.ledger, '\n' + report.ledger_entry);
    if (!args.json) {
      console.log(`${report.project}: ${report.found}/${report.n_sampled} found, ${report.found_but_wrong_answer} wrong-answer, ${report.not_found} not-found${report.errors ? `, ${report.errors} error` : ''} → ${outPath}`);
      if (!args.dryRun) console.log('\n' + report.ledger_entry);
    }
  }
  if (args.json) process.stdout.write(JSON.stringify(reports.length === 1 ? reports[0] : reports, null, 2) + '\n');
}

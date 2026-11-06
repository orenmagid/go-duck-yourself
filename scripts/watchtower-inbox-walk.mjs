#!/usr/bin/env node

// watchtower-inbox-walk — the one-time backlog walk of the inbox categories
// that LEFT the inbox (Simplification Project 2, plan C0–C1, act:2c133dc3;
// plan: .claude/plans/simplification-2-inbox.md). Maintainer-run from the
// dogfood checkout; copied to the runtime tier by the watchtower-setup glob;
// not something a consumer ever runs.
//
// WHO DOES WHAT. The operator chose a full read (interview Q3: "highly
// dependent on Claude, not me"): Claude reads EVERY pending item in a leaving
// category and writes one disposition with a one-line, item-specific reason;
// the operator sees the grouped map before anything moves and judges the
// `ask` lines. This script owns only what must be mechanical:
//
//   --assert-quiet --since <iso>   refuse to start while a producer still files
//                                  a leaving category (an item filed after the
//                                  last reinstall, or a redirect-ledger line).
//   --backup [--date d]            copy queue/items/ to
//                                  queue/items-backup-<date>-p2-walk/ (C0).
//   --sheet --category <c>         write state/inbox-walk-<date>/sheet-<c>.jsonl:
//                                  one line per PENDING item of the category
//                                  (minus the slices that STAY) and per item
//                                  expired AFTER A1 (not in expired-at-a1.txt).
//                                  Every item — no sampling.
//   --render --category <c> --map  print the grouped disposition map so the
//                                  operator sees everything before the apply.
//   --apply --category <c> --map   apply ONE approved category: side effects
//                                  first (each recorded in applied.jsonl so a
//                                  re-run skips what landed), then closures
//                                  through the queue's real doors.
//
// THE VOCABULARY (plan C1 / decision default 29):
//   keep           stays pending (a novel pattern, a seat proposal, config-change)
//   count          an instance of an encoded rule → rule-instances/<project>.jsonl, then close
//   route-feedback a live cc-feedback report → ONE digest outbox entry per batch of ≤10, then close
//   route-thread   a live significance proposal → the thread's proposed_recolors, then close
//   route-record   worth keeping, not acting on → the project's walk record, then close
//   file-action    fully specified now → an ordinary action through the project's OWN pib-db CLI
//   carry          needs that project's session context → its carry ledger, then close
// A carry, route-record or file-action line may add `project: '<config key>'`
// to land its side effect in a project other than the one the item was filed
// under (pattern items are all filed under the project that runs the rings).
//   close-done     already happened (the reason names the commit / fid / file)
//   close-noise    not a finding, a duplicate, or no longer true → dismissed
//   ask            Claude cannot judge; the operator decides (an unanswered ask refuses the apply)
//
// THE DOORS. Plain pending items close through `applyBatch` — the ONE bulk
// path — grouped by disposition, `resolution_type` = the disposition,
// `resolution` = the map path, the item's own reason in resolution_notes.
// Held items (applyBatch refuses them) and gated items (every
// significance-change) close one at a time through resolveItem/dismissItem
// with the same fields: the operator's approval of that item's own map line
// IS the individual attention the hold and the gate exist to require, and the
// hold stays in evidence as history (T15). An item that expired after A1 gets
// its side effect only — its status stays `expired`; no reopen verb exists.
// Every item is re-read before closing; one no longer pending is skipped and
// reported. Nothing is deleted anywhere.
//
// FRICTION NEVER LANDS AS A WAVE. route-feedback items are delivered in
// operator-paced batches of at most ROUTE_FEEDBACK_BATCH (10): each batch is
// ONE clustered digest entry in the feedback outbox (Ring 1 delivers it to the
// CC repo's feedback/ root), and `--apply` releases only the first
// `--feedback-batches` (default 1) batches; the rest stay pending until the
// operator has triaged the previous digest and re-runs with a higher number.

import {
  readFileSync, writeFileSync, appendFileSync, existsSync, mkdirSync, readdirSync, copyFileSync,
} from 'fs';
import { join, dirname, resolve } from 'path';
import { pathToFileURL, fileURLToPath } from 'url';
import { execFileSync } from 'child_process';
import {
  getWatchtowerDir, loadConfig, atomicWrite, resolveProjectIdentity, resolveProjectFromTranscriptSlug,
  encodeSlugComponent,
} from './watchtower-lib.mjs';
import {
  listItems, getItem, applyBatch, resolveItem, dismissItem, GATED_CATEGORIES,
  appendCarryEntry, appendThreadProposal, appendFeedbackOutboxEntry, redirectLedgerPath, slugify,
} from './watchtower-queue.mjs';

const here = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// The leaving set
// ---------------------------------------------------------------------------

// Whole categories that left (retired in the registry) plus the three whose
// meaning narrowed: pattern-promotion (instances leave, novel stays),
// upstream-friction (cc-feedback + project-tracker leave, config-change
// stays), advisor-finding (seat proposals stay). Walk order is smallest
// first (decision default 33), re-counted at sheet time.
export const RETIRED_WALKED = ['coverage-warning', 'verify-backfill', 'methodology-capture', 'doc-drift', 'skill-candidate', 'significance-change', 'raised-unhandled'];
export const NARROWED_WALKED = ['advisor-finding', 'upstream-friction', 'pattern-promotion'];
export const LEAVING_CATEGORIES = [...RETIRED_WALKED, ...NARROWED_WALKED];
export const WALK_ORDER = ['coverage-warning', 'verify-backfill', 'methodology-capture', 'doc-drift', 'skill-candidate', 'significance-change', 'advisor-finding', 'upstream-friction', 'raised-unhandled', 'pattern-promotion'];

export const DISPOSITIONS = ['keep', 'count', 'route-feedback', 'route-thread', 'route-record', 'file-action', 'carry', 'close-done', 'close-noise', 'ask'];
export const ROUTE_FEEDBACK_BATCH = 10;

/** Is this pending item in the slice that leaves the inbox? */
export function isLeaving(item) {
  if (!item) return false;
  const ev = item.evidence || {};
  if (RETIRED_WALKED.includes(item.category)) return true;
  if (item.category === 'pattern-promotion') return !!ev.instance_of_rule;
  if (item.category === 'upstream-friction') return !!(ev.route && ev.route.type) && ev.route.type !== 'config-change';
  if (item.category === 'advisor-finding') return !ev.roster_kind;
  return false;
}

// What the SHEET lists for a category: the leaving slice, plus — for
// pattern-promotion — the novel items too, so Claude confirms each `keep`
// by reading it rather than by predicate (a pattern whose rule was deleted
// is novel now; a novel pattern that now matches a rule is an instance).
function onSheet(item, category) {
  if (item.category !== category) return false;
  if (category === 'pattern-promotion') return true;
  return isLeaving(item);
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

export function today() { return new Date().toISOString().slice(0, 10); }
export function walkDir(date, watchtowerDir = getWatchtowerDir()) { return join(watchtowerDir, 'state', `inbox-walk-${date}`); }
export function sheetPath(category, date, watchtowerDir) { return join(walkDir(date, watchtowerDir), `sheet-${category}.jsonl`); }
export function appliedPath(date, watchtowerDir) { return join(walkDir(date, watchtowerDir), 'applied.jsonl'); }
export function expiredAtA1Path(watchtowerDir = getWatchtowerDir()) { return join(watchtowerDir, 'state', 'inbox-walk', 'expired-at-a1.txt'); }
export function backupDirFor(date, watchtowerDir = getWatchtowerDir()) { return join(watchtowerDir, 'queue', `items-backup-${date}-p2-walk`); }
function itemsDir(watchtowerDir = getWatchtowerDir()) { return join(watchtowerDir, 'queue', 'items'); }
function walkRecordPath(slug, date, watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', 'projects', slug, 'sessions', `${date}-inbox-walk.md`);
}

function readExpiredAtA1(watchtowerDir) {
  const p = expiredAtA1Path(watchtowerDir);
  if (!existsSync(p)) throw new Error(`the C0 snapshot ${p} is missing — without it the walk cannot tell which expired items expired after A1`);
  return new Set(readFileSync(p, 'utf8').split('\n').map((l) => l.trim()).filter(Boolean));
}

function readJsonl(p) {
  if (!existsSync(p)) return [];
  return readFileSync(p, 'utf8').split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l));
}

// ---------------------------------------------------------------------------
// --assert-quiet
// ---------------------------------------------------------------------------

/**
 * Is any producer still filing a leaving category? Looks at (1) pending
 * leaving-slice items filed after `since` and (2) redirect-ledger lines after
 * `since` — a redirected filing is already where it will be read, but the
 * producer behind it is still running the old way and must be found first.
 */
export function assertQuiet({ since, watchtowerDir = getWatchtowerDir() }) {
  if (!since || Number.isNaN(new Date(since).getTime())) throw new Error('--assert-quiet needs --since <iso> (the last dogfood reinstall)');
  const sinceMs = new Date(since).getTime();
  const items = listItems({ statuses: ['pending'] })
    .filter((i) => isLeaving(i) && new Date(i.filed_at).getTime() > sinceMs)
    .map((i) => ({ id: i.id, category: i.category, project: i.project, filed_by: i.filed_by, filed_at: i.filed_at, title: i.title }));
  const redirects = readJsonl(redirectLedgerPath()).filter((r) => new Date(r.ts).getTime() > sinceMs);
  return { quiet: items.length === 0 && redirects.length === 0, since, items, redirects, watchtowerDir };
}

// ---------------------------------------------------------------------------
// --backup (C0)
// ---------------------------------------------------------------------------

// The draft-archive precedent: copy every top-level item file BEFORE anything
// changes; an existing same-day backup is the true pre-walk state and is
// kept, but it must hold every item the queue holds now that it would have
// held then — a short or empty copy is refused.
export function backupQueue({ date = today(), watchtowerDir = getWatchtowerDir() } = {}) {
  const src = itemsDir(watchtowerDir);
  const dst = backupDirFor(date, watchtowerDir);
  const names = readdirSync(src, { withFileTypes: true }).filter((e) => e.isFile() && e.name.endsWith('.json')).map((e) => e.name);
  if (existsSync(dst)) {
    const have = new Set(readdirSync(dst));
    const missing = names.filter((n) => !have.has(n));
    const empty = names.filter((n) => have.has(n) && readFileSync(join(dst, n), 'utf8').trim() === '');
    if (missing.length > 0 || empty.length > 0) {
      throw new Error(`backup refused: ${dst} exists but does not hold ${missing.length + empty.length} of the queue's ${names.length} item files intact (first: ${(missing[0] || empty[0])}). Move it aside and re-run.`);
    }
    return { path: dst, files: have.size, created: false };
  }
  mkdirSync(dst, { recursive: true });
  for (const n of names) copyFileSync(join(src, n), join(dst, n));
  const copied = readdirSync(dst).length;
  if (copied !== names.length) throw new Error(`backup refused: ${dst} holds ${copied} files, the queue listed ${names.length}. Remove it and re-run.`);
  return { path: dst, files: copied, created: true };
}

// ---------------------------------------------------------------------------
// --sheet
// ---------------------------------------------------------------------------

function ageDays(iso, nowMs) {
  const t = new Date(iso).getTime();
  return Number.isNaN(t) ? null : Math.floor((nowMs - t) / 86400000);
}

export function buildSheet({ category, date = today(), watchtowerDir = getWatchtowerDir(), now = Date.now() }) {
  if (!LEAVING_CATEGORIES.includes(category)) throw new Error(`${category} is not a leaving category; the walk covers ${LEAVING_CATEGORIES.join(', ')}`);
  const expiredAtA1 = readExpiredAtA1(watchtowerDir);
  const rows = listItems({ category, statuses: ['pending', 'expired'] })
    .filter((i) => i.status === 'pending' || !expiredAtA1.has(i.id))
    .filter((i) => onSheet(i, category))
    .map((i) => ({
      id: i.id,
      status: i.status,
      expired_after_a1: i.status === 'expired',
      category: i.category,
      project: i.project,
      project_unresolved: i.project_unresolved === true,
      project_path: i.project_path || null,
      filed_by: i.filed_by,
      filed_at: i.filed_at,
      age_days: ageDays(i.filed_at, now),
      urgency: i.urgency,
      title: i.title,
      summary: i.summary,
      evidence: i.evidence || {},
      route: (i.evidence && i.evidence.route) || null,
      instance_of_rule: (i.evidence && i.evidence.instance_of_rule) || null,
      thread_ids: i.thread_ids || [],
      context_anchor: i.context_anchor || null,
      transcript_ref: i.transcript_ref || null,
      held: !!(i.evidence && i.evidence.held),
      gated: GATED_CATEGORIES.has(i.category),
    }));
  rows.sort((a, b) => a.filed_at < b.filed_at ? -1 : a.filed_at > b.filed_at ? 1 : a.id.localeCompare(b.id));
  void date;
  return rows;
}

export function writeSheet({ category, date = today(), watchtowerDir = getWatchtowerDir() }) {
  const rows = buildSheet({ category, date, watchtowerDir });
  const p = sheetPath(category, date, watchtowerDir);
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length ? '\n' : ''));
  return { path: p, count: rows.length, pending: rows.filter((r) => r.status === 'pending').length, expired_after_a1: rows.filter((r) => r.expired_after_a1).length };
}

function readSheet(category, date, watchtowerDir) {
  const p = sheetPath(category, date, watchtowerDir);
  if (!existsSync(p)) throw new Error(`no sheet for ${category} at ${p} — run --sheet --category ${category} first`);
  return readJsonl(p);
}

// ---------------------------------------------------------------------------
// The map
// ---------------------------------------------------------------------------

function readMap(mapPath) {
  if (!mapPath || !existsSync(mapPath)) throw new Error(`map not found: ${mapPath}`);
  const parsed = JSON.parse(readFileSync(mapPath, 'utf8'));
  const lines = Array.isArray(parsed) ? parsed : Object.entries(parsed).map(([id, v]) => ({ id, ...v }));
  const byId = new Map();
  for (const l of lines) {
    if (!l || typeof l.id !== 'string') throw new Error('every map line needs an id');
    byId.set(l.id, l);
  }
  return byId;
}

// The map must cover the whole sheet, every line must carry a legal
// disposition and a reason, and no `ask` may be left open — otherwise nothing
// in the category moves (plan Edge Cases: "nothing partial lands").
function validateMapForSheet(sheet, map, category) {
  const problems = [];
  const missing = sheet.filter((s) => !map.has(s.id)).map((s) => s.id);
  if (missing.length) problems.push(`the map does not cover ${missing.length} sheet item(s) — no item may be silently skipped: ${missing.slice(0, 5).join(', ')}${missing.length > 5 ? ', …' : ''}`);
  const asks = [];
  for (const s of sheet) {
    const l = map.get(s.id);
    if (!l) continue;
    if (!DISPOSITIONS.includes(l.disposition)) problems.push(`${s.id}: unknown disposition ${JSON.stringify(l.disposition)}`);
    if (typeof l.reason !== 'string' || !l.reason.trim()) problems.push(`${s.id}: every line needs a one-line, item-specific reason`);
    if (l.disposition === 'ask') asks.push(s.id);
    if (l.disposition === 'file-action' && !(l.action && typeof l.action.text === 'string' && typeof l.action.projectFid === 'string' && typeof l.action.notes === 'string')) {
      problems.push(`${s.id}: file-action needs action { text, projectFid, notes }`);
    }
  }
  if (asks.length) problems.push(`${asks.length} unanswered \`ask\` line(s) in ${category} — the operator decides them before this category applies: ${asks.slice(0, 5).join(', ')}${asks.length > 5 ? ', …' : ''}`);
  if (problems.length) throw new Error(`--apply refused for ${category}; nothing was changed:\n  - ${problems.join('\n  - ')}`);
}

// ---------------------------------------------------------------------------
// --render
// ---------------------------------------------------------------------------

const one = (s) => String(s ?? '').replace(/\s+/g, ' ').trim();

export function renderMap({ category, date = today(), mapPath, watchtowerDir = getWatchtowerDir() }) {
  const sheet = readSheet(category, date, watchtowerDir);
  const map = readMap(mapPath);
  const groups = new Map();
  const special = [];
  const uncovered = [];
  for (const s of sheet) {
    const l = map.get(s.id);
    if (!l) { uncovered.push(s); continue; }
    if (!groups.has(l.disposition)) groups.set(l.disposition, []);
    groups.get(l.disposition).push({ s, l });
    if (s.held || s.gated) special.push({ s, l });
  }
  const out = [];
  out.push(`${category} — ${sheet.length} item(s) on the sheet (${sheet.filter((s) => s.expired_after_a1).length} expired after A1)`);
  const order = [...DISPOSITIONS.filter((d) => groups.has(d)), ...[...groups.keys()].filter((d) => !DISPOSITIONS.includes(d))];
  for (const d of order) {
    const rows = groups.get(d);
    out.push('', `${d} (${rows.length})`);
    for (const { s, l } of rows) {
      const flags = [s.expired_after_a1 ? 'expired' : null, s.held ? 'held' : null, s.gated ? 'gated' : null].filter(Boolean);
      out.push(`  ${s.id} · ${s.project}${s.age_days != null ? ` · ${s.age_days}d` : ''}${flags.length ? ` · ${flags.join(',')}` : ''} · ${one(s.title).slice(0, 90)} — ${one(l.reason)}${l.action ? ` → action: ${one(l.action.text).slice(0, 80)}` : ''}`);
    }
  }
  out.push('', `held or gated (${special.length})`);
  for (const { s, l } of special) out.push(`  ${s.id} · ${l.disposition} · ${one(s.title).slice(0, 90)}`);
  if (uncovered.length) {
    out.push('', `NOT IN THE MAP (${uncovered.length}) — the apply will refuse until these are covered`);
    for (const s of uncovered) out.push(`  ${s.id} · ${one(s.title).slice(0, 90)}`);
  }
  return out.join('\n') + '\n';
}

// ---------------------------------------------------------------------------
// --apply
// ---------------------------------------------------------------------------

// Which registered project does this item belong to? The draft archive's
// two routes (config key, then the transcript-dir slug), both ending in the
// canonical resolver — never a slug built by hand — plus a third for the
// items ring3-close filed with `project_unresolved: true` and the MAIN
// checkout's transcript-dir slug as `project` (`-Users-x-flow`, no
// transcript_ref): that slug is matched against the encoding of each
// registered path, which the worktree decoder cannot do because it anchors
// on the mux worktrees root. Returns null when nothing registered claims it;
// dispositions that need a project then refuse.
function resolveItemProject(item, config, cache) {
  const projects = (config && config.projects) || {};
  const key = item && item.project;
  const memo = (k, fn) => { if (!cache.has(k)) cache.set(k, fn()); return cache.get(k); };
  let identity = null;
  if (typeof key === 'string' && item.project_unresolved !== true
      && Object.prototype.hasOwnProperty.call(projects, key) && projects[key] && projects[key].path) {
    identity = memo(`key:${key}`, () => { try { return resolveProjectIdentity(projects[key].path, config); } catch { return null; } });
  }
  if (!identity || !identity.registered) {
    const tPath = item && item.transcript_ref && item.transcript_ref.path;
    if (typeof tPath === 'string' && tPath) {
      const slugDir = tPath.split('/').slice(-2, -1)[0];
      identity = memo(`slug:${slugDir}`, () => { try { return resolveProjectFromTranscriptSlug(slugDir, config); } catch { return null; } });
    } else identity = null;
  }
  if ((!identity || !identity.registered) && typeof key === 'string' && key.startsWith('-')) {
    identity = memo(`pathslug:${key}`, () => {
      const owner = Object.entries(projects).find(([, p]) => p && p.path && encodeSlugComponent(p.path) === key);
      if (!owner) return null;
      try { return resolveProjectIdentity(owner[1].path, config); } catch { return null; }
    });
  }
  if (!identity || !identity.registered) return null;
  return { name: identity.name, slug: identity.slug, path: identity.path };
}

// A map line may name the project its side effect belongs to
// (`project: '<config key>'`). The pattern producer files every observation
// under the project that RUNS the rings, so a lesson about another project's
// skill would otherwise land in the wrong ledger, record or tracker. The key
// must be a registered project: an unknown key throws, and the item stays
// pending with the error reported (never a silent fall-back to the item's own
// project).
function projectForLine(line, item, config, cache) {
  const named = line && typeof line.project === 'string' ? line.project : '';
  if (!named) return resolveItemProject(item, config, cache);
  const projects = (config && config.projects) || {};
  const entry = Object.prototype.hasOwnProperty.call(projects, named) ? projects[named] : null;
  if (!entry || !entry.path) throw new Error(`the map line names project ${JSON.stringify(named)}, which is not a registered project`);
  const k = `key:${named}`;
  if (!cache.has(k)) cache.set(k, (() => { try { return resolveProjectIdentity(entry.path, config); } catch { return null; } })());
  const identity = cache.get(k);
  if (!identity || !identity.registered) throw new Error(`the map line names project ${JSON.stringify(named)}, which does not resolve to a registered project`);
  return { name: identity.name, slug: identity.slug, path: identity.path };
}

function appliedSet(date, watchtowerDir) {
  const set = new Set();
  for (const r of readJsonl(appliedPath(date, watchtowerDir))) set.add(`${r.id}:${r.effect}`);
  return set;
}
function recordApplied(date, watchtowerDir, record) {
  const p = appliedPath(date, watchtowerDir);
  mkdirSync(dirname(p), { recursive: true });
  appendFileSync(p, JSON.stringify({ ts: new Date().toISOString(), ...record }) + '\n');
}

// The walk record — a session-record-shaped file under the project's
// sessions dir, so /recall indexes it (decision default 34). Created with its
// own header; sections appended per category.
function appendWalkRecord(slug, date, category, lines, watchtowerDir) {
  const p = walkRecordPath(slug, date, watchtowerDir);
  let content = existsSync(p) ? readFileSync(p, 'utf8')
    : `# Inbox backlog walk ${date}\n\nDate: ${date}\n\nItems from the inbox categories that left it (Simplification Project 2, plan C1), kept for the record rather than acted on. Each line names the inbox item it came from.\n`;
  const heading = `## ${category}`;
  const body = lines.map((l) => one(l)).filter(Boolean);
  if (!body.length) return { path: p, written: 0 };
  const idx = content.indexOf(`\n${heading}\n`);
  if (idx < 0) content = `${content.trimEnd()}\n\n${heading}\n${body.join('\n')}\n`;
  else {
    const start = idx + 1 + heading.length + 1;
    const next = content.indexOf('\n## ', start);
    const end = next > 0 ? next : content.length;
    content = `${content.slice(0, end).trimEnd()}\n${body.join('\n')}\n${next > 0 ? content.slice(next) : ''}`;
  }
  atomicWrite(p, content);
  return { path: p, written: body.length };
}

function recordLine(item, reason) {
  return `- ${item.id} (${item.filed_at.slice(0, 10)}, ${item.project}): ${one(item.title)} — ${one(item.summary).slice(0, 400)} [walk: ${one(reason)}]`;
}

// file-action: the project's OWN CLI, cwd = the project, so the consumer's
// own library validates (decision default 32). `pibCli` overrides the script
// path (tests point it at the repo's; the db is still the fixture's because
// the CLI resolves pib.db from cwd).
function createActionViaCli({ projectPath, pibCli, action }) {
  const cli = pibCli || join(projectPath, 'scripts', 'pib-db.mjs');
  if (!existsSync(cli)) return { error: `no tracker CLI at ${cli}` };
  const args = [cli, 'create-action', action.text, '--projectFid', action.projectFid, '--notes', action.notes];
  try {
    const out = execFileSync('node', args, { cwd: projectPath, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
    // printResult prints the library's message ("Created action act:xxxxxxxx: …")
    // on success and the refusal on stderr with exit 1 (caught below).
    const m = out.match(/\b(act:[0-9a-f]{8})\b/);
    if (!m) return { error: `unexpected CLI output: ${out.slice(0, 200)}` };
    return { fid: m[1], deduplicated: /dedup/i.test(out) };
  } catch (e) {
    // printResult exits 1 and writes the refusal to stderr.
    const msg = (e.stderr && String(e.stderr).trim()) || e.message;
    return { error: msg.split('\n')[0] || 'create-action failed' };
  }
}

function digestBody(rows, { k, n, date, mapPath }) {
  const lines = [
    `Friction backlog walk (Simplification Project 2, plan C1), batch ${k} of ${n}, ${date}. Each report below was a pending cc-feedback upstream-friction inbox item that Claude read and judged still live; the operator approved this batch. Triage each at arrival as usual. Map: ${mapPath}`,
    '',
  ];
  for (const { s, l } of rows) {
    lines.push(`### ${one(s.title)}`, `- inbox item: ${s.id} · project: ${s.project} · filed: ${s.filed_at.slice(0, 10)}`);
    if (s.route && s.route.detail) lines.push(`- route: ${one(s.route.detail)}`);
    if (s.summary) lines.push(`- summary: ${one(s.summary)}`);
    lines.push(`- walk reason: ${one(l.reason)}`, '');
  }
  return lines.join('\n');
}

/**
 * Apply one approved category. Returns a report; throws (changing nothing)
 * when the map does not cover the sheet or holds an unanswered ask.
 * @param {object} o
 * @param {number} [o.feedbackBatches=1] how many route-feedback batches the operator has released
 * @param {string} [o.pibCli] override the tracker CLI path (tests)
 * @param {object} [o.deps] { applyBatch } (tests observe the bulk door)
 */
export async function applyCategory({ category, date = today(), mapPath, watchtowerDir = getWatchtowerDir(), feedbackBatches = 1, pibCli = null, deps = {} }) {
  const batchFn = deps.applyBatch || applyBatch;
  const sheet = readSheet(category, date, watchtowerDir);
  const map = readMap(mapPath);
  validateMapForSheet(sheet, map, category);
  const config = loadConfig(watchtowerDir);
  const cache = new Map();
  const done = appliedSet(date, watchtowerDir);
  const mark = (id, effect, extra = {}) => { recordApplied(date, watchtowerDir, { id, category, effect, ...extra }); done.add(`${id}:${effect}`); };

  const report = {
    category, map: mapPath, sheet: sheet.length, closed: 0, kept: 0, expired_side_effect_only: 0,
    skipped_not_pending: [], errors: [], by_disposition: {},
    feedback: { total_batches: 0, delivered_batches: 0, deferred_items: 0 },
    file_action: { created: 0, refused_to_carry: 0 },
    records: {}, carry_entries: 0, instances_appended: 0, thread_proposals: 0,
  };

  // Re-read every item now; the sheet is a snapshot.
  const live = new Map();
  for (const s of sheet) {
    const fresh = getItem(s.id);
    if (!fresh) { report.skipped_not_pending.push(s.id); continue; }
    if (fresh.status !== s.status) { report.skipped_not_pending.push(s.id); continue; }
    live.set(s.id, { s, fresh, l: map.get(s.id) });
  }

  // Per-item notes for closure (the disposition's reason, plus what the side
  // effect produced); effective dispositions (a refused file-action → carry).
  const notes = new Map();
  const effective = new Map();
  const closable = (id) => { const e = live.get(id); return e && e.fresh.status === 'pending'; };
  const noteFor = (id) => notes.get(id) || `inbox backlog walk ${date}: ${one(live.get(id).l.reason)}`;

  for (const [id, { l }] of live) effective.set(id, l.disposition);

  // ---- side effects, each idempotent through applied.jsonl ----

  // route-feedback: batches of ≤10, operator-paced.
  // Batches are numbered from the SHEET (every route-feedback line, whatever
  // its current status), so a re-run after batch 1 closed still calls the
  // next one "batch 2 of N" and never re-delivers batch 1.
  const feedbackRows = sheet
    .filter((s) => map.get(s.id) && map.get(s.id).disposition === 'route-feedback')
    .sort((a, b) => a.id.localeCompare(b.id))
    .map((s) => ({ s, l: map.get(s.id) }));
  const totalBatches = Math.ceil(feedbackRows.length / ROUTE_FEEDBACK_BATCH);
  report.feedback.total_batches = totalBatches;
  const released = new Set();
  for (let k = 1; k <= totalBatches; k++) {
    const rows = feedbackRows.slice((k - 1) * ROUTE_FEEDBACK_BATCH, k * ROUTE_FEEDBACK_BATCH);
    if (k > feedbackBatches) { report.feedback.deferred_items += rows.length; continue; }
    const key = `batch-${k}`;
    const already = rows.every(({ s }) => done.has(`${s.id}:route-feedback:${key}`));
    if (!already) {
      try {
        const r = appendFeedbackOutboxEntry({
          source: 'inbox backlog walk', date, component: 'inbox-walk',
          title: `Friction backlog walk, batch ${k} of ${totalBatches} (${date})`,
          body: digestBody(rows, { k, n: totalBatches, date, mapPath }),
        });
        for (const { s } of rows) mark(s.id, `route-feedback:${key}`, { outbox: r.path, appended: r.appended, duplicate_of: r.duplicate_of || null });
      } catch (e) {
        report.errors.push({ batch: k, error: e.message });
        continue;
      }
    }
    report.feedback.delivered_batches += 1;
    for (const { s, l } of rows) {
      if (!live.has(s.id)) continue; // already closed by an earlier run, or no longer pending
      released.add(s.id);
      notes.set(s.id, `inbox backlog walk ${date}: ${one(l.reason)} — delivered in feedback digest batch ${k} of ${totalBatches}`);
    }
  }

  for (const [id, { s, fresh, l }] of live) {
    const d = l.disposition;
    try {
      if (d === 'count') {
        if (!done.has(`${id}:count`)) {
          const proj = resolveItemProject(fresh, config, cache);
          if (!proj) throw new Error('no registered project for this item — count needs a project to file the instance under');
          const ring2 = await loadRing2();
          const ev = fresh.evidence || {};
          const rule = ev.instance_of_rule || {};
          // Already counted ANYWHERE: the ring writes an instance to the ledger of
          // the project whose rule it broke, while the inbox item is filed under
          // the project that runs the rings. Checking only the item's own project
          // re-appended, dated today, instances the rule's own project already
          // held — and the counter windows on filed_at, so that manufactures a
          // promotion proposal out of month-old observations.
          if (!cache.has('rule-instance-hashes')) {
            const dir = join(watchtowerDir, 'state', ring2.RULE_INSTANCE_DIR);
            const seen = new Set();
            const names = existsSync(dir) ? readdirSync(dir).filter((f) => f.endsWith('.jsonl')).map((f) => f.slice(0, -'.jsonl'.length)) : [];
            for (const name of names) for (const x of ring2.readRuleInstances(watchtowerDir, name)) if (x && x.pattern_hash) seen.add(x.pattern_hash);
            cache.set('rule-instance-hashes', seen);
          }
          const seenHashes = cache.get('rule-instance-hashes');
          const existing = !!ev.pattern_hash && seenHashes.has(ev.pattern_hash);
          if (!existing) {
            if (ev.pattern_hash) seenHashes.add(ev.pattern_hash);
            ring2.appendRuleInstance(watchtowerDir, proj.name, {
              rule: rule.rule || null, rule_hash: rule.rule_hash || null, rule_kind: rule.rule_kind || null, rule_file: rule.rule_file || null,
              rule_scope: rule.rule_scope || null, project: proj.name, session_id: ev.session_id || null,
              pattern_hash: ev.pattern_hash || null, pattern_title: ev.pattern_title || fresh.title, source_date: ev.source_date || null,
              evidence: (ev.pattern_text || fresh.summary || '').slice(0, 300), theme: ev.theme || null,
              filed_at: new Date().toISOString(), classifier_on: true, filed_item: false, walked_from: id,
            });
            report.instances_appended += 1;
          }
          mark(id, 'count', { project: proj.name, appended: !existing });
        }
        notes.set(id, `inbox backlog walk ${date}: ${one(l.reason)} — counted as a rule instance`);
      } else if (d === 'route-thread') {
        if (!done.has(`${id}:route-thread`)) {
          const ev = fresh.evidence || {};
          const r = appendThreadProposal(join(watchtowerDir, 'state', 'threads'), ev.thread, { ...ev, explanation: ev.explanation || fresh.summary || '' }, { allowQueueItems: [id] });
          if (!r) throw new Error(`thread file for ${JSON.stringify(ev.thread)} is missing — mark this item close-noise ("thread gone") instead`);
          if (r.outcome === 'appended' || r.outcome === 'corroborated') report.thread_proposals += 1;
          mark(id, 'route-thread', { outcome: r.outcome, path: r.path });
          notes.set(id, `inbox backlog walk ${date}: ${one(l.reason)} — written to the thread (${r.outcome})`);
        }
      } else if (d === 'route-record') {
        if (!done.has(`${id}:route-record`)) {
          const proj = projectForLine(l, fresh, config, cache);
          if (!proj) throw new Error('no registered project for this item — route-record needs a project record to land in');
          const r = appendWalkRecord(proj.slug, date, category, [recordLine(fresh, l.reason)], watchtowerDir);
          report.records[proj.slug] = (report.records[proj.slug] || 0) + 1;
          mark(id, 'route-record', { path: r.path });
          notes.set(id, `inbox backlog walk ${date}: ${one(l.reason)} — kept in ${r.path}`);
        }
      } else if (d === 'carry') {
        if (!done.has(`${id}:carry`)) {
          const proj = projectForLine(l, fresh, config, cache);
          if (!proj) throw new Error('no registered project for this item — carry needs a project ledger');
          const entry = appendCarryEntry(proj.name, carryShape(fresh, l.reason, mapPath));
          report.carry_entries += 1;
          mark(id, 'carry', { project: proj.name, carry_id: entry.id });
          notes.set(id, `inbox backlog walk ${date}: ${one(l.reason)} — carried to ${proj.name}'s next session (${entry.id})`);
        }
      } else if (d === 'file-action') {
        if (!done.has(`${id}:file-action`) && !done.has(`${id}:carry`)) {
          const proj = projectForLine(l, fresh, config, cache);
          if (!proj) throw new Error('no registered project for this item — file-action needs the project\'s tracker');
          const r = createActionViaCli({ projectPath: proj.path, pibCli, action: l.action });
          if (r.fid) {
            report.file_action.created += 1;
            mark(id, 'file-action', { project: proj.name, fid: r.fid, deduplicated: r.deduplicated });
            notes.set(id, `inbox backlog walk ${date}: ${one(l.reason)} — filed as ${r.fid} in ${proj.name}`);
          } else {
            // The library refused: the line becomes carry with the refusal as its reason (plan C1).
            const entry = appendCarryEntry(proj.name, carryShape(fresh, `file-action refused by the tracker (${one(r.error)}); ${one(l.reason)}`, mapPath));
            report.file_action.refused_to_carry += 1;
            report.carry_entries += 1;
            effective.set(id, 'carry');
            mark(id, 'carry', { project: proj.name, carry_id: entry.id, refused: r.error });
            notes.set(id, `inbox backlog walk ${date}: file-action refused — ${one(r.error)}; carried to ${proj.name}'s next session (${entry.id})`);
          }
        } else if (done.has(`${id}:carry`)) effective.set(id, 'carry');
      }
      // keep, close-done, close-noise, route-feedback: no side effect here.
    } catch (e) {
      report.errors.push({ id, disposition: d, error: e.message });
      effective.set(id, 'error');
    }
    void s;
  }

  // ---- closures ----
  const batches = new Map(); // disposition → ids (plain pending items)
  for (const [id, { s, fresh, l }] of live) {
    const d = effective.get(id);
    report.by_disposition[d] = (report.by_disposition[d] || 0) + 1;
    if (d === 'error' || d === 'ask') continue;
    if (d === 'keep') { report.kept += 1; continue; }
    if (d === 'route-feedback' && !released.has(id)) continue; // a later batch
    if (fresh.status === 'expired') { report.expired_side_effect_only += 1; continue; }
    if (!closable(id)) continue;
    const isSpecial = s.held || GATED_CATEGORIES.has(fresh.category);
    if (isSpecial) {
      const r = d === 'close-noise'
        ? dismissItem(id, { notes: noteFor(id), resolution_type: d })
        : resolveItem(id, { resolution: mapPath, resolution_notes: noteFor(id), resolution_type: d });
      if (r) report.closed += 1; else report.skipped_not_pending.push(id);
      continue;
    }
    if (!batches.has(d)) batches.set(d, []);
    batches.get(d).push(id);
    void l;
  }
  for (const [d, ids] of batches) {
    // applyBatch stamps ONE notes string on every item of a batch; the
    // item-specific reason must still ride on each item, so batches are per
    // disposition AND per note text (most items have their own).
    const byNote = new Map();
    for (const id of ids) { const n = noteFor(id); if (!byNote.has(n)) byNote.set(n, []); byNote.get(n).push(id); }
    for (const [n, group] of byNote) {
      try {
        const r = batchFn(group, d === 'close-noise'
          ? { disposition: 'dismiss', resolution_type: d, notes: n }
          : { disposition: 'resolve', resolution_type: d, notes: n, resolution: mapPath });
        report.closed += r.applied.length;
        report.skipped_not_pending.push(...r.skipped_not_pending);
      } catch (e) {
        report.errors.push({ ids: group, disposition: d, error: e.message });
      }
    }
  }
  return report;
}

function carryShape(item, reason, mapPath) {
  const ev = item.evidence || {};
  const kind = item.category === 'upstream-friction' ? 'friction' : 'loose-end';
  const detail = [one(item.summary), ev.route && ev.route.detail ? `Route: ${ev.route.type} — ${one(ev.route.detail)}` : null, `Walk: ${one(reason)}`].filter(Boolean).join('\n');
  return {
    kind, title: one(item.title), detail, source_session: ev.session_id || null,
    source: { category: item.category, filed_by: item.filed_by, project_path: item.project_path || null, urgency: item.urgency || null, item_id: item.id, walk_map: mapPath },
  };
}

// ring2 is imported lazily — it pulls in the whole Ring 3 module graph, and
// only `count` needs its rule-instance writer (one definition, never a copy).
let ring2Module = null;
async function loadRing2() {
  if (!ring2Module) ring2Module = await import(pathToFileURL(join(here, 'watchtower-ring2.mjs')).href);
  return ring2Module;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const o = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) { o._.push(a); continue; }
    const k = a.slice(2);
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--')) { o[k] = next; i++; } else o[k] = true;
  }
  return o;
}

async function main() {
  const o = parseArgs(process.argv.slice(2));
  const date = typeof o.date === 'string' ? o.date : today();
  const json = o.json === true;
  const say = (x) => console.log(json && typeof x !== 'string' ? JSON.stringify(x, null, 2) : x);
  if (o['assert-quiet']) {
    const r = assertQuiet({ since: o.since });
    if (r.quiet) { say(json ? r : `quiet: no leaving-category item filed and no redirect logged since ${r.since}`); return; }
    say(json ? r : [`NOT QUIET since ${r.since}:`, ...r.items.map((i) => `  item ${i.id} ${i.category} ${i.project} by ${i.filed_by} at ${i.filed_at}: ${i.title}`), ...r.redirects.map((x) => `  redirect ${x.ts} ${x.category} ${x.project} by ${x.filed_by} → ${x.to || 'refused'}`)].join('\n'));
    process.exit(1);
  }
  if (o.backup) { const r = backupQueue({ date }); say(json ? r : `${r.created ? 'backed up' : 'backup already present'}: ${r.files} item files at ${r.path}`); return; }
  if (o.sheet) {
    if (typeof o.category !== 'string') throw new Error('--sheet needs --category <c>');
    const r = writeSheet({ category: o.category, date });
    say(json ? r : `${r.count} item(s) on the sheet (${r.pending} pending, ${r.expired_after_a1} expired after A1): ${r.path}`);
    return;
  }
  if (o.render) {
    if (typeof o.category !== 'string') throw new Error('--render needs --category <c>');
    say(renderMap({ category: o.category, date, mapPath: typeof o.map === 'string' ? o.map : join(walkDir(date), 'map.json') }));
    return;
  }
  if (o.apply) {
    if (typeof o.category !== 'string') throw new Error('--apply needs --category <c>');
    const mapPath = typeof o.map === 'string' ? o.map : join(walkDir(date), 'map.json');
    const r = await applyCategory({
      category: o.category, date, mapPath,
      feedbackBatches: typeof o['feedback-batches'] === 'string' ? Number(o['feedback-batches']) : 1,
      pibCli: typeof o['pib-cli'] === 'string' ? resolve(o['pib-cli']) : null,
    });
    say(json ? r : [
      `${r.category}: ${r.closed} closed, ${r.kept} kept, ${r.expired_side_effect_only} expired (side effect only), ${r.skipped_not_pending.length} no longer pending`,
      `  by disposition: ${Object.entries(r.by_disposition).map(([k, v]) => `${k} ${v}`).join(', ') || 'none'}`,
      r.feedback.total_batches ? `  feedback: ${r.feedback.delivered_batches} of ${r.feedback.total_batches} batch(es) delivered, ${r.feedback.deferred_items} item(s) waiting for the next release` : null,
      r.file_action.created || r.file_action.refused_to_carry ? `  actions: ${r.file_action.created} created, ${r.file_action.refused_to_carry} refused → carried` : null,
      r.carry_entries ? `  carry entries: ${r.carry_entries}` : null,
      r.instances_appended ? `  rule instances appended: ${r.instances_appended}` : null,
      r.thread_proposals ? `  thread proposals written: ${r.thread_proposals}` : null,
      Object.keys(r.records).length ? `  records: ${Object.entries(r.records).map(([k, v]) => `${k} ${v}`).join(', ')}` : null,
      r.errors.length ? `  ERRORS (${r.errors.length}):\n${r.errors.map((e) => `    ${JSON.stringify(e)}`).join('\n')}` : null,
    ].filter(Boolean).join('\n'));
    if (r.errors.length) process.exit(1);
    return;
  }
  throw new Error('usage: watchtower-inbox-walk.mjs --assert-quiet --since <iso> | --backup | --sheet --category <c> | --render --category <c> [--map f] | --apply --category <c> [--map f] [--feedback-batches n] [--pib-cli p]   [--date YYYY-MM-DD] [--json]');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((e) => { console.error(e.message); process.exit(2); });
}

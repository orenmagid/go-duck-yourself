#!/usr/bin/env node

// Prospective-commitment detection — the shared seam between /close's
// commitment sweep and Ring 3's dedup (act:aa554774).
//
// THE DISTINCTION THIS MODULE ENCODES. /close delegates all capture to the
// background rings, on the reasoning that the operator should be able to walk
// away in 15–30 seconds. That reasoning holds for RETROSPECTIVE capture —
// lessons, decisions, knowledge extraction. Losing a day costs nothing, so the
// inbox is the right home.
//
// It does not hold for PROSPECTIVE, TIME-BOUND commitments made during the
// session. Those have a clock. On 2026-07-30 a session produced two of them —
// "set the new device's vibration to High on arrival" and "cancel the 3-month
// trial before it auto-renews at $9.99/mo" — and both were written into the
// notes of an action that was being marked COMPLETED. Neither became an
// action. /close ran clean. The inbox they would otherwise have landed in
// opened that day at 275 pending, oldest 26 days.
//
// The test, and it is the whole rule: CAN THIS WAIT IN A QUEUE FOR A WEEK
// WITHOUT COST? Retrospective, yes — leave it to the rings. A dated
// commitment, no — it has to be filed while the operator is still there to
// confirm it.
//
// WHAT THIS MODULE IS AND IS NOT. It is a SCREEN and a shared vocabulary, not
// an oracle. /close runs inside the session and can read it semantically;
// this gives that reading a concrete, testable definition and — more
// importantly — gives Ring 3 the SAME definition, so the ring can recognize
// that a commitment it is about to extract has already been filed as an
// action minutes earlier. Two sides of one seam: /close files forward,
// Ring 3 declines to duplicate.
//
// Deliberately NOT in watchtower-lib.mjs: that module is ring-loaded and
// soak-frozen, and this is consumed by a skill and one ring phase.

import { foldTokens, overlapCoefficient } from './watchtower-queue.mjs';

// --- The cue vocabulary ---------------------------------------------------
//
// Two independent signals must BOTH be present for a line to be a candidate:
// a FUTURE-BOUND cue (this refers to something not yet true) and an
// OBLIGATION shape (somebody has to do something). Requiring both is what
// keeps ordinary past-tense narration and hypotheticals out. Each cue class
// is named so a detection can say WHY it fired.

// Something happens at, by, or before a moment — the clock half.
export const TEMPORAL_CUES = [
  ['explicit-date', /\b(19|20)\d{2}-\d{2}-\d{2}\b/i],
  ['explicit-date', /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}(st|nd|rd|th)?\b/i],
  ['deadline', /\b(by|before|no later than|ahead of|in time for)\s+(the\s+)?(\d|next|end|close|monday|tuesday|wednesday|thursday|friday|saturday|sunday|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i],
  ['deadline', /\b(due|deadline|expires?|expiry|expiration|cut ?off)\b/i],
  ['renewal', /\b(auto-?renew(s|ed|al|ing)?|renews? on|renewal date|rebills?|next billing|free trial ends)\b/i],
  ['on-event', /\b(when|once|as soon as)\s+(it|they|he|she|the\s+\w+)\s+(arrives?|lands?|ships?|returns?|comes? back|is delivered|gets? here)\b/i],
  ['on-event', /\bon\s+(arrival|delivery|receipt|landing)\b/i],
  ['relative', /\b(tomorrow|tonight|next\s+(week|month|monday|tuesday|wednesday|thursday|friday|saturday|sunday)|in\s+\d+\s+(days?|weeks?|months?))\b/i],
];

// Somebody has to DO something — the obligation half.
export const OBLIGATION_CUES = [
  ['owed', /\b(still owed|still need(s)? to|need(s)? to|must|have to|has to|should)\b/i],
  ['imperative', /\b(cancel|renew|unsubscribe|return|ship|send|pay|book|call|email|schedule|order|install|reinstall|enable|disable|set|switch|turn (on|off)|follow up|check|confirm|remind)\b/i],
  ['commitment', /\b(i'?ll|we'?ll|i will|we will|going to|plan to|promised to|committed to|agreed to)\b/i],
  ['todo', /\b(don'?t forget|remember to|make sure to|todo|to-do|action item)\b/i],
];

// Past-tense / already-handled markers. A line carrying one of these is
// reporting, not committing — "cancelled the trial before it renewed" is a
// record of a thing that HAPPENED. Without this the imperative cue list turns
// every completed-work sentence into a commitment.
export const SETTLED_MARKERS = [
  /\b(already|cancell?ed|renewed|returned|shipped|sent|paid|booked|called|emailed|scheduled|ordered|installed|enabled|disabled|switched|confirmed|done|completed|finished|no longer needed)\b/i,
];

// A commitment is a sentence, not a paragraph. Anything longer is prose that
// happens to contain a cue.
export const MAX_COMMITMENT_CHARS = 400;

/**
 * Detect prospective, time-bound obligations in a block of text.
 *
 * A line qualifies when it carries BOTH a temporal cue and an obligation cue
 * and does NOT carry a settled marker. Returns one entry per qualifying line,
 * each naming the cues that fired so a caller can explain itself.
 *
 * This is a SCREEN. It is tuned to surface candidates for a human to confirm,
 * never to file anything on its own — the operator approves every action.
 *
 * @param {string} text
 * @returns {Array<{text: string, temporal: string, obligation: string}>}
 */
export function detectCommitments(text) {
  if (typeof text !== 'string' || !text.trim()) return [];
  const out = [];
  const seen = new Set();
  // Split on sentence and line boundaries — a commitment lives in one clause.
  const lines = text
    .split(/\n+|(?<=[.!?])\s+/)
    .map((l) => l.trim())
    .filter(Boolean);
  for (const line of lines) {
    if (line.length > MAX_COMMITMENT_CHARS) continue;
    if (SETTLED_MARKERS.some((re) => re.test(line))) continue;
    const temporal = TEMPORAL_CUES.find(([, re]) => re.test(line));
    if (!temporal) continue;
    const obligation = OBLIGATION_CUES.find(([, re]) => re.test(line));
    if (!obligation) continue;
    const key = commitmentKey(line);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({ text: line, temporal: temporal[0], obligation: obligation[0] });
  }
  return out;
}

/**
 * A normalized dedup key for a commitment — sorted meaningful tokens. Two
 * wordings of the same obligation collapse to the same key only when they are
 * near-identical; for the fuzzier judgment use commitmentMatchesAction.
 * @param {string} text
 * @returns {string}
 */
export function commitmentKey(text) {
  return [...foldTokens(String(text || ''))].sort().join(' ');
}

// How much of the shorter side two texts must share to count as the SAME
// obligation. Deliberately high: the cost of a false match is a real
// commitment silently not filed, which is the exact failure this whole action
// exists to fix. The cost of a miss is one duplicate inbox item the operator
// dismisses in a second.
export const COMMITMENT_MATCH_THRESHOLD = 0.6;
// Below this many meaningful tokens, overlap is not evidence of anything.
export const COMMITMENT_MIN_TOKENS = 3;

/**
 * Does this candidate commitment describe the same obligation as an existing
 * action's text? Overlap coefficient over the shared fold tokenizer — the same
 * recipe the queue's fold pass uses, at a much higher bar.
 * @param {string} candidateText
 * @param {string} actionText
 * @returns {boolean}
 */
export function commitmentMatchesAction(candidateText, actionText) {
  const a = foldTokens(String(candidateText || ''));
  const b = foldTokens(String(actionText || ''));
  if (a.size < COMMITMENT_MIN_TOKENS || b.size < COMMITMENT_MIN_TOKENS) return false;
  return overlapCoefficient(a, b) >= COMMITMENT_MATCH_THRESHOLD;
}

/**
 * Find the already-filed action covering this commitment, if any.
 *
 * `actions` are rows of {fid, text} — the caller decides the window (Ring 3
 * passes actions CREATED during the session, which is precisely the set
 * /close's sweep could have filed).
 *
 * Returns the matching action or null. Never throws on malformed rows.
 * @param {string} candidateText
 * @param {Array<{fid: string, text: string}>} actions
 * @returns {{fid: string, text: string}|null}
 */
export function findFiledCommitment(candidateText, actions) {
  for (const a of Array.isArray(actions) ? actions : []) {
    if (!a || typeof a.text !== 'string') continue;
    if (commitmentMatchesAction(candidateText, a.text)) return a;
  }
  return null;
}

// ---------------------------------------------------------------------------
// The /close session-residue check — the MECHANICAL half (Simplification
// Project 2, prj:4a433dac, plan B3; act:73dd31d4)
// ---------------------------------------------------------------------------
//
// `--residue --session-id <id> --cwd <dir>` prints JSON { findings: [...] } —
// EVERY finding; the skill caps at render time (T7). Four kinds:
//   commitment  — detectCommitments over the transcript's prose (the sweep
//                 this module always was; now residue category (a));
//   verify-plan — an action CREATED or COMPLETED in the session (read from
//                 the transcript's own pib_* tool calls — no `updated_at`
//                 column exists and `created_at` is absent on older schemas,
//                 so the tracker is never asked "what changed", T7) whose
//                 notes lack `## Verify Plan`, only when the project has
//                 e2e/features/ (the gate Phase 2q and the deleted
//                 verify-backfill scan used);
//   coverage    — UI paths changed in the session's commits with no .feature
//                 edit (detectUncoveredUi + resolveUiPaths), same gate;
// Each carries a proposed disposition the skill shows and the operator
// accepts with one word or edits by number. Carried friction and loose ends
// are NOT findings (act:b94d21b0): they print in a separate `carried` array
// (carriedEntries — the same filter /briefing's --carry uses). /close numbers
// one only when the session fixed it (it appends that finding itself, with
// proposed "fixed: <sha>") and shows the rest as one pointer line; for an
// unregistered project (`carried_offer: true`) /close still offers them,
// because /briefing cannot address that ledger.
//
// `--record --session-id <id>` writes the receipt (state/residue/<id>.json,
// keyed by session id — T4) from stdin or `--json '<string>'`, never a file
// (the close-scope guard blocks Write and `cat`). It is written BEFORE the
// question is asked, with answered: false. It MERGES: advisor_findings
// accumulate across writes (the by-hand /debrief and /orient filers write
// them, plan A4), and a write with advisor_findings but no findings array
// leaves the residue fields untouched — on a fresh receipt it writes no
// findings array at all, so Ring 3 does not read it as "/close asked".
// `--record-answer --session-id <id>` takes the dispositions the same way,
// stamps answered: true, and settles every carry entry a finding line names
// through settleCarry (a second answer on a resolved entry is a reported
// no-op). With carried lines and no resolvable project it exits 1 BEFORE any
// write, naming --cwd.
//
// /briefing's doors (act:b94d21b0): `--carry --project <name|slug>` lists the
// project's unresolved friction + loose ends; `--settle --project … --json
// '{"settlements":{"<carry_id>":{"disposition","proposed"}}}'` settles them.
// --record-answer and --settle share ONE path, settleCarry → one
// lib.resolveCarryEntries write per call.
//
// Why this script and not a new one (decision default 12): it is already
// /close's, already on the close-scope guard's node allowlist, and already the
// commitment sweep the residue check absorbs. The lib/ring3 imports are
// dynamic and CLI-only so the module stays light for Ring 3's own import.

export const VERIFY_PLAN_RE = /^##\s*Verify Plan\b/m;
export const PIB_ACTION_TOOL_RE = /pib_(create|update|complete)_action$/;
const FID_RE = /\bact:[0-9a-f]{8}\b/g;

function blocksOf(message) {
  const c = message && message.content;
  if (typeof c === 'string') return [{ type: 'text', text: c }];
  return Array.isArray(c) ? c.filter((b) => b && typeof b === 'object') : [];
}

function resultText(block) {
  const c = block.content;
  if (typeof c === 'string') return c;
  if (Array.isArray(c)) return c.map((x) => (x && typeof x.text === 'string' ? x.text : '')).join('\n');
  return '';
}

/**
 * Read a Claude Code transcript (JSONL) for what the residue check needs:
 * the session start (first timestamp), the operator/assistant PROSE (tool
 * results excluded — a pasted action note is not the session's commitment),
 * and the action fids the session created / completed / updated, from its
 * own pib_* tool calls.
 */
export function parseTranscriptForResidue(raw) {
  const out = { sessionStart: null, text: [], created: new Set(), completed: new Set(), updated: new Set() };
  const createCalls = new Map(); // tool_use id → true
  for (const line of String(raw || '').split('\n')) {
    const t = line.trim();
    if (!t) continue;
    let o;
    try { o = JSON.parse(t); } catch { continue; }
    const ts = o.timestamp || o.time || o.ts;
    if (!out.sessionStart && typeof ts === 'string' && ts) out.sessionStart = ts;
    if (o.type !== 'user' && o.type !== 'assistant') continue;
    if (o.isSidechain) continue;
    for (const b of blocksOf(o.message)) {
      if (b.type === 'text' && typeof b.text === 'string') out.text.push(b.text);
      else if (b.type === 'tool_use' && typeof b.name === 'string' && PIB_ACTION_TOOL_RE.test(b.name)) {
        const kind = b.name.match(PIB_ACTION_TOOL_RE)[1];
        const fid = b.input && typeof b.input.fid === 'string' ? b.input.fid : null;
        if (kind === 'create') createCalls.set(b.id, true);
        else if (fid && FID_RE.test(fid)) {
          FID_RE.lastIndex = 0;
          (kind === 'complete' ? out.completed : out.updated).add(fid);
        }
        FID_RE.lastIndex = 0;
      } else if (b.type === 'tool_result' && createCalls.has(b.tool_use_id)) {
        for (const m of resultText(b).match(FID_RE) || []) out.created.add(m);
      }
    }
  }
  return out;
}

// The transcript under ~/.claude/projects/<any dir>/<session-id>.jsonl — the
// first match wins.
export function findTranscript(sessionId, { home = process.env.HOME, existsSync, readdirSync, join } = {}) {
  if (!sessionId || !/^[A-Za-z0-9._-]+$/.test(sessionId)) return null;
  const root = join(home || '', '.claude', 'projects');
  if (!existsSync(root)) return null;
  let dirs = [];
  try { dirs = readdirSync(root); } catch { return null; }
  for (const d of dirs) {
    const p = join(root, d, `${sessionId}.jsonl`);
    if (existsSync(p)) return p;
  }
  return null;
}

async function residueDeps() {
  const fs = await import('fs');
  const path = await import('path');
  const child = await import('child_process');
  const lib = await import('./watchtower-lib.mjs');
  const ring3 = await import('./watchtower-ring3-close.mjs');
  return { fs, path, child, lib, ring3 };
}

// Identity for --cwd: the registered project (a worktree resolves to its main
// repo) or, unregistered, the directory itself — the same slug Ring 3's
// resolver mints for the fallback (slugify(basename)).
function resolveResidueProject(cwd, config, { lib, path }) {
  let identity = null;
  try { identity = lib.resolveProjectIdentity(cwd, config); } catch { identity = null; }
  if (identity && identity.registered) {
    return { name: identity.name, slug: identity.slug, path: identity.path, registered: true };
  }
  const name = path.basename(cwd);
  return { name, slug: lib.slugify(name), path: cwd, registered: false };
}

/**
 * Compute every residue finding for a session. Pure in effect (reads the
 * transcript, the tracker read-only, git, the carry ledger; writes nothing).
 */
export async function residueFindings({ sessionId, cwd, transcriptPath = null } = {}) {
  const { fs, path, child, lib, ring3 } = await residueDeps();
  let config;
  try { config = lib.loadConfig(); } catch { config = { projects: {}, defaults: {} }; }
  const project = resolveResidueProject(cwd, config, { lib, path });

  const tp = transcriptPath || findTranscript(sessionId, { existsSync: fs.existsSync, readdirSync: fs.readdirSync, join: path.join });
  const parsed = parseTranscriptForResidue(tp ? fs.readFileSync(tp, 'utf8') : '');
  const findings = [];

  // (a) commitments — over the session's prose.
  for (const c of detectCommitments(parsed.text.join('\n'))) {
    findings.push({ kind: 'commitment', text: c.text, proposed: 'file as action', cues: `${c.temporal}/${c.obligation}` });
  }

  const hasFeatures = fs.existsSync(path.join(project.path, 'e2e', 'features'));
  const touched = [...new Set([...parsed.created, ...parsed.completed])];

  // (b) verify plans — touched actions whose notes lack ## Verify Plan.
  let planned = new Set();
  if (hasFeatures && touched.length) {
    const dbPath = path.join(project.path, 'pib.db');
    const Database = fs.existsSync(dbPath) ? lib.loadBetterSqlite3(project.path) : null;
    if (Database) {
      let db = null;
      try {
        db = new Database(dbPath, { readonly: true, timeout: 5000 });
        const rows = db.prepare(`SELECT fid, text, notes FROM actions WHERE fid IN (${touched.map(() => '?').join(',')})`).all(...touched);
        for (const r of rows) {
          if (VERIFY_PLAN_RE.test(String(r.notes || ''))) { planned.add(r.fid); continue; }
          findings.push({
            kind: 'verify-plan', fid: r.fid,
            text: `${r.fid} "${String(r.text || '').slice(0, 80)}" was ${parsed.created.has(r.fid) ? 'created' : 'completed'} this session and has no ## Verify Plan`,
            proposed: 'write verify plan',
          });
        }
      } catch (e) {
        findings.push({ kind: 'verify-plan', text: `could not read the tracker for ${touched.length} touched action(s): ${String(e.message).split('\n')[0]}`, proposed: 'drop', fid: null });
      } finally {
        try { if (db) db.close(); } catch { /* ignore */ }
      }
    }
  }

  // (c) coverage — UI paths changed in the session's commits, no .feature edit.
  if (hasFeatures && parsed.sessionStart) {
    let out = '';
    try {
      out = child.execFileSync('git', ['log', `--since=${parsed.sessionStart}`, '--name-only', '--pretty=format:', '--no-renames'],
        { cwd: project.path, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
    } catch { out = ''; }
    const changed = [...new Set(out.split('\n').map((s) => s.trim()).filter(Boolean))];
    const { paths, exclude } = lib.resolveUiPaths(config);
    const { uncovered, uiPaths } = ring3.detectUncoveredUi(changed, paths, exclude);
    if (uncovered) {
      const anchor = touched.find((f) => !planned.has(f)) || touched[0] || null;
      findings.push({
        kind: 'coverage', text: `UI paths changed with no .feature edit: ${uiPaths.join(', ')}`,
        proposed: anchor ? 'write verify plan' : 'drop', ...(anchor ? { fid: anchor } : {}),
      });
    }
  }

  // (d) carried friction and loose ends — unresolved entries of the two
  // persistent kinds. They are NOT findings any more (act:b94d21b0, operator
  // ruling 2026-11-07): /briefing settles them, and /close numbers one only
  // when THIS session fixed it, collapsing the rest into one pointer line.
  // An UNREGISTERED project's ledger cannot be addressed by /briefing (it
  // reaches config.projects keys only), so there /close keeps offering them —
  // signalled by carried_offer: true.
  const carried = carriedEntries(project.slug, { lib });
  findings.forEach((f, i) => { f.n = i + 1; });
  return {
    session_id: sessionId, project: project.name, project_slug: project.slug, project_path: project.path,
    session_start: parsed.sessionStart, transcript: tp, findings, carried,
    ...(project.registered ? {} : { carried_offer: true }),
  };
}

/**
 * The unresolved friction + loose-end entries of one project's carry ledger,
 * oldest first — the ONE filter behind both `--residue`'s `carried` array and
 * /briefing's `--carry` list. `origin` is the category the entry was filed
 * under (`source.category`), or null.
 */
export function carriedEntries(slug, { lib, now = Date.now() } = {}) {
  const ts = (e) => { const t = Date.parse(e.created_at || ''); return Number.isFinite(t) ? t : Infinity; };
  return lib.readCarry(slug).entries
    .filter((e) => e && (e.kind === 'friction' || e.kind === 'loose-end') && !e.resolved)
    .sort((a, b) => ts(a) - ts(b))
    .map((e) => ({
      carry_id: e.id, kind: e.kind, title: e.title, detail: e.detail || '',
      created_at: e.created_at || null,
      age_days: Number.isFinite(ts(e)) ? Math.floor((now - ts(e)) / 86400000) : null,
      source_session: e.source_session || null,
      origin: (e.source && e.source.category) || null,
      deferred_count: Number.isInteger(e.deferred_count) ? e.deferred_count : 0,
    }));
}

const ACCEPTED_DISPOSITIONS = 'ok | fixed: <ref> | file as action act:xxxxxxxx | drop: <reason> | not now';

/**
 * The ONE settle path for carried entries (act:b94d21b0). Both doors call it:
 * `--record-answer` (/close, by: 'close') and `--settle` (/briefing, by:
 * 'briefing'). `items` is [{ carry_id, disposition, proposed }]; "ok" applies
 * the item's own `proposed`. `resolveMany` is REQUIRED — callers pass the
 * loaded lib.resolveCarryEntries (the lib is imported dynamically, so no
 * synchronous default can reference it). Every write happens in ONE
 * resolveMany call. An explicit "not now" is recorded as a deferral only when
 * by === 'briefing' (the decay count /briefing reads); an omitted answer is
 * never a deferral — no reply is not consent.
 * @returns {{resolved: object[], deferred: object[], noop: object[], skipped: object[]}}
 */
export function settleCarry(slug, items, { session_id = null, by = null, resolveMany } = {}) {
  if (!slug) throw new Error('settleCarry: a project slug is required');
  if (typeof resolveMany !== 'function') throw new Error('settleCarry: resolveMany (lib.resolveCarryEntries) is required');
  const out = { resolved: [], deferred: [], noop: [], skipped: [] };
  const batch = [];
  for (const it of Array.isArray(items) ? items : []) {
    if (!it || !it.carry_id) continue;
    const d = it.disposition;
    if (d === undefined || d === null || String(d).trim() === '') {
      out.skipped.push({ carry_id: it.carry_id, reason: 'no disposition — stays carried' });
      continue;
    }
    if (/^not now$/i.test(String(d).trim())) {
      if (by === 'briefing') batch.push({ carry_id: it.carry_id, item: { id: it.carry_id, defer: true } });
      else out.skipped.push({ carry_id: it.carry_id, reason: 'not now — stays carried' });
      continue;
    }
    const outcome = carryOutcomeFor(d, it.proposed);
    if (!outcome) {
      out.skipped.push({ carry_id: it.carry_id, reason: `unrecognized disposition ${JSON.stringify(d)}${/^ok$/i.test(String(d).trim()) ? ' (no proposal to accept)' : ''} — stays carried; accepted: ${ACCEPTED_DISPOSITIONS}` });
      continue;
    }
    if (outcome.outcome === 'filed' && outcome.ref) outcome.ref = String(outcome.ref).toLowerCase();
    if (outcome.outcome === 'filed' && !/^act:[0-9a-f]{8}$/.test(String(outcome.ref || ''))) {
      out.skipped.push({ carry_id: it.carry_id, reason: 'file as action needs the filed fid (file as action act:xxxxxxxx) — create the action first; stays carried' });
      continue;
    }
    batch.push({ carry_id: it.carry_id, outcome, item: { id: it.carry_id, ...outcome, session_id, by } });
  }
  if (batch.length === 0) return out;
  const results = resolveMany(slug, batch.map((b) => b.item));
  batch.forEach((b, i) => {
    const r = (results && results[i]) || { ok: false, reason: 'no result' };
    if (r.ok && r.deferred) out.deferred.push({ carry_id: b.carry_id, deferred_count: r.entry && r.entry.deferred_count });
    else if (r.ok) out.resolved.push({ carry_id: b.carry_id, outcome: b.outcome.outcome, ref: b.outcome.ref, reason: b.outcome.reason });
    else out.noop.push({ carry_id: b.carry_id, reason: r.reason });
  });
  return out;
}

/**
 * Resolve a --project argument for /briefing's modes: an exact config.projects
 * key wins, else the unique key whose slug equals the argument. Never cwd —
 * /briefing runs from any desk. Returns { name, slug, path } or null.
 */
export function resolveNamedProject(arg, config, { lib }) {
  const keys = Object.keys((config && config.projects) || {});
  if (!arg) return null;
  let key = keys.includes(arg) ? arg : null;
  if (!key) {
    const hits = keys.filter((k) => lib.slugify(k) === arg);
    if (hits.length === 1) key = hits[0];
  }
  if (!key) return null;
  return { name: key, slug: lib.slugify(key), path: config.projects[key].path || null };
}

// A disposition string → the carry resolution it means. 'ok' applies the
// finding's own proposal. Anything unrecognized leaves the entry carried.
export function carryOutcomeFor(disposition, proposed) {
  const d = String(disposition || '').trim();
  const text = /^ok$/i.test(d) ? String(proposed || '') : d;
  let m;
  if ((m = /^fixed\s*[:\-—]?\s*(.*)$/i.exec(text))) return { outcome: 'fixed', ref: m[1].trim() || null, reason: null };
  if ((m = /^file(?:d| as action)?\s*[:\-—]?\s*(.*)$/i.exec(text))) {
    const fid = ((m[1].match(/act:[0-9a-f]{8}/i) || [])[0] || '').toLowerCase() || null;
    return { outcome: 'filed', ref: fid || (m[1].trim() || null), reason: null };
  }
  if ((m = /^drop(?:ped)?\s*[:\-—]?\s*(.*)$/i.exec(text))) return { outcome: 'dropped', ref: null, reason: m[1].trim() || null };
  return null;
}

async function readJsonArg(argv) {
  const idx = argv.indexOf('--json');
  let raw = null;
  if (idx !== -1 && typeof argv[idx + 1] === 'string') raw = argv[idx + 1];
  else if (!process.stdin.isTTY) {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    raw = Buffer.concat(chunks).toString('utf8');
  }
  if (!raw || !raw.trim()) return null;
  return JSON.parse(raw);
}

function argAfter(argv, flag) {
  const i = argv.indexOf(flag);
  return i !== -1 && typeof argv[i + 1] === 'string' ? argv[i + 1] : null;
}

const USAGE = [
  'usage: watchtower-commitments.mjs --scan <file>',
  '       watchtower-commitments.mjs --residue --session-id <id> --cwd <dir> [--transcript <path>]',
  '       watchtower-commitments.mjs --record --session-id <id> [--json <receipt-json>]   (else stdin)',
  '       watchtower-commitments.mjs --record-answer --session-id <id> [--cwd <dir>] [--json <answer-json>] (else stdin)',
  '       watchtower-commitments.mjs --carry --project <config name|slug>',
  '       watchtower-commitments.mjs --settle --project <config name|slug> [--session-id <id>] [--json \'{"settlements":{"<carry_id>":{"disposition":"ok","proposed":"drop: stale"}}}\'] (else stdin)',
].join('\n');

// CLI entry. `--scan <file>` prints detected commitments as JSON (so the
// detector can be exercised by hand against a real transcript); the three
// residue modes are /close Step 6c's.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  const argv = process.argv;
  const mode = ['--scan', '--residue', '--record-answer', '--record', '--carry', '--settle'].find((f) => argv.includes(f));
  if (!mode) {
    console.error(USAGE);
    process.exit(2);
  }
  if (mode === '--scan') {
    const file = argAfter(argv, '--scan');
    if (!file) { console.error(USAGE); process.exit(2); }
    const { readFileSync } = await import('fs');
    let text;
    try {
      text = readFileSync(file, 'utf8');
    } catch (e) {
      console.error(`cannot read ${file}: ${e.message}`);
      process.exit(1);
    }
    console.log(JSON.stringify(detectCommitments(text), null, 2));
  } else if (mode === '--carry' || mode === '--settle') {
    // /briefing's two doors (act:b94d21b0). Portfolio-wide: the project is
    // named, never taken from cwd.
    const lib = await import('./watchtower-lib.mjs');
    let config;
    try { config = lib.loadConfig(); } catch { config = { projects: {}, defaults: {} }; }
    const arg = argAfter(argv, '--project');
    const project = resolveNamedProject(arg, config, { lib });
    if (!project) {
      console.error(`unknown project ${JSON.stringify(arg)}; known: ${Object.keys(config.projects || {}).join(', ') || '(none)'}`);
      process.exit(1);
    }
    if (mode === '--carry') {
      console.log(JSON.stringify({ project: project.name, slug: project.slug, carried: carriedEntries(project.slug, { lib }) }, null, 2));
    } else {
      let body;
      try { body = await readJsonArg(argv); } catch (e) { console.error(`--settle: bad JSON: ${e.message}`); process.exit(1); }
      const map = body && typeof body === 'object' && body.settlements && typeof body.settlements === 'object' ? body.settlements : null;
      if (!map) {
        console.error('--settle: a JSON { "settlements": { "<carry_id>": { "disposition": "…", "proposed": "…" } } } is required (via --json or stdin)');
        process.exit(1);
      }
      // A bare string is { disposition, proposed: null } — so a bare "ok" has
      // nothing to accept and is reported as skipped, never silently settled.
      // Only the persistent kinds are /briefing's to settle: an advisor or
      // recolor id (or an unknown one) is reported, never resolved here.
      const persistent = new Set(carriedEntries(project.slug, { lib }).map((c) => c.carry_id));
      const known = new Map(lib.readCarry(project.slug).entries.map((e) => [e.id, e]));
      const refused = [];
      const items = [];
      for (const [carry_id, v] of Object.entries(map)) {
        const e = known.get(carry_id);
        if (e && !e.resolved && !persistent.has(carry_id)) {
          refused.push({ carry_id, reason: `kind ${e.kind} is not settled in /briefing — stays as it is` });
          continue;
        }
        items.push(v && typeof v === 'object'
          ? { carry_id, disposition: v.disposition, proposed: v.proposed ?? null }
          : { carry_id, disposition: v, proposed: null });
      }
      const out = settleCarry(project.slug, items, {
        session_id: argAfter(argv, '--session-id'), by: 'briefing', resolveMany: lib.resolveCarryEntries,
      });
      out.skipped.push(...refused);
      console.log(JSON.stringify({ project: project.name, slug: project.slug, ...out }, null, 2));
    }
  } else {
    const sessionId = argAfter(argv, '--session-id');
    if (!sessionId) { console.error(USAGE); process.exit(2); }
    if (mode === '--residue') {
      const cwd = argAfter(argv, '--cwd') || process.cwd();
      const out = await residueFindings({ sessionId, cwd, transcriptPath: argAfter(argv, '--transcript') });
      console.log(JSON.stringify(out, null, 2));
    } else if (mode === '--record') {
      let receipt;
      try { receipt = await readJsonArg(argv); } catch (e) { console.error(`--record: bad JSON: ${e.message}`); process.exit(1); }
      const hasFindings = receipt && typeof receipt === 'object' && Array.isArray(receipt.findings);
      const hasAdvisor = receipt && typeof receipt === 'object' && Array.isArray(receipt.advisor_findings) && receipt.advisor_findings.length > 0;
      if (!hasFindings && !hasAdvisor) {
        console.error('--record: a receipt JSON with a findings array (/close) or an advisor_findings array (/debrief, /orient) is required (via --json or stdin)');
        process.exit(1);
      }
      const lib = await import('./watchtower-lib.mjs');
      const path = await import('path');
      let config;
      try { config = lib.loadConfig(); } catch { config = { projects: {}, defaults: {} }; }
      // Merge, never clobber (plan A4 / A9's table): the by-hand advisor
      // filers (/debrief, /orient) and /close write the SAME receipt at
      // different moments. Advisor findings accumulate across writes; a
      // /close write sets the residue fields; an advisor-only write leaves
      // them as they were — and on a fresh receipt it writes NO findings
      // array, so Ring 3 never mistakes "an advisor wrote here" for "/close
      // asked" (which would skip its fallback lenses).
      const prior = lib.readResidueReceipt(sessionId) || null;
      const priorAdvisor = prior && Array.isArray(prior.advisor_findings) ? prior.advisor_findings : [];
      const incomingAdvisor = Array.isArray(receipt.advisor_findings) ? receipt.advisor_findings : [];
      const stored = {
        ...(prior || {}),
        session_id: sessionId,
        // Fall back to --cwd, then the process cwd (act:b94d21b0): a receipt
        // with no project_path left --record-answer unable to find the carry
        // ledger, and every carried line was skipped while exit was 0.
        project_path: receipt.project_path || (prior && prior.project_path) || argAfter(argv, '--cwd') || process.cwd(),
        // The carry ledger is keyed by the RESOLVED project's slug (a
        // registered project's name; a worktree resolves to its main repo) —
        // the same slug --residue and Ring 3 use, so --record-answer finds
        // the entries --residue listed.
        project_slug: receipt.project_slug
          || (receipt.project_path ? resolveResidueProject(receipt.project_path, config, { lib, path }).slug : null)
          || (prior && prior.project_slug)
          || resolveResidueProject((prior && prior.project_path) || argAfter(argv, '--cwd') || process.cwd(), config, { lib, path }).slug
          || null,
        advisor_findings: [...priorAdvisor, ...incomingAdvisor],
      };
      if (hasFindings) {
        stored.asked_at = receipt.asked_at || new Date().toISOString();
        stored.findings = receipt.findings;
        stored.answered = receipt.answered === true;
        stored.dispositions = receipt.dispositions || null;
      } else if (!prior) {
        stored.answered = false;
      }
      const p = lib.writeResidueReceipt(stored);
      console.log(JSON.stringify({
        ok: true, path: p,
        findings: Array.isArray(stored.findings) ? stored.findings.length : null,
        advisor_findings: stored.advisor_findings.length, answered: stored.answered === true,
      }));
    } else {
      let answer;
      try { answer = await readJsonArg(argv); } catch (e) { console.error(`--record-answer: bad JSON: ${e.message}`); process.exit(1); }
      if (!answer || typeof answer !== 'object') {
        console.error('--record-answer: an answer JSON { dispositions: { "<n>": "ok" | "drop: …" | … } } is required (via --json or stdin)');
        process.exit(1);
      }
      const lib = await import('./watchtower-lib.mjs');
      const path = await import('path');
      const receipt = lib.readResidueReceipt(sessionId);
      if (!receipt) {
        console.error(`--record-answer: no receipt for session ${sessionId} — run --record first`);
        process.exit(1);
      }
      const dispositions = answer.dispositions && typeof answer.dispositions === 'object' ? answer.dispositions : {};
      let config;
      try { config = lib.loadConfig(); } catch { config = { projects: {}, defaults: {} }; }
      const slugSource = receipt.project_path || argAfter(argv, '--cwd') || null;
      const slug = receipt.project_slug
        || (slugSource ? resolveResidueProject(slugSource, config, { lib, path }).slug : null);
      const carriedFindings = (receipt.findings || []).filter((f) => f && f.carry_id);
      if (carriedFindings.length > 0 && !slug) {
        // Refuse BEFORE any write (act:b94d21b0): the receipt stays unanswered,
        // so a retry with --cwd works. This used to skip every carried line
        // with "no project slug" and exit 0 — a silent loss.
        console.error(`--record-answer: cannot resolve the project for session ${sessionId}'s carried lines — re-run with --cwd <project dir>`);
        process.exit(1);
      }
      const settled = carriedFindings.length > 0
        ? settleCarry(slug, carriedFindings.map((f) => ({
          carry_id: f.carry_id, proposed: f.proposed,
          disposition: dispositions[String(f.n)] ?? dispositions[f.n],
        })), { session_id: sessionId, by: 'close', resolveMany: lib.resolveCarryEntries })
        : { resolved: [], deferred: [], noop: [], skipped: [] };
      const result = { session_id: sessionId, answered: true, ...settled };
      receipt.answered = true;
      receipt.answered_at = new Date().toISOString();
      receipt.dispositions = dispositions;
      lib.writeResidueReceipt(receipt);
      console.log(JSON.stringify(result, null, 2));
    }
  }
}

#!/usr/bin/env node
// refute-pass-check.mjs — is there a CURRENT refute pass for this phase?
//
// The mechanical half of the refute-pass bar (cabinet/dispatch-contract.md §1
// item 6). The skill (/refute-pass) does the judgment work; this script
// answers one question the same way every time and fails CLOSED: given a
// plan and a phase, does the plan's `## Dispatch` section carry a record
// line for that phase, and is that record still current against the
// repository as it stands? Staleness is COMPUTED from git on every run,
// never stored, so the record cannot rot into a stored conclusion.
//
//   node refute-pass-check.mjs --plan <repo-relative path> [--phase <N>]
//                              [--ref <git ref>] [--json] [--brief-inputs]
//
// Outcomes, in the order they are evaluated (the FIRST that applies wins):
//
//   ERROR          exit 2   anything that prevents a sound answer — never CURRENT
//   SINGLE-PHASE   exit 0   the plan is not multi-phase; no pass is required
//   OPTED-OUT      exit 0   `- refute-pass: opted-out reason=…` in ## Dispatch
//   PHASE-REQUIRED exit 5   multi-phase and --phase absent or unknown
//   BEHIND         exit 6   checkout behind main — sync first
//   MISSING        exit 3   no record line for the phase
//   STALE          exit 4   plan-changed / surface-moved / fold-not-on-main
//   CURRENT        exit 0
//
// Exit 1 is left to Node's uncaught-exception path; callers treat any exit
// other than 0, 3, 4, 5 or 6 as ERROR.
//
// "Main" is the LOCAL default branch (origin/HEAD's target, else main, else
// master), because the next build lane is cut from the main checkout's local
// HEAD — that is exactly the code the worker will meet — and because a check
// that needs the network goes blind on a dropped connection. The checker never
// fetches; BEHIND covers local main trailing its remote as last fetched.
//
// Multi-phase is DEFINED in dispatch-contract.md §1 item 6; this file
// implements it (PHASE_HEADING is exported and a test pins the two agree).
//
// No dependencies beyond Node built-ins and git. Synchronous; no network; no
// loop to bound. main() runs only when this file is the entry point, so tests
// import the pure functions without running it.

import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import path from 'node:path';

// ── the contract's patterns ──────────────────────────────────────────────

// STRICT phase heading (the contract's definition, verbatim).
export const PHASE_HEADING = /^### Phase (\d+)(?=$|[\s:])/;
// Phase-LOOKING heading: any level; text starts (case-insensitively) with
// `phase`, whitespace, then a digit or a single letter followed by a
// non-letter. `## Phase Order` is not phase-looking (O is followed by r).
export const PHASE_LOOKING = /^#+\s*phase\s+(?:\d|[a-z](?=[^a-z]|$))/i;

const SHA = '[0-9a-f]{40}';
const DATE = '\\d{4}-\\d{2}-\\d{2}';
const RANGE = '(?:none|P\\d+d?-\\d+(?:\\.\\.P\\d+d?-\\d+)?)';
const RE_RECORD_FULL = new RegExp(`^- refute-pass: phase=(\\d+) reviewed=(${SHA}) fold=(${SHA}) date=(${DATE}) rulings=(${RANGE}) mode=full$`);
const RE_RECORD_DELTA = new RegExp(`^- refute-pass: phase=(\\d+) reviewed=(${SHA}) fold=(${SHA}) date=(${DATE}) rulings=(${RANGE}) mode=delta renews=(${SHA})$`);
const RE_OPT_OUT = /^- refute-pass: opted-out reason=(.*)$/;
const RE_PHASE_BUILT = new RegExp(`^- phase-built: phase=(\\d+) merge=(${SHA}) date=(${DATE})$`);
const RE_ANY_RECORD_LINE = /^- (?:refute-pass|phase-built): /;
const RE_RANGE_PARTS = /^P(\d+)(d?)-(\d+)(?:\.\.P(\d+)(d?)-(\d+))?$/;

const EXIT = { ERROR: 2, 'SINGLE-PHASE': 0, 'OPTED-OUT': 0, 'PHASE-REQUIRED': 5, BEHIND: 6, MISSING: 3, STALE: 4, CURRENT: 0 };

class CheckError extends Error {}
const fail = (msg) => { throw new CheckError(msg); };

// ── pure parsers (exported for tests) ────────────────────────────────────

// Walk lines with fence awareness. cb(line, index, inFence).
function eachLine(text, cb) {
  const ls = text.split('\n');
  let fenced = false;
  for (let i = 0; i < ls.length; i++) {
    const l = ls[i];
    if (/^\s*```/.test(l)) { fenced = !fenced; cb(l, i, true); continue; }
    cb(l, i, fenced);
  }
  return ls;
}

// Lines of the section opened by a `## <name>` heading, to the next `## `
// heading (outside fences). Returns null when the heading is absent.
function sectionLines(text, headingTest) {
  const out = [];
  let inside = false;
  let found = false;
  eachLine(text, (l, i, fenced) => {
    if (!fenced && /^## /.test(l)) {
      if (inside) { inside = false; return; }
      if (headingTest(l)) { inside = true; found = true; return; }
    }
    if (inside) out.push({ line: l, index: i, fenced });
  });
  return found ? out : null;
}

/**
 * parsePhases(text) → { phases: number[] (distinct, ascending), errors: [{line, text}] }
 * A phase-looking heading that is not STRICT (`### Phase N`, inside
 * `## Implementation`) is an error, never silently single-phase.
 */
export function parsePhases(text) {
  const errors = [];
  const phases = new Set();
  let inImpl = false;
  eachLine(text, (l, i, fenced) => {
    if (fenced) return;
    if (/^## /.test(l)) { inImpl = /^## Implementation\s*$/.test(l); }
    if (!/^#+\s/.test(l) && !/^#+$/.test(l)) return;
    if (!PHASE_LOOKING.test(l)) return;
    const m = l.match(PHASE_HEADING);
    if (m && inImpl) { phases.add(Number(m[1])); return; }
    errors.push({ line: i + 1, text: l, reason: m ? 'strict heading outside ## Implementation' : 'not the strict `### Phase N` form' });
  });
  return { phases: [...phases].sort((a, b) => a - b), errors };
}

const normPath = (p) => {
  let r = p.trim();
  while (r.startsWith('./')) r = r.slice(2);
  return r.replace(/\/+$/, '');
};

/**
 * parseSurfaceArea(text) → { found, entries: [{kind, path, shared, new, trackable, annotation}] }
 * Follows templates/hooks/surface-area-guard.sh exactly: a list item whose
 * body starts `files:`/`dirs:`, first whitespace-delimited token is the path,
 * backticks and trailing `,;` stripped, `./` and trailing `/` normalised.
 * Everything after the token is annotation; SHARED when a parenthesised
 * annotation contains the word `shared` (operator ruling O4).
 */
export function parseSurfaceArea(text) {
  const ls = text.split('\n');
  const start = ls.findIndex((l) => l.trim() === '## Surface Area');
  if (start < 0) return { found: false, entries: [] };
  const entries = [];
  for (const l of ls.slice(start + 1)) {
    if (l.startsWith('## ')) break;
    const s = l.trim();
    if (!s.startsWith('-')) continue;
    const body = s.replace(/^-+/, '').trim();
    const colon = body.indexOf(':');
    if (colon < 0) continue;
    const kind = body.slice(0, colon).trim();
    if (kind !== 'files' && kind !== 'dirs') continue;
    const rest = body.slice(colon + 1).trim();
    if (!rest) continue;
    const parts = rest.split(/\s+/);
    const token = parts[0].replace(/^[`,;]+|[`,;]+$/g, '');
    if (!token) continue;
    const annotation = parts.slice(1).join(' ');
    const parens = [...annotation.matchAll(/\(([^)]*)\)/g)].map((m) => m[1]);
    const has = (word) => parens.some((p) => new RegExp(`\\b${word}\\b`, 'i').test(p));
    const p = normPath(token);
    const trackable = !path.isAbsolute(token) && !/^(\.\.(\/|$)|~)/.test(token) && p !== '';
    entries.push({ kind, path: p, shared: has('shared'), new: has('new'), trackable, annotation });
  }
  return { found: true, entries };
}

/**
 * Files that many unrelated commits touch — the directory indexes and the
 * CLAUDE.md docs. Listed in a Surface Area without `(shared)`, every edit
 * to them by unrelated work stales the refute pass (measured: a research
 * write-up touching .claude/plans/README.md cost a phase two extra delta
 * reviews). The checker WARNS on these, never fails: a phase that really
 * owns one of them may leave it unmarked on purpose.
 */
export const WELL_KNOWN_SHARED = [
  '.claude/plans/README.md',
  '.claude/methodology/README.md',
  'CLAUDE.md',
  'lib/CLAUDE.md',
  'templates/CLAUDE.md',
];

/** unmarkedSharedEntries(entries) → paths of well-known shared files listed without `(shared)`. */
export function unmarkedSharedEntries(entries) {
  return entries.filter((e) => !e.shared && WELL_KNOWN_SHARED.includes(e.path)).map((e) => e.path);
}

function parseRange(range, phase, mode) {
  if (range === 'none') return [];
  const m = range.match(RE_RANGE_PARTS);
  if (!m) fail(`ruling range "${range}" is malformed`);
  const [, p1, d1, a, p2, d2, b] = m;
  if (Number(p1) !== phase || (p2 !== undefined && Number(p2) !== phase)) {
    fail(`ruling range "${range}" names phase ${p1}${p2 && p2 !== p1 ? `/${p2}` : ''} but the record says phase=${phase}`);
  }
  if ((mode === 'delta') !== (d1 === 'd') || (d2 !== undefined && d2 !== d1)) {
    fail(`ruling range "${range}" does not match mode=${mode} (delta rulings are P${phase}d-<k>, full rulings P${phase}-<k>)`);
  }
  const from = Number(a); const to = b === undefined ? from : Number(b);
  if (to < from) fail(`ruling range "${range}" runs backwards`);
  const ids = [];
  for (let k = from; k <= to; k++) ids.push(`P${phase}${d1}-${k}`);
  return ids;
}

/**
 * parseRecords(text) → { records: Map<phase, record>, built: Map<phase, built>, optOut|null }
 * Throws (CheckError) on a malformed line, a duplicate per phase, an empty
 * opt-out reason, a range/phase or range/mode mismatch, or a record line
 * outside `## Dispatch`.
 */
export function parseRecords(text) {
  const records = new Map();
  const built = new Map();
  let optOut = null;
  const dispatch = sectionLines(text, (l) => l.trim() === '## Dispatch');
  const dispatchIdx = new Set((dispatch || []).map((e) => e.index));
  eachLine(text, (l, i, fenced) => {
    if (fenced || !RE_ANY_RECORD_LINE.test(l)) return;
    const where = `line ${i + 1}: "${l}"`;
    if (!dispatchIdx.has(i)) fail(`record line outside ## Dispatch — ${where}`);
    let m;
    if ((m = l.match(RE_OPT_OUT))) {
      if (!m[1].trim()) fail(`opt-out with an empty reason — ${where}`);
      if (optOut) fail(`duplicated opt-out line — ${where}`);
      optOut = { reason: m[1].trim(), line: l, index: i };
      return;
    }
    if ((m = l.match(RE_RECORD_FULL)) || (m = l.match(RE_RECORD_DELTA))) {
      const mode = m[6] ? 'delta' : 'full';
      const phase = Number(m[1]);
      if (records.has(phase)) fail(`duplicated refute-pass line for phase ${phase} — ${where}`);
      const rec = { phase, reviewed: m[2], fold: m[3], date: m[4], rulings: m[5], mode, renews: m[6] || null, line: l, index: i };
      rec.rulingIds = parseRange(rec.rulings, phase, mode);
      records.set(phase, rec);
      return;
    }
    if ((m = l.match(RE_PHASE_BUILT))) {
      const phase = Number(m[1]);
      if (built.has(phase)) fail(`duplicated phase-built line for phase ${phase} — ${where}`);
      built.set(phase, { phase, merge: m[2], date: m[3], line: l, index: i });
      return;
    }
    fail(`record line matches no pattern — ${where}`);
  });
  return { records, built, optOut };
}

/** parsePhaseBuilt(text, phase) → the `phase-built` entry for the phase, else null. */
export function parsePhaseBuilt(text, phase) {
  return parseRecords(text).built.get(Number(phase)) || null;
}

/**
 * normalisePlan(text) — record and phase-built lines removed, trailing
 * whitespace trimmed per line. The record line is necessarily committed
 * AFTER the fold (a commit cannot contain its own sha), so without this
 * every fresh record would read stale.
 */
export function normalisePlan(text) {
  return text.split('\n')
    .filter((l) => !/^- refute-pass: /.test(l) && !/^- phase-built: /.test(l))
    .map((l) => l.replace(/\s+$/, ''))
    .join('\n');
}

/**
 * rulingsLackingEvidence(planText, record) → [{id, reason}]
 * Every ruling id in the record's range must appear EXACTLY once in the plan
 * as `- <id>: <what> — evidence: <…>` or `- <id>: <what> — carried, unverified`.
 */
export function rulingsLackingEvidence(planText, record) {
  const out = [];
  const ids = record.rulingIds || parseRange(record.rulings, record.phase, record.mode);
  const ls = planText.split('\n');
  for (const id of ids) {
    const esc = id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const rulingLines = ls.filter((l) => new RegExp(`^\\s*- ${esc}: `).test(l));
    if (rulingLines.length === 0) { out.push({ id, reason: 'no ruling line' }); continue; }
    if (rulingLines.length > 1) { out.push({ id, reason: `${rulingLines.length} ruling lines` }); continue; }
    const l = rulingLines[0];
    const ok = / — evidence: \S/.test(l) || / — carried, unverified\s*$/.test(l);
    if (!ok) out.push({ id, reason: 'no evidence and not marked "carried, unverified"' });
  }
  return out;
}

// ── git ───────────────────────────────────────────────────────────────────

// The ONE place a git failure is detected. Every caller maps !ok to ERROR
// unless it documents a non-zero exit as meaningful.
function makeGit(cwd) {
  return function git(args) {
    const r = spawnSync('git', args, { cwd, encoding: 'utf8' });
    if (r.error) return { ok: false, status: -1, stdout: '', stderr: String(r.error.message || r.error) };
    return { ok: r.status === 0, status: r.status, stdout: r.stdout || '', stderr: r.stderr || '' };
  };
}

function must(res, what) {
  if (!res.ok) fail(`${what}: git exited ${res.status}${res.stderr.trim() ? ` — ${res.stderr.trim()}` : ''}`);
  return res.stdout;
}

const toPathspecs = (entries) => ({
  include: entries.filter((e) => e.trackable && !e.shared).map((e) => `:(literal)${e.path}${e.kind === 'dirs' ? '/' : ''}`),
  exclude: entries.filter((e) => e.trackable && e.shared).map((e) => `:(exclude,literal)${e.path}${e.kind === 'dirs' ? '/' : ''}`),
});

// Parse `git log --format=%H%x09%s --name-only` output into commits.
function parseLogNames(out) {
  const commits = [];
  let cur = null;
  for (const raw of out.split('\n')) {
    const l = raw.replace(/\r$/, '');
    const m = l.match(/^([0-9a-f]{40})\t(.*)$/);
    if (m) { cur = { sha: m[1], subject: m[2], files: [] }; commits.push(cur); continue; }
    if (cur && l.trim()) cur.files.push(l);
  }
  return commits;
}

// ── the evaluation ───────────────────────────────────────────────────────

function parseArgs(argv) {
  const o = { plan: null, phase: null, ref: null, json: false, briefInputs: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => { if (i + 1 >= argv.length) fail(`${a} needs a value`); return argv[++i]; };
    if (a === '--plan') o.plan = next();
    else if (a === '--phase') {
      const v = next();
      if (!/^\d+$/.test(v)) fail(`--phase must be a number, got "${v}"`);
      o.phase = Number(v);
    } else if (a === '--ref') o.ref = next();
    else if (a === '--json') o.json = true;
    else if (a === '--brief-inputs') o.briefInputs = true;
    else fail(`unknown argument "${a}"`);
  }
  if (!o.plan) fail('--plan <repo-relative path> is required');
  return o;
}

export function run(argv, cwd = process.cwd()) {
  const result = {
    outcome: null, exit: null, plan: null, phase: null, phases: [], record: null, optOut: null,
    reasons: [], touching: [], shared: [], notChecked: [], notes: [],
  };
  let opts;
  try {
    opts = parseArgs(argv);
    result.phase = opts.phase;
    const git0 = makeGit(cwd);
    const top = git0(['rev-parse', '--show-toplevel']);
    if (!top.ok) fail(`not a git repository: ${cwd}`);
    const repoRoot = top.stdout.trim();
    const git = makeGit(repoRoot);

    // The plan path, repo-relative.
    let plan = opts.plan;
    if (path.isAbsolute(plan)) plan = path.relative(repoRoot, plan);
    plan = normPath(plan);
    if (plan.startsWith('..') || plan === '') fail(`--plan must name a file inside the repository (got "${opts.plan}")`);
    result.plan = plan;

    // Which ref is main.
    let refName; let refIsBranch = true;
    if (opts.ref) {
      refName = opts.ref.replace(/^refs\/heads\//, '');
      if (!git(['rev-parse', '--verify', '--quiet', `${opts.ref}^{commit}`]).ok) fail(`ref does not resolve: ${opts.ref}`);
      refIsBranch = git(['rev-parse', '--verify', '--quiet', `refs/heads/${refName}`]).ok;
    } else {
      const oh = git(['rev-parse', '--abbrev-ref', 'origin/HEAD']);
      const candidates = [];
      if (oh.ok) candidates.push(oh.stdout.trim().replace(/^origin\//, ''));
      candidates.push('main', 'master');
      refName = candidates.find((c) => c && git(['rev-parse', '--verify', '--quiet', `refs/heads/${c}`]).ok);
      if (!refName) fail('no main branch: origin/HEAD, main and master all fail to resolve locally');
    }
    const ref = opts.ref || refName;
    const refSha = must(git(['rev-parse', `${ref}^{commit}`]), 'resolving the ref').trim();

    // HEAD: on the resolved main branch?
    const headBranchRes = git(['symbolic-ref', '--quiet', '--short', 'HEAD']);
    const headBranch = headBranchRes.ok ? headBranchRes.stdout.trim() : null;
    const headOnMain = refIsBranch && headBranch === refName;

    // The plan at the ref — never the working tree.
    const show = git(['show', `${ref}:${plan}`]);
    if (!show.ok) fail(`plan unreadable at ${ref}: ${plan}`);
    const planText = show.stdout;

    // ── ERROR stage ──
    const { phases, errors } = parsePhases(planText);
    result.phases = phases;
    if (errors.length) {
      fail(errors.map((e) => `phase-looking heading is ${e.reason}: line ${e.line}: "${e.text}"`).join('\n'));
    }
    const sa = parseSurfaceArea(planText);
    if (!sa.found) fail('the plan has no ## Surface Area section');
    if (!sa.entries.length) fail('the plan\'s ## Surface Area lists no files:/dirs: entries');
    result.shared = sa.entries.filter((e) => e.shared).map((e) => e.path);
    result.notChecked = sa.entries.filter((e) => !e.trackable).map((e) => e.path);
    for (const p of unmarkedSharedEntries(sa.entries)) {
      result.notes.push(`warning: ${p} is listed without (shared) — unrelated commits to it will stale this pass; mark it "- files: ${p} (shared)" unless this phase really owns it`);
    }
    const surface = sa.entries.filter((e) => e.path !== plan);
    const specs = toPathspecs(surface);

    const { records, built, optOut } = parseRecords(planText);
    const isCommit = (sha) => git(['rev-parse', '--verify', '--quiet', `${sha}^{commit}`]).ok;
    for (const rec of records.values()) {
      for (const [k, sha] of [['reviewed', rec.reviewed], ['fold', rec.fold], ['renews', rec.renews]]) {
        if (sha && !isCommit(sha)) fail(`phase ${rec.phase} record: ${k}=${sha} is not a commit in this repository`);
      }
      const rf = git(['merge-base', '--is-ancestor', rec.reviewed, rec.fold]);
      if (rf.status !== 0) fail(`phase ${rec.phase} record: reviewed=${rec.reviewed} is not an ancestor of fold=${rec.fold}`);
      const rm = git(['merge-base', '--is-ancestor', rec.reviewed, refSha]);
      if (rm.status !== 0) fail(`phase ${rec.phase} record: reviewed=${rec.reviewed} is not an ancestor of ${ref}`);
      const lacking = rulingsLackingEvidence(planText, rec);
      if (lacking.length) {
        fail(lacking.map((x) => `phase ${rec.phase} record: ruling ${x.id} has ${x.reason} (each ruling needs "— evidence: …" or "— carried, unverified")`).join('\n'));
      }
    }
    for (const b of built.values()) {
      if (!isCommit(b.merge)) fail(`phase-built phase ${b.phase}: merge=${b.merge} is not a commit in this repository`);
    }

    const record = opts.phase != null ? records.get(opts.phase) || null : null;
    result.record = record ? { ...record, rulingIds: undefined } : null;
    result.optOut = optOut ? { reason: optOut.reason } : null;

    // surface-moved, computed once (used by STALE and by --brief-inputs).
    // The review lane's OWN commits are excluded: they sit on the lane's
    // first-parent chain reviewed..fold AND touch nothing but the plan and
    // its directory's README (the fold's index edit) — which is exactly what
    // the skill's porcelain check lets a lane commit touch. Both halves are
    // needed: the lane syncs main BEFORE its first commit, so that sync is a
    // fast-forward and main's commits land on the lane's first-parent chain
    // too; topology alone would hide the very commits the reader never read.
    const laneOnlyFiles = new Set([plan, `${path.posix.dirname(plan)}/README.md`]);
    const surfaceMoved = (rec) => {
      if (!specs.include.length) return { skipped: true, commits: [] };
      const log = must(git(['log', '--no-merges', '--format=%H%x09%s', '--name-only', `${rec.reviewed}..${refSha}`, '--', ...specs.include, ...specs.exclude]), 'listing commits since reviewed');
      const lane = new Set(must(git(['rev-list', '--no-merges', '--first-parent', `${rec.reviewed}..${rec.fold}`]), 'listing the review lane\'s commits').split('\n').map((s) => s.trim()).filter(Boolean));
      const isLaneCommit = (sha) => {
        if (!lane.has(sha)) return false;
        const files = must(git(['show', '--format=', '--name-only', sha]), `listing files of ${sha}`).split('\n').map((s) => s.trim()).filter(Boolean);
        return files.every((f) => laneOnlyFiles.has(f));
      };
      return { skipped: false, commits: parseLogNames(log).filter((c) => !isLaneCommit(c.sha)) };
    };

    if (opts.briefInputs) {
      if (phases.length >= 2 && (opts.phase == null || !phases.includes(opts.phase))) {
        result.outcome = 'PHASE-REQUIRED';
        result.reasons.push(`--phase is required: the plan's phases are ${phases.join(', ')}`);
      } else {
        result.outcome = 'BRIEF-INPUTS';
        result.brief = briefInputs(planText, plan, opts.phase, phases, sa.entries, record, built, record ? surfaceMoved(record).commits : null, git);
      }
      result.exit = result.outcome === 'PHASE-REQUIRED' ? 5 : 0;
      return result;
    }

    // ── SINGLE-PHASE ──
    if (phases.length < 2) {
      result.outcome = 'SINGLE-PHASE';
      result.notes.push(`the plan has ${phases.length === 0 ? 'no' : 'one'} strict \`### Phase N\` heading; no refute pass is required`);
      return done(result);
    }

    // ── OPTED-OUT ──
    if (optOut) {
      const intro = must(git(['log', '-1', `-S${optOut.line}`, '--format=%H %as', ref, '--', plan]), 'finding the commit that introduced the opt-out').trim();
      if (!intro) fail('the opt-out line is present at the ref but no commit introduced it (git log -S returned nothing)');
      const [sha, date] = intro.split(' ');
      result.outcome = 'OPTED-OUT';
      result.optOut = { reason: optOut.reason, commit: sha, date };
      result.reasons.push(`reason: ${optOut.reason}`, `introduced by ${sha} on ${date}`);
      return done(result);
    }

    // ── PHASE-REQUIRED ──
    if (opts.phase == null || !phases.includes(opts.phase)) {
      result.outcome = 'PHASE-REQUIRED';
      result.reasons.push(opts.phase == null ? `--phase is required; the plan's phases are ${phases.join(', ')}` : `phase ${opts.phase} is not in the plan; its phases are ${phases.join(', ')}`);
      return done(result);
    }

    // ── BEHIND ──
    if (!headOnMain) {
      result.notes.push(`working tree not checked (HEAD is not on ${refName})`);
      const behind = must(git(['log', '--format=%H', `HEAD..${refSha}`, '--', plan]), 'comparing HEAD with the ref').trim();
      if (behind) {
        result.outcome = 'BEHIND';
        result.reasons.push(`checkout behind main — sync first: ${ref} has ${behind.split('\n').length} commit(s) touching ${plan} that HEAD lacks`);
        return done(result);
      }
    } else {
      const up = git(['rev-parse', '--abbrev-ref', '--symbolic-full-name', `${refName}@{upstream}`]);
      if (!up.ok) {
        result.notes.push(`no upstream for ${refName}: the behind-remote check was skipped`);
      } else {
        const upstream = up.stdout.trim();
        const planSpec = `:(literal)${plan}`;
        const excl = specs.exclude.filter((e) => e !== `:(exclude,literal)${plan}`);
        const behind = must(git(['log', '--format=%H', `${refSha}..${upstream}`, '--', planSpec, ...specs.include, ...excl]), 'comparing main with its upstream').trim();
        if (behind) {
          result.outcome = 'BEHIND';
          result.reasons.push(`checkout behind main — sync first: ${upstream} (as last fetched) has ${behind.split('\n').length} commit(s) touching the plan or an unshared Surface Area entry that ${refName} lacks`);
          return done(result);
        }
      }
    }

    // ── MISSING ──
    if (!record) {
      result.outcome = 'MISSING';
      result.reasons.push(`no refute-pass record for phase ${opts.phase} in ## Dispatch`);
      return done(result);
    }

    // ── STALE ──
    const anc = git(['merge-base', '--is-ancestor', record.fold, refSha]);
    if (anc.status === 1) result.reasons.push(`fold-not-on-main: fold=${record.fold} is not an ancestor of ${ref}`);
    else if (!anc.ok) fail(`checking fold ancestry: git exited ${anc.status} — ${anc.stderr.trim()}`);

    const atFold = git(['show', `${record.fold}:${plan}`]);
    if (!atFold.ok) fail(`plan unreadable at fold=${record.fold}: ${plan}`);
    if (normalisePlan(atFold.stdout) !== normalisePlan(planText)) {
      result.reasons.push(`plan-changed: ${plan} differs between fold=${record.fold} and ${ref} after normalisation`);
    }
    if (headOnMain) {
      const wt = git(['diff', '--quiet', 'HEAD', '--', plan]);
      if (wt.status === 1) result.reasons.push('plan-changed (uncommitted edit in the working tree)');
      else if (!wt.ok) fail(`comparing the working tree: git exited ${wt.status} — ${wt.stderr.trim()}`);
    }
    const moved = surfaceMoved(record);
    if (moved.skipped) {
      result.notes.push('surface-moved not checked: no unshared, trackable Surface Area entry remains (an empty pathspec would match every commit)');
    } else {
      result.touching = moved.commits;
      for (const c of moved.commits) {
        result.reasons.push(`surface-moved: ${c.sha} ${c.subject} — ${c.files.join(', ')}`);
      }
    }
    if (result.shared.length) result.notes.push(`shared entries (their churn does not stale the record): ${result.shared.join(', ')}`);
    if (result.notChecked.length) result.notes.push(`not checked: ${result.notChecked.join(', ')} (not git-trackable)`);

    result.outcome = result.reasons.length ? 'STALE' : 'CURRENT';
    return done(result);
  } catch (e) {
    if (!(e instanceof CheckError)) throw e;
    result.outcome = 'ERROR';
    result.exit = 2;
    result.reasons.push(...String(e.message).split('\n'));
    return result;
  }
}

function done(result) {
  result.exit = EXIT[result.outcome];
  return result;
}

// ── --brief-inputs ───────────────────────────────────────────────────────

function sliceSection(text, headingRe, maxLevel) {
  const ls = text.split('\n');
  let start = -1; let fenced = false;
  for (let i = 0; i < ls.length; i++) {
    if (/^\s*```/.test(ls[i])) { fenced = !fenced; continue; }
    if (!fenced && headingRe.test(ls[i])) { start = i; break; }
  }
  if (start < 0) return null;
  fenced = false;
  let end = ls.length;
  for (let i = start + 1; i < ls.length; i++) {
    if (/^\s*```/.test(ls[i])) { fenced = !fenced; continue; }
    if (fenced) continue;
    const m = ls[i].match(/^(#+)\s/);
    if (m && m[1].length <= maxLevel) { end = i; break; }
  }
  return ls.slice(start, end).join('\n');
}

function briefInputs(planText, plan, phase, phases, entries, record, built, movedSinceReviewed, git) {
  const statusLine = planText.split('\n').find((l) => /^\*\*Status:\*\*/.test(l)) || '';
  const parentM = statusLine.match(/·\s*parent\s+(\S+)/);
  const actionM = statusLine.match(/action (act:[0-9a-f]+)/);
  const ac = sliceSection(planText, /^## Acceptance Criteria\b/, 2);
  let phaseAcceptance = null;
  if (ac && phase != null) {
    const ls = ac.split('\n');
    const s = ls.findIndex((l) => l.trim() === `Phase ${phase}`);
    if (s >= 0) {
      let e = ls.length;
      for (let i = s + 1; i < ls.length; i++) if (/^Phase \d+$/.test(ls[i].trim())) { e = i; break; }
      phaseAcceptance = ls.slice(s, e).join('\n');
    }
  }
  const phaseRe = new RegExp(`^### Phase ${phase}(?=$|[\\s:])`);
  const foldParagraphs = planText.split(/\n\s*\n/).filter((p) => {
    const m = p.trim().match(/^\*\*([^*]+)\*\*/);
    return m && /refute pass/i.test(m[1]) && /folded/i.test(m[1]);
  });
  const prev = phase != null ? built.get(phase - 1) || null : null;
  return {
    plan, phase, phases,
    phaseText: phase != null ? sliceSection(planText, phaseRe, 3) : null,
    acceptanceCriteria: ac,
    phaseAcceptance,
    surfaceArea: entries.map(({ kind, path: p, shared, new: isNew, trackable }) => ({ kind, path: p, shared, new: isNew, trackable })),
    parent: parentM ? parentM[1] : null,
    action: actionM ? actionM[1] : null,
    record: record ? { phase: record.phase, reviewed: record.reviewed, fold: record.fold, date: record.date, rulings: record.rulings, mode: record.mode, renews: record.renews, line: record.line } : null,
    previousPhaseBuilt: prev ? { phase: prev.phase, merge: prev.merge, date: prev.date, line: prev.line } : null,
    movedSinceReviewed,
    foldParagraphs,
    statusLine,
  };
}

// ── output ───────────────────────────────────────────────────────────────

function render(result) {
  const lines = [`REFUTE-PASS ${result.outcome} plan=${result.plan ?? '-'} phase=${result.phase ?? '-'}`];
  for (const r of result.reasons) lines.push(`  ${r}`);
  for (const n of result.notes) lines.push(`  note: ${n}`);
  return lines.join('\n');
}

export function main(argv = process.argv.slice(2)) {
  const result = run(argv);
  const wantJson = argv.includes('--json') || argv.includes('--brief-inputs');
  if (result.outcome === 'BRIEF-INPUTS') {
    process.stdout.write(JSON.stringify(result.brief, null, 2) + '\n');
  } else if (wantJson) {
    const { brief, ...rest } = result;
    process.stdout.write(JSON.stringify(rest, null, 2) + '\n');
  } else {
    process.stdout.write(render(result) + '\n');
  }
  process.exit(result.exit);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}

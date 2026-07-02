#!/usr/bin/env node

// Watchtower Ring 4 — Periodic truth reconciliation.
// Weekly cadence. Compares documentary CLAIMS (CLAUDE.md sections, briefing
// files, plan files, memory entries) against codebase REALITY and files drift
// as inbox items. Reconciliation is COMPARISON, not generation — Ring 4 never
// rewrites a document; it flags drift with enough context to fix and the
// operator triages it.
//
// Where it sits among the rings (the nervous-system principle, not a stack):
//   - R1 catches individual files; R2 individual patterns; R3 individual
//     sessions. None catches CUMULATIVE drift — the slow rot where twenty
//     sessions each move reality a little further from what documents claim.
//   - R3 catches ACUTE drift ("this session broke the briefing"); R4 catches
//     CHRONIC drift ("the briefing has been wrong for three weeks").
//   - R4 CONSUMES R1's signal (git recency) to PRIORITIZE which projects to
//     reconcile, rather than blind-sweeping the whole portfolio.
//
// Scope (ledger §C doc-staleness + §D dropped embedded-pulse spot-checks):
//   1. Missing-path reconciliation — backtick-fenced repo-relative paths in a
//      document that no longer exist on disk (the maginnis "Layer 4 Not built"
//      and the memory-to-codebase "references a renamed/removed file" cases).
//   2. Count-claim reconciliation — config-declared count rules ("N cabinet
//      members", "N specs") checked against a mechanical file count. The
//      systematic replacement for orient's ~15s description-accuracy spot check.
//
// Memory is DRIFT-SUPERSEDE ONLY here ("is what we wrote still true?" — the
// missing-path pass applied to memory files). Memory HYGIENE (budget / decay /
// consolidate) stays Ring 2 slow's runMemoryHygiene — Ring 4 does NOT fork a
// second memory-health producer (act:36dae795 2026-06-16 scope note).
//
// Stage 1 is PURELY MECHANICAL — no Claude API call. The cron stays cheap and
// the whole ring is hermetically testable. Semantic comparison (does this prose
// paragraph still describe the code?) is a documented future extension.

import {
  readFileSync, readdirSync, existsSync, statSync, mkdirSync, realpathSync,
} from 'fs';
import { join } from 'path';
import { pathToFileURL } from 'url';
import { execSync } from 'child_process';
import { homedir } from 'os';
import {
  atomicWrite, loadConfig, log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending,
} from './watchtower-lib.mjs';

const WATCHTOWER_DIR = getWatchtowerDir();

// Weekly cadence — heavy per run, runs rarely (contract: "Daily or weekly, not
// per-session or per-minute"). The orchestrator's own gate enforces it; the
// cron interval is a /watchtower install concern, not this file's.
export const RECONCILE_INTERVAL_DAYS = 7;
// Bounded cost: at most this many projects reconciled per run (rotation +
// git-recency priority pick the set).
export const RING4_PROJECT_CAP = 3;
// Bounded noise: at most this many drift items filed per run across all
// reconciled projects.
export const RING4_ITEM_CAP = 8;
// Bound the document set scanned per project so a plans/ dir with hundreds of
// files can't blow up a run.
const RING4_DOC_CAP = 40;

function log(msg) { _log('ring4', msg); }
function logError(msg) { _logError('ring4', msg); }

// ---------------------------------------------------------------------------
// Path-claim extraction (pure)
// ---------------------------------------------------------------------------

// Tokens that are paths but legitimately absent from the repo working tree, or
// that point outside it — never flagged as drift.
const PATH_DENY_PREFIXES = ['node_modules/', '.git/', 'dist/', 'build/', 'coverage/'];
// Generated-artifact directory names that are never an authored doc claim,
// wherever they appear in a path (PATH_DENY_PREFIXES only catches them at the
// root). `templates/mux/config/__pycache__/` is the real case: a Python
// bytecode cache mentioned in a fixed-bug note, flagged because its top segment
// (`templates/`) is real. Same intent as the deny-prefixes, segment-anywhere.
const PATH_DENY_SEGMENTS = new Set(['__pycache__', 'node_modules', '.git']);

/**
 * A candidate is a repo-relative path: at least one slash, every segment built
 * only from word / dot / dash / @ characters (so spaces, glob chars `* ? { }`,
 * shell vars `$`, angle placeholders `< >`, and URLs are all excluded — those
 * are intentionally not literal files). Absolute, home, and URL-rooted tokens
 * are not repo-relative and are skipped.
 */
export function isCandidatePath(token) {
  if (typeof token !== 'string') return false;
  const t = token.trim();
  if (!t || t.startsWith('/') || t.startsWith('~') || t.startsWith('.git/')) return false;
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(t)) return false; // scheme://… (URLs)
  if (!/^[\w.@-]+(\/[\w.@-]+)+\/?$/.test(t)) return false;
  // Reject `.` / `..` segments (relative-to-the-document, not repo-relative-
  // from-root — resolving them against the project root is wrong and a frequent
  // false-positive source) and generated-artifact segments (`__pycache__` etc.).
  if (t.split('/').some(seg => seg === '.' || seg === '..' || PATH_DENY_SEGMENTS.has(seg))) return false;
  for (const deny of PATH_DENY_PREFIXES) {
    if (t === deny || t.startsWith(deny)) return false;
  }
  return true;
}

/**
 * Extract unique candidate repo-relative path tokens from backtick-fenced spans
 * in a markdown document. Only inline-code spans are considered — prose words
 * are never treated as paths, which is what keeps the signal clean.
 */
export function extractPathClaims(text) {
  if (typeof text !== 'string') return [];
  const seen = new Set();
  const out = [];
  // Inline code spans: `...`. Fenced blocks (```) also match span-by-span here,
  // which is fine — a path mentioned inside a code block is still a claim.
  const spanRe = /`([^`\n]+)`/g;
  let m;
  while ((m = spanRe.exec(text)) !== null) {
    const raw = m[1].trim().replace(/[).,;:]+$/, ''); // trailing punctuation
    if (isCandidatePath(raw) && !seen.has(raw)) {
      seen.add(raw);
      out.push(raw);
    }
  }
  return out;
}

// Removal/replacement verbs that, when they attach directly to a path's code
// span, mark the doc as correctly NOTING a removal — not claiming the file
// still exists. Kept deliberately TIGHT (unambiguous removal verbs only): the
// suppression must never silently swallow a live existence claim, so weaker
// signals like "stale"/"old"/"orphan" are excluded.
const REMOVAL_RE = /\b(deleted|removed|renamed|replaced|dropped|retired|superseded|deprecated)\b/i;

// A clause break is a list/sentence separator. A removal verb only counts when
// it sits in the SAME clause as the path span (no intervening separator) — that
// is what distinguishes "`X` ... deleted" (X IS the thing deleted → suppress)
// from "`X`, and Y were removed" (X is one item in a list; the verb is across a
// comma → still a live reference, keep flagging). The comma is the structural
// tell that kept the real `lib/omega-setup.js` drift caught while the
// `.claude/workflows/execute-group.js` removal note was suppressed.
const CLAUSE_BREAK = /[,;.:]|—|–| - /;
const REMOVAL_WINDOW = 80;

/**
 * True when a path's code span sits inside removal/replacement narrative: a
 * removal verb in the immediate FOLLOWING clause (parentheticals like
 * `(gitignored)` skipped over) or the immediate PRECEDING clause. `before` /
 * `after` are the raw text windows flanking the span.
 */
function inRemovalNarrative(before, after) {
  const afterClause = after.replace(/\([^)]*\)/g, ' ').split(CLAUSE_BREAK)[0];
  if (REMOVAL_RE.test(afterClause)) return true;
  const beforeParts = before.replace(/\([^)]*\)/g, ' ').split(CLAUSE_BREAK);
  return REMOVAL_RE.test(beforeParts[beforeParts.length - 1]);
}

/**
 * Per-span path extraction WITH removal-narrative context. Unlike
 * `extractPathClaims` (which returns unique tokens for the public API), this
 * returns one entry per inline-code span — `{ token, removal }` — so
 * `reconcileDocPaths` can decide per-occurrence whether a claim is a live
 * existence claim or a correct note that a file was removed.
 */
function extractPathSpans(text) {
  const spans = [];
  if (typeof text !== 'string') return spans;
  const spanRe = /`([^`\n]+)`/g;
  let m;
  while ((m = spanRe.exec(text)) !== null) {
    const raw = m[1].trim().replace(/[).,;:]+$/, '');
    if (!isCandidatePath(raw)) continue;
    const before = text.slice(Math.max(0, m.index - REMOVAL_WINDOW), m.index);
    const after = text.slice(spanRe.lastIndex, spanRe.lastIndex + REMOVAL_WINDOW);
    spans.push({ token: raw, removal: inRemovalNarrative(before, after) });
  }
  return spans;
}

/**
 * Precision gate: only flag a missing path whose TOP-LEVEL segment is a real
 * entry in the project root. `lib/renamed.js` where `lib/` exists but the file
 * is gone → real drift. `webapp/frontend/x.tsx` referenced in a CC memory file,
 * where `webapp/` is not a repo dir → an illustrative/foreign path, skipped.
 * This trades recall (a wholesale dir rename is missed) for precision, which is
 * the right call for a background nag: false positives erode trust fastest.
 */
export function topSegmentExists(projectRoot, token) {
  const top = token.split('/')[0];
  if (!top) return false;
  return existsSync(join(projectRoot, top));
}

/**
 * Reconcile one document's path claims against the project tree. Returns the
 * sorted list of claimed paths that are gated-in (top segment real) but absent.
 * `projectRoot` is always the CODE root, even for memory files that live
 * elsewhere — a memory entry's paths are relative to the project it describes.
 *
 * Two precision gates beyond the top-segment check (both confirmed on the real
 * system-status.md, act:acbf5442):
 *   (a) Removal narrative — a path whose EVERY occurrence sits in removal /
 *       replacement narrative ("`X` ... deleted", "replaced by", "renamed") is
 *       the doc correctly NOTING a removal, not claiming the file exists. One
 *       present-tense ("live") occurrence anywhere re-arms the claim.
 *   (b) Suffix-fragment — a missing path that is only a segment-boundary suffix
 *       of a longer real path referenced in the SAME doc (`bin/mux` pulled out
 *       of `templates/mux/bin/mux`) is an abbreviated reference, not drift.
 */
export function reconcileDocPaths(docText, projectRoot) {
  const spans = extractPathSpans(docText);
  // Aggregate per unique token. `liveClaim` = at least one occurrence is NOT in
  // removal narrative; only an all-removal token is suppressed under gate (a).
  const tokens = new Map(); // token -> { liveClaim: bool }
  for (const { token, removal } of spans) {
    const info = tokens.get(token) || { liveClaim: false };
    if (!removal) info.liveClaim = true;
    tokens.set(token, info);
  }
  // Doc-referenced tokens that resolve on disk — the longer "real paths" that a
  // suffix fragment is checked against (gate b).
  const realTokens = [];
  for (const token of tokens.keys()) {
    const clean = token.replace(/\/$/, '');
    if (existsSync(join(projectRoot, clean))) realTokens.push(clean);
  }
  const missing = [];
  for (const [token, info] of tokens) {
    if (!info.liveClaim) continue;                      // (a) removal narrative only
    const clean = token.replace(/\/$/, '');
    if (!topSegmentExists(projectRoot, clean)) continue; // foreign / illustrative
    if (existsSync(join(projectRoot, clean))) continue;  // present → not drift
    if (realTokens.some(q => q.length > clean.length && q.endsWith(`/${clean}`))) {
      continue;                                          // (b) suffix-fragment of a real ref
    }
    missing.push(token);
  }
  return missing.sort();
}

// ---------------------------------------------------------------------------
// Count-claim reconciliation (pure)
// ---------------------------------------------------------------------------

/**
 * Extract the first integer captured by `pattern` (a regex source string with
 * exactly one capture group around the number) from a document. Returns the
 * number, or null when the pattern doesn't match or captures a non-integer.
 */
export function parseClaimedCount(docText, pattern) {
  if (typeof docText !== 'string') return null;
  let re;
  try { re = new RegExp(pattern); } catch { return null; }
  const m = docText.match(re);
  if (!m || m[1] === undefined) return null;
  const n = Number.parseInt(String(m[1]).replace(/[,_]/g, ''), 10);
  return Number.isFinite(n) ? n : null;
}

/**
 * Count entries matching a small, dependency-free glob grammar, relative to
 * `projectRoot`. Supported forms (one wildcard segment):
 *   dir/*        → entries (files + dirs) directly in dir
 *   dir/* /      → subdirectories only (trailing slash)         [no space in use]
 *   dir/prefix-* → entries whose name starts with "prefix-"
 *   dir/*.ext    → files whose name ends with ".ext"
 *   dir/prefix-* /→ subdirs starting with "prefix-"
 *   dir/ ** /*.ext → recursive (bounded depth) files ending ".ext"
 * Returns the count, or null when the form is unsupported or the base dir is
 * missing (null = "can't count", which the caller treats as no-claim, never a
 * mismatch — silence beats a false drift flag).
 */
export function countGlob(projectRoot, glob) {
  if (typeof glob !== 'string' || !glob.includes('*')) return null;
  const dirsOnly = glob.endsWith('/');
  const g = dirsOnly ? glob.slice(0, -1) : glob;

  // Recursive form: dir/**/*.ext  (or dir/**/*)
  const recMatch = g.match(/^(.*)\/\*\*\/\*(\.[\w.]+)?$/);
  if (recMatch) {
    const baseDir = join(projectRoot, recMatch[1]);
    const ext = recMatch[2] || '';
    if (!existsSync(baseDir)) return null;
    return countRecursive(baseDir, ext, dirsOnly, 0);
  }

  const slash = g.lastIndexOf('/');
  if (slash < 0) return null;
  const dirPart = g.slice(0, slash);
  const namePart = g.slice(slash + 1);
  const baseDir = join(projectRoot, dirPart);
  if (!existsSync(baseDir)) return null;

  let entries;
  try { entries = readdirSync(baseDir, { withFileTypes: true }); }
  catch { return null; }

  const matcher = buildNameMatcher(namePart);
  if (!matcher) return null;

  let count = 0;
  for (const e of entries) {
    if (e.name.startsWith('.') && !namePart.startsWith('.')) continue; // ignore dotfiles unless asked
    if (dirsOnly && !e.isDirectory()) continue;
    if (matcher(e.name)) count += 1;
  }
  return count;
}

/** Build a name predicate from a single-wildcard pattern (`*`, `pre*`, `*.ext`,
 *  `pre*suf`). Returns null for unsupported (multi-wildcard) names. */
function buildNameMatcher(namePart) {
  const star = namePart.indexOf('*');
  if (star < 0) return (name) => name === namePart;
  if (namePart.indexOf('*', star + 1) !== -1) return null; // >1 wildcard unsupported
  const pre = namePart.slice(0, star);
  const suf = namePart.slice(star + 1);
  return (name) => name.length >= pre.length + suf.length
    && name.startsWith(pre) && name.endsWith(suf);
}

function countRecursive(dir, ext, dirsOnly, depth) {
  if (depth > 8) return 0; // bounded walk
  let count = 0;
  let entries;
  try { entries = readdirSync(dir, { withFileTypes: true }); } catch { return 0; }
  for (const e of entries) {
    if (e.name.startsWith('.')) continue;
    const full = join(dir, e.name);
    if (e.isDirectory()) {
      if (dirsOnly) count += 1;
      count += countRecursive(full, ext, dirsOnly, depth + 1);
    } else if (!dirsOnly) {
      if (!ext || e.name.endsWith(ext)) count += 1;
    }
  }
  return count;
}

/**
 * Reconcile one declared count rule against the tree. A rule is
 * `{ label, doc, pattern, glob }`: read `<projectRoot>/<doc>`, parse the claimed
 * number via `pattern`, count `glob`, and return a mismatch finding or null.
 * Returns null (no finding) whenever the claim can't be read or the count can't
 * be taken — a count rule never invents drift from a thing it couldn't measure.
 */
export function reconcileCountRule(projectRoot, rule, readFile = readFileSync) {
  if (!rule || !rule.doc || !rule.pattern || !rule.glob) return null;
  const docPath = join(projectRoot, rule.doc);
  if (!existsSync(docPath)) return null;
  let text;
  try { text = readFile(docPath, 'utf8'); } catch { return null; }
  const claimed = parseClaimedCount(text, rule.pattern);
  if (claimed === null) return null;
  const actual = countGlob(projectRoot, rule.glob);
  if (actual === null) return null;
  if (claimed === actual) return null;
  return { label: rule.label || rule.glob, doc: rule.doc, claimed, actual, glob: rule.glob };
}

// ---------------------------------------------------------------------------
// Cadence + project selection (pure)
// ---------------------------------------------------------------------------

/** Weekly gate: true when never run, timestamp unparseable, or last run older
 *  than intervalDays. Mirrors Ring 2's shouldRunRosterReview. */
export function shouldRunReconciliation(state, nowMs, intervalDays = RECONCILE_INTERVAL_DAYS) {
  if (!state || !state.last_run) return true;
  const last = Date.parse(state.last_run);
  if (Number.isNaN(last)) return true;
  return (nowMs - last) >= intervalDays * 86400000;
}

/**
 * Select which projects to reconcile this run. The nervous-system principle in
 * mechanical form: a project with commits since its last reconcile (gitActive)
 * jumps the queue; ties and inactive projects fall back to least-recently-
 * reconciled. Capped. Pure for tests.
 */
export function selectReconcileProjects(projectNames, checkedMap = {}, gitActiveSet = new Set(), cap = RING4_PROJECT_CAP) {
  return [...projectNames]
    .sort((a, b) => {
      const aActive = gitActiveSet.has(a) ? 1 : 0;
      const bActive = gitActiveSet.has(b) ? 1 : 0;
      if (aActive !== bActive) return bActive - aActive; // active first
      return (checkedMap[a] || 0) - (checkedMap[b] || 0); // then oldest-checked
    })
    .slice(0, cap);
}

// ---------------------------------------------------------------------------
// Document collection (fs)
// ---------------------------------------------------------------------------

/** Claude Code encodes a project path as dash-joined under ~/.claude/projects.
 *  Returns the memory dir or null. (Same derivation Ring 2 uses.) */
function findMemoryDir(projectRoot) {
  const encoded = projectRoot.replace(/\//g, '-');
  const dir = join(homedir(), '.claude', 'projects', encoded, 'memory');
  return existsSync(dir) ? dir : null;
}

function pushDoc(docs, label, fullPath) {
  if (docs.length >= RING4_DOC_CAP) return;
  if (!existsSync(fullPath)) return;
  try {
    const text = readFileSync(fullPath, 'utf8');
    docs.push({ label, path: fullPath, text });
  } catch { /* unreadable — skip */ }
}

/**
 * The CURRENT-REALITY document set — docs that are consumed AS TRUTH and so
 * must stay true: CLAUDE.md architecture/convention claims, system-status,
 * briefing files (read by cabinet members during audits — stale briefings
 * degrade findings), and memory entries (drift-supersede). Returns
 * [{label, path, text}], capped at RING4_DOC_CAP. `label` is the human-facing
 * doc name used in the inbox title/dedup key.
 *
 * Deliberately EXCLUDES `.claude/plans/` and `.claude/methodology/`: those are
 * HISTORICAL records — a plan for retired work SHOULD reference now-deleted
 * files, so path-existence over them is mostly noise (the CC dogfood produced
 * ~90 false drift claims from historical plans alone). Whether a plan is
 * actually done — plan-to-reality drift — is a distinct SEMANTIC check,
 * deferred to a later stage.
 */
export function collectDocuments(projectRoot) {
  const docs = [];

  pushDoc(docs, 'CLAUDE.md', join(projectRoot, 'CLAUDE.md'));
  pushDoc(docs, 'system-status.md', join(projectRoot, 'system-status.md'));
  pushDoc(docs, '.claude/system-status.md', join(projectRoot, '.claude', 'system-status.md'));

  // Briefing files (.claude/cabinet/_briefing*.md) lead the priority list.
  readDirDocs(docs, join(projectRoot, '.claude', 'cabinet'), '.claude/cabinet/',
    (name) => /^_briefing.*\.md$/.test(name));

  // Memory entries (drift-supersede): MEMORY.md index + curated *.md. Paths
  // referenced here are relative to the PROJECT root, not the memory dir.
  const memoryDir = findMemoryDir(projectRoot);
  if (memoryDir) {
    readDirDocs(docs, memoryDir, 'memory/', (name) => name.endsWith('.md'));
  }

  return docs;
}

function readDirDocs(docs, dir, prefix, filter) {
  if (!existsSync(dir)) return;
  let names;
  try { names = readdirSync(dir); } catch { return; }
  for (const name of names.sort()) {
    if (docs.length >= RING4_DOC_CAP) return;
    if (!filter(name)) continue;
    pushDoc(docs, `${prefix}${name}`, join(dir, name));
  }
}

// ---------------------------------------------------------------------------
// Per-project reconciliation (fs)
// ---------------------------------------------------------------------------

/** Default gitignore probe: one `git check-ignore --stdin` call returns the
 *  subset of paths git considers ignored. A gitignored path that's absent is a
 *  generated/local artifact (e.g. `.ccrc.json`, runtime state files documented
 *  as "never shipped") — its absence is EXPECTED, not drift. Fail-open: on any
 *  git error the empty set is returned, so the base flag-it behavior holds. */
export function defaultGitIgnored(projectRoot, paths) {
  if (!paths || paths.length === 0) return new Set();
  try {
    const out = execSync('git check-ignore --stdin', {
      cwd: projectRoot,
      input: paths.join('\n'),
      encoding: 'utf8',
      timeout: 5000,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    return new Set(out.split('\n').map(s => s.trim().replace(/\/$/, '')).filter(Boolean));
  } catch (e) {
    // check-ignore exits 1 when NO path is ignored — that's a clean "none",
    // and execSync throws on non-zero. Recover stdout if present.
    const out = (e && e.stdout) ? String(e.stdout) : '';
    return new Set(out.split('\n').map(s => s.trim().replace(/\/$/, '')).filter(Boolean));
  }
}

/**
 * Reconcile one project. Returns drift findings:
 *   { kind: 'missing-path', doc, drift_key, paths: [...] }
 *   { kind: 'count-mismatch', doc, drift_key, label, claimed, actual, glob }
 * `countRules` is the per-project `reconcile.count_rules` (default []).
 * `gitIgnored(projectRoot, paths) -> Set` suppresses gitignored claims
 * (injectable for tests; defaults to a real `git check-ignore`).
 */
export function reconcileProject({ projectRoot, countRules = [], gitIgnored = defaultGitIgnored }) {
  const findings = [];
  if (!projectRoot || !existsSync(projectRoot)) return findings;

  // 1. Missing-path pass over the document set. Collect all misses first, run
  //    ONE gitignore check across the union, then drop the expected-absent.
  const perDoc = [];
  const union = new Set();
  for (const doc of collectDocuments(projectRoot)) {
    const missing = reconcileDocPaths(doc.text, projectRoot);
    if (missing.length > 0) {
      perDoc.push({ doc: doc.label, missing });
      for (const p of missing) union.add(p.replace(/\/$/, ''));
    }
  }
  const ignored = union.size > 0 ? gitIgnored(projectRoot, [...union]) : new Set();
  for (const { doc, missing } of perDoc) {
    const real = missing.filter(p => !ignored.has(p.replace(/\/$/, '')));
    if (real.length > 0) {
      findings.push({
        kind: 'missing-path',
        doc,
        drift_key: `path:${doc}`,
        paths: real,
      });
    }
  }

  // 2. Count-claim pass over declared rules.
  for (const rule of (Array.isArray(countRules) ? countRules : [])) {
    const mismatch = reconcileCountRule(projectRoot, rule);
    if (mismatch) {
      findings.push({
        kind: 'count-mismatch',
        doc: mismatch.doc,
        drift_key: `count:${mismatch.doc}:${mismatch.label}`,
        label: mismatch.label,
        claimed: mismatch.claimed,
        actual: mismatch.actual,
        glob: mismatch.glob,
      });
    }
  }

  return findings;
}

// ---------------------------------------------------------------------------
// Inbox filing
// ---------------------------------------------------------------------------

function findingTitle(projectName, finding) {
  if (finding.kind === 'missing-path') {
    return `Doc drift: ${finding.doc} references ${finding.paths.length} missing path${finding.paths.length === 1 ? '' : 's'}`;
  }
  return `Doc drift: ${finding.doc} claims ${finding.claimed} ${finding.label}, found ${finding.actual}`;
}

function findingSummary(finding) {
  if (finding.kind === 'missing-path') {
    return `This document references repo paths that no longer exist on disk — a file or directory was renamed or removed and the claim was left behind:\n${finding.paths.map(p => `- \`${p}\``).join('\n')}\n\nFix the document to match reality, or restore the path if the claim is correct. (Only paths whose top-level directory still exists are flagged, so these point inside the real tree.)`;
  }
  return `This document claims "${finding.claimed} ${finding.label}" but a mechanical count of \`${finding.glob}\` found ${finding.actual}. Update the count, or investigate why the artifacts and the claim disagree.`;
}

/**
 * File a deduped doc-drift inbox item for one finding. Dedup is one pending
 * item per drift_key per project (mirrors Ring 2's roster_kind convention) —
 * a still-open drift item is a reference, not a duplicate. Returns true if a
 * new item was filed.
 */
function fileDriftItem({ projectName, projectPath, finding, file, listPendingItems }) {
  const pending = listPendingItems({ project: projectName, category: 'doc-drift' });
  if (pending.some(qi => qi.evidence?.drift_key === finding.drift_key)) {
    return false; // already surfaced — don't re-file
  }
  file({
    project: projectName,
    project_path: projectPath,
    filed_by: 'ring4',
    category: 'doc-drift',
    urgency: 'low',
    title: findingTitle(projectName, finding),
    summary: findingSummary(finding),
    context_anchor: finding.doc,
    evidence: { drift_key: finding.drift_key, kind: finding.kind, doc: finding.doc, ...finding },
    options: [
      { value: 'fix-doc', label: 'Fix the document', description: 'Update the claim to match reality' },
      { value: 'fix-code', label: 'Restore / fix the code', description: 'The claim is right; reality drifted' },
      { value: 'dismiss', label: 'Dismiss', description: 'Acceptable / intentional' },
    ],
  });
  return true;
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/** Default git-recency probe: unix-ms of the last commit, or 0 on any failure
 *  (not a git repo, no commits, git absent). */
function defaultGitLastCommitMs(projectRoot) {
  try {
    const out = execSync('git log -1 --format=%ct', {
      cwd: projectRoot, encoding: 'utf8', timeout: 5000, stdio: ['pipe', 'pipe', 'pipe'],
    }).trim();
    const secs = Number.parseInt(out, 10);
    return Number.isFinite(secs) ? secs * 1000 : 0;
  } catch {
    return 0;
  }
}

/** Read per-project count rules from config: projects[name].reconcile.count_rules
 *  (default []). Tolerates the bare-string project form (no rules). */
function countRulesFor(projConfig) {
  if (!projConfig || typeof projConfig !== 'object') return [];
  const rules = projConfig.reconcile?.count_rules;
  return Array.isArray(rules) ? rules : [];
}

/**
 * Main reconciliation pass. deps.{now, file, listPendingItems, gitLastCommitMs}
 * are injectable so tests stay hermetic. Honors the weekly cadence gate and the
 * per-run project + item caps. Stamps state regardless of whether work was
 * found, so the cadence gate holds even on a clean portfolio.
 */
export function runReconciliation(config, deps = {}) {
  const now = deps.now || Date.now();
  const file = deps.file || createItem;
  const listPendingItems = deps.listPendingItems || listPending;
  const gitLastCommitMs = deps.gitLastCommitMs || defaultGitLastCommitMs;
  const gitIgnored = deps.gitIgnored || defaultGitIgnored;

  const statePath = join(WATCHTOWER_DIR, 'state', 'ring4-reconcile.json');
  let state = {};
  if (existsSync(statePath)) {
    try { state = JSON.parse(readFileSync(statePath, 'utf8')); } catch { state = {}; }
  }

  if (!deps.force && !shouldRunReconciliation(state, now)) {
    log('Reconciliation not due (weekly cadence)');
    return { filed: 0, reconciled: 0, skipped: 'cadence' };
  }

  const projects = config.projects || {};
  const projectNames = Object.keys(projects);
  if (projectNames.length === 0) {
    state.last_run = new Date(now).toISOString();
    mkdirSync(join(WATCHTOWER_DIR, 'state'), { recursive: true });
    atomicWrite(statePath, JSON.stringify(state, null, 2));
    log('Reconciliation — no projects configured');
    return { filed: 0, reconciled: 0 };
  }

  const checked = state.projects_checked || {};

  // Compute the git-active set: a project committed to since it was last
  // reconciled. Drives priority (nervous-system: consume R1's mechanical truth).
  const gitActive = new Set();
  const resolvedPaths = {};
  for (const name of projectNames) {
    const projectPath = projects[name].path || projects[name];
    resolvedPaths[name] = projectPath;
    if (!projectPath || !existsSync(projectPath)) continue;
    const lastCommit = gitLastCommitMs(projectPath);
    if (lastCommit > (checked[name] || 0)) gitActive.add(name);
  }

  const selected = selectReconcileProjects(projectNames, checked, gitActive);
  log(`Reconciliation — ${selected.length} of ${projectNames.length} project(s) (${gitActive.size} git-active)`);

  let filed = 0;
  let reconciled = 0;
  for (const name of selected) {
    const projectPath = resolvedPaths[name];
    if (!projectPath || !existsSync(projectPath)) { checked[name] = now; continue; }
    try {
      const findings = reconcileProject({
        projectRoot: projectPath,
        countRules: countRulesFor(projects[name]),
        gitIgnored,
      });
      reconciled += 1;
      for (const finding of findings) {
        if (filed >= RING4_ITEM_CAP) break;
        if (fileDriftItem({ projectName: name, projectPath, finding, file, listPendingItems })) {
          filed += 1;
          log(`Filed doc-drift for ${name}: ${finding.drift_key}`);
        }
      }
    } catch (e) {
      logError(`Reconciliation failed for ${name}: ${e.message}`);
    }
    checked[name] = now;
    if (filed >= RING4_ITEM_CAP) {
      log(`Reconciliation item cap (${RING4_ITEM_CAP}) reached — remaining projects deferred to next run`);
      break;
    }
  }

  state.projects_checked = checked;
  state.last_run = new Date(now).toISOString();
  mkdirSync(join(WATCHTOWER_DIR, 'state'), { recursive: true });
  atomicWrite(statePath, JSON.stringify(state, null, 2));

  log(`Reconciliation complete — ${reconciled} reconciled, ${filed} drift item(s) filed`);
  return { filed, reconciled };
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

async function main() {
  const startTime = Date.now();
  let status = 'success';
  let errorMessage = null;

  log('Ring 4 starting');
  try {
    const config = loadConfig();
    if (config.defaults?.truth_reconciliation === false) {
      log('Ring 4 disabled by config (defaults.truth_reconciliation = false)');
    } else {
      runReconciliation(config);
    }
  } catch (e) {
    status = 'error';
    errorMessage = e.message;
    logError(`Ring 4 error: ${e.message}`);
  }

  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring4-health.json');
  mkdirSync(join(WATCHTOWER_DIR, 'state'), { recursive: true });
  atomicWrite(healthPath, JSON.stringify({
    schema_version: 1,
    last_run: new Date().toISOString(),
    duration_ms: Date.now() - startTime,
    ring: 'ring4',
    status,
    error: errorMessage,
  }, null, 2));
  log('Ring 4 complete');
}

// Entry guard so tests can import the pure helpers without executing main().
// realpathSync matters: node realpath-resolves the main module for
// import.meta.url while argv[1] keeps the given path — a symlinked invocation
// would otherwise make main() silently never run.
const isMain = (() => {
  try { return process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
})();
if (isMain) {
  main().catch(e => {
    logError(`fatal: ${e.message}`);
    process.exit(1);
  });
}

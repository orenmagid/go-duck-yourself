#!/usr/bin/env node

// Watchtower inbox queue CRUD library.
// All writes use atomic temp+rename per watchtower-contracts.md.
// Queue uses directory listing, not index files (no-index convention).

import { readFileSync, writeFileSync, readdirSync, existsSync, mkdirSync, renameSync, unlinkSync, realpathSync, appendFileSync, statSync } from 'fs';
import { join, dirname } from 'path';
import { randomBytes } from 'crypto';
import { pathToFileURL } from 'url';
import { execFileSync } from 'child_process';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const QUEUE_DIR = join(WATCHTOWER_DIR, 'queue', 'items');
const STATE_DIR = join(WATCHTOWER_DIR, 'state');

// mux's dispatch queue (bin/mux `qa` verbs — the single desk-dispatch path,
// carrying qa-handoffs AND routine dispatches). Descriptors are routing
// convenience keyed <desk>/<item_id>.json (plus <desk>/in-flight/ once
// drained); the inbox item is the durable record. Terminal exits here must
// clear the matching descriptor or the two stores drift (act:796fe6dc —
// resolved ghosts get re-offered by `mux qa drain`). The dir name is legacy.
const MUX_QA_DIR = process.env.MUX_QA_DIR
  || join(process.env.HOME, '.local', 'share', 'mux', 'qa-handoff');

// Categories whose items are pushed to a desk via the mux dispatch queue.
// Every terminal exit (resolve / dismiss / supersede) on an item in
// one of these categories clears its dispatch descriptor(s). Exported so
// consumers (e.g. the /session-handoff epilogue drain) exclude dispatched
// items by importing this set rather than re-listing categories in prose —
// a hand-maintained copy would drift the moment a third category is added.
export const DISPATCHED_CATEGORIES = new Set(['qa-handoff', 'routine']);

// --- Helpers ---

function ensureDir(dir) {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

// Shape guard: ids normally come from generateId()/readdirSync basenames,
// but exported helpers take caller-supplied ids and evidence fields
// (possible_duplicate_of) persist id arrays that later flow back in here —
// reject anything that could escape QUEUE_DIR instead of joining it. Every
// caller-supplied id → path join goes through this (itemPath AND
// getEnrichment's directory join).
function assertLegalItemId(id) {
  if (typeof id !== 'string' || !id || id.startsWith('.')
      || id.includes('/') || id.includes('\\') || id.includes('..')) {
    throw new Error(`watchtower-queue: illegal item id ${JSON.stringify(id)}`);
  }
  return id;
}

function itemPath(id) {
  return join(QUEUE_DIR, `${assertLegalItemId(id)}.json`);
}

function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp';
  writeFileSync(tmp, JSON.stringify(data, null, 2) + '\n');
  renameSync(tmp, filePath);
}

function readItem(filePath) {
  const raw = readFileSync(filePath, 'utf8');
  const item = JSON.parse(raw);
  if (item.schema_version !== 1) {
    throw new Error(`Unsupported schema version ${item.schema_version} in ${filePath}`);
  }
  return item;
}

function generateId() {
  return 'dec-' + randomBytes(4).toString('hex');
}

// Best-effort removal of the mux dispatch-queue descriptor(s) for an item —
// both the queued copy and the in-flight copy, across every desk dir (desk
// names differ from project names, so we sweep rather than guess). Never
// throws: a routing-cleanup failure must not block a gate exit.
//
// No tmux involvement, deliberately — this runs from cron and headless gate
// exits. mux owns re-projecting the ·N badge and reconciles it against this
// dir on every `mux qa` verb, on desk open, and on every window switch (its
// after-select-window hook). An earlier version of this comment claimed the
// badge was picked up "on mux's next mutation", which was the false premise
// behind act:ca6a19a0 — a mutation mux never makes never arrives, and a desk
// showed a stale ·2 over an emptied queue for ~40 minutes.
function clearDispatchEntries(item) {
  try {
    // item.id comes from disk CONTENT (which can differ from the gated
    // filename) — same shape discipline as the write side in
    // writeStage2Dispatch: never join an illegal token into unlink paths.
    if (!isLegalPathToken(item.id)) return;
    if (!existsSync(MUX_QA_DIR)) return;
    for (const desk of readdirSync(MUX_QA_DIR, { withFileTypes: true })) {
      if (!desk.isDirectory()) continue;
      for (const sub of ['.', 'in-flight']) {
        const fp = join(MUX_QA_DIR, desk.name, sub, `${item.id}.json`);
        try {
          if (existsSync(fp)) unlinkSync(fp);
        } catch { /* best-effort per file */ }
      }
    }
  } catch { /* best-effort overall */ }
}

// Urgency sort order: urgent < normal < low (urgent first)
const URGENCY_ORDER = { urgent: 0, normal: 1, low: 2 };

// --- qa-handoff category contract (the staff-QA recipient gate) ---
//
// A qa-handoff item cannot leave the queue silently: resolution requires a
// structured, field-validated qa_verdict; dismissal/supersession require a
// typed reason; no expiry exists (nothing in the queue ages out). The gate's playbook, tiers, and the
// verdict shape are defined ONCE in the qa-handoff skill (SKILL.md, "The
// recipient gate") — this section validates the SHAPE at the API so the one
// sin, a silent stamp with no coverage look, is impossible rather than
// discouraged. Keep all qa-handoff domain knowledge fenced here; the CRUD
// functions below only call into it.
//
// merge_state (evidence.merge_state): a handoff is normally filed AFTER the
// worktree branch merged ('merged' — the original and default contract). A
// handoff filed BEFORE the merge (the operator is gating the merge on an
// in-flight staging deploy) carries 'merge-pending': the skill defers stage-2
// dispatch and the pickup prompt says "merge then QA" instead of asserting a
// merge — no more hand-flagging merged_into: "PENDING". The recipient gate is
// unchanged in what it DEMANDS (a stamped qa_verdict against the post-merge
// commit) and tightened in when it can FIRE: a merge-pending item cannot
// resolve at all until markHandoffMerged records the verified merge
// (act:3d1ac2b7).

const QA_CATEGORY = 'qa-handoff';
const KNOWLEDGE_CATEGORY = 'knowledge-extraction';

// --- Knowledge-extraction four-axis vocabulary (act:471dd701) ---
//
// type / home / derivable / subject is the taxonomy Ring 3's extraction
// prompt, Ring 2's sweep, and /inbox all read. SSOT here (createItem's
// boundary check needs it directly, with zero imports — watchtower-lib.mjs
// already imports FROM this file to re-export createItem, so the reverse
// import would be a cycle); watchtower-lib.mjs re-exports both consts
// alongside createItem. A prompt-parity test asserts the extraction
// prompt's JSON schema literal matches these arrays — a doc is not
// enforcement.
//
// 'unclassifiable' (type) is the junk drawer that produced this taxonomy:
// a required `unclassifiable_reason` makes model uncertainty visible
// instead of forcing a confident wrong type/home guess — home is not
// validated when type is 'unclassifiable' (the model isn't asked to guess
// one). 'derivation' (home) is the route for a DERIVABLE fact (recomputable
// on demand — a memory of it is a stale cache): a `derivable: true` item
// requires `derivation`, a short instruction for how to recompute it, and
// is ALWAYS filed (never dropped) — home becomes the recomputation itself,
// not a written record.
export const KNOWLEDGE_EXTRACTION_TYPES = ['decision', 'constraint', 'lesson', 'preference', 'unclassifiable'];
export const KNOWLEDGE_EXTRACTION_HOMES = ['memory', 'claude-md', 'pib-db-trigger', 'upstream-feedback', 'derivation', 'session-record'];

// Categories whose items carry a structural recipient gate: they may never be
// included in a batch disposition — each leaves the queue only through its own
// gate (per-item resolve with a validated verdict, or a typed per-item
// dismissal). Distinct from DISPATCHED_CATEGORIES: 'routine' is dispatched but
// NOT gated — stale routines are legal batch fodder. Future gated categories
// join this set and inherit batch refusal for free.
//
// The RELATIONAL categories (pending-relation, significance-change — the
// retro-remeaning proposal verbs, act:5182beda/act:bcb7edd4) are gated per
// the plan's non-goal: "relational verbs go in GATED_CATEGORIES so applyBatch
// structurally refuses to bulk them." A relation proposal is a judgment about
// how two pieces of knowledge relate — rubber-stamping a batch of them is
// re-meaning without a real human gate, which is how a system gaslights its
// operator (MANIFESTO principle 4). Unlike qa-handoff they have no verdict
// shape; the durable relation_key filing exclusion guarantees a closed
// proposal (any status, including the historical `expired`) never refiles.
export const GATED_CATEGORIES = new Set([QA_CATEGORY, 'pending-relation', 'significance-change']);

// --- Retired and narrowed categories (Simplification Project 2, plan A9 as
// narrowed by EP4; act:73dd31d4) -------------------------------------------
//
// The operator's ruling (EP4): "no feedback line closes." A producer that
// still files a category the inbox no longer carries — a consumer's hook, a
// by-hand skill step, a runtime that has not been refreshed — must neither
// lose its finding nor refill a queue nobody reads. So createItem REDIRECTS
// a stray filing to the channel that is actually read, and REFUSES only a
// category with no live home at all.
//
// RETIRED_CATEGORIES: category → live home | null. The keys are pinned to
// INBOX_DETECTOR_REGISTRY's retired entries by queue-retired-categories.test
// (a category retired in one place and not the other is the drift that pin
// exists to catch); each later tear-out lane adds its categories in the same
// commit that retires them. Homes:
//   'carry' — the project's carry ledger (state/projects/<slug>/carry.json),
//             rendered in the next start block (plan B2) and settled by the
//             /close residue question (plan B3);
//   'doc-drift-state' — the project's documentation-drift state
//             (state/projects/<slug>/doc-drift.json), upserted by drift_key
//             and rendered by Ring 1 into Standing Issues (plan A4);
//   'thread' — the thread file's `proposed_recolors` (state/threads/<slug>.json,
//             appendThreadProposal), shown by /threads (plan A8 / EP1);
//   null    — no live home: the filing throws, loudly, and is logged.
export const RETIRED_CATEGORIES = new Map([
  ['knowledge-extraction', null],
  ['pending-relation', null],
  ['watchtower-health', null],
  ['routing-decision', null],
  ['raised-unhandled', 'carry'],
  ['skill-candidate', 'carry'],
  ['coverage-warning', 'carry'],
  ['methodology-capture', 'carry'],
  ['verify-backfill', 'carry'],
  ['doc-drift', 'doc-drift-state'],
  ['significance-change', 'thread'],
]);

// NARROWED_CATEGORIES: the two LIVE categories whose meaning this project
// narrows. Each predicate returns the live home for the slice that LEAVES the
// inbox, or null for the slice that stays:
//   advisor-finding   — a seat proposal (evidence.roster_kind, Ring 2's roster
//                       review) is a decision and stays; every other advisor
//                       finding → 'carry' (shown once in the next start block,
//                       Decision 4);
//   upstream-friction — route project-tracker → 'carry' (EP2: carried to the
//                       next session in that project until settled); route
//                       cc-feedback → 'outbox' (Q1: the /cc-feedback outbox,
//                       delivered by Ring 1, deduplicated per T8); config-change
//                       (or no route) stays in the inbox.
export const NARROWED_CATEGORIES = new Map([
  ['advisor-finding', (evidence) => (evidence && evidence.roster_kind ? null : 'carry')],
  ['upstream-friction', (evidence) => {
    const type = evidence && evidence.route && evidence.route.type;
    if (type === 'project-tracker') return 'carry';
    if (type === 'cc-feedback') return 'outbox';
    return null;
  }],
]);

/**
 * Where a filing of this category goes instead of the queue: a home string,
 * or null when it files as usual. Throws for a retired category with no home.
 */
export function redirectHomeFor(category, evidence) {
  if (RETIRED_CATEGORIES.has(category)) {
    const home = RETIRED_CATEGORIES.get(category);
    if (home === null) {
      const err = new Error(`createItem: '${category}' is retired and has no live home — nothing reads it any more; see INBOX_DETECTOR_REGISTRY (watchtower-lib.mjs) for where that kind of finding went`);
      err.code = 'RETIRED_NO_HOME';
      throw err;
    }
    return home;
  }
  const narrow = NARROWED_CATEGORIES.get(category);
  return narrow ? narrow(evidence) : null;
}

const QA_TERMINAL_VERDICTS = ['runtime-verified', 'blocked'];
// The parameterized token. Canonical form uses U+00B7 (·); input tolerates
// common separator drift (-, –, —, *, •) because sessions retype labels.
const QA_GAPS_TOKEN_RE = /^verified\s*[·•*\-–—]\s*(\d+)\s+gaps?\s+filed$/;
const QA_COVERAGE_ASSESSMENTS = ['adequate', 'extended', 'gap-filed'];

// The KIND of debt a filed gap represents — captured at the moment of filing,
// because that is the one moment the answer is free. Piece 1 of
// `.claude/plans/qa-debt-payer-build.md`. Exported so the debt ledger imports
// this list rather than re-listing it (one machine definition, one place).
//
// DISTINCT from the engagement module's `needs:` set (`{decision, credential}`,
// engagement-schema.md) — they overlap on 'decision' only and answer different
// questions. This one answers "what kind of work is owed"; engagement's answers
// "what does the client need to supply". Adding a value to one is NOT a reason
// to add it to the other.
//
// 'event-gated' additionally REQUIRES `releasing_event` — it is the write-time
// shape requirement that stops event-waiting work being filed as ordinary debt
// and then skipped by the payer every drain.
export const QA_GAP_KINDS = ['code-fix', 'qa-verify', 'decision', 'operator', 'event-gated'];

// A fid is `act:` + exactly 8 lowercase hex. Anchored, and the ONLY spelling in
// this file — the ledger imports it too. (pib-db-lib exports its own FID_PATTERN
// covering `prj:` as well; this one is action-only by intent, since a gap is
// always filed as an action.)
export const QA_FILED_AS_FID_RE = /^act:[0-9a-f]{8}$/;

// Deliberate non-filing, as closed vocabulary rather than free text. These were
// already a live convention in coverage_posture.gap_filed / class_sweep.filed
// (10 of 300 live entries: 'not-filed-by-design', 'fixed-in-drain', and seven
// 'n/a — …' forms). Legal ONLY in those two arrays: `filed_gaps` feeds the
// N-count in the verdict label, and a confessed gap has its own three-way
// disposition, so a sentinel there would be a way to say nothing.
const QA_FILED_AS_SENTINELS = ['not-filed-by-design', 'fixed-in-drain'];

/**
 * Normalize a qa-handoff verdict token to canonical form.
 * Returns 'runtime-verified' | 'blocked' | 'verified · N gaps filed' (N >= 1),
 * or null when the input is not a legal token (including bare 'verified').
 * @param {string} raw
 * @returns {string|null}
 */
export function normalizeQaVerdictToken(raw) {
  if (typeof raw !== 'string') return null;
  const t = raw.normalize('NFKC').trim().replace(/\s+/g, ' ');
  if (QA_TERMINAL_VERDICTS.includes(t)) return t;
  const m = t.match(QA_GAPS_TOKEN_RE);
  if (m) {
    const n = parseInt(m[1], 10);
    if (Number.isSafeInteger(n) && n >= 1) return `verified · ${n} gaps filed`;
  }
  return null;
}

// The two legal merge states for a qa-handoff item.
export const QA_MERGE_STATES = ['merged', 'merge-pending'];

/**
 * Normalize a qa-handoff merge_state token to canonical form.
 * Absent (null/undefined) defaults to 'merged' — the original contract, where
 * a handoff was always filed AFTER the worktree branch merged. A handoff filed
 * before the merge carries 'merge-pending'. Tolerates separator/case drift
 * (merge_pending, "merge pending", pending, unmerged); returns null only for a
 * present-but-illegal token, so a typo cannot silently read as 'merged'.
 * @param {*} raw
 * @returns {'merged'|'merge-pending'|null}
 */
export function normalizeMergeState(raw) {
  if (raw == null) return 'merged';
  if (typeof raw !== 'string') return null;
  const t = raw.normalize('NFKC').trim().toLowerCase().replace(/[\s_]+/g, '-');
  if (t === 'merged') return 'merged';
  if (['merge-pending', 'mergepending', 'pending', 'unmerged'].includes(t)) return 'merge-pending';
  return null;
}

// --- merged-by-construction (act:3d1ac2b7) ---
//
// A 'merged' qa-handoff claim is VERIFIED against git at the boundary, not
// asserted: sha-shaped merged_commit required (omission was the trivial
// bypass), ancestry checked where git can answer. Reject only on positive
// refutation (git says "not an ancestor") — the detector-symmetry discipline;
// when git CANNOT answer (no repo, no resolvable main ref, timeout) the filing
// proceeds with a visible evidence.ancestry_verified: 'unverifiable' stamp,
// and the drain's Step 0 live check remains the second layer. 'merge-pending'
// requires a named external blocker (evidence.merge_gate) — the lazy silent
// default fails loud at file time, with a message that teaches the merge-first
// close-out so a session on older skill text self-corrects.

// Sha shape: hex, 7-40 chars. Also the option-injection guard — a '-'-prefixed
// or ref-name value (HEAD, @{u}) never reaches the git argv.
const SHA_RE = /^[0-9a-f]{7,40}$/i;

function runGit(projectPath, args) {
  // Args only, never shell-interpolated (Ring 1 CP3 convention); the 5s
  // timeout bounds each git call (Ring 2's openPibDb timeout precedent).
  // Known seam: the existsSync gate upstream is a synchronous stat with no
  // timeout — a hard-hung network mount can stall there before git ever
  // runs; sync Node offers no stat timeout, and all live project paths are
  // local disks.
  return execFileSync('git', ['-C', projectPath, ...args], {
    encoding: 'utf8', timeout: 5000, stdio: ['ignore', 'pipe', 'pipe'],
  });
}

// Queue-local main-ref resolution: origin/HEAD → origin/main → origin/master
// → local main/master. Deliberately NOT imported from Ring 1's resolveMainRef:
// watchtower-lib re-exports createItem FROM this file and ring1 imports both,
// so queue→ring1/lib is an ESM cycle. Never fetches — resolution is against
// local refs only (a network call inside every interactive filing is wrong,
// and the merge-first close-out just pushed, so the tracking ref is current).
function resolveAncestryRef(projectPath) {
  try {
    const head = runGit(projectPath, ['symbolic-ref', '--short', 'refs/remotes/origin/HEAD']).trim();
    if (head) return head;
  } catch { /* fall through to candidates */ }
  for (const cand of ['origin/main', 'origin/master', 'main', 'master']) {
    try {
      runGit(projectPath, ['rev-parse', '--verify', '--quiet', `${cand}^{commit}`]);
      return cand;
    } catch { /* next candidate */ }
  }
  return null;
}

/**
 * Verify a merged claim against git. Returns one of:
 *   { state: 'verified', ref }   — merged_commit IS an ancestor of the main ref
 *   { state: 'refuted', ref }    — git answered definitively: NOT an ancestor
 *   { state: 'unverifiable', reason } — git could not answer (no repo/ref/git,
 *                                  unknown object, timeout)
 * Callers reject ONLY on 'refuted'. Exported for tests.
 */
export function verifyMergedAncestry(projectPath, mergedCommit) {
  if (typeof mergedCommit !== 'string' || !SHA_RE.test(mergedCommit)) {
    return { state: 'unverifiable', reason: 'merged_commit is not sha-shaped' };
  }
  if (typeof projectPath !== 'string' || !projectPath || !existsSync(projectPath)) {
    return { state: 'unverifiable', reason: 'project_path missing or not on disk' };
  }
  // resolveAncestryRef never throws (every git call inside it is caught
  // per-candidate) — a git-absent ENOENT surfaces here as a null ref, so
  // this one reason covers both no-ref and no-git.
  const ref = resolveAncestryRef(projectPath);
  if (!ref) return { state: 'unverifiable', reason: 'no main ref resolvable (or git unavailable)' };
  try {
    runGit(projectPath, ['merge-base', '--is-ancestor', mergedCommit, ref]);
    return { state: 'verified', ref };
  } catch (err) {
    // Exit 1 is git's definitive "not an ancestor"; anything else (128 =
    // unknown object / not a repo, killed = timeout) is can't-determine —
    // an unknown sha is not PROOF of unmergedness (shallow clones exist).
    if (err && err.status === 1) return { state: 'refuted', ref };
    return { state: 'unverifiable', reason: `git error${err && err.status ? ` (exit ${err.status})` : ''}` };
  }
}

/**
 * Does `sha` name a real commit in the project repo? (act:b9986c1b)
 *
 * Sibling of verifyMergedAncestry with the same three-state honesty: a
 * sha-shaped string is necessary, git is the proof. Four times a verdict or
 * handoff carried a fabricated hash — a short sha padded to 40 hex to satisfy
 * the shape check, twice by a session that had just filed the same failure as
 * a lesson. Shape checks cannot see that; `rev-parse --verify` can, and it
 * also expands a real short sha to the full form so the stored verdict is
 * unambiguous.
 *
 *   verified     — the sha resolves; `sha` carries the FULL form
 *   refuted      — git answered definitively "no such object" (exit 128 on
 *                  --verify) → the verdict is rejected
 *   unverifiable — no project_path on disk, or git itself unavailable →
 *                  the verdict proceeds with a visible commit_verified stamp
 *
 * Same runGit / 5s timeout / args-only discipline as the ancestry check.
 * @returns {{state: 'verified', sha: string} | {state: 'refuted'|'unverifiable', reason: string}}
 */
export function verifyCommitExists(projectPath, sha) {
  if (typeof sha !== 'string' || !SHA_RE.test(sha.trim())) {
    return { state: 'unverifiable', reason: 'commit_tested is not sha-shaped' };
  }
  if (typeof projectPath !== 'string' || !projectPath || !existsSync(projectPath)) {
    return { state: 'unverifiable', reason: 'project_path missing or not on disk' };
  }
  try {
    const full = runGit(projectPath, ['rev-parse', '--verify', '--quiet', `${sha.trim()}^{commit}`]).trim();
    return full ? { state: 'verified', sha: full } : { state: 'refuted', reason: 'no such commit' };
  } catch (err) {
    // `--verify --quiet` exits 1 for an unknown object; 128 for "not a git
    // repository" and friends; a killed process (timeout) has no status.
    if (err && err.status === 1) return { state: 'refuted', reason: 'no such commit' };
    return { state: 'unverifiable', reason: `git error${err && err.status ? ` (exit ${err.status})` : ''}` };
  }
}

function gateError(item, message) {
  // Never echo resolution/notes content back — name fields only.
  return new Error(`qa-handoff gate: cannot close ${item.id} — ${message}`);
}

/**
 * Validate the structured verdict for a qa-handoff item. Throws naming the
 * missing/invalid field; returns the canonical verdict token on success.
 * Validates the RESOLUTION shape only — legacy items (no risk_surface /
 * tier_hint, absent or non-array could_not_verify) stay resolvable.
 * @param {object} item
 * @param {object} qa_verdict
 * @returns {string} canonical verdict token
 */
/**
 * Validate one gap-bearing entry that carries a `filed_as`.
 *
 * `allowSentinel` is the ONLY axis that differs between the four arrays:
 * coverage_posture.gap_filed and class_sweep.filed may say "deliberately not
 * filed" in closed vocabulary; filed_gaps and confessed_gap.deferred may not.
 *
 * A fid-shaped entry always requires a `kind`. A sentinel never does — there is
 * no work to type.
 */
function validateFiledEntry(item, entry, where, { allowSentinel }) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
    throw gateError(item, `${where} must be an object carrying {gap, filed_as}`);
  }
  const filedAs = typeof entry.filed_as === 'string' ? entry.filed_as.trim() : '';
  if (!filedAs) {
    throw gateError(item, `${where}.filed_as is required`);
  }
  if (allowSentinel && QA_FILED_AS_SENTINELS.includes(filedAs)) return;
  if (!QA_FILED_AS_FID_RE.test(filedAs)) {
    const legal = allowSentinel
      ? `an action fid (act: + 8 hex), or one of: ${QA_FILED_AS_SENTINELS.join(' | ')}`
      : 'an action fid (act: + 8 hex)';
    throw gateError(item, `${where}.filed_as must be ${legal} — got ${JSON.stringify(filedAs)}`);
  }
  if (!QA_GAP_KINDS.includes(entry.kind)) {
    throw gateError(item, `${where}.kind is required and must be one of: ${QA_GAP_KINDS.join(' | ')} — it is what lets the debt payer pick this item up, and the drain is the only place the answer is free`);
  }
  if (entry.kind === 'event-gated') {
    const ev = typeof entry.releasing_event === 'string' ? entry.releasing_event.trim() : '';
    if (!ev) {
      throw gateError(item, `${where}.kind is 'event-gated' so ${where}.releasing_event is required — name what must happen for this to become workable, or pick a kind that means attention rather than waiting`);
    }
  }
}

/**
 * Validate every entry of a filed-gap array, when the array is present.
 *
 * Keyed on PRESENCE of the array, never on a sibling field. The previous
 * gap_filed check ran only under `assessment === 'gap-filed'`, which left 115
 * of 364 stored verdicts (32%) unvalidated and would have preserved the exact
 * cheapest-legal-place gradient this guard exists to close, merely re-keyed.
 */
function validateFiledArray(item, arr, where, opts) {
  if (arr === undefined || arr === null) return;
  if (!Array.isArray(arr)) {
    throw gateError(item, `${where} must be an array when present`);
  }
  arr.forEach((entry, i) => validateFiledEntry(item, entry, `${where}[${i}]`, opts));
}

export function validateQaVerdict(item, qa_verdict) {
  if (!qa_verdict || typeof qa_verdict !== 'object' || Array.isArray(qa_verdict)) {
    throw gateError(item, "missing structured qa_verdict object (pass qa_verdict to resolveItem; shape: see 'The recipient gate' in the qa-handoff skill)");
  }
  if (typeof qa_verdict.commit_tested !== 'string' || !qa_verdict.commit_tested.trim()) {
    throw gateError(item, 'qa_verdict.commit_tested is required (the main commit the QA ran against)');
  }
  // Sha-shaped is not a commit (act:b9986c1b). Refute a fabricated hash
  // outright; expand a real short one; stamp the cases git cannot answer so
  // "verified" is never inferred from silence. The stamp lands on the verdict
  // object the caller stores, the same way the ancestry check stamps evidence.
  {
    const check = verifyCommitExists(item && item.project_path, qa_verdict.commit_tested);
    if (check.state === 'refuted') {
      throw gateError(item, `qa_verdict.commit_tested "${qa_verdict.commit_tested}" is not a commit in the project repo — a sha-shaped string is not a commit; get the real value from git before stamping`);
    }
    if (check.state === 'verified') qa_verdict.commit_tested = check.sha;
    qa_verdict.commit_verified = check.state === 'verified' ? 'verified' : `unverifiable: ${check.reason}`;
  }
  if (qa_verdict.tier !== 'narrow' && qa_verdict.tier !== 'full') {
    throw gateError(item, "qa_verdict.tier is required ('narrow' | 'full')");
  }

  const posture = qa_verdict.coverage_posture;
  if (!posture || typeof posture !== 'object' || Array.isArray(posture)) {
    throw gateError(item, 'qa_verdict.coverage_posture is required (the coverage look, separate from check results)');
  }
  if (!QA_COVERAGE_ASSESSMENTS.includes(posture.assessment)) {
    throw gateError(item, `qa_verdict.coverage_posture.assessment must be one of: ${QA_COVERAGE_ASSESSMENTS.join(' | ')}`);
  }
  if (posture.assessment === 'gap-filed') {
    const filed = posture.gap_filed;
    if (!Array.isArray(filed) || filed.length === 0) {
      throw gateError(item, "coverage_posture.assessment is 'gap-filed' but coverage_posture.gap_filed is not a non-empty array of {gap, filed_as, kind}");
    }
  }
  // Entry validation is keyed on the array being PRESENT, not on the assessment
  // — see validateFiledArray. Sentinels are legal here.
  validateFiledArray(item, posture.gap_filed, 'coverage_posture.gap_filed', { allowSentinel: true });

  // class_sweep is validated NOWHERE today; this is a new branch from zero.
  // Its documented shape is `{...} | 'not-triggered'`, so the string form stays
  // legal and unvalidated — a bare truthiness check would throw on an honest
  // verdict, and an Array.isArray-only check would silently pass a malformed
  // `filed`. Validate iff it is an object carrying a `filed` key.
  const sweep = qa_verdict.class_sweep;
  if (sweep && typeof sweep === 'object' && !Array.isArray(sweep) && sweep.filed !== undefined) {
    validateFiledArray(item, sweep.filed, 'class_sweep.filed', { allowSentinel: true });
  }

  const verdict = normalizeQaVerdictToken(qa_verdict.verdict);
  if (!verdict) {
    throw gateError(item, "qa_verdict.verdict is not a legal token — use 'runtime-verified', 'blocked', or 'verified · N gaps filed' (N >= 1; bare 'verified' is illegal: the label may not out-run its substance)");
  }

  // The gaps-bearing label must match its substance, and a clean label may
  // not hide filed gaps. ('blocked' + filed_gaps is deliberately legal — a
  // blocked QA can still file follow-ups; the block itself is the headline.)
  const gapsMatch = verdict.match(QA_GAPS_TOKEN_RE);
  const filedGaps = Array.isArray(qa_verdict.filed_gaps) ? qa_verdict.filed_gaps : [];
  if (gapsMatch) {
    const n = parseInt(gapsMatch[1], 10);
    if (filedGaps.length !== n
        || filedGaps.some((g) => !g || typeof g.filed_as !== 'string' || !g.filed_as.trim())) {
      throw gateError(item, `verdict claims ${n} gaps filed but qa_verdict.filed_gaps has ${filedGaps.length} entries with a filed_as fid — the count in the label must equal the gaps actually filed`);
    }
  } else if (verdict === 'runtime-verified' && filedGaps.length > 0) {
    throw gateError(item, `verdict 'runtime-verified' with ${filedGaps.length} filed_gaps — use 'verified · ${filedGaps.length} gaps filed'`);
  }

  // Entry validation runs on PRESENCE of the array, so a 'blocked' verdict's
  // filed gaps are typed too (4 live cases; the old nesting inside `gapsMatch`
  // skipped them). It runs AFTER the label/count checks ON PURPOSE: a
  // mislabeled verdict must still report the MISLABEL, not a missing `kind`
  // — the stricter guard must not mask the older, more important one.
  // No sentinels here: this array feeds the N-count above.
  validateFiledArray(item, qa_verdict.filed_gaps, 'filed_gaps', { allowSentinel: false });

  // Confessed gaps cannot be laundered: every could_not_verify entry must be
  // discharged as fixed-in-session or an explicitly typed deferral. Absent /
  // non-array / empty confession lists demand nothing (legacy compatibility).
  const confessed = Array.isArray(item.evidence?.could_not_verify)
    ? item.evidence.could_not_verify : [];
  if (confessed.length > 0) {
    const cg = qa_verdict.confessed_gap;
    if (!cg || typeof cg !== 'object' || Array.isArray(cg)) {
      throw gateError(item, `this handoff confessed ${confessed.length} could_not_verify entr${confessed.length === 1 ? 'y' : 'ies'} — qa_verdict.confessed_gap {written_and_run, deferred} is required to discharge them`);
    }
    const written = Array.isArray(cg.written_and_run) ? cg.written_and_run : [];
    const deferred = Array.isArray(cg.deferred) ? cg.deferred : [];
    // THIRD disposition. A confessed gap whose surface was deleted, or whose
    // check became meaningless, can say neither `written_and_run` (false — no
    // test was run) nor `deferred` (which demands a real fid, filing an action
    // nobody wants). Leaving it at two forks reproduces the exact
    // no-legal-way-to-state-the-truth shape that killed an earlier draft of
    // this guard: a drain forced to choose between two false statements picks
    // one, and the record is wrong either way.
    const obsolete = Array.isArray(cg.obsolete) ? cg.obsolete : [];
    written.forEach((w, i) => {
      if (typeof w !== 'string' || !w.trim()) {
        throw gateError(item, `confessed_gap.written_and_run[${i}] must be a non-empty string naming the test written and run`);
      }
    });
    deferred.forEach((d, i) => {
      if (!d || typeof d !== 'object'
          || typeof d.gap !== 'string' || !d.gap.trim()
          || typeof d.justification !== 'string' || !d.justification.trim()) {
        throw gateError(item, `confessed_gap.deferred[${i}] must be a typed deferral {gap, filed_as, kind, justification} — a confessed gap is fixed in-session (written_and_run), deferred with a filed action, or declared obsolete; never folded into 'filed'`);
      }
      validateFiledEntry(item, d, `confessed_gap.deferred[${i}]`, { allowSentinel: false });
    });
    obsolete.forEach((o, i) => {
      if (!o || typeof o !== 'object'
          || typeof o.gap !== 'string' || !o.gap.trim()
          || typeof o.justification !== 'string' || !o.justification.trim()) {
        throw gateError(item, `confessed_gap.obsolete[${i}] must be {gap, justification} — say why the gap no longer needs closing (the surface went away, the check became meaningless); it files no action and asserts no test was run`);
      }
    });
    if (written.length + deferred.length + obsolete.length < confessed.length) {
      throw gateError(item, `${confessed.length} confessed could_not_verify entr${confessed.length === 1 ? 'y' : 'ies'} but only ${written.length + deferred.length + obsolete.length} dispositioned (written_and_run + typed deferrals + obsolete) — every confessed gap must be discharged`);
    }
  }

  return verdict;
}

/**
 * Emit a pattern-promotion inbox item from a qa-gate class sweep.
 * Threshold-gated (default >= 3 instances), deduplicated against pending
 * pattern-promotion items by (source_item_id, failure_class), and refuses
 * to file into a directory that is not a real watchtower install.
 * @param {object} params
 * @returns {string|null} The item id (existing id when deduplicated),
 *   or null when instance_count is below threshold.
 */
export function emitPatternPromotion({
  project,
  project_path,
  source_item_id,
  failure_class,
  instance_count,
  pattern_text,
  population = null,
  desk = null,
  threshold = 3,
}) {
  if (typeof failure_class !== 'string' || !failure_class.trim()) {
    throw new Error('emitPatternPromotion: failure_class is required');
  }
  if (typeof source_item_id !== 'string' || !source_item_id.trim()) {
    throw new Error('emitPatternPromotion: source_item_id is required (the qa-handoff item the sweep ran for — it is half the dedup key)');
  }
  if (!Number.isInteger(threshold) || threshold < 1) {
    throw new Error('emitPatternPromotion: threshold must be a positive integer');
  }
  if (!Number.isInteger(instance_count) || instance_count < threshold) return null;
  if (!existsSync(join(WATCHTOWER_DIR, 'config.json'))) {
    throw new Error(`emitPatternPromotion: no watchtower install at ${WATCHTOWER_DIR} (config.json missing) — refusing to file into a phantom queue; record the promotion candidate in the stamped verdict instead`);
  }
  const existing = listPending({ category: 'pattern-promotion' }).find(
    (i) => i.evidence?.source_item_id === source_item_id
        && i.evidence?.failure_class === failure_class,
  );
  if (existing) return existing.id;
  return createItem({
    project,
    project_path,
    category: 'pattern-promotion',
    urgency: 'normal',
    title: `Pattern promotion: ${failure_class}`,
    summary: `qa-gate class sweep found ${instance_count} instances${population ? ` (${population})` : ''} — recurring failure class, promotion candidate`,
    context_anchor: `qa-handoff item ${source_item_id}`,
    evidence: {
      source: 'qa-gate-sweep',
      source_item_id,
      failure_class,
      instance_count,
      population,
      pattern_text,
    },
    options: [
      { value: 'write', label: 'Write pattern', description: 'Capture to the project pattern dir for later promotion review' },
      { value: 'dismiss', label: 'Dismiss', description: 'Not a recurring class worth capturing' },
    ],
    filed_by: 'qa-gate',
    desk,
  });
}

// --- High-confidence sign-off predicate (bulk-triage batching) ---
//
// Most pending items are knowledge-extraction items the rings already DRAFTED
// (Ring 3 mints a memory `draft_artifact` from the session transcript). These
// aren't judgment calls — they're SIGN-OFFS on work the system already did.
// Because each is uniquely titled, title-grouping degrades bulk triage to a
// per-item walk, burning the same decision fuel on a sign-off as on a hard
// choice. The predicate below isolates that class so bulk triage can offer it
// as ONE explicit batch sign-off (still human-confirmed — never auto-resolved;
// the frozen-at-propose invariant holds because the operator approves the
// whole batch before any applyBatch write).
//
// The signal reuses the SAME "drafts ready" cue the SessionStart context
// builder already counts (watchtower-build-context.mjs: knowledge-extraction +
// non-empty draft_artifact) — single source for "the system pre-authored this".
// An item is held back for individual attention (NOT signed off in a batch)
// when it is urgent, low-confidence, or carries no pre-drafted artifact —
// those are the ones that actually want a human look. Gated categories
// (qa-handoff) are never high-confidence sign-off by construction: they exit
// only through their own recipient gate.

// Categories whose pre-drafted, normal-priority items are sign-off-shaped.
// Today only knowledge-extraction (Ring 3's memory drafts). A future
// pre-drafted category joins here and inherits batch sign-off for free.
const SIGNOFF_CATEGORIES = new Set(['knowledge-extraction']);

/**
 * True when an item is a high-confidence SIGN-OFF — pre-drafted work the
 * system already did, safe to approve as part of an explicit one-shot batch
 * rather than as its own micro-decision. False for anything that wants an
 * individual human look (urgent, low-confidence, or no pre-drafted artifact)
 * and for any gated-category item (those leave only through their own gate).
 *
 * Conservative by design: when in doubt the item is surfaced individually.
 * The operator still confirms the whole batch before any write — this only
 * decides WHICH items are eligible to ride in a batch sign-off.
 * @param {object} item
 * @returns {boolean}
 */
export function isHighConfidenceSignoff(item) {
  if (!item || typeof item !== 'object') return false;
  if (item.status && item.status !== 'pending') return false;
  if (GATED_CATEGORIES.has(item.category)) return false;
  if (!SIGNOFF_CATEGORIES.has(item.category)) return false;
  // The system must have actually drafted something — a non-empty artifact is
  // the proof the work is done and only needs a sign-off.
  if (typeof item.draft_artifact !== 'string' || !item.draft_artifact.trim()) return false;
  // Urgent items want eyes, not a batch nod.
  if (item.urgency === 'urgent') return false;
  // An explicit low-confidence stamp means "look at this one".
  if (item.confidence === 'low') return false;
  // Surfacing-intelligence annotations (act:00051dca) demote to individual
  // review: a fold candidate wants a human look, not a batch nod. Key on the
  // SEMANTIC value, never on field presence. (The sibling freshness/overtaken
  // demotion was RETIRED — act:471dd701, N2 of grp:retro-remeaning: it fired
  // on 62-of-66 TRUE drafts and 70-of-84 flags were false by construction,
  // catching less than chance. See runDraftAnnotationSweep in
  // watchtower-ring2.mjs for the measurement.)
  //
  // act:3edf14ee — demote on the CLUSTER, not on the pairwise flag. Demoting on
  // a non-empty `possible_duplicate_of` collapsed the partition to EMPTY at
  // scale: on a 252-item queue, 151 items carried the flag and signoff came back
  // 0. A second desk measured the same day that 10 of 10 demoted draft-carrying
  // items portfolio-wide were demoted by this flag ALONE — none by staleness or
  // urgency. When a flag fires on ~60% of a queue, a "conservative" demotion is
  // arithmetically equivalent to disabling the feature, and it disables it
  // exactly when the pile is deepest, which is the only time it matters.
  //
  // The pairwise flag is STRICTLY PAIRWISE by design (see proposeFolds) and is
  // NOT a group. `duplicate_cluster` is the cohesion-checked component computed
  // once at detection time. Three states, handled explicitly rather than by
  // falsy-coercion, because they mean different things:
  //
  //   object            — a surviving, bounded cluster: there IS a coherent
  //                       group to read side by side. DEMOTE; this is the case
  //                       the demotion was written for.
  //   null + rejected   — the matcher examined the component and could not
  //                       substantiate it (oversized / chained). ALLOW: there is
  //                       nothing to compare against, so individual review buys
  //                       the operator no information and costs the partition.
  //   field absent      — annotated before clustering shipped (285 of 564 live
  //                       flagged items at time of writing). ALLOW, for the same
  //                       reason: no substantiation exists. Absence is
  //                       "unanalyzed", not "analyzed and confirmed", and the
  //                       finding here is precisely that the bare pairwise flag
  //                       was never a sufficient basis for demotion. The sweep
  //                       backfills the field on a later pass.
  //
  // The advisory contract is preserved throughout: the annotation still never
  // SUPPRESSES an item, it only routes it to individual review.
  if (item.evidence && Array.isArray(item.evidence.possible_duplicate_of)
      && item.evidence.possible_duplicate_of.length > 0
      && item.evidence.duplicate_cluster
      && typeof item.evidence.duplicate_cluster === 'object') return false;
  // Held items (holdItem) are deliberate human retention — never a batch nod.
  if (item.evidence && item.evidence.held) return false;
  return true;
}

// --- Classification pass at scale (act:471941c9, part 5) ---
//
// The batch-signoff predicate is deliberately conservative, which is right when
// the individual set is small and wrong when it is enormous. On 2026-07-30 it
// routed 105 of 110 knowledge items into "needs a human look" and the surface
// offered a 5-item batch against a 145-item pile — a remedy shaped for a
// problem the operator did not have. What actually cleared the queue was a
// CLASSIFICATION PASS: group by shape, propose one disposition per group. The
// operator had to ask for it in so many words ("I am not going to look at them
// one at a time. You're intelligent, right?"), so it becomes the offered path
// rather than something they have to think of.
//
// The threshold is the point where a per-item walk stops being a real offer.
export const INBOX_CLASSIFICATION_THRESHOLD = 30;

/**
 * Group items by SHAPE for a classification pass — the unit the operator
 * dispositions when the individual set is too large to walk. Shape is the
 * tuple a single decision can reasonably span: category, the extraction type
 * and routing home when present, and urgency.
 *
 * Pure read helper — no writes, no I/O. Returns groups sorted largest-first, so
 * the biggest win is the first decision offered.
 * @param {Array} items
 * @returns {Array<{key: string, label: string, category: string, type: string|null, home: string|null, urgency: string, items: Array}>}
 */
export function groupItemsByShape(items) {
  const groups = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    if (!item || typeof item !== 'object') continue;
    const category = item.category || 'uncategorized';
    const ev = item.evidence || {};
    const type = typeof ev.type === 'string' ? ev.type : null;
    const home = typeof ev.home === 'string' ? ev.home : null;
    const urgency = item.urgency || 'normal';
    const key = [category, type || '-', home || '-', urgency].join('|');
    if (!groups.has(key)) {
      const parts = [category];
      if (type) parts.push(type);
      if (home) parts.push(`→ ${home}`);
      if (urgency !== 'normal') parts.push(urgency);
      groups.set(key, {
        key, label: parts.join(' · '), category, type, home, urgency, items: [],
      });
    }
    groups.get(key).items.push(item);
  }
  return [...groups.values()].sort((a, b) => b.items.length - a.items.length);
}

/**
 * Partition pending items into the high-confidence sign-off set (eligible for
 * a one-shot batch approval) and the individual set (everything that wants a
 * human look — unusual / low-confidence items AND every gated item). Gated
 * items are ALWAYS individual: the predicate excludes them, so they can never
 * leak into the batch, and applyBatch would reject them anyway.
 *
 * Pure read helper — no writes, no I/O. Bulk triage uses it to decide what to
 * offer as a batch vs. one by one; the operator still approves the batch.
 * @param {Array} items - pending inbox items (e.g. from listPending())
 * @returns {{signoff: Array, individual: Array}}
 */
export function partitionForBatchSignoff(items) {
  const signoff = [];
  const individual = [];
  for (const item of Array.isArray(items) ? items : []) {
    if (isHighConfidenceSignoff(item)) signoff.push(item);
    else individual.push(item);
  }
  return { signoff, individual };
}

// --- Surfacing intelligence: fold proposals (act:00051dca) ---
//
// Ring 2's slow tier sweeps PENDING knowledge-extraction drafts and attaches
// an ADDITIVE evidence annotation (no new category, no schema change, and
// NEVER an auto-dismissal — the annotation demotes confidence so the item
// routes to `individual` in partitionForBatchSignoff; the human decides):
//
//   evidence.possible_duplicate_of = ['dec-...']  (sorted; reciprocal)
//     A fold proposal: another PENDING draft in the same project words the
//     same lesson (unigram Jaccard >= FOLD_SIMILARITY_THRESHOLD). Both sides
//     of a surfaced pair are annotated (reciprocity is the self-validating
//     criterion). The annotation persists after its partner resolves — a
//     draft whose twin was already dispositioned still wants a human look,
//     not a batch sign-off.
//
//     STRICTLY PAIRWISE — NEVER CHAIN THESE (act:471941c9). This field is a
//     claim about ONE pair. Similarity is not transitive: taking the
//     transitive closure of these flags on 2026-07-30 produced a single
//     70-item "cluster" of entirely unrelated drafts. If you want a group,
//     read `evidence.duplicate_cluster` — computed once at detection time
//     with size and density bounds, and explicitly null when the component
//     is a hub artifact rather than a real group.
//
//   evidence.duplicate_cluster = {id, size, members} | null
//   evidence.duplicate_cluster_rejected = 'oversized' | 'chained'   (when null)
//     The cohesive-group view of the same pairs — see buildFoldClusters.
//
// The sibling `evidence.freshness` annotation (a draft citing a now-closed
// act: fid, demoted as "possibly overtaken") was RETIRED (act:471dd701, N2
// of grp:retro-remeaning): measured against the 2026-07-14 read-pass answer
// key, it fired on 62-of-66 TRUE drafts and 70-of-84 flags fired on actions
// that closed BEFORE the draft was even filed — false by construction, it
// caught less than chance. `extractCitedActFids` (its sole caller) was
// deleted alongside it.
//
// This apparatus is the deliberate SIBLING of ring3-close's dedup tokenizer
// (STOPWORDS/meaningfulTokens/OVERLAP_THRESHOLD): same concept ("do these two
// short texts describe the same thing"), different corpus and metric. Lane
// boundaries forced the fork (ring3-close and watchtower-lib are other lanes'
// files); ring3-close already imports from this module, so a later
// consolidation can flow ring3 → queue.

// The fold recipe — calibrated 2026-07-12 against the live pairs that a full
// human read of 510 pending drafts surfaced as real fold candidates:
//   Boolean#cast trio  dec-5f850429 / dec-b2dc5070 / dec-f3446e84
//     (hub pairs 0.246 and 0.311 surface; the third pair measures 0.218 —
//     just under — so the trio connects through its hub, and reciprocal
//     annotation still marks all three)
//   testimonials pair  dec-eb6dcdb4 / dec-dd944a5b  (0.600)
// with a noise ceiling of 0.12 across random unrelated drafts. Tokens are
// lowercase alphanumeric RUNS ("Boolean#cast" → boolean, cast) —
// compound-preserving tokenization scored the trio at 0.13–0.18 and missed
// it. Exact per-pair figures are asserted in draft-surfacing.test.mjs F1.
export const FOLD_SIMILARITY_THRESHOLD = 0.22;

// Short-text floor (mirrors ring3-close's SHORT-TITLE FLOOR): a draft that
// can't muster this many meaningful tokens never proposes a fold — two
// near-empty token sets trivially score high on shared boilerplate.
export const FOLD_MIN_TOKENS = 3;

const FOLD_STOPWORDS = new Set(('a about after all also an and any are as at be because been before '
  + 'being but by can could did do does done for from had has have how i if in into is it its just '
  + 'like may more most much my new no not now of on one only or other our out over so some than '
  + 'that the then there these they this to too two under up use used using very via was we were '
  + 'what when where which while who why will with within would you your lesson').split(' '));

/**
 * Tokenize text for the fold pass: lowercase alphanumeric runs, length >= 3,
 * stopworded. Returns a Set.
 * @param {string} text
 * @returns {Set<string>}
 */
export function foldTokens(text) {
  const tokens = new Set();
  if (typeof text !== 'string' || !text) return tokens;
  for (const w of text.toLowerCase().match(/[a-z0-9]+/g) || []) {
    if (w.length >= 3 && !FOLD_STOPWORDS.has(w)) tokens.add(w);
  }
  return tokens;
}

/**
 * Unigram Jaccard over two token Sets. Empty sets never match (0, never NaN).
 * Kept for callers that want it directly; proposeFolds no longer uses it as
 * of act:421a8ab2 — see overlapCoefficient below.
 * @param {Set<string>} a
 * @param {Set<string>} b
 * @returns {number}
 */
export function unigramJaccard(a, b) {
  if (!(a instanceof Set) || !(b instanceof Set) || a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  for (const t of a) if (b.has(t)) inter++;
  const union = a.size + b.size - inter;
  return union === 0 ? 0 : inter / union;
}

/**
 * Overlap coefficient over two token Sets: intersection / min(|a|, |b|).
 * The fold retrieval metric as of act:421a8ab2 (N5 of grp:retro-remeaning) —
 * replaces unigramJaccard, whose union denominator punishes LENGTH
 * ASYMMETRY: two genuine duplicate drafts phrased as cause vs. effect (one
 * short, one long) share most of the short side's vocabulary but score low
 * under Jaccard because the long side's extra tokens inflate the union. The
 * live specimen this fixes: a JSONB read-merge-write pair scored 0.109 under
 * Jaccard (below the 0.22 threshold, invisible to the fold pass) and ranks
 * 19th of 1081 under the overlap coefficient over the same corpus. Empty
 * sets never match (0, never NaN) — same contract as unigramJaccard.
 * @param {Set<string>} a
 * @param {Set<string>} b
 * @returns {number}
 */
export function overlapCoefficient(a, b) {
  if (!(a instanceof Set) || !(b instanceof Set) || a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  for (const t of a) if (b.has(t)) inter++;
  const denom = Math.min(a.size, b.size);
  return denom === 0 ? 0 : inter / denom;
}

/**
 * Propose fold candidates over a set of items (pending drafts, or memory
 * files shaped the same way — {id, title, draft_artifact}). Pure — no I/O,
 * no writes.
 *
 * DUAL-SCORED as of act:421a8ab2 (N5): title-alone AND title+artifact are
 * each scored independently via overlapCoefficient, and a pair proposes on
 * whichever clears `threshold` (the max of the two, reported). Combined
 * scoring alone can miss a real duplicate whose BODIES differ more than
 * their titles (the live Meta specimen: 0.25 on titles alone, above
 * threshold, but diluted when the differing bodies are folded in) — titles
 * are short and specific, so they carry the strongest signal on their own.
 * Each score is independently floor-gated by FOLD_MIN_TOKENS: a title too
 * short to score never contributes a false floor-cleared title score, and
 * vice versa for the combined text.
 * @param {Array} items
 * @param {object} [opts]
 * @param {number} [opts.threshold]
 * @returns {Array<{a: string, b: string, similarity: number}>}
 */
export function proposeFolds(items, { threshold = FOLD_SIMILARITY_THRESHOLD } = {}) {
  const eligible = [];
  for (const item of Array.isArray(items) ? items : []) {
    if (!item || typeof item.id !== 'string' || !item.id) continue;
    const titleTokens = foldTokens(item.title || '');
    const combinedTokens = foldTokens(`${item.title || ''}\n${item.draft_artifact || ''}`);
    if (titleTokens.size < FOLD_MIN_TOKENS && combinedTokens.size < FOLD_MIN_TOKENS) continue;
    eligible.push({ id: item.id, titleTokens, combinedTokens });
  }
  const pairs = [];
  for (let i = 0; i < eligible.length; i++) {
    for (let j = i + 1; j < eligible.length; j++) {
      const x = eligible[i];
      const y = eligible[j];
      let similarity = 0;
      if (x.combinedTokens.size >= FOLD_MIN_TOKENS && y.combinedTokens.size >= FOLD_MIN_TOKENS) {
        similarity = Math.max(similarity, overlapCoefficient(x.combinedTokens, y.combinedTokens));
      }
      if (x.titleTokens.size >= FOLD_MIN_TOKENS && y.titleTokens.size >= FOLD_MIN_TOKENS) {
        similarity = Math.max(similarity, overlapCoefficient(x.titleTokens, y.titleTokens));
      }
      if (similarity >= threshold) {
        pairs.push({ a: x.id, b: y.id, similarity });
      }
    }
  }
  return pairs;
}

// --- Cluster-safe duplicate flags (act:471941c9, part 3) ---
//
// `evidence.possible_duplicate_of` is STRICTLY PAIRWISE. It says "this draft
// and that draft look alike"; it does NOT say "these drafts form a group", and
// the two are not the same claim. On 2026-07-30 a consumer took the transitive
// closure of 75 pairwise flags and got a single 70-item "cluster" whose members
// were about a stub-Notion harness, token metering, a count-check design, and
// batch verification — entirely unrelated. Every individual pair was
// defensible. Chained, they were meaningless, because similarity is not
// transitive and a few hub drafts connect everything to everything.
//
// So the cluster is computed ONCE, here, at detection time — and a component
// only becomes a cluster if it is actually cohesive:
//
//   - SIZE BOUND. A component above FOLD_CLUSTER_MAX_SIZE is a hub artifact,
//     not a group a human can act on in one decision.
//   - DENSITY BOUND. A component must be a near-clique: at least
//     FOLD_CLUSTER_MIN_DENSITY of its possible member pairs must ACTUALLY have
//     been proposed. A chain (a–b, b–c, c–d …) has density approaching zero as
//     it grows, which is exactly how the 70-item blob formed, so a chain can
//     never present itself as a cluster.
//
// A rejected component still keeps its pairwise flags — nothing is lost, the
// item still demotes out of batch sign-off. It simply carries an explicit
// `duplicate_cluster: null` with a reason, so a consumer reading the field sees
// "no cluster here" rather than being tempted to build one.
export const FOLD_CLUSTER_MAX_SIZE = 6;
export const FOLD_CLUSTER_MIN_DENSITY = 0.6;

/**
 * Group fold pairs into cohesive clusters. Pure — no I/O.
 *
 * Returns a Map from item id to a cluster annotation:
 *   { cluster: {id, size, members} }            — cohesive component
 *   { cluster: null, rejected: 'oversized' }    — component beyond the size bound
 *   { cluster: null, rejected: 'chained' }      — component below the density bound
 *
 * The cluster id is derived from sorted membership, so the same group of drafts
 * always produces the same id across sweeps without any stored counter.
 * @param {Array<{a: string, b: string}>} pairs
 * @param {object} [opts]
 * @returns {Map<string, {cluster: object|null, rejected?: string}>}
 */
export function buildFoldClusters(pairs, {
  maxSize = FOLD_CLUSTER_MAX_SIZE,
  minDensity = FOLD_CLUSTER_MIN_DENSITY,
} = {}) {
  const out = new Map();
  const adjacency = new Map();
  const pairKeys = new Set();
  for (const p of Array.isArray(pairs) ? pairs : []) {
    if (!p || typeof p.a !== 'string' || typeof p.b !== 'string' || p.a === p.b) continue;
    if (!adjacency.has(p.a)) adjacency.set(p.a, new Set());
    if (!adjacency.has(p.b)) adjacency.set(p.b, new Set());
    adjacency.get(p.a).add(p.b);
    adjacency.get(p.b).add(p.a);
    pairKeys.add([p.a, p.b].sort().join('|'));
  }

  const seen = new Set();
  for (const start of adjacency.keys()) {
    if (seen.has(start)) continue;
    // Connected component by breadth-first walk — the SAME transitive closure
    // the consumer took by hand. Computing it here is what lets us judge it.
    const component = [];
    const queue = [start];
    seen.add(start);
    while (queue.length) {
      const node = queue.shift();
      component.push(node);
      for (const next of adjacency.get(node) || []) {
        if (seen.has(next)) continue;
        seen.add(next);
        queue.push(next);
      }
    }
    const members = component.sort();
    if (members.length > maxSize) {
      for (const id of members) out.set(id, { cluster: null, rejected: 'oversized' });
      continue;
    }
    const possible = (members.length * (members.length - 1)) / 2;
    let actual = 0;
    for (let i = 0; i < members.length; i++) {
      for (let j = i + 1; j < members.length; j++) {
        if (pairKeys.has([members[i], members[j]].sort().join('|'))) actual++;
      }
    }
    const density = possible === 0 ? 0 : actual / possible;
    if (density < minDensity) {
      for (const id of members) out.set(id, { cluster: null, rejected: 'chained' });
      continue;
    }
    const cluster = {
      id: `fold:${members[0]}+${members.length}`,
      size: members.length,
      members,
    };
    for (const id of members) out.set(id, { cluster });
  }
  return out;
}

/**
 * Merge ADDITIVE annotation fields into a PENDING item's evidence.
 * The write discipline (every clause is load-bearing):
 *   - fresh read immediately before the write — the pending fence is checked
 *     at WRITE time, so a sweep racing an operator disposition can never
 *     resurrect a resolved item from a stale in-memory copy;
 *   - only the named evidence fields are touched, on the FRESH copy
 *     (schema_version and every other field ride through unchanged);
 *   - deep-equal values skip the write entirely (idempotent re-sweeps).
 * Returns null when the item is missing or no longer pending (skip, not an
 * error), else { item, changed }.
 * @param {string} id
 * @param {object} patch - evidence fields to set (values replace wholesale)
 * @returns {{item: object, changed: boolean}|null}
 */
export function annotateItemEvidence(id, patch) {
  const fp = itemPath(id);
  if (!existsSync(fp)) return null;
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  // The merge VERIFICATION RECORD is not annotatable on a qa-handoff — an
  // evidence patch that could flip merge_state, rewrite the verified sha,
  // falsify the ancestry stamp, or pre-plant a 'dispatched' status would
  // bypass every createItem guard (or make markHandoffMerged short-circuit
  // without verifying). The one legal doorway is markHandoffMerged (same
  // ancestry verification + the deferred stage-2 dispatch). Fenced to
  // qa-handoff, so Ring 2's freshness/fold sweeps on other categories are
  // untouched. (act:3d1ac2b7)
  const QA_VERIFICATION_KEYS = ['merge_state', 'merged_commit', 'ancestry_verified', 'stage2_dispatch'];
  if (item.category === QA_CATEGORY && patch) {
    const hit = QA_VERIFICATION_KEYS.find((k) => Object.prototype.hasOwnProperty.call(patch, k));
    if (hit) {
      throw new Error(`annotateItemEvidence: ${hit} on a qa-handoff is part of the merge verification record and is not annotatable — use markHandoffMerged(id, { merged_commit }) so the merge claim is verified and the deferred stage-2 dispatch fires.`);
    }
  }
  const evidence = { ...(item.evidence || {}) };
  let changed = false;
  for (const [key, value] of Object.entries(patch || {})) {
    if (JSON.stringify(evidence[key]) !== JSON.stringify(value)) {
      evidence[key] = value;
      changed = true;
    }
  }
  if (!changed) return { item, changed: false };
  item.evidence = evidence;
  atomicWrite(fp, item);
  return { item, changed: true };
}

// Non-throwing shape check for a mux desk-dir token — same discipline as
// assertLegalItemId (the desk name becomes a path component under MUX_QA_DIR,
// so a slash/dot-dot token must never reach the join).
function isLegalPathToken(t) {
  return typeof t === 'string' && !!t && !t.startsWith('.')
    && !t.includes('/') && !t.includes('\\') && !t.includes('..');
}

// The stage-2 pickup prompt for a handoff whose merge landed AFTER filing.
// Wording keeps parity with the qa-handoff skill's Step 6 merged prompt (the
// skill's inline heredoc is the interactive spelling; this is the library
// spelling for the deferred-dispatch path). Exported for tests.
export function buildQaPickupPrompt(item, mergedCommit) {
  const short = String(mergedCommit).slice(0, 7);
  return `Post-merge QA: ${item.title} — merged to main at ${short}. `
    + `Read qa-handoff inbox item ${item.id} (run /inbox), then run the recipient gate `
    + `(the qa-handoff skill's 'The recipient gate' section) at the tier the handoff's fields select — `
    + `re-validate risk_surface against the merged file list. Resolve only with a stamped qa_verdict `
    + `(resolveItem rejects an incomplete shape).`;
}

// Write the stage-2 mux dispatch descriptor for an item directly into the mux
// dispatch queue dir — fs-only on purpose: this runs from whatever context
// called markHandoffMerged (possibly cron), where mux/tmux/python3 may be off
// PATH (the routines library shells mux and degrades; here the enqueue file IS
// the contract — `mux qa drain` reads the desk dir, and mux reconciles the ·N
// badge against it on every `mux qa` verb, on desk open, and on every window
// switch; see clearDispatchEntries for why "on mux's next mutation" was not
// good enough). Never throws; returns a status the caller
// records in evidence so a failed dispatch re-triggers by STATE on the next
// markHandoffMerged call, not lost to a write-skip:
//   'dispatched'        — descriptor written (or already queued: idempotent)
//   'already-in-flight' — a drain already holds it; do not double-offer
//   'no-desk'           — item carries no legal desk token to route to
//   'failed: <why>'     — write failed; re-callable
function writeStage2Dispatch(item, mergedCommit) {
  try {
    const desk = item.desk;
    if (!isLegalPathToken(desk)) return 'no-desk';
    // item.id was read from disk, not from a caller — a hand-edited queue
    // file must not become a path escape (same discipline as assertLegalItemId).
    if (!isLegalPathToken(item.id)) return 'failed: illegal item id';
    const deskDir = join(MUX_QA_DIR, desk);
    if (existsSync(join(deskDir, 'in-flight', `${item.id}.json`))) return 'already-in-flight';
    ensureDir(deskDir);
    atomicWrite(join(deskDir, `${item.id}.json`), {
      project: desk,
      project_path: item.project_path,
      item_id: item.id,
      merged_commit: mergedCommit,
      what: item.title,
      pickup_prompt: buildQaPickupPrompt(item, mergedCommit),
    });
    return 'dispatched';
  } catch (err) {
    return `failed: ${err && err.message ? err.message.slice(0, 80) : 'unknown'}`;
  }
}

/**
 * The ONE legal merge-pending → merged transition for a qa-handoff item
 * (act:3d1ac2b7). Verifies the merge claim exactly as createItem does
 * (sha-shaped merged_commit; git ancestry — reject on positive refutation,
 * visible 'unverifiable' stamp when git can't answer), flips
 * evidence.merge_state, and performs the stage-2 mux dispatch that
 * merge-pending deferred. The dispatch outcome is recorded in
 * evidence.stage2_dispatch {status, at}; calling again on an already-merged
 * item retries ONLY a non-dispatched dispatch (state-keyed, so a mux hiccup
 * is recoverable and a success never double-injects).
 * annotateItemEvidence refuses merge_state patches on qa-handoffs, making
 * this the only doorway. Named callers: the qa-handoff drain playbook's
 * merge-then-QA step and the amend-after-merge path (both in the qa-handoff
 * skill text).
 * @param {string} id
 * @param {{merged_commit: string}} params
 * @returns {{item: object, dispatch: string}|null} null when missing/not pending
 */
export function markHandoffMerged(id, { merged_commit } = {}) {
  const fp = itemPath(id);
  if (!existsSync(fp)) return null;
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (item.category !== QA_CATEGORY) {
    throw new Error(`markHandoffMerged: ${id} is category '${item.category}', not qa-handoff`);
  }
  const sha = typeof merged_commit === 'string' ? merged_commit.trim() : merged_commit;
  if (typeof sha !== 'string' || !SHA_RE.test(sha)) {
    throw new Error(`markHandoffMerged: requires a sha-shaped merged_commit (got ${JSON.stringify(merged_commit ?? null)}) — the post-merge sha on main.`);
  }
  const evidence = { ...(item.evidence || {}) };
  const already = normalizeMergeState(evidence.merge_state) === 'merged'
    && evidence.stage2_dispatch && evidence.stage2_dispatch.status === 'dispatched';
  if (already) {
    // Never silently swallow a DIFFERENT sha — an amend session correcting a
    // wrong recorded sha must hear the mismatch, not a success shape.
    if (typeof evidence.merged_commit === 'string' && evidence.merged_commit !== sha) {
      throw new Error(`markHandoffMerged: ${id} is already recorded merged+dispatched at ${evidence.merged_commit.slice(0, 12)} — refusing to silently ignore differing sha ${sha.slice(0, 12)}. If the recorded sha is wrong, that is a hand-repair with an operator, not a re-flip.`);
    }
    return { item, dispatch: 'dispatched' };
  }
  const v = verifyMergedAncestry(item.project_path, sha);
  if (v.state === 'refuted') {
    throw new Error(`markHandoffMerged: ${sha.slice(0, 12)} is NOT an ancestor of ${v.ref} in ${item.project_path} — the merge hasn't landed on the main ref (did the push happen? or your local ${v.ref} may be behind: fetch and retry). Item left unchanged.`);
  }
  const dispatch = writeStage2Dispatch(item, sha);
  item.evidence = {
    ...evidence,
    merge_state: 'merged',
    merged_commit: sha,
    ancestry_verified: v.state === 'verified' ? 'verified' : 'unverifiable',
    stage2_dispatch: { status: dispatch, at: new Date().toISOString() },
  };
  atomicWrite(fp, item);
  return { item, dispatch };
}

// ---------------------------------------------------------------------------
// Shared identity helpers — defined HERE because watchtower-lib imports the
// queue and never the reverse (plan A9's placement rule), and the redirect
// writers below need both. watchtower-lib re-exports them unchanged, so every
// existing `import { slugify, resolveCcSourceRepo } from './watchtower-lib.mjs'`
// keeps working and the definition stays single.
// ---------------------------------------------------------------------------

// slugify — lowercase, replace non-alphanum with dash, trim dashes. The ONE
// slug algorithm: state/projects/<slug> keys, carry-ledger paths and the
// identity resolver all derive from it.
export function slugify(text) {
  return String(text).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

const CC_PACKAGE_NAME = 'create-claude-cabinet';

// resolveCcSourceRepo — the registered project whose package.json name is
// create-claude-cabinet. The one place "which checkout is the CC source?" is
// answered, so callers (feedback delivery, the Ring 1 runtime-drift check,
// the outbox dedup below) never re-derive it.
export function resolveCcSourceRepo(registryPath) {
  if (!registryPath || !existsSync(registryPath)) return null;
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

// ---------------------------------------------------------------------------
// The carry ledger — state/projects/<slug>/carry.json (Simplification
// Project 2, plan B3 / decision default 20; act:73dd31d4)
// ---------------------------------------------------------------------------
//
// ONE per-project carrier for what rides from one session to the next session
// of the same project: project friction (`kind: 'friction'`, shown every start
// block until resolved — EP2), loose ends (`'loose-end'` — a residue finding
// caught by createItem's redirect: unfinished work, not a tool complaint, with
// friction's shown-every-start-block-until-settled semantics and its own
// start-block label; operator ruling 2026-10-31, act:dafa9afa), advisor
// findings (`'advisor'`, shown once) and thread re-reading proposals
// (`'recolor'`, shown once; written by plan A8).
// The session record is the history; this file can say "settled" and
// "already shown", which a record cannot. Shape:
//   { entries: [{ id, kind, title, detail, source_session, created_at,
//                 shown: [{ session_id, at }],
//                 resolved: null | { outcome: 'fixed'|'filed'|'dropped', ref, reason, session_id, at } }] }
// fs-only writers (the markHandoffMerged precedent: a queue-side write into
// another component's directory), read-modify-atomicWrite with one retry on a
// changed mtime, never throw from the readers; the appender throws so
// createItem's redirect can fail toward the queue (visibility, never loss).

export function carryLedgerPath(slug) {
  return join(STATE_DIR, 'projects', slugify(slug), 'carry.json');
}

function readCarryFile(path) {
  try {
    if (!existsSync(path)) return { entries: [] };
    const parsed = JSON.parse(readFileSync(path, 'utf8'));
    if (!parsed || !Array.isArray(parsed.entries)) return { entries: [] };
    return parsed;
  } catch {
    return { entries: [] };
  }
}

function mtimeOf(path) {
  try { return statSync(path).mtimeMs; } catch { return null; }
}

// Read-modify-write with one retry when the file changed under us — the
// ledger is shared between Ring 3, the start-block builder, /close and a
// redirecting createItem in another process.
function updateCarryFile(path, mutate) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const before = mtimeOf(path);
    const ledger = readCarryFile(path);
    const result = mutate(ledger);
    if (mtimeOf(path) !== before && attempt === 0) continue;
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    atomicWrite(path, ledger);
    return result;
  }
  // Second attempt also raced — write anyway; the loser's entry is at worst
  // re-read by the next writer (the shape is append-mostly).
  const ledger = readCarryFile(path);
  const result = mutate(ledger);
  atomicWrite(path, ledger);
  return result;
}

/**
 * Append one entry to a project's carry ledger. Returns the entry written.
 * @param {string} slug - the project slug (or name; slugified)
 * @param {{kind: 'friction'|'loose-end'|'advisor'|'recolor', title: string, detail?: string, source_session?: string|null, source?: object}} entry
 */
export const CARRY_KINDS = ['friction', 'loose-end', 'advisor', 'recolor'];
// The kinds that ride every start block until a /close settles them (fixed /
// filed / dropped) — the others render once and are stamped `shown`.
export const CARRY_PERSISTENT_KINDS = ['friction', 'loose-end'];

export function appendCarryEntry(slug, { kind, title, detail = '', source_session = null, source = null, created_at = null } = {}) {
  if (!CARRY_KINDS.includes(kind)) {
    throw new Error(`appendCarryEntry: kind must be ${CARRY_KINDS.join(' | ')} (got ${JSON.stringify(kind)})`);
  }
  if (typeof title !== 'string' || !title.trim()) {
    throw new Error('appendCarryEntry: a non-empty title is required');
  }
  const entry = {
    id: 'carry-' + randomBytes(4).toString('hex'),
    kind,
    title: title.trim(),
    detail: typeof detail === 'string' ? detail : '',
    source_session: source_session || null,
    created_at: created_at || new Date().toISOString(),
    shown: [],
    resolved: null,
    ...(source ? { source } : {}),
  };
  updateCarryFile(carryLedgerPath(slug), (ledger) => { ledger.entries.push(entry); });
  return entry;
}

/** Read a project's carry ledger. A missing or corrupt file reads as empty. */
export function readCarry(slug) {
  return readCarryFile(carryLedgerPath(slug));
}

/**
 * Stamp `shown` on the given entries (advisor/recolor render-once bookkeeping;
 * the start block's one write). Fail-open: a failed stamp means the entry
 * shows again — duplication, never loss.
 * @returns {number} entries stamped
 */
export function markCarryShown(slug, ids, { session_id = null, at = null } = {}) {
  const want = new Set(Array.isArray(ids) ? ids : [ids]);
  try {
    return updateCarryFile(carryLedgerPath(slug), (ledger) => {
      let n = 0;
      for (const e of ledger.entries) {
        if (!want.has(e.id)) continue;
        if (!Array.isArray(e.shown)) e.shown = [];
        e.shown.push({ session_id, at: at || new Date().toISOString() });
        n++;
      }
      return n;
    });
  } catch {
    return 0;
  }
}

/**
 * Resolve a carry entry. Resolving an already-resolved entry is a no-op that
 * REPORTS it ({ ok: false, reason: 'already-resolved', entry }); an unknown id
 * reports 'not-found'. Never throws.
 * @param {{outcome: 'fixed'|'filed'|'dropped', ref?: string|null, reason?: string|null, session_id?: string|null}} resolution
 */
export function resolveCarryEntry(slug, id, { outcome, ref = null, reason = null, session_id = null, at = null } = {}) {
  if (!['fixed', 'filed', 'dropped'].includes(outcome)) {
    return { ok: false, reason: 'bad-outcome', entry: null };
  }
  try {
    return updateCarryFile(carryLedgerPath(slug), (ledger) => {
      const entry = ledger.entries.find((e) => e.id === id);
      if (!entry) return { ok: false, reason: 'not-found', entry: null };
      if (entry.resolved) return { ok: false, reason: 'already-resolved', entry };
      entry.resolved = { outcome, ref, reason, session_id, at: at || new Date().toISOString() };
      return { ok: true, entry };
    });
  } catch (e) {
    return { ok: false, reason: `write-failed: ${e.message}`, entry: null };
  }
}

// ---------------------------------------------------------------------------
// Thread re-reading proposals — state/threads/<slug>.json `proposed_recolors`
// (Simplification Project 2, plan A8 / EP1; act:b57df8e4)
// ---------------------------------------------------------------------------
//
// A significance proposal ("this session reframes / answers / contradicts the
// <date> entry of thread X") is written ONTO the thread it reinterprets, in
// the additive array `proposed_recolors`, instead of the inbox — shown by
// /threads and once in the next start block of the project (a `recolor`
// carry entry, written by the caller), until Project 3's contradiction gate
// decides which kind returns to the inbox. Old thread readers are unaffected
// (additive field); cursor_history and recolor_log are never touched.
//
// The durable relation_key exclusion moved here with the proposal. The key
// is thread|prior_session|verb — the ANCHOR being reinterpreted and how,
// never the noticing session (act:1be47d42). A key is refused when it is
// already APPLIED (in recolor_log — appendThreadRecolor's entries carry the
// anchor and verb), already a HISTORICAL queue item in any status (a
// decided or walked proposal never re-files), or already proposed — in
// which case a different noticing session is appended to also_noticed_by
// (corroboration strengthens the proposal; it is never a second entry).
// NOTE for the backlog walk (plan C1 route-thread): the queue check matches
// the pending item being walked; the walk must account for that.
//
// Queue-side and fs-only for the same reason as the carry ledger: createItem's
// 'thread' redirect needs it, and lib imports the queue, never the reverse;
// lib re-exports it, so Ring 3 and the walk call this one definition.

function threadKeyOf(slug, e) {
  return e && e.prior_session_id && e.verb ? `${slug}|${e.prior_session_id}|${e.verb}` : null;
}

/**
 * Append a re-reading proposal to a thread file's `proposed_recolors`.
 * Returns null when the thread file is missing, unreadable or the slug is not
 * a plain file name (never thrown — the caller logs); otherwise
 * `{ outcome, path, entry, display_name }` where outcome is
 *   'appended'     — a new proposal was written;
 *   'corroborated' — the key was already proposed; a new noticing session
 *                    was appended to also_noticed_by;
 *   'duplicate'    — already proposed and already noticed by this session;
 *   'excluded'     — the key is applied (recolor_log) or a historical queue
 *                    item (`reason` says which); nothing written.
 * Throws only on a proposal with no key, or a failed write (createItem's
 * redirect then falls back to the queue — visibility, never loss).
 *
 * `allowQueueItems` — ids of historical `significance-change` queue items
 * that may carry this key WITHOUT excluding the write. The backlog walk
 * (plan C1 `route-thread`) moves a pending queue item onto its thread, and
 * that item is itself the legacy record this exclusion would otherwise find
 * (carried from lane 6, act:2c133dc3); the walk passes the walked item's own
 * id. Any OTHER queue item carrying the key still excludes.
 */
export function appendThreadProposal(threadsDir, slug, proposal = {}, { allowQueueItems = [] } = {}) {
  if (typeof slug !== 'string' || !slug || slug.startsWith('.') || /[/\\]/.test(slug)) return null;
  const path = join(threadsDir, `${slug}.json`);
  if (!existsSync(path)) return null;
  let thread;
  try {
    thread = JSON.parse(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
  if (!thread || typeof thread !== 'object' || Array.isArray(thread)) return null;
  const key = (typeof proposal.relation_key === 'string' && proposal.relation_key)
    || threadKeyOf(slug, proposal);
  if (!key) throw new Error('appendThreadProposal: a relation_key (or prior_session_id + verb) is required');
  const display_name = thread.display_name || slug;

  if ((Array.isArray(thread.recolor_log) ? thread.recolor_log : []).some((e) => threadKeyOf(slug, e) === key)) {
    return { outcome: 'excluded', reason: 'applied', path, entry: null, display_name };
  }
  const allowed = new Set(Array.isArray(allowQueueItems) ? allowQueueItems : [allowQueueItems]);
  const legacy = listItems({ category: 'significance-change' })
    .find((i) => !allowed.has(i.id) && i.evidence && i.evidence.relation_key === key);
  if (legacy) {
    return { outcome: 'excluded', reason: 'queue', item_id: legacy.id, path, entry: null, display_name };
  }

  const list = Array.isArray(thread.proposed_recolors) ? thread.proposed_recolors : [];
  const existing = list.find((e) => e && e.relation_key === key);
  const sid = proposal.new_session_id || null;
  if (existing) {
    const also = Array.isArray(existing.also_noticed_by) ? existing.also_noticed_by : [];
    if (!sid || sid === existing.new_session_id || also.includes(sid)) {
      return { outcome: 'duplicate', path, entry: existing, display_name };
    }
    existing.also_noticed_by = [...also, sid];
    thread.proposed_recolors = list;
    atomicWrite(path, thread);
    return { outcome: 'corroborated', path, entry: existing, display_name };
  }

  const entry = {
    relation_key: key,
    verb: proposal.verb ?? null,
    prior_session_id: proposal.prior_session_id ?? null,
    prior_date: proposal.prior_date ?? null,
    prior_excerpt: typeof proposal.prior_excerpt === 'string' ? proposal.prior_excerpt : '',
    new_session_id: sid,
    explanation: typeof proposal.explanation === 'string' ? proposal.explanation : '',
    proposed_at: proposal.proposed_at || new Date().toISOString(),
    also_noticed_by: [],
  };
  thread.proposed_recolors = [...list, entry];
  atomicWrite(path, thread);
  return { outcome: 'appended', path, entry, display_name };
}

// ---------------------------------------------------------------------------
// Documentation-drift state — state/projects/<slug>/doc-drift.json
// (Simplification Project 2, plan A4 / decision default 18; act:dafa9afa)
// ---------------------------------------------------------------------------
//
// Ring 4 has no session, so its findings are PROJECT facts, not inbox items:
// one entry per claim, keyed by drift_key. Shape:
//   { entries: [{ drift_key, kind, doc, title, summary, filed_by,
//                 first_seen, last_seen, ...finding fields }] }
// Writers: Ring 4 (upsert each finding, then retract the claims it actually
// re-checked and no longer found) and createItem's 'doc-drift-state' redirect
// (a stray doc-drift filing from a consumer hook). Reader: Ring 1, one
// Standing Issues line naming the count and this path. Queue-side and
// fs-only for the same reason as the carry ledger (createItem needs it, and
// lib imports the queue, never the reverse); lib re-exports.

export function docDriftStatePath(slug) {
  return join(STATE_DIR, 'projects', slugify(slug), 'doc-drift.json');
}

/** Read a project's drift state. A missing or corrupt file reads as empty. */
export function readDocDrift(slug) {
  return readCarryFile(docDriftStatePath(slug));
}

/**
 * Upsert one drift claim by drift_key. A new key is appended with
 * first_seen = last_seen = now; a known key keeps first_seen and takes the
 * fresh fields. Throws on a failed write (createItem's redirect falls back to
 * the queue on a throw — visibility, never loss).
 * @returns {{added: boolean, entry: object}}
 */
export function upsertDocDriftEntry(slug, finding, { now = null } = {}) {
  if (!finding || typeof finding.drift_key !== 'string' || !finding.drift_key) {
    throw new Error('upsertDocDriftEntry: a drift_key is required');
  }
  const at = now ? new Date(now).toISOString() : new Date().toISOString();
  return updateCarryFile(docDriftStatePath(slug), (state) => {
    const i = state.entries.findIndex((e) => e && e.drift_key === finding.drift_key);
    if (i === -1) {
      const entry = { ...finding, first_seen: at, last_seen: at };
      state.entries.push(entry);
      return { added: true, entry };
    }
    const entry = { ...state.entries[i], ...finding, first_seen: state.entries[i].first_seen || at, last_seen: at };
    state.entries[i] = entry;
    return { added: false, entry };
  });
}

/**
 * Remove the claims a pass actually RE-CHECKED and no longer found: every
 * entry whose drift_key is in `checkedKeys` and not in `foundKeys`. An entry
 * the pass never checked (its document was not collected, its project was not
 * selected, or a consumer hook wrote it under a key Ring 4 does not mint)
 * stays — absence of evidence is not evidence a claim cleared.
 * @returns {number} entries removed
 */
export function retractDocDriftEntries(slug, { checkedKeys, foundKeys } = {}) {
  const checked = checkedKeys instanceof Set ? checkedKeys : new Set(checkedKeys || []);
  const found = foundKeys instanceof Set ? foundKeys : new Set(foundKeys || []);
  const path = docDriftStatePath(slug);
  if (!existsSync(path)) return 0;
  return updateCarryFile(path, (state) => {
    const before = state.entries.length;
    state.entries = state.entries.filter((e) => !(e && checked.has(e.drift_key) && !found.has(e.drift_key)));
    return before - state.entries.length;
  });
}

// ---------------------------------------------------------------------------
// The feedback outbox writer — ~/.claude/cc-feedback-outbox.json (plan A9/B4,
// Q1, T8). The shape is /cc-feedback's (templates/skills/cc-feedback/SKILL.md
// §3): { source, date, component, title, body, status: 'pending',
// delivered: false }. Ring 1's flushFeedbackOutbox (watchtower-lib) delivers
// it to the CC source repo's feedback/ root unchanged; its slug check stays
// as a second guard. B4's routeFriction calls this — one definition.
// ---------------------------------------------------------------------------

export function feedbackOutboxPath() {
  return join(process.env.HOME || '', '.claude', 'cc-feedback-outbox.json');
}
function ccRegistryPath() {
  return join(process.env.HOME || '', '.claude', 'cc-registry.json');
}

// Duplicates are JUDGED, never guessed (act:08aca7a9). Lane 5 shipped a
// lexical suppressor here: overlapCoefficient over foldTokens of the report
// against the WHOLE text of every pending outbox entry and every feedback/ and
// feedback/resolved/ file, at the fold threshold 0.22. min() puts the short
// report in the denominator, and a long feedback file holds most ordinary
// words, so on its first live run (2026-10-31) it held back 4 of 4 real
// reports as "duplicates" of unrelated April files — a nonsense sentence
// "matched" 2 of 285. That is the landmine this repo already names for the
// fold threshold: calibrated for ADVISORY retrieval; a lexical matcher may
// propose, never silently suppress. So the writer below suppresses only an
// IDENTICAL pending title (reprocess idempotence), and similarity is a
// SHORTLIST for a model to judge (Ring 3's judgeFeedbackDuplicates). A caller
// with no model — createItem's redirect — delivers, and triage-at-arrival
// judges.

const normTitle = (t) => String(t || '').toLowerCase().replace(/\s+/g, ' ').trim();

function titleOfFeedbackFile(name, content) {
  const h = /^#\s+(.+)$/m.exec(content || '');
  if (h) return h[1].trim();
  return name.replace(/\.md$/, '').replace(/^\d{4}-\d{2}-\d{2}-/, '').replace(/[-_]+/g, ' ').trim();
}

function jaccard(a, b) {
  if (!a.size || !b.size) return { score: 0, shared: 0 };
  let shared = 0;
  for (const t of a) if (b.has(t)) shared++;
  return { score: shared / (a.size + b.size - shared), shared };
}

export const FEEDBACK_SHORTLIST_MIN_JACCARD = 0.2;
export const FEEDBACK_SHORTLIST_MIN_SHARED = 2;

/**
 * The SHORTLIST for a duplicate judgment: pending outbox entries and the CC
 * source repo's feedback/ + feedback/resolved/ files whose TITLE overlaps the
 * report's title (symmetric Jaccard over foldTokens, at least two shared
 * tokens). Bodies are never compared — a long file sharing ordinary words is
 * exactly the false match this replaced. Proposes only; never suppresses.
 * @returns {Array<{ref: string, title: string, excerpt: string, score: number}>}
 */
export function findFeedbackDuplicateCandidates({ title } = {},
  { limit = 5, outboxPath = feedbackOutboxPath(), registryPath = ccRegistryPath() } = {}) {
  const want = foldTokens(String(title || ''));
  if (want.size < 2) return [];
  const out = [];
  const consider = (ref, candTitle, excerpt) => {
    const { score, shared } = jaccard(want, foldTokens(candTitle));
    if (score >= FEEDBACK_SHORTLIST_MIN_JACCARD && shared >= FEEDBACK_SHORTLIST_MIN_SHARED) {
      out.push({ ref, title: candTitle, excerpt: String(excerpt || '').replace(/\s+/g, ' ').trim().slice(0, 300), score });
    }
  };
  try {
    if (existsSync(outboxPath)) {
      const parsed = JSON.parse(readFileSync(outboxPath, 'utf8'));
      if (Array.isArray(parsed)) parsed.forEach((o, i) => {
        if (o && o.delivered !== true && o.title) consider(`outbox[${i}] ${o.title}`, o.title, o.body);
      });
    }
  } catch { /* an unreadable outbox shortlists nothing; the writer reports it */ }
  const ccRepo = resolveCcSourceRepo(registryPath);
  if (ccRepo) {
    for (const dir of [join(ccRepo, 'feedback'), join(ccRepo, 'feedback', 'resolved')]) {
      if (!existsSync(dir)) continue;
      let names = [];
      try { names = readdirSync(dir).filter((n) => n.endsWith('.md')); } catch { continue; }
      for (const name of names) {
        let content = '';
        try { content = readFileSync(join(dir, name), 'utf8'); } catch { continue; }
        const t = titleOfFeedbackFile(name, content);
        consider(join(dir, name), t, content.replace(/^#.*$/m, ''));
      }
    }
  }
  return out.sort((a, b) => b.score - a.score).slice(0, limit);
}

/**
 * Append one entry to the feedback outbox. Never suppresses on similarity
 * (act:08aca7a9): it skips only when a PENDING entry carries the identical
 * title (a reprocessed close must not send twice), or when the caller passes
 * `duplicateOf` — a duplicate a model already judged. Read-modify-atomicWrite
 * with one retry on a changed mtime — the outbox is shared with /cc-feedback.
 * @returns {{appended: boolean, path: string, index?: number, duplicate_of?: string}}
 */
export function appendFeedbackOutboxEntry({ source, date = null, component = '', title, body = '' } = {},
  { outboxPath = feedbackOutboxPath(), duplicateOf = null } = {}) {
  if (typeof title !== 'string' || !title.trim()) {
    throw new Error('appendFeedbackOutboxEntry: a non-empty title is required');
  }
  if (duplicateOf) return { appended: false, path: outboxPath, duplicate_of: String(duplicateOf) };
  const entry = {
    source: source || 'unknown',
    date: date || new Date().toISOString().slice(0, 10),
    component: component || '',
    title: title.trim(),
    body: String(body || ''),
    status: 'pending',
    delivered: false,
  };
  const want = normTitle(entry.title);

  for (let attempt = 0; attempt < 2; attempt++) {
    const before = mtimeOf(outboxPath);
    let outbox = [];
    try {
      if (existsSync(outboxPath)) {
        const parsed = JSON.parse(readFileSync(outboxPath, 'utf8'));
        if (Array.isArray(parsed)) outbox = parsed;
      }
    } catch { outbox = []; }
    for (let i = 0; i < outbox.length; i++) {
      const o = outbox[i];
      if (!o || o.delivered === true) continue;
      if (normTitle(o.title) === want) {
        return { appended: false, path: outboxPath, duplicate_of: `outbox[${i}] ${o.title || ''}`.trim() };
      }
    }
    if (mtimeOf(outboxPath) !== before && attempt === 0) continue;
    outbox.push(entry);
    const dir = dirname(outboxPath);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    atomicWrite(outboxPath, outbox);
    return { appended: true, path: outboxPath, index: outbox.length - 1 };
  }
  throw new Error('appendFeedbackOutboxEntry: the outbox changed twice during the write');
}

/**
 * The outbox entry for a cc-feedback FRICTION report (plan B4): `body` = the
 * summary + the route detail + the session id (+ who filed it). The one
 * composition, shared by Ring 3's routeFriction and createItem's 'outbox'
 * redirect, so a report reads the same whichever door it came through. Pass
 * the result to appendFeedbackOutboxEntry, which owns the seven-field shape.
 */
export function frictionOutboxEntry({ project, title, summary = '', route = {}, sessionId = null, filedBy = 'ring3-close', component = null } = {}) {
  const r = route || {};
  const lines = [String(summary || '').trim()];
  if (!/^Route:/m.test(lines[0])) lines.push('', `Route: ${r.type || 'cc-feedback'} — ${r.detail || ''}`.trim());
  lines.push(`Session: ${sessionId || 'unknown'}`, `Filed by: ${filedBy}`);
  return { source: project, component: component || r.detail || '', title, body: lines.join('\n') };
}

// ---------------------------------------------------------------------------
// The redirect ledger — state/inbox-redirects.jsonl. Every redirect and every
// refusal appends { ts, category, project, filed_by, to | refused } so the
// PRODUCER gets fixed (B6's standing line counts redirects per producer over
// 28 days; the C1 walk's --assert-quiet refuses on any since the reinstall).
// Fail-open: a dropped ledger line never blocks the redirect itself.
// ---------------------------------------------------------------------------

export function redirectLedgerPath() {
  return join(STATE_DIR, 'inbox-redirects.jsonl');
}

function appendRedirectLedger(record) {
  try {
    const p = redirectLedgerPath();
    if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
    appendFileSync(p, JSON.stringify({ ts: new Date().toISOString(), ...record }) + '\n');
  } catch { /* observability only */ }
}

// Write a would-be inbox item into its live home. Returns { to, path, ... }.
// Throws on a failed write so createItem can fall back to the queue.
function writeToHome(home, { project, project_path, category, title, summary, evidence, filed_by, urgency }) {
  const sessionId = evidence && typeof evidence.session_id === 'string' ? evidence.session_id : null;
  if (home === 'carry') {
    // A non-seat advisor finding rides as 'advisor' (shown once, Decision 4);
    // project friction (EP2) rides as 'friction'; a residue finding filed the
    // old way (the five retired residue categories) rides as 'loose-end' —
    // unfinished work, not a tool complaint (operator ruling 2026-10-31,
    // act:dafa9afa). friction and loose-end are the two kinds the /close
    // residue question reads and settles (fixed / file as action / drop).
    // The origin is kept on `source` so nothing is lost in the relabel.
    const kind = category === 'advisor-finding' ? 'advisor'
      : category === 'upstream-friction' ? 'friction'
        : 'loose-end';
    const entry = appendCarryEntry(project, {
      kind, title, detail: summary, source_session: sessionId,
      source: {
        category, filed_by, project_path: project_path || null, urgency: urgency || null,
        ...(evidence && evidence.member ? { member: evidence.member } : {}),
        ...(evidence && evidence.directive_key ? { directive_key: evidence.directive_key } : {}),
      },
    });
    return { to: 'carry', path: carryLedgerPath(project), carry_id: entry.id };
  }
  if (home === 'doc-drift-state') {
    // A stray doc-drift filing (a consumer hook — flow's pulse hook files with
    // no drift_key at all). Ring 4 mints `path:<doc>` / `count:<doc>:<label>`;
    // a key derived from the title can never collide with those, so Ring 4's
    // retract-on-rescan never removes an entry it did not write.
    const driftKey = (evidence && typeof evidence.drift_key === 'string' && evidence.drift_key)
      || `filed:${String(title || '').trim()}`;
    const { entry } = upsertDocDriftEntry(project, {
      ...(evidence || {}), drift_key: driftKey, title, summary: summary || '', filed_by,
      project_path: project_path || null,
    });
    return { to: 'doc-drift-state', path: docDriftStatePath(project), drift_key: entry.drift_key };
  }
  if (home === 'outbox') {
    const r = appendFeedbackOutboxEntry(frictionOutboxEntry({
      project, title, summary, route: (evidence && evidence.route) || {}, sessionId, filedBy: filed_by,
      component: (evidence && evidence.route && evidence.route.detail) || category,
    }));
    return { to: 'outbox', path: r.path, ...(r.appended ? { index: r.index } : { duplicate_of: r.duplicate_of }) };
  }
  if (home === 'thread') {
    // A stray significance-change filing (an un-refreshed runtime's Ring 3)
    // carries the proposal in its evidence; it lands on the thread it
    // reinterprets. A missing thread throws, so the filing falls back to the
    // queue marked redirect_failed rather than vanishing.
    const slug = (evidence && evidence.thread) || null;
    const r = appendThreadProposal(join(STATE_DIR, 'threads'), slug, {
      ...(evidence || {}), explanation: (evidence && evidence.explanation) || summary || '',
    });
    if (!r) throw new Error(`thread file for ${JSON.stringify(slug)} is missing or unreadable`);
    return { to: 'thread', path: r.path, outcome: r.outcome };
  }
  throw new Error(`createItem: unknown redirect home '${home}'`);
}

// --- Exports ---

/**
 * Create a new inbox item.
 *
 * A category that LEFT the inbox is not stored here (plan A9/EP4): a retired
 * category with a live home, and the narrowed slices of advisor-finding and
 * upstream-friction, are written to that home and the call returns
 * `{ redirected: true, to, path }` — never an id, never a throw. A retired
 * category with no live home throws. Every redirect and refusal is logged to
 * state/inbox-redirects.jsonl. If the redirect write itself fails, the item
 * is filed into the queue as before with evidence.redirect_failed — fail
 * toward visibility, never loss.
 * @param {object} params
 * @returns {string|{redirected: true, to: string, path: string}} The generated item id, or the redirect record
 */
export function createItem({
  project,
  project_path,
  category,
  urgency = 'normal',
  title,
  summary,
  context_anchor,
  evidence = {},
  options = [],
  draft_artifact = null,
  transcript_ref = null,
  filed_by = 'manual',
  plan_fid = null,
  thread_ids = [],
  confidence = null,
  project_unresolved = false,
  desk = null,
}) {
  // Retired / narrowed categories: redirect to the live home, or refuse a
  // category with no home (plan A9 / EP4). Before any other boundary check —
  // a filing that will not enter the queue is not validated as a queue item.
  let home;
  try {
    home = redirectHomeFor(category, evidence);
  } catch (e) {
    appendRedirectLedger({ category, project, filed_by, title, refused: true, reason: e.message });
    throw e;
  }
  if (home) {
    try {
      const r = writeToHome(home, { project, project_path, category, title, summary, evidence, filed_by, urgency });
      // The title rides on every line (act:08aca7a9): a report held back as a
      // duplicate leaves no queue item and no record line, so this line is the
      // only trace of what it said.
      appendRedirectLedger({ category, project, filed_by, title, to: home, ...(r.duplicate_of ? { duplicate: true, duplicate_of: r.duplicate_of } : {}) });
      return { redirected: true, ...r };
    } catch (e) {
      // Fail toward visibility: the finding lands in the queue, marked.
      evidence = { ...(evidence || {}), redirect_failed: `${home}: ${e.message}` };
      appendRedirectLedger({ category, project, filed_by, title, to: home, failed: e.message });
    }
  }
  ensureDir(QUEUE_DIR);
  // qa-handoff merge_state is structural: a present-but-illegal token is
  // rejected at the boundary (it can't silently read as 'merged'), and a legal
  // one is canonicalized. Absent stays absent — readers default to 'merged'
  // via normalizeMergeState (additive-field discipline, like desk/project_
  // unresolved). Only qa-handoff items carry it; other categories are untouched.
  if (category === QA_CATEGORY && evidence && evidence.merge_state !== undefined) {
    const ms = normalizeMergeState(evidence.merge_state);
    if (!ms) {
      throw new Error(`createItem: illegal qa-handoff merge_state ${JSON.stringify(evidence.merge_state)} — must be one of: ${QA_MERGE_STATES.join(' | ')}`);
    }
    evidence = { ...evidence, merge_state: ms };
  }
  // Knowledge-extraction four-axis boundary check (act:471dd701) — reject an
  // illegal type/home token at the boundary, as merge_state does above.
  if (category === KNOWLEDGE_CATEGORY && evidence) {
    if (evidence.type !== undefined && !KNOWLEDGE_EXTRACTION_TYPES.includes(evidence.type)) {
      throw new Error(`createItem: illegal knowledge-extraction type ${JSON.stringify(evidence.type)} — must be one of: ${KNOWLEDGE_EXTRACTION_TYPES.join(' | ')}`);
    }
    const isUnclassifiable = evidence.type === 'unclassifiable';
    if (isUnclassifiable && (typeof evidence.unclassifiable_reason !== 'string' || !evidence.unclassifiable_reason.trim())) {
      throw new Error("createItem: type 'unclassifiable' requires evidence.unclassifiable_reason (a one-line reason) — unclassifiable is a verdict, not a silent drop");
    }
    // home is not validated for an unclassifiable item — the model is not
    // asked to guess one when it can't confidently classify the item at all.
    if (!isUnclassifiable && evidence.home !== undefined && !KNOWLEDGE_EXTRACTION_HOMES.includes(evidence.home)) {
      throw new Error(`createItem: illegal knowledge-extraction home ${JSON.stringify(evidence.home)} — must be one of: ${KNOWLEDGE_EXTRACTION_HOMES.join(' | ')}`);
    }
    if (evidence.derivable === true && (typeof evidence.derivation !== 'string' || !evidence.derivation.trim())) {
      throw new Error('createItem: derivable:true requires evidence.derivation naming how to recompute it — a derivable fact is routed to its derivation, never filed as an opaque flag');
    }
    // The perishable twin (act:a69c21f8): a fact that is true at a moment and
    // will become false, with no live source to recompute it from. perishes_when
    // is to perishable what derivation is to derivable — the flag is never
    // filed opaque.
    if (evidence.perishable === true && (typeof evidence.perishes_when !== 'string' || !evidence.perishes_when.trim())) {
      throw new Error('createItem: perishable:true requires evidence.perishes_when naming what event or time makes it false — a perishable fact is filed as a dated snapshot, never as an opaque flag');
    }
  }
  // A 'merged' handoff must reference work actually on main. merge_state
  // replaced the old hand-flagged `merged_into: "PENDING — not yet merged"`
  // antipattern, but a producer can still free-text a not-yet-merged marker
  // into merged_into/merged_commit while leaving merge_state at its 'merged'
  // default — handing off UNMERGED work the drain is then told to merge
  // (field incident 2026-06-17). Reject it at the boundary: a pending marker
  // means the merge has not happened, so the honest state is
  // merge_state:'merge-pending' (which defers stage-2 dispatch), never a
  // 'merged' handoff. Pure string check; the drain's Step 0 does the
  // git-ancestry verification of merged_commit. (act:e859b3a3)
  if (category === QA_CATEGORY && evidence
      && normalizeMergeState(evidence.merge_state) === 'merged') {
    const NOT_YET_MERGED = /\b(pending|unmerged|not[\s-]*yet[\s-]*merged|to[\s-]*be[\s-]*merged|will[\s-]*merge)\b/i;
    for (const k of ['merged_into', 'merged_commit']) {
      const v = evidence[k];
      if (typeof v === 'string' && NOT_YET_MERGED.test(v)) {
        throw new Error(`createItem: qa-handoff filed as 'merged' but evidence.${k} flags the merge as not yet done (${JSON.stringify(v.slice(0, 60))}) — merge the branch first, or file with merge_state:'merge-pending' (which defers stage-2 dispatch). A 'merged' handoff must reference work actually on main.`);
      }
    }
  }
  // Merged-by-construction guards (act:3d1ac2b7). The string check above
  // catches free-texted pending markers; these verify the POSITIVE claim.
  // Firing matrix (pinned — see the taxonomy note at verifyMergedAncestry):
  //   merged (explicit OR the absent-field default):
  //     - merged_commit absent / not sha-shaped → REJECT (omission was the
  //       trivial bypass; the skill's producer contract has always required
  //       the sha on merged handoffs);
  //     - git refutes ancestry → REJECT, message teaches merge-first;
  //     - verified / can't-determine → file, stamped 'verified'/'unverifiable'.
  //   merge-pending:
  //     - evidence.merge_gate absent/empty → REJECT. An INTERNAL failure
  //       (merge conflict, gate red) is not a gate — HALT and fix instead.
  if (category === QA_CATEGORY) {
    // MINT-ONLY fields: the boundary owns the verification record. A
    // caller-supplied ancestry stamp or dispatch status is discarded, never
    // trusted — a pre-planted stage2_dispatch:'dispatched' would make
    // markHandoffMerged short-circuit without verifying or dispatching, and
    // a re-filed item (standing-debt refile, supersede+refile) legitimately
    // carries stale stamps that must not survive into the fresh filing.
    if (evidence && (evidence.ancestry_verified !== undefined || evidence.stage2_dispatch !== undefined)) {
      const { ancestry_verified: _av, stage2_dispatch: _sd, ...minted } = evidence;
      evidence = minted;
    }
    const ms = normalizeMergeState(evidence && evidence.merge_state);
    if (ms === 'merged') {
      const sha = evidence && typeof evidence.merged_commit === 'string'
        ? evidence.merged_commit.trim() : evidence?.merged_commit;
      if (typeof sha !== 'string' || !SHA_RE.test(sha)) {
        throw new Error(`createItem: a 'merged' qa-handoff requires a sha-shaped evidence.merged_commit (got ${JSON.stringify(sha ?? null)}). Merge-first close-out: merge the branch into main, push, then file with the post-merge sha — or, if the merge is genuinely gated on something external, file merge_state:'merge-pending' with evidence.merge_gate naming the blocker.`);
      }
      const v = verifyMergedAncestry(project_path, sha);
      if (v.state === 'refuted') {
        throw new Error(`createItem: qa-handoff filed as 'merged' but ${sha.slice(0, 12)} is NOT an ancestor of ${v.ref} in ${project_path} — the merge hasn't landed on the main ref (did the push happen? or your local ${v.ref} may be behind: fetch and retry). Merge + push first and file with the post-merge sha, or file merge_state:'merge-pending' with evidence.merge_gate naming the external blocker.`);
      }
      // Store the TRIMMED sha (the value the check ran against) so the
      // stamp and the stored value can never disagree — markHandoffMerged
      // canonicalizes the same way.
      evidence = { ...evidence, merged_commit: sha, ancestry_verified: v.state === 'verified' ? 'verified' : 'unverifiable' };
    } else if (ms === 'merge-pending') {
      const gate = evidence && evidence.merge_gate;
      if (typeof gate !== 'string' || !gate.trim()) {
        throw new Error(`createItem: a 'merge-pending' qa-handoff requires evidence.merge_gate — a non-empty string naming the EXTERNAL blocker and when it clears (e.g. "staging deploy <id> in flight — clears on deploy SUCCESS"). An internal failure (merge conflict, red gate) is not a merge gate: HALT and fix, then merge-first (merge → push → file 'merged'). The silent merge-pending default is retired.`);
      }
    }
  }
  const id = generateId();
  const item = {
    schema_version: 1,
    id,
    project,
    project_path,
    // Additive fields, present only when meaningful (older readers and items
    // omit them): project_unresolved marks a basename-fallback identity,
    // desk preserves the mux desk name as display metadata.
    ...(project_unresolved ? { project_unresolved: true } : {}),
    ...(desk ? { desk } : {}),
    filed_at: new Date().toISOString(),
    filed_by,
    status: 'pending',
    enrichment_status: 'bare',
    category,
    urgency,
    title,
    summary,
    context_anchor,
    evidence,
    options,
    draft_artifact,
    transcript_ref,
    plan_fid,
    thread_ids,
    confidence,
    enrichment_dir: null,
    resolved_at: null,
    resolution: null,
    resolution_type: null,
    resolution_notes: null,
  };
  atomicWrite(itemPath(id), item);
  return id;
}

/**
 * Resolve an inbox item.
 * For qa-handoff items, a structured `qa_verdict` is REQUIRED and validated
 * (validateQaVerdict) — invalid shapes THROW naming the missing field; the
 * bare-null return remains reserved for "not pending". The validated verdict
 * is stamped onto the item with its token canonicalized.
 * @param {string} id
 * @param {object} params
 * @returns {object} The updated item
 */
export function resolveItem(id, { resolution, resolution_notes = null, resolution_type = null, qa_verdict = null }) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (item.category === QA_CATEGORY) {
    // A merge-pending handoff cannot be RESOLVED — resolution asserts QA of
    // merged work, and the merge was never recorded. The drain merges, calls
    // markHandoffMerged (verified flip + deferred dispatch), THEN resolves.
    // Dismissal (the audited escape hatch below) stays legal for abandoned
    // work. (act:3d1ac2b7 — the widest bypass around the flip doorway.)
    if (normalizeMergeState(item.evidence && item.evidence.merge_state) === 'merge-pending') {
      throw gateError(item, `item is merge_state 'merge-pending' — the merge has not been recorded. Merge the branch, run markHandoffMerged(id, { merged_commit }) so the merge is verified and the deferred dispatch fires, then resolve with the qa_verdict`);
    }
    const verdict = validateQaVerdict(item, qa_verdict);
    item.qa_verdict = { ...qa_verdict, verdict };
  }
  item.status = 'resolved';
  item.resolved_at = new Date().toISOString();
  item.resolution = resolution;
  item.resolution_type = resolution_type;
  item.resolution_notes = resolution_notes;
  atomicWrite(fp, item);
  if (DISPATCHED_CATEGORIES.has(item.category)) clearDispatchEntries(item);
  return item;
}

/**
 * Dismiss an inbox item.
 * @param {string} id
 * @param {object} params
 * @returns {object} The updated item
 */
export function dismissItem(id, { notes = null, resolution_type = null } = {}) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (item.category === QA_CATEGORY
      && (typeof resolution_type !== 'string' || !resolution_type.trim()
          || typeof notes !== 'string' || !notes.trim())) {
    throw gateError(item, 'dismissing a qa-handoff item requires a typed resolution_type AND notes naming why post-merge QA is being waived — dismissal is an audited escape hatch, not a bypass of the recipient gate');
  }
  item.status = 'dismissed';
  item.resolved_at = new Date().toISOString();
  item.resolution_type = resolution_type;
  item.resolution_notes = notes;
  atomicWrite(fp, item);
  if (DISPATCHED_CATEGORIES.has(item.category)) clearDispatchEntries(item);
  return item;
}

/**
 * Mark an inbox item as superseded.
 * @param {string} id
 * @param {object} params
 * @returns {object} The updated item
 */
export function supersedeItem(id, { reason = null } = {}) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (item.category === QA_CATEGORY && (typeof reason !== 'string' || !reason.trim())) {
    throw gateError(item, 'superseding a qa-handoff item requires a reason naming what replaces it (e.g. the newer handoff covering the same merge)');
  }
  item.status = 'superseded';
  item.resolution_notes = reason;
  atomicWrite(fp, item);
  if (DISPATCHED_CATEGORIES.has(item.category)) clearDispatchEntries(item);
  return item;
}

// The resolution_type archiveItem stamps. A MACHINE type in the inbox
// assessment (watchtower-cross-ring-reader.mjs MACHINE_TYPES) — these closures
// are a migration, not an operator keeping anything.
export const ARCHIVED_RESOLUTION_TYPE = 'archived';

/**
 * Close a knowledge-extraction item whose content has been written to the
 * recall archive (Simplification Project 1, plan C1, act:93a52285 — the
 * category's producers are deleted; watchtower-archive-drafts.mjs is the one
 * caller). The closure is `resolved` + resolution_type 'archived' — NOT a new
 * status — so the resolved/dismissed/superseded title corpus already covers
 * these items and nothing refiles.
 *
 * Why its own door instead of resolveItem: it must accept an EXPIRED item
 * (resolveItem refuses anything non-pending, and half of what the migration
 * archives had already expired unread), and it must not erase the expiry
 * history — the prior status and the prior resolution_notes are PREPENDED to
 * the new note.
 *
 * The door is fenced to its one category: any other category THROWS, so this
 * can never be used to walk a gated item (a qa-handoff, a relation proposal)
 * out of the queue without its gate. An item that is already resolved,
 * dismissed or superseded is a no-op (null) — never an error, never a
 * re-stamp. The archive file must already exist: an item is never closed
 * against a path nothing was written to.
 * @param {string} id
 * @param {object} params
 * @param {string} params.archive_path - absolute path of the written archive file
 * @returns {object|null} the updated item; null when not pending/expired
 */
export function archiveItem(id, { archive_path } = {}) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.category !== KNOWLEDGE_CATEGORY) {
    throw new Error(`archiveItem: cannot close ${item.id} — this door closes ${KNOWLEDGE_CATEGORY} items only; a ${item.category} item leaves the queue through its own door (resolve / dismiss / supersede), never through the archive`);
  }
  if (item.status !== 'pending' && item.status !== 'expired') return null;
  if (typeof archive_path !== 'string' || !archive_path.trim() || !existsSync(archive_path)) {
    throw new Error(`archiveItem: archive_path must name the archive file already written for ${id} — got ${JSON.stringify(archive_path ?? null)}`);
  }
  const priorStatus = item.status;
  const priorNotes = typeof item.resolution_notes === 'string' && item.resolution_notes.trim()
    ? item.resolution_notes
    : null;
  item.status = 'resolved';
  item.resolved_at = new Date().toISOString();
  item.resolution = archive_path;
  item.resolution_type = ARCHIVED_RESOLUTION_TYPE;
  item.resolution_notes = `[was ${priorStatus}${priorNotes ? `: ${priorNotes}` : ''}] Archived as a searchable recall draft; nothing deleted.`;
  atomicWrite(fp, item);
  return item;
}

/**
 * Hold a pending inbox item — mark it for deliberate human retention rather
 * than resolving/dismissing it. The item STAYS pending (still visible, still
 * editable) but carries `evidence.held = { reason, held_at }` and drops out
 * of batch disposition (isHighConfidenceSignoff demotes on evidence.held;
 * applyBatch refuses a held item outright). Delegates to
 * annotateItemEvidence for the write, so hold inherits its discipline: a
 * fresh read at write time, the pending fence, additive-only fields.
 *
 * A reason is required — hold marks deliberate retention, not silent limbo.
 * The id comes from the item under review (never hand-transcribed): the
 * 2026-07-14 read-pass held 13 specimens via a scratchpad script that
 * hand-copied hex ids into a Map literal, and its one error (dec-3609fe4a
 * held under dec-837d172c's reason — the true duplicate escaping to memory)
 * was CAUSED by that transcription step. Calling holdItem(item.id, ...)
 * against the item object under review makes that error class impossible.
 * @param {string} id
 * @param {object} params
 * @param {string} params.reason - required; why this item is held
 * @param {object} [params.marker] - additional evidence fields to stamp in
 *   the SAME write (e.g. `{ specimen: { class, reason } }`)
 * @returns {{item: object, changed: boolean}|null} null when missing/not pending
 */
export function holdItem(id, { reason, marker = null } = {}) {
  if (typeof reason !== 'string' || !reason.trim()) {
    throw new Error('holdItem: a reason is required — hold marks deliberate retention, not silent limbo');
  }
  const patch = { held: { reason, held_at: new Date().toISOString() } };
  if (marker && typeof marker === 'object' && !Array.isArray(marker)) {
    Object.assign(patch, marker);
  }
  return annotateItemEvidence(id, patch);
}

/**
 * Release a held item: clears `evidence.held` (and `evidence.specimen`, if
 * present) so it re-enters normal disposition. annotateItemEvidence can only
 * MERGE evidence fields, never delete one, so this reads+writes directly —
 * same fresh-read + pending-fence discipline.
 * @param {string} id
 * @returns {object|null} the updated item; null when missing/not pending
 */
export function releaseHold(id) {
  const fp = itemPath(id);
  if (!existsSync(fp)) return null;
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (!item.evidence || !item.evidence.held) return item;
  const evidence = { ...item.evidence };
  delete evidence.held;
  delete evidence.specimen;
  item.evidence = evidence;
  atomicWrite(fp, item);
  return item;
}

// No expiry lives here any more. Simplification Project 2 (plan A1,
// act:15aeeafb, 2026-10-30) deleted both age engines — the per-category table
// and single-item expirer that stood here, and the /inbox + /briefing sweep
// that stood just above the CLI section (Ring 2's cron branch went with them). An
// item leaves the queue only when a mechanical check retracts it or the
// operator closes it; age is shown ([AGING], urgent decay in ring2) and never
// acted on. The `expired` STATUS stays a legal value: ~1,200 historical items
// carry it, and every reader (archiveItem, the dedup corpora, the snapshot
// tallies) keeps reading it — they simply see no new ones.

/**
 * Apply ONE disposition to a batch of ungated items — the single bulk path,
 * used by /briefing's batch dispositions and /inbox's bulk triage. A batch is one
 * operator decision applied to many items, so the typed reason is REQUIRED:
 * it lands on every item as its audit trail.
 *
 * All-or-nothing pre-validation: throws BEFORE any write when (a) any id is
 * unknown, (b) any item is in a GATED_CATEGORIES category — gated items never
 * batch; each goes through its own gate — or (c) the typed reason is absent.
 * A batch that would hit a gate fails whole, never half-applies.
 * Non-pending items are not an error (another session may have raced the
 * disposition); they are skipped and reported.
 * @param {string[]} ids
 * @param {object} params
 * @param {'dismiss'|'resolve'} params.disposition
 * @param {string} params.resolution_type - typed reason (e.g. 'stale',
 *   'noise', 'captured-to-memory', 'acted-on') — required
 * @param {string} params.notes - plain-language reason — required
 * @param {string} [params.resolution] - resolve batches: the resolution
 *   value stamped on each item (defaults to resolution_type)
 * @returns {{applied: string[], skipped_not_pending: string[]}}
 */
export function applyBatch(ids, { disposition, resolution_type, notes, resolution = null } = {}) {
  if (!Array.isArray(ids) || ids.length === 0) {
    throw new Error('applyBatch: ids must be a non-empty array');
  }
  if (disposition !== 'dismiss' && disposition !== 'resolve') {
    throw new Error("applyBatch: disposition must be 'dismiss' or 'resolve'");
  }
  if (typeof resolution_type !== 'string' || !resolution_type.trim()
      || typeof notes !== 'string' || !notes.trim()) {
    throw new Error('applyBatch: a typed resolution_type AND notes are required — a batch is one decision applied to many items, and the reason lands on every one');
  }
  const items = ids.map((id) => {
    const item = getItem(id);
    if (!item) throw new Error(`applyBatch: unknown item ${id} — batches pre-validate whole; nothing was applied`);
    return item;
  });
  const gated = items.filter((i) => GATED_CATEGORIES.has(i.category));
  if (gated.length > 0) {
    throw new Error(`applyBatch: ${gated.map((i) => `${i.id} (${i.category})`).join(', ')} carr${gated.length === 1 ? 'ies' : 'y'} a recipient gate — gated items never batch; handle each through its own gate. Nothing was applied`);
  }
  const held = items.filter((i) => i.evidence && i.evidence.held);
  if (held.length > 0) {
    throw new Error(`applyBatch: ${held.map((i) => i.id).join(', ')} ${held.length === 1 ? 'is' : 'are'} held — held items are excluded from batch disposition; releaseHold(id) first if this should proceed. Nothing was applied`);
  }
  const applied = [];
  const skipped_not_pending = [];
  for (const item of items) {
    const result = disposition === 'dismiss'
      ? dismissItem(item.id, { notes, resolution_type })
      : resolveItem(item.id, { resolution: resolution ?? resolution_type, resolution_notes: notes, resolution_type });
    if (result) applied.push(item.id);
    else skipped_not_pending.push(item.id);
  }
  return { applied, skipped_not_pending };
}

/**
 * List pending inbox items with optional filters.
 * @param {object} filters
 * @param {boolean} [filters.held] - true: only held items (evidence.held
 *   truthy); false: only non-held items; omit: no filter on held state
 * @returns {Array} Sorted array of pending items
 */
export function listPending({ project, category, urgency, maxAge, held } = {}) {
  if (!existsSync(QUEUE_DIR)) return [];
  const entries = readdirSync(QUEUE_DIR, { withFileTypes: true });
  const items = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
    try {
      const item = readItem(join(QUEUE_DIR, entry.name));
      if (item.status !== 'pending') continue;
      if (project && item.project !== project) continue;
      if (category && item.category !== category) continue;
      if (urgency && item.urgency !== urgency) continue;
      const isHeld = !!(item.evidence && item.evidence.held);
      if (held === true && !isHeld) continue;
      if (held === false && isHeld) continue;
      if (maxAge) {
        const filed = new Date(item.filed_at);
        const cutoff = new Date(Date.now() - maxAge);
        if (filed < cutoff) continue;
      }
      items.push(item);
    } catch {
      // Skip unparseable items
    }
  }
  // Sort: urgent first, then by filed_at descending (newest first)
  items.sort((a, b) => {
    const urgDiff = (URGENCY_ORDER[a.urgency] ?? 99) - (URGENCY_ORDER[b.urgency] ?? 99);
    if (urgDiff !== 0) return urgDiff;
    return new Date(b.filed_at) - new Date(a.filed_at);
  });
  return items;
}

/**
 * List inbox items across ALL statuses with optional filters — the single
 * source for "read non-pending items". Ring 2's private readAllQueueItems
 * fork (which predated this helper) was deleted and now reads through this
 * helper (act:3975348f).
 * @param {object} filters
 * @param {string} [filters.project] - exact match on item.project
 * @param {string} [filters.category] - exact match on item.category
 * @param {string[]} [filters.statuses] - keep items whose status is in the
 *   array (omit = all statuses)
 * @param {string} [filters.since] - ISO date; keep items whose
 *   `resolved_at || filed_at` >= since (bounds corpus growth for dedup callers)
 * @returns {Array} items sorted newest first by (resolved_at || filed_at)
 */
export function listItems({ project, category, statuses, since, onUnreadable } = {}) {
  if (!existsSync(QUEUE_DIR)) return [];
  const sinceMs = since ? new Date(since).getTime() : null;
  const entries = readdirSync(QUEUE_DIR, { withFileTypes: true });
  const items = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
    try {
      const item = readItem(join(QUEUE_DIR, entry.name));
      if (project && item.project !== project) continue;
      if (category && item.category !== category) continue;
      if (Array.isArray(statuses) && !statuses.includes(item.status)) continue;
      if (sinceMs != null) {
        const ts = new Date(item.resolved_at || item.filed_at).getTime();
        if (!(ts >= sinceMs)) continue;
      }
      items.push(item);
    } catch (err) {
      // Skipping is right — one corrupt file must not take down a whole read —
      // but doing it SILENTLY is an invisible drop channel: it shrinks any
      // denominator computed from this call with no signal, which is the
      // understate-by-construction failure a debt ledger cannot afford.
      // Optional, so every existing caller is unchanged.
      if (typeof onUnreadable === 'function') onUnreadable(entry.name, err);
    }
  }
  items.sort((a, b) =>
    new Date(b.resolved_at || b.filed_at) - new Date(a.resolved_at || a.filed_at));
  return items;
}

/**
 * Get a single inbox item by id.
 * @param {string} id
 * @returns {object|null} The item, or null if not found
 */
export function getItem(id) {
  const fp = itemPath(id);
  if (!existsSync(fp)) return null;
  return readItem(fp);
}

/**
 * Get enrichment data for an inbox item.
 * Returns {code_context, related_decisions, memory_refs, options_analysis}
 * with nulls for missing files. Never throws on missing files.
 * @param {string} id
 * @returns {object}
 */
export function getEnrichment(id) {
  const enrichDir = join(QUEUE_DIR, assertLegalItemId(id), 'enrichment');
  const files = {
    code_context: 'code-context.md',
    related_decisions: 'related-decisions.md',
    memory_refs: 'memory-refs.md',
    options_analysis: 'options-analysis.md',
  };
  const result = {};
  for (const [key, filename] of Object.entries(files)) {
    const fp = join(enrichDir, filename);
    try {
      result[key] = existsSync(fp) ? readFileSync(fp, 'utf8') : null;
    } catch {
      result[key] = null;
    }
  }
  return result;
}

// --- CLI ---
//
// watchtower-queue.mjs is primarily a LIBRARY (every other caller `import`s it).
// It also exposes ONE operator-invoked subcommand — `resolve` — so a qa-handoff
// verdict can be stamped from a JSON file instead of an inline
// `node -e '<...>'` whose single-quoted string the shell breaks on apostrophes
// in justification text (that hazard hit the drain station EVERY time;
// act:00030e1d). The verdict file is the FULL resolveItem params object
// {resolution, resolution_type, resolution_notes, qa_verdict} — apostrophes
// live in resolution_notes AND qa_verdict.confessed_gap.deferred[].justification,
// so a verdict-only file would leave notes inline and the hazard would survive.
// The qa_verdict SHAPE itself is single-sourced in the qa-handoff skill's
// "The recipient gate"; this CLI adds no validation — it reuses resolveItem
// (and its validateQaVerdict gate) verbatim.
//
// Exit policy (operator-invoked → fail LOUD; the INVERSE of the hook-invoked
// watchtower-snapshot.mjs, which swallows everything with exit 0):
//   0  resolved (or resolved-but-could-not-read-back-to-confirm)
//   1  well-formed call could not proceed: item not found, item not pending,
//      or the recipient gate rejected the verdict (item left pending)
//   2  malformed INVOCATION: bad args, missing/unreadable/non-JSON/non-object
//      verdict file, or an unknown top-level key
// NOTE: this 1/2 mapping is the OPPOSITE of the sibling watchtower-lib.mjs CLI
// (there 1=usage, 2=operational). The taxonomy here follows the sysexits /
// argparse convention — 2 means the invocation itself is malformed — and the
// divergence from watchtower-lib is deliberate, not an oversight.

const RESOLVE_PARAM_KEYS = ['resolution', 'resolution_type', 'resolution_notes', 'qa_verdict'];

class UsageError extends Error {}

/**
 * Parse `resolve <id> --verdict-file <path>` argv into {id, verdictFile}.
 * Pure (no I/O) so it is unit-testable. Throws UsageError on any malformed
 * form — a missing value, an id that looks like a flag, a repeated or unknown
 * flag, or an extra positional — so the CLI maps every one to exit 2 rather
 * than silently consuming it and failing later. Accepts both
 * `--verdict-file <path>` and `--verdict-file=<path>`.
 * @param {string[]} argv  full process.argv ([node, script, 'resolve', ...])
 * @returns {{id: string, verdictFile: string}}
 */
export function parseResolveArgs(argv) {
  const args = argv.slice(3); // drop [node, script, 'resolve']
  let id = null;
  let verdictFile = null;
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--verdict-file' || a.startsWith('--verdict-file=')) {
      if (verdictFile !== null) throw new UsageError('--verdict-file given more than once');
      if (a.startsWith('--verdict-file=')) {
        verdictFile = a.slice('--verdict-file='.length);
      } else {
        verdictFile = args[i + 1];
        i++;
      }
      if (typeof verdictFile !== 'string' || verdictFile === '' || verdictFile.startsWith('-')) {
        throw new UsageError('--verdict-file requires a path argument');
      }
    } else if (a.startsWith('-')) {
      throw new UsageError(`unknown flag: ${a}`);
    } else if (id === null) {
      id = a;
    } else {
      throw new UsageError(`unexpected extra argument: ${a}`);
    }
  }
  if (id === null || id.trim() === '') throw new UsageError('missing <id> (the inbox item id, e.g. dec-abcd1234)');
  if (verdictFile === null) throw new UsageError('missing --verdict-file <path>');
  return { id, verdictFile };
}

/**
 * Run the `resolve` subcommand: read the verdict-params JSON file, call
 * resolveItem, then read the item back from disk to confirm the stamp. Returns
 * the process exit code per the policy above; never throws (each failure mode
 * is mapped to a code with a distinct, non-misleading message).
 * @param {string[]} argv  full process.argv
 * @returns {number} exit code
 */
export function runResolveCli(argv) {
  let id;
  let verdictFile;
  try {
    ({ id, verdictFile } = parseResolveArgs(argv));
  } catch (err) {
    process.stderr.write(`resolve: ${err.message}\n`);
    process.stderr.write('usage: watchtower-queue.mjs resolve <id> --verdict-file <path.json>\n');
    return 2;
  }

  let raw;
  try {
    raw = readFileSync(verdictFile, 'utf8');
  } catch (err) {
    process.stderr.write(err.code === 'ENOENT'
      ? `resolve: verdict file not found: ${verdictFile}\n`
      : `resolve: cannot read verdict file ${verdictFile}: ${err.message}\n`);
    return 2;
  }

  let params;
  try {
    params = JSON.parse(raw);
  } catch (err) {
    process.stderr.write(`resolve: invalid JSON in ${verdictFile}: ${err.message}\n`);
    return 2;
  }

  // typeof null === 'object' AND typeof [] === 'object' — guard both explicitly,
  // or an array/null file would reach resolveItem and mis-resolve (exit 1)
  // instead of being caught here as a malformed invocation (exit 2).
  if (params === null || typeof params !== 'object' || Array.isArray(params)) {
    process.stderr.write(`resolve: ${verdictFile} must be a JSON object of resolve params {${RESOLVE_PARAM_KEYS.join(', ')}}\n`);
    return 2;
  }

  // resolveItem destructures exactly the 4 RESOLVE_PARAM_KEYS and JSON.stringify
  // drops undefined — so a typo'd key (e.g. resolution_note) would be SILENTLY
  // dropped, leaving the item resolved with a missing note and no warning. The
  // non-qa path has no gate at all. Reject unknown keys here so a typo fails
  // loud instead of vanishing.
  const unknown = Object.keys(params).filter((k) => !RESOLVE_PARAM_KEYS.includes(k));
  if (unknown.length > 0) {
    process.stderr.write(`resolve: unknown key(s) in ${verdictFile}: ${unknown.join(', ')} — allowed: ${RESOLVE_PARAM_KEYS.join(', ')} (an unrecognized key is silently dropped, so this is rejected)\n`);
    return 2;
  }

  let item;
  try {
    item = resolveItem(id, params);
  } catch (err) {
    // Three distinct throw sources, and only the gate's message is safe to echo:
    if (err.code === 'ENOENT') {
      // missing item file — friendly, and never leak the absolute queue path.
      process.stderr.write(`resolve: item ${id} not found\n`);
    } else if (typeof err.message === 'string' && err.message.startsWith('qa-handoff gate:')) {
      // recipient-gate rejection (gateError) — names the offending field and
      // embeds NO item content or path, so surface it verbatim. The item is
      // left pending (validateQaVerdict throws before the atomicWrite).
      process.stderr.write(`resolve: ${err.message}\n`);
    } else {
      // anything else — a corrupt/wrong-schema item file (readItem) or a write
      // failure (atomicWrite) — embeds the ABSOLUTE queue path in err.message.
      // Do NOT echo it; name only the id and the error code.
      process.stderr.write(`resolve: could not resolve item ${id} (${err.code || 'unexpected error'})\n`);
    }
    return 1;
  }

  if (item === null) {
    let status = 'not pending';
    try {
      const cur = getItem(id);
      if (cur && cur.status) status = cur.status;
    } catch { /* best-effort status lookup for the message only */ }
    process.stderr.write(`resolve: ${id} is not pending (status: ${status}) — an earlier stamp is intact, nothing to do\n`);
    return 1;
  }

  // Verify the write by re-reading from disk, not trusting the in-memory return
  // (verify-your-own-writes). A read-back failure must NOT read as a resolve
  // failure — the stamp already landed (resolveItem's atomicWrite returned).
  try {
    const stored = getItem(id);
    const token = stored && stored.qa_verdict && stored.qa_verdict.verdict
      ? stored.qa_verdict.verdict
      : (stored && stored.resolution) || '(resolved)';
    const notes = stored && stored.resolution_notes ? 'present' : 'absent';
    process.stdout.write(`resolved ${id} — verdict: ${token}; resolution_notes: ${notes}\n`);
  } catch (err) {
    process.stdout.write(`resolved ${id}, but could not read it back to confirm: ${err.message}\n`);
  }
  return 0;
}

// Entry guard — importing this module must NOT run the CLI (mirrors the ring
// scripts and watchtower-snapshot.mjs so tests can import the pure functions).
const isMain = (() => {
  try {
    return process.argv[1]
      && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
  } catch {
    return false;
  }
})();

if (isMain) {
  const sub = process.argv[2];
  if (sub === 'resolve') {
    process.exit(runResolveCli(process.argv));
  } else {
    process.stderr.write(`watchtower-queue.mjs: unknown command ${sub ? `'${sub}'` : '(none)'}\n`);
    process.stderr.write('usage: watchtower-queue.mjs resolve <id> --verdict-file <path.json>\n');
    process.exit(2);
  }
}

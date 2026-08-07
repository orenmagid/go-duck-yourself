#!/usr/bin/env node

// Watchtower inbox queue CRUD library.
// All writes use atomic temp+rename per watchtower-contracts.md.
// Queue uses directory listing, not index files (no-index convention).

import { readFileSync, writeFileSync, readdirSync, existsSync, mkdirSync, renameSync, unlinkSync, realpathSync } from 'fs';
import { join } from 'path';
import { randomBytes } from 'crypto';
import { pathToFileURL } from 'url';
import { execFileSync } from 'child_process';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const QUEUE_DIR = join(WATCHTOWER_DIR, 'queue', 'items');

// mux's dispatch queue (bin/mux `qa` verbs — the single desk-dispatch path,
// carrying qa-handoffs AND routine dispatches). Descriptors are routing
// convenience keyed <desk>/<item_id>.json (plus <desk>/in-flight/ once
// drained); the inbox item is the durable record. Terminal exits here must
// clear the matching descriptor or the two stores drift (act:796fe6dc —
// resolved ghosts get re-offered by `mux qa drain`). The dir name is legacy.
const MUX_QA_DIR = process.env.MUX_QA_DIR
  || join(process.env.HOME, '.local', 'share', 'mux', 'qa-handoff');

// Categories whose items are pushed to a desk via the mux dispatch queue.
// Every terminal exit (resolve / dismiss / supersede / expire) on an item in
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
// typed reason; expiry never applies. The gate's playbook, tiers, and the
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
// shape and MAY auto-expire; the durable relation_key filing exclusion
// guarantees an expired proposal never refiles.
export const GATED_CATEGORIES = new Set([QA_CATEGORY, 'pending-relation', 'significance-change']);

const QA_TERMINAL_VERDICTS = ['runtime-verified', 'blocked'];
// The parameterized token. Canonical form uses U+00B7 (·); input tolerates
// common separator drift (-, –, —, *, •) because sessions retype labels.
const QA_GAPS_TOKEN_RE = /^verified\s*[·•*\-–—]\s*(\d+)\s+gaps?\s+filed$/;
const QA_COVERAGE_ASSESSMENTS = ['adequate', 'extended', 'gap-filed'];

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
export function validateQaVerdict(item, qa_verdict) {
  if (!qa_verdict || typeof qa_verdict !== 'object' || Array.isArray(qa_verdict)) {
    throw gateError(item, "missing structured qa_verdict object (pass qa_verdict to resolveItem; shape: see 'The recipient gate' in the qa-handoff skill)");
  }
  if (typeof qa_verdict.commit_tested !== 'string' || !qa_verdict.commit_tested.trim()) {
    throw gateError(item, 'qa_verdict.commit_tested is required (the main commit the QA ran against)');
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
    if (!Array.isArray(filed) || filed.length === 0
        || filed.some((g) => !g || typeof g.filed_as !== 'string' || !g.filed_as.trim())) {
      throw gateError(item, "coverage_posture.assessment is 'gap-filed' but coverage_posture.gap_filed is not a non-empty array of {gap, filed_as}");
    }
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
    written.forEach((w, i) => {
      if (typeof w !== 'string' || !w.trim()) {
        throw gateError(item, `confessed_gap.written_and_run[${i}] must be a non-empty string naming the test written and run`);
      }
    });
    deferred.forEach((d, i) => {
      if (!d || typeof d !== 'object'
          || typeof d.gap !== 'string' || !d.gap.trim()
          || typeof d.filed_as !== 'string' || !d.filed_as.trim()
          || typeof d.justification !== 'string' || !d.justification.trim()) {
        throw gateError(item, `confessed_gap.deferred[${i}] must be a typed deferral {gap, filed_as, justification} — a confessed gap is fixed in-session or explicitly deferred, never folded into 'filed'`);
      }
    });
    if (written.length + deferred.length < confessed.length) {
      throw gateError(item, `${confessed.length} confessed could_not_verify entr${confessed.length === 1 ? 'y' : 'ies'} but only ${written.length + deferred.length} dispositioned (written_and_run + typed deferrals) — every confessed gap must be discharged`);
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

// --- Exports ---

/**
 * Create a new inbox item.
 * @param {object} params
 * @returns {string} The generated item id
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

/**
 * Mark an inbox item as expired.
 * @param {string} id
 * @returns {object} The updated item
 */
// ---------------------------------------------------------------------------
// Category expiry — the ONE age policy (act:ea23b3a5)
// ---------------------------------------------------------------------------
//
// There are TWO expiry engines: Ring 2's `escalateQueueItems` (the 5-minute
// cron) and `runExpiry` below (called by /inbox and /briefing). Before this,
// each hardcoded 30 days and each inlined its own qa-handoff carve-out — so a
// per-category policy set in one would be silently violated by whichever engine
// reached the item first. Both now delegate here.
//
// `categoryNeverExpires` lives beside `expireItem`'s throw deliberately: the
// carve-out is STRUCTURAL, not a preference. `expireItem` throws on a
// qa-handoff, so returning anything but null for it would crash the caller's
// loop. One fact, one place, no drift.
export const DEFAULT_EXPIRE_DAYS = 30;

// Per-category overrides. completion-review at 14d: the category's whole value
// is time-bound — a completion candidate nobody confirmed in two weeks is not
// going to be confirmed, and a real completion re-detects when the action is
// next touched. NOTE the coupling to COMPLETION_REVIEW_DEDUP_DAYS (also 14) in
// ring3-close: because the two are equal, that phase's dedup corpus MUST
// include `expired` and `superseded`, or a fid leaves every corpus at exactly
// the moment its item expires and refiles forever.
export const CATEGORY_EXPIRE_DAYS = { 'completion-review': 14 };

export function categoryNeverExpires(category) {
  return category === QA_CATEGORY;
}

/** Days before an item of this category expires; null = never. */
export function expiryDaysFor(category) {
  if (categoryNeverExpires(category)) return null;
  return CATEGORY_EXPIRE_DAYS[category] ?? DEFAULT_EXPIRE_DAYS;
}

export function expireItem(id) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (categoryNeverExpires(item.category)) {
    throw gateError(item, 'qa-handoff items never expire — QA debt stays surfaced until a stamped verdict resolves it (or a typed dismissal waives it)');
  }
  item.status = 'expired';
  item.resolution_notes = 'Auto-expired by age policy';
  atomicWrite(fp, item);
  if (DISPATCHED_CATEGORIES.has(item.category)) clearDispatchEntries(item);
  return item;
}

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
export function listItems({ project, category, statuses, since } = {}) {
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
    } catch {
      // Skip unparseable items
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

/**
 * Run expiry check on pending items.
 * Items older than expireDays are marked expired with conservative defaults.
 * Items older than warnDays (but not yet expireDays) are flagged for warning.
 * @param {object} params
 * @returns {object} { warned: [], expired: [] }
 */
// `expireDays` is the DEFAULT, not a blanket: per-category overrides come from
// expiryDaysFor (act:ea23b3a5), so this engine and Ring 2's cron cannot give
// two different answers for the same item.
export function runExpiry({ warnDays = 14, expireDays = DEFAULT_EXPIRE_DAYS } = {}) {
  const now = Date.now();
  const warnMs = warnDays * 24 * 60 * 60 * 1000;
  const pending = listPending();
  const warned = [];
  const expired = [];

  for (const item of pending) {
    const age = now - new Date(item.filed_at).getTime();
    // qa-handoff items never auto-expire: an expired handoff is exactly the
    // silent QA gap the recipient gate forbids. They warn (by item) instead.
    const days = categoryNeverExpires(item.category)
      ? null
      : (CATEGORY_EXPIRE_DAYS[item.category] ?? expireDays);
    if (days === null) {
      if (age >= warnMs) warned.push(item);
      continue;
    }
    if (age >= days * 24 * 60 * 60 * 1000) {
      item.status = 'expired';
      item.resolution_notes = `Auto-expired after ${days} days. If still relevant, re-file with updated context.`;
      atomicWrite(itemPath(item.id), item);
      if (DISPATCHED_CATEGORIES.has(item.category)) clearDispatchEntries(item);
      expired.push(item);
    } else if (age >= warnMs) {
      warned.push(item);
    }
  }

  return { warned, expired };
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

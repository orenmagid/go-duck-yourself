#!/usr/bin/env node

// Watchtower inbox queue CRUD library.
// All writes use atomic temp+rename per watchtower-contracts.md.
// Queue uses directory listing, not index files (no-index convention).

import { readFileSync, writeFileSync, readdirSync, existsSync, mkdirSync, renameSync, unlinkSync, realpathSync } from 'fs';
import { join } from 'path';
import { randomBytes } from 'crypto';
import { pathToFileURL } from 'url';

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
// throws: a routing-cleanup failure must not block a gate exit. The ·N badge
// is recomputed by mux on its next mutation/desk-open, so a deletion here is
// picked up without tmux involvement.
function clearDispatchEntries(item) {
  try {
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
// unchanged: a merge-pending handoff still resolves only through a stamped
// qa_verdict, cited against the post-merge commit once the merge has happened.

const QA_CATEGORY = 'qa-handoff';

// Categories whose items carry a structural recipient gate: they may never be
// included in a batch disposition — each leaves the queue only through its own
// gate (per-item resolve with a validated verdict, or a typed per-item
// dismissal). Distinct from DISPATCHED_CATEGORIES: 'routine' is dispatched but
// NOT gated — stale routines are legal batch fodder. Future gated categories
// join this set and inherit batch refusal for free.
export const GATED_CATEGORIES = new Set([QA_CATEGORY]);

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
  // review: an overtaken draft (cites since-closed work) or a fold candidate
  // wants a human look, not a batch nod. Key on the SEMANTIC value, never on
  // field presence — a future benign "checked, still fresh" stamp under the
  // same key must not silently demote the whole knowledge-extraction class.
  if (item.evidence && item.evidence.freshness
      && item.evidence.freshness.overtaken === true) return false;
  if (item.evidence && Array.isArray(item.evidence.possible_duplicate_of)
      && item.evidence.possible_duplicate_of.length > 0) return false;
  return true;
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

// --- Surfacing intelligence: draft freshness + fold proposals (act:00051dca) ---
//
// Ring 2's slow tier sweeps PENDING knowledge-extraction drafts and attaches
// two ADDITIVE evidence annotations (no new categories, no schema change,
// and NEVER an auto-dismissal — an annotation demotes confidence so the item
// routes to `individual` in partitionForBatchSignoff; the human decides):
//
//   evidence.freshness = { overtaken: true, cited: [{fid, closed_at}] }
//     The draft cites act: fids that are now closed — possibly overtaken.
//     Written ONLY when overtaken (negative-only), and deliberately carries
//     no timestamp: the value is a pure function of (draft, pib-db state),
//     so a re-sweep recomputes the identical object and annotateItemEvidence
//     skips the write (idempotent across the 30-min slow ticks). The cited
//     fid + close date ride along so /inbox can render "cites act:X, closed
//     YYYY-MM-DD" without re-resolving another project's pib-db.
//
//   evidence.possible_duplicate_of = ['dec-...']  (sorted; reciprocal)
//     A fold proposal: another PENDING draft in the same project words the
//     same lesson (unigram Jaccard >= FOLD_SIMILARITY_THRESHOLD). Both sides
//     of a surfaced pair are annotated (reciprocity is the self-validating
//     criterion). The annotation persists after its partner resolves — a
//     draft whose twin was already dispositioned still wants a human look,
//     not a batch sign-off.
//
// This apparatus is the deliberate SIBLING of ring3-close's dedup tokenizer
// (STOPWORDS/meaningfulTokens/OVERLAP_THRESHOLD): same concept ("do these two
// short texts describe the same thing"), different corpus and metric. Lane
// boundaries forced the fork (ring3-close and watchtower-lib are other lanes'
// files); ring3-close already imports from this module, so a later
// consolidation can flow ring3 → queue.

// Fid extraction: matches are the ONLY values that ever reach a pib-db query
// (bound as parameters, never interpolated) — the regex is the validation.
const ACT_FID_RE = /\bact:[0-9a-f]{8}\b/g;

// Cap per draft so a pathological draft can't build an unbounded lookup.
const MAX_CITED_FIDS = 50;

/**
 * Extract unique cited `act:` fids from draft text, in order of first
 * appearance, capped at MAX_CITED_FIDS. Non-string/empty input → [].
 * @param {string} text
 * @returns {string[]}
 */
export function extractCitedActFids(text) {
  if (typeof text !== 'string' || !text) return [];
  const out = [];
  for (const m of text.match(ACT_FID_RE) || []) {
    if (!out.includes(m)) {
      out.push(m);
      if (out.length >= MAX_CITED_FIDS) break;
    }
  }
  return out;
}

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
 * Propose fold candidates over a set of pending drafts (one project's).
 * Pure — no I/O, no writes. Compares title + draft_artifact pairwise;
 * items below the FOLD_MIN_TOKENS floor or without an id never pair.
 * @param {Array} items
 * @param {object} [opts]
 * @param {number} [opts.threshold]
 * @returns {Array<{a: string, b: string, similarity: number}>}
 */
export function proposeFolds(items, { threshold = FOLD_SIMILARITY_THRESHOLD } = {}) {
  const eligible = [];
  for (const item of Array.isArray(items) ? items : []) {
    if (!item || typeof item.id !== 'string' || !item.id) continue;
    const tokens = foldTokens(`${item.title || ''}\n${item.draft_artifact || ''}`);
    if (tokens.size < FOLD_MIN_TOKENS) continue;
    eligible.push({ id: item.id, tokens });
  }
  const pairs = [];
  for (let i = 0; i < eligible.length; i++) {
    for (let j = i + 1; j < eligible.length; j++) {
      const similarity = unigramJaccard(eligible[i].tokens, eligible[j].tokens);
      if (similarity >= threshold) {
        pairs.push({ a: eligible[i].id, b: eligible[j].id, similarity });
      }
    }
  }
  return pairs;
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
 * Mark an inbox item as expired.
 * @param {string} id
 * @returns {object} The updated item
 */
export function expireItem(id) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  if (item.category === QA_CATEGORY) {
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
 * @returns {Array} Sorted array of pending items
 */
export function listPending({ project, category, urgency, maxAge } = {}) {
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
export function runExpiry({ warnDays = 14, expireDays = 30 } = {}) {
  const now = Date.now();
  const warnMs = warnDays * 24 * 60 * 60 * 1000;
  const expireMs = expireDays * 24 * 60 * 60 * 1000;
  const pending = listPending();
  const warned = [];
  const expired = [];

  for (const item of pending) {
    const age = now - new Date(item.filed_at).getTime();
    // qa-handoff items never auto-expire: an expired handoff is exactly the
    // silent QA gap the recipient gate forbids. They warn (by item) instead.
    if (item.category === QA_CATEGORY) {
      if (age >= warnMs) warned.push(item);
      continue;
    }
    if (age >= expireMs) {
      item.status = 'expired';
      item.resolution_notes = `Auto-expired after ${expireDays} days. If still relevant, re-file with updated context.`;
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

#!/usr/bin/env node

// Watchtower inbox queue CRUD library.
// All writes use atomic temp+rename per watchtower-contracts.md.
// Queue uses directory listing, not index files (no-index convention).

import { readFileSync, writeFileSync, readdirSync, existsSync, mkdirSync, renameSync, unlinkSync } from 'fs';
import { join } from 'path';
import { randomBytes } from 'crypto';

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

function itemPath(id) {
  return join(QUEUE_DIR, `${id}.json`);
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
 * source for "read non-pending items". (Ring 2's private readAllQueueItems
 * fork predates this helper; consolidating it is a separate-lane follow-up.)
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
  const enrichDir = join(QUEUE_DIR, id, 'enrichment');
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

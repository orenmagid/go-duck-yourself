#!/usr/bin/env node

// Watchtower inbox ASSESSMENT reader — the read-only "is the watchtower
// working?" trust surface. It answers ONE question per project, answer-first:
// is this project KEEPING its knowledge, LOSING it, NOT CONSUMING its inbox, or
// is there TOO LITTLE DATA to tell yet? A projection over stores that already
// exist — it WRITES NOTHING (there is no write path in this module, and the
// hermetic test asserts the filesystem is untouched). Sibling of
// watchtower-cross-ring-reader.mjs / watchtower-narrative-corpus.mjs (same
// read-only reader family); touches NOTHING the rings load (soak-safe) — the
// recall canary is read DIRECTLY off its sidecar file, never through Ring 2.
//
// Four axes feed the one derived state (plus a fifth honesty signal):
//
//   kept knowledge — the last-N-decision KEEP RATIO. An EVENT window
//     (newest-N resolved/dismissed items, NO since-filter — a keep ratio must
//     survive months-apart decisions) AND an all-time resolution mix. Every
//     ratio carries its denominator (decided_recent / decided_total) so a null
//     ratio is EXPLAINED ("0 typed decisions yet"), never a blank or a
//     fabricated number. Classified through the cross-ring reader's ONE
//     bucketResolution classifier — the enum is not re-derived here.
//   backlog rot — pending count, oldest age, count over 30d / 90d. CO-EQUAL
//     with engagement (the survivorship guard): a fine keep ratio on top of an
//     old, unworked pile with ~0 recent decisions is NOT "keeping" — it is
//     "not-consumed". Judging keep behavior only from what got decided is
//     survivorship bias; the pile that never got looked at is the real signal.
//   thread health — freshness (newest thread update) + segmentation smells
//     (a session living in >5 threads = over-eager cutting; threads with no
//     related_fids and no lineage = unlinked). The /threads Step-3 heuristics,
//     read-only.
//   rings alive — each ring's health sidecar: recency vs its cadence + status.
//   not-discarding — the project's recall-canary entry (rate / baseline /
//     alert / net_durably_saved), null-aware: a null net_durably_saved means
//     durable usefulness is UNKNOWN, and we say so rather than imply zero.
//
// Derived state ∈ { keeping, losing, not-consumed, too-little-data }.
//
// CLI (reads only, answer-first plain English — verbs + counts, never
// decimals):
//   --portfolio                one derived-state line per configured project
//   --project <name-or-slug>   full per-project drill-down
//   --window <N>               event-window size (default 20)
//
// The project set is resolved from watchtower config.projects, exactly like the
// sibling readers. Never throws on a missing/partial store — every reader
// degrades to honest-empty.

import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { pathToFileURL } from 'url';
import { realpathSync } from 'fs';
import {
  bucketResolution,
  ENGAGED_TYPES,
  DISCARDED_TYPES,
} from './watchtower-cross-ring-reader.mjs';
import { listPending, listItems } from './watchtower-queue.mjs';
import { loadActiveThreads, threadMatchesProject, slugify } from './watchtower-lib.mjs';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME || '', '.claude-cabinet', 'watchtower');

export const INBOX_ASSESSMENT_SCHEMA_VERSION = 1;

// The four derived states, exported so a consumer never re-spells them.
export const DERIVED_STATES = ['keeping', 'losing', 'not-consumed', 'too-little-data'];

// Re-export the classifier vocabulary so callers/tests read it from ONE place
// (these are re-derived from the reader, not re-declared).
export { bucketResolution, ENGAGED_TYPES, DISCARDED_TYPES };

const DAY_MS = 86400000;

// Tunables — all exported so a test (or a future config) can bind them.
export const DEFAULT_EVENT_WINDOW_N = 20; // newest-N resolved/dismissed events
export const RECENT_DECISION_DAYS = 30;   // "being worked" = a disposition this recent
export const ROT_AGE_DAYS = 30;           // a pending item this old is "rotting"
export const DEEP_ROT_AGE_DAYS = 90;      // deep rot
export const MIN_PILE_FOR_ROT = 3;        // "not-consumed" needs a REAL pile, not one stale item
export const SMALL_PILE = 3;              // fewer pending than this = small
export const FEW_DECISIONS_EVER = 3;      // fewer typed decisions ever than this = sparse
export const HEALTHY_KEEP_RATIO = 0.5;    // engaged >= discarded to read as "keeping"
export const OVER_EAGER_THREAD_COUNT = 5; // a session in >5 threads = over-eager cutting

// Ring cadences (ms) for the staleness check — 2x cadence = stale. Ring 3 has
// no fixed cadence (it fires at session close), so it is never called stale on
// age, only "down" on a failed status.
const RING_SPECS = [
  ['ring1', 'ring1-health.json', 5 * 60 * 1000],
  ['ring2-fast', 'ring2-fast-health.json', 15 * 60 * 1000],
  ['ring2-slow', 'ring2-slow-health.json', 6 * 60 * 60 * 1000],
  ['ring3', 'ring3-health.json', null],
  ['ring4', 'ring4-health.json', 7 * DAY_MS],
];
const HEALTHY_RING_STATUSES = new Set(['success', 'ok']);

function safeReadJSON(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function resolveNow(now) {
  if (typeof now === 'number' && Number.isFinite(now)) return now;
  if (typeof now === 'string') {
    const t = Date.parse(now);
    if (Number.isFinite(t)) return t;
  }
  return Date.now();
}

// --- Axis 1: kept knowledge --------------------------------------------------

// assessKeptKnowledge — the keep ratio over the EVENT window (newest-N
// resolved/dismissed, no since-filter) and all-time. Both ratios carry their
// denominator so a null ratio is explained, never blank or fabricated. The
// event window has NO time filter on purpose: a keep ratio must survive
// months-apart decisions (a real project may decide twice a quarter) — that is
// the ratio's job. Separately, `worked_recent` IS time-windowed
// (RECENT_DECISION_DAYS): it answers "is the operator still dispositioning this
// inbox at all?", the signal the survivorship guard needs — a great ratio built
// entirely from decisions made months ago must not read as "being worked".
export function assessKeptKnowledge(name, {
  eventWindowN = DEFAULT_EVENT_WINDOW_N,
  nowMs = Date.now(),
  recentDays = RECENT_DECISION_DAYS,
} = {}) {
  let all = [];
  try {
    all = listItems({ project: name, statuses: ['resolved', 'dismissed'] });
  } catch {
    all = [];
  }
  // listItems returns newest-first by (resolved_at || filed_at).
  const windowItems = all.slice(0, eventWindowN);

  // Time-windowed engagement — dispositions in the last recentDays (any type;
  // even an untyped resolve is the operator touching the inbox).
  const recentCutoff = nowMs - recentDays * DAY_MS;
  let workedRecent = 0;
  for (const it of all) {
    const ts = Date.parse(it.resolved_at || it.filed_at);
    if (Number.isFinite(ts) && ts >= recentCutoff) workedRecent++;
  }

  let engagedRecent = 0;
  let discardedRecent = 0;
  let otherRecent = 0;
  for (const it of windowItems) {
    const b = bucketResolution(it.resolution_type);
    if (b === 'engaged') engagedRecent++;
    else if (b === 'discarded') discardedRecent++;
    else otherRecent++;
  }
  const decidedRecent = engagedRecent + discardedRecent;

  let engagedTotal = 0;
  let discardedTotal = 0;
  let otherTotal = 0;
  for (const it of all) {
    const b = bucketResolution(it.resolution_type);
    if (b === 'engaged') engagedTotal++;
    else if (b === 'discarded') discardedTotal++;
    else otherTotal++;
  }
  const decidedTotal = engagedTotal + discardedTotal;

  return {
    event_window_n: eventWindowN,
    events_in_window: windowItems.length,
    engaged_recent: engagedRecent,
    discarded_recent: discardedRecent,
    other_recent: otherRecent,
    decided_recent: decidedRecent,
    // Time-windowed "being worked" signal (distinct from the untimed ratio).
    worked_recent: workedRecent,
    recent_days: recentDays,
    // null (not 0, not a fabricated ratio) when nothing typed was decided.
    keep_ratio_recent: decidedRecent > 0 ? engagedRecent / decidedRecent : null,
    // All-time mix, same shape/vocabulary as the cross-ring reader's.
    resolution_mix: {
      engaged: engagedTotal,
      discarded: discardedTotal,
      other: otherTotal,
      total: all.length,
    },
    decided_total: decidedTotal,
    keep_ratio_all: decidedTotal > 0 ? engagedTotal / decidedTotal : null,
  };
}

// --- Axis 2: backlog rot -----------------------------------------------------

export function assessBacklogRot(name, { nowMs = Date.now() } = {}) {
  let pending = [];
  try {
    pending = listPending({ project: name });
  } catch {
    pending = [];
  }
  let oldest = 0;
  let over30 = 0;
  let over90 = 0;
  for (const it of pending) {
    const filed = Date.parse(it.filed_at);
    if (!Number.isFinite(filed)) continue;
    const ageDays = (nowMs - filed) / DAY_MS;
    if (ageDays > oldest) oldest = ageDays;
    if (ageDays >= ROT_AGE_DAYS) over30++;
    if (ageDays >= DEEP_ROT_AGE_DAYS) over90++;
  }
  return {
    pending_count: pending.length,
    oldest_age_days: Math.floor(oldest),
    count_over_30d: over30,
    count_over_90d: over90,
  };
}

// --- Axis 3: thread health ---------------------------------------------------

function threadLastUpdated(t) {
  if (t && typeof t.last_updated === 'string' && t.last_updated) return t.last_updated;
  const ch = Array.isArray(t && t.cursor_history) ? t.cursor_history : [];
  let best = null;
  for (const h of ch) {
    if (h && typeof h.date === 'string' && (best == null || h.date > best)) best = h.date;
  }
  return best;
}

// assessThreadHealth — freshness + the /threads Step-3 segmentation smells,
// scoped to this project's threads (slug membership, the shared predicate).
export function assessThreadHealth(slug, stateDir, { nowMs = Date.now() } = {}) {
  const threadsDir = join(stateDir, 'threads');
  let threads = [];
  try {
    threads = loadActiveThreads(threadsDir);
  } catch {
    threads = [];
  }
  const mine = threads.filter((t) => {
    try {
      return threadMatchesProject(t, slug).match;
    } catch {
      return false;
    }
  });

  let mostRecent = null;
  for (const t of mine) {
    const cand = threadLastUpdated(t);
    if (cand && (mostRecent == null || cand > mostRecent)) mostRecent = cand;
  }
  const mostRecentMs = mostRecent ? Date.parse(mostRecent) : NaN;

  // Over-eager cutting: a session that lives in more than OVER_EAGER_THREAD_COUNT
  // of THIS project's threads (overlap-as-laziness, not overlap-as-judgment).
  const sessionThreadCount = new Map();
  for (const t of mine) {
    const seen = new Set();
    for (const s of Array.isArray(t.sessions) ? t.sessions : []) {
      if (s && s.id && !seen.has(s.id)) {
        seen.add(s.id);
        sessionThreadCount.set(s.id, (sessionThreadCount.get(s.id) || 0) + 1);
      }
    }
  }
  let overEagerSessions = 0;
  for (const c of sessionThreadCount.values()) if (c > OVER_EAGER_THREAD_COUNT) overEagerSessions++;

  // Unlinked: no related_fids AND no lineage — a thread with no stated
  // relationships (the /threads "orphan" smell, relationship dimension).
  let unlinkedThreads = 0;
  // Empty cursor history: the /threads orphan smell, cursor dimension.
  let emptyCursorThreads = 0;
  for (const t of mine) {
    const hasRelated = Array.isArray(t.related_fids) && t.related_fids.length > 0;
    const hasLineage = t.lineage
      && (Array.isArray(t.lineage) ? t.lineage.length > 0 : Object.keys(t.lineage).length > 0);
    if (!hasRelated && !hasLineage) unlinkedThreads++;
    const ch = Array.isArray(t.cursor_history) ? t.cursor_history : [];
    const hasLoneCursor = t.cursor && typeof t.cursor === 'object';
    if (ch.length === 0 && !hasLoneCursor) emptyCursorThreads++;
  }

  return {
    thread_count: mine.length,
    most_recent_update: mostRecent,
    days_since_update: Number.isFinite(mostRecentMs)
      ? Math.floor((nowMs - mostRecentMs) / DAY_MS)
      : null,
    over_eager_sessions: overEagerSessions,
    unlinked_threads: unlinkedThreads,
    empty_cursor_threads: emptyCursorThreads,
  };
}

// --- Axis 4: rings alive -----------------------------------------------------

// assessRingsAlive — portfolio-global (rings are not per-project). Computed
// once and shared across the per-project views. Recency vs cadence + status.
export function assessRingsAlive(stateDir, { nowMs = Date.now() } = {}) {
  const rings = {};
  let anyDown = false;
  let anyStale = false;
  for (const [key, file, cadence] of RING_SPECS) {
    const h = safeReadJSON(join(stateDir, file));
    if (!h) {
      rings[key] = { present: false, state: 'not-active', status: null, days_since_run: null };
      continue;
    }
    const lastRunMs = Date.parse(h.last_run);
    const ageMs = Number.isFinite(lastRunMs) ? nowMs - lastRunMs : null;
    let state = 'ok';
    if (h.status && !HEALTHY_RING_STATUSES.has(h.status)) {
      state = 'down';
      anyDown = true;
    } else if (cadence != null && ageMs != null && ageMs > 2 * cadence) {
      state = 'stale';
      anyStale = true;
    }
    rings[key] = {
      present: true,
      state,
      status: h.status || null,
      days_since_run: ageMs != null ? Math.floor(ageMs / DAY_MS) : null,
    };
  }
  return { rings, all_ok: !anyDown && !anyStale, any_down: anyDown, any_stale: anyStale };
}

// --- Axis 5: not-discarding (recall canary) ----------------------------------

export function assessNotDiscarding(name, stateDir) {
  const canary = safeReadJSON(join(stateDir, 'recall-canary.json'));
  const entry = canary && canary.projects && canary.projects[name]
    ? canary.projects[name]
    : null;
  if (!entry) {
    return {
      present: false,
      alert: false,
      rate: null,
      baseline: null,
      suppressed: null,
      net_durably_saved: null,
      // No canary entry ⇒ we cannot claim durable usefulness either way.
      durable_usefulness_unknown: true,
    };
  }
  const net = typeof entry.net_durably_saved === 'number' ? entry.net_durably_saved : null;
  return {
    present: true,
    alert: entry.alert === true,
    rate: typeof entry.rate === 'number' ? entry.rate : null,
    baseline: typeof entry.baseline === 'number' ? entry.baseline : null,
    suppressed: typeof entry.suppressed === 'number' ? entry.suppressed : null,
    net_durably_saved: net,
    // A null net_durably_saved means "durable usefulness unknown" — surfaced,
    // never silently read as zero.
    durable_usefulness_unknown: net == null,
  };
}

// --- The derived state (pure) ------------------------------------------------

// deriveAssessmentState — the ONE derived state from the axis numbers. PURE,
// no I/O — the survivorship-guard and the four-state coverage tests target this
// directly. Order is precedence, and it is load-bearing:
//
//   1. not-consumed — a REAL rotting pile (>= MIN_PILE_FOR_ROT items past 30d)
//      with ~0 recent dispositions (worked_recent, time-windowed). The
//      SURVIVORSHIP GUARD: this wins even when the keep ratio is high, because a
//      great keep ratio computed only from the handful that got decided (and
//      possibly long ago) says nothing about the pile nobody has looked at
//      lately. Requires a real pile (not one stale item) so a lone old item
//      cannot masquerade as "not consuming".
//   2. losing — the recall canary is alerting (dedup may be over-suppressing
//      NOVEL knowledge before it is even filed). Active knowledge loss.
//   3. too-little-data — sparse decisions ever AND a small pile: honestly not
//      enough signal to judge. Checked before "keeping" so a tiny history never
//      reads as a confident "keeping".
//   4. keeping — a healthy keep ratio and the inbox is actually being worked
//      recently, with no alert.
//   5. residual — sparse ⇒ too-little-data; otherwise keeping (being worked,
//      no rot, no alert).
export function deriveAssessmentState(axes = {}) {
  const {
    keep_ratio_all = null,
    keep_ratio_recent = null,
    decided_total = 0,
    worked_recent = 0,
    pending_count = 0,
    count_over_30d = 0,
    recall_alert = false,
  } = axes;

  const keepRatio = keep_ratio_all != null ? keep_ratio_all : keep_ratio_recent;
  const oldPile = count_over_30d >= MIN_PILE_FOR_ROT;
  const noRecentWork = worked_recent === 0;
  const beingWorked = worked_recent >= 1;
  const smallPile = pending_count < SMALL_PILE;
  const fewEver = decided_total < FEW_DECISIONS_EVER;

  if (oldPile && noRecentWork) return 'not-consumed';
  if (recall_alert) return 'losing';
  if (fewEver && smallPile) return 'too-little-data';
  if (keepRatio != null && keepRatio >= HEALTHY_KEEP_RATIO && beingWorked) return 'keeping';
  if (fewEver) return 'too-little-data';
  return 'keeping';
}

// --- Assemblers --------------------------------------------------------------

// assembleProjectAssessment — one project's full assessment. Pure w.r.t. its
// injected inputs: `stateDir` (or `watchtowerDir`, from which stateDir is
// derived) and `now` are injectable so age math and file reads are
// deterministic in tests. `ringsAlive` may be injected to avoid re-reading the
// (portfolio-global) ring sidecars per project.
export function assembleProjectAssessment({
  name,
  slug,
  path = null,
  watchtowerDir = WATCHTOWER_DIR,
  stateDir,
  now,
  eventWindowN = DEFAULT_EVENT_WINDOW_N,
  ringsAlive,
} = {}) {
  const nowMs = resolveNow(now);
  const resolvedSlug = slug || slugify(name || '');
  const sd = stateDir || join(watchtowerDir, 'state');

  const kept = assessKeptKnowledge(name, { eventWindowN, nowMs });
  const backlog = assessBacklogRot(name, { nowMs });
  const threads = assessThreadHealth(resolvedSlug, sd, { nowMs });
  const rings = ringsAlive || assessRingsAlive(sd, { nowMs });
  const recall = assessNotDiscarding(name, sd);

  const state = deriveAssessmentState({
    keep_ratio_all: kept.keep_ratio_all,
    keep_ratio_recent: kept.keep_ratio_recent,
    decided_total: kept.decided_total,
    worked_recent: kept.worked_recent,
    pending_count: backlog.pending_count,
    count_over_30d: backlog.count_over_30d,
    recall_alert: recall.alert,
  });

  return {
    name: name || null,
    slug: resolvedSlug,
    path: path || null,
    state,
    kept_knowledge: kept,
    backlog_rot: backlog,
    thread_health: threads,
    rings_alive: rings,
    not_discarding: recall,
  };
}

const emptyPortfolio = (reason) => ({
  schema_version: INBOX_ASSESSMENT_SCHEMA_VERSION,
  generated_at_source: 'live-read',
  reason,
  rings_alive: null,
  projects: [],
});

// assemblePortfolioAssessment — every configured project's assessment + the
// shared ring health. Resolves the project set from config.projects, exactly
// like the sibling readers.
export function assemblePortfolioAssessment({
  watchtowerDir = WATCHTOWER_DIR,
  now,
  eventWindowN = DEFAULT_EVENT_WINDOW_N,
} = {}) {
  const nowMs = resolveNow(now);
  const config = safeReadJSON(join(watchtowerDir, 'config.json'));
  if (!config) return emptyPortfolio('no-watchtower');
  const entries = Object.entries(config.projects || {});
  if (entries.length === 0) return emptyPortfolio('no-projects');

  const stateDir = join(watchtowerDir, 'state');
  const ringsAlive = assessRingsAlive(stateDir, { nowMs });

  const projects = entries.map(([key, proj]) => assembleProjectAssessment({
    name: key,
    slug: slugify(key),
    path: (proj && proj.path) || null,
    watchtowerDir,
    stateDir,
    now: nowMs,
    eventWindowN,
    ringsAlive,
  }));

  return {
    schema_version: INBOX_ASSESSMENT_SCHEMA_VERSION,
    generated_at_source: 'live-read',
    reason: null,
    rings_alive: ringsAlive,
    projects,
  };
}

// assembleProjectAssessmentByName — CLI --project resolver. Exact config-key
// match first, else a UNIQUE slug match (the /briefing scope filter is
// slug-shaped), mirroring the cross-ring reader.
export function assembleProjectAssessmentByName({
  project,
  watchtowerDir = WATCHTOWER_DIR,
  now,
  eventWindowN = DEFAULT_EVENT_WINDOW_N,
} = {}) {
  const config = safeReadJSON(join(watchtowerDir, 'config.json'));
  if (!config) return { ...emptyPortfolio('no-watchtower'), project: null };
  let key = null;
  if (config.projects && Object.prototype.hasOwnProperty.call(config.projects, project)) {
    key = project;
  } else if (config.projects) {
    const bySlug = Object.keys(config.projects)
      .filter((k) => slugify(k) === slugify(project || ''));
    if (bySlug.length === 1) key = bySlug[0];
  }
  if (key == null) return { ...emptyPortfolio('project-not-in-config'), project: null };
  const view = assembleProjectAssessment({
    name: key,
    slug: slugify(key),
    path: (config.projects[key] && config.projects[key].path) || null,
    watchtowerDir,
    now,
    eventWindowN,
  });
  return {
    schema_version: INBOX_ASSESSMENT_SCHEMA_VERSION,
    generated_at_source: 'live-read',
    reason: null,
    rings_alive: view.rings_alive,
    project: view,
  };
}

// --- Plain-English rendering (verbs + counts, NEVER decimals) ----------------

// plainState — the one-line verdict for a project, answer-first. Counts only;
// a keep ratio becomes "kept N of M", never "0.8".
export function plainState(view) {
  const k = view.kept_knowledge;
  const b = view.backlog_rot;
  const r = view.not_discarding;
  switch (view.state) {
    case 'losing':
      return `losing knowledge — the dedup may be over-suppressing new lessons (recall alert)`;
    case 'not-consumed': {
      const pile = b.count_over_30d;
      return `not being worked — ${pile} item${pile === 1 ? '' : 's'} sitting 30+ days with about no recent decisions`;
    }
    case 'too-little-data':
      return `too little to tell yet — ${b.pending_count} pending, ${k.decided_total} decision${k.decided_total === 1 ? '' : 's'} on record`;
    case 'keeping':
    default: {
      const kept = k.keep_ratio_all != null
        ? `kept ${k.resolution_mix.engaged} of ${k.decided_total} decided`
        : `${k.decided_recent} recent decision${k.decided_recent === 1 ? '' : 's'}`;
      const alertNote = r.durable_usefulness_unknown ? ' — durable usefulness unknown yet' : '';
      return `keeping knowledge — being worked, ${kept}${alertNote}`;
    }
  }
}

function renderPortfolioText(portfolio) {
  if (portfolio.reason) {
    return `Watchtower assessment: ${portfolio.reason} — nothing to assess.\n`;
  }
  const lines = [];
  lines.push('Is the watchtower working? (per project)');
  lines.push('');
  const rings = portfolio.rings_alive;
  if (rings) {
    const bad = Object.entries(rings.rings)
      .filter(([, v]) => v.state === 'down' || v.state === 'stale')
      .map(([k, v]) => `${k} ${v.state}`);
    lines.push(rings.all_ok
      ? 'Rings: all alive.'
      : `Rings: needs a look — ${bad.join(', ')}.`);
    lines.push('');
  }
  for (const v of portfolio.projects) {
    lines.push(`  ${v.name}: ${plainState(v)}`);
  }
  lines.push('');
  return lines.join('\n');
}

function renderProjectText(view) {
  const k = view.kept_knowledge;
  const b = view.backlog_rot;
  const t = view.thread_health;
  const r = view.not_discarding;
  const rings = view.rings_alive;
  const lines = [];
  lines.push(`${view.name} — ${plainState(view)}`);
  lines.push('');
  // Kept knowledge
  const recentRatio = k.keep_ratio_recent != null
    ? `kept ${k.engaged_recent} of ${k.decided_recent} recent decisions`
    : `no typed decisions in the last ${k.events_in_window} resolved item${k.events_in_window === 1 ? '' : 's'}`;
  const allRatio = k.keep_ratio_all != null
    ? `all time, kept ${k.resolution_mix.engaged} of ${k.decided_total}`
    : `all time, ${k.decided_total} typed decision${k.decided_total === 1 ? '' : 's'}`;
  lines.push(`  Kept knowledge: ${recentRatio}; ${allRatio}.`);
  // Backlog rot
  lines.push(`  Backlog: ${b.pending_count} pending, oldest ${b.oldest_age_days}d`
    + `, ${b.count_over_30d} past 30d, ${b.count_over_90d} past 90d.`);
  // Thread health
  const smells = [];
  if (t.over_eager_sessions > 0) smells.push(`${t.over_eager_sessions} over-cut session${t.over_eager_sessions === 1 ? '' : 's'}`);
  if (t.unlinked_threads > 0) smells.push(`${t.unlinked_threads} unlinked`);
  if (t.empty_cursor_threads > 0) smells.push(`${t.empty_cursor_threads} with no cursor`);
  const fresh = t.days_since_update != null ? `updated ${t.days_since_update}d ago` : 'never updated';
  lines.push(`  Threads: ${t.thread_count} active, ${fresh}${smells.length ? ` (${smells.join(', ')})` : ''}.`);
  // Rings
  if (rings) {
    const bad = Object.entries(rings.rings)
      .filter(([, v]) => v.state === 'down' || v.state === 'stale')
      .map(([key, v]) => `${key} ${v.state}`);
    lines.push(`  Rings: ${rings.all_ok ? 'all alive' : `needs a look — ${bad.join(', ')}`}.`);
  }
  // Not-discarding
  if (!r.present) {
    lines.push('  Not-discarding: no recall data yet — durable usefulness unknown.');
  } else if (r.alert) {
    lines.push('  Not-discarding: RECALL ALERT — dedup may be over-suppressing new lessons; eyeball the sample.');
  } else if (r.durable_usefulness_unknown) {
    lines.push('  Not-discarding: no alert, but durable usefulness unknown (net saved not yet computed).');
  } else {
    lines.push(`  Not-discarding: no alert; about ${r.net_durably_saved} lesson${r.net_durably_saved === 1 ? '' : 's'} durably saved.`);
  }
  lines.push('');
  return lines.join('\n');
}

// --- CLI ---------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = { mode: null, project: null, eventWindowN: DEFAULT_EVENT_WINDOW_N };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--portfolio') out.mode = 'portfolio';
    else if (a === '--project' && args[i + 1]) { out.mode = 'project'; out.project = args[++i]; }
    else if (a === '--window' && args[i + 1]) {
      const n = parseInt(args[++i], 10);
      if (Number.isInteger(n) && n > 0) out.eventWindowN = n;
    }
  }
  return out;
}

function main() {
  const { mode, project, eventWindowN } = parseArgs(process.argv);

  if (mode === 'portfolio') {
    process.stdout.write(renderPortfolioText(assemblePortfolioAssessment({ eventWindowN })));
    return;
  }

  if (mode === 'project') {
    const res = assembleProjectAssessmentByName({ project, eventWindowN });
    if (res.reason || !res.project) {
      process.stdout.write(`Watchtower assessment: ${res.reason || 'project-not-found'} — nothing to assess.\n`);
      return;
    }
    process.stdout.write(renderProjectText(res.project));
    return;
  }

  process.stderr.write(
    'usage: watchtower-inbox-assessment.mjs (--portfolio | --project <name-or-slug>) [--window <N>]\n',
  );
}

// Entry guard — importing this module must NOT run the CLI (mirrors the sibling
// readers so tests import the pure functions).
const isMain = (() => {
  try {
    return process.argv[1]
      && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
  } catch {
    return false;
  }
})();

if (isMain) {
  try {
    main();
  } catch {
    process.exit(0);
  }
}

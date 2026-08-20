#!/usr/bin/env node
/**
 * qa-debt-ledger — what QA drains have filed, and how much of it is still owed.
 *
 * Piece 2 of `.claude/plans/qa-debt-payer-build.md`. Carrier: act:cdba8618
 * ("the payer cannot pay what it cannot read, so this is upstream of it").
 *
 * THE PROBLEM THIS EXISTS FOR. Every QA drain that files a gap writes the fid
 * into its stamped verdict, and the qa-handoff skill promised for months that
 * `/briefing` re-surfaced those until they closed. Measured 2026-08-18: 425
 * filed gap entries across 265 handoffs, and NO code outside the queue library
 * ever read `qa_verdict` at all. 217 filed actions were open, the oldest ~2
 * months. This module is the reader that promise assumed.
 *
 * WHY IT IS NOT NAMED `watchtower-*`. `lib/watchtower-setup.js` copies
 * `/^watchtower-.*\.mjs$/` from templates into the GLOBAL runtime at
 * ~/.claude-cabinet/watchtower/scripts/ — a directory with no pib-db sibling
 * and no node_modules above it. A ledger that must import the pib-db library
 * would resolve there and break, and a future ring author would reasonably
 * assume anything in that directory is ring-safe. It ships with work-tracking
 * and runs from a project checkout, where the queue library and pib-db are
 * siblings. Do not rename it into the glob.
 *
 * WHAT IT IS NOT. It does not decide anything and it writes nothing. It answers
 * one question — what did drains file, and what of it is still open — and the
 * payer (`/qa-drain` Step 4) and `/briefing` both read THIS, so there is one
 * debt list rather than two that drift.
 *
 * THE UNION RULE, STATED ONCE, HERE. A verdict carries `filed_as` in FOUR
 * arrays: filed_gaps, confessed_gap.deferred, coverage_posture.gap_filed,
 * class_sweep.filed. Measured, they are NOT nested: 327 distinct fids from
 * filed_gaps alone, 397 across all four. Today's briefing reads filed_gaps only
 * and is therefore 65 fids short of reality. The ledger reads the union. (An
 * earlier draft instead required every fid to appear in filed_gaps — a
 * "superset rule" — and it was withdrawn after measurement: it changed behavior
 * on 26% of drains, silently redefined the operator-facing N in the verdict
 * label, and made 27 live `runtime-verified` verdicts inexpressible.)
 *
 * RESOLUTION IS PROJECT-SCOPED, AND THAT IS LOAD-BEARING. Fids are minted
 * per-database with no namespace, so "does this fid resolve in any tracker" is
 * a coincidence test, and there is already one live false positive (a fid filed
 * by a flow drain resolves to an unrelated action in claude-cabinet). Each
 * queue item's fids resolve ONLY against its own project's tracker.
 */

import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { createRequire } from 'node:module';

import { listItems, QA_FILED_AS_FID_RE, QA_GAP_KINDS } from './watchtower-queue.mjs';

/**
 * Projects that do not keep their work in a pib-db `pib.db`.
 *
 * KEYED ON NAME, NEVER ON "does a pib.db exist". flow HAS one — fully migrated,
 * holding 5 rows — beside the flow.db that holds its actual 2286. A
 * file-existence test admits it and reports its fids as unresolvable defects.
 * It also carries a project rule forbidding the pib.db read outright, and a
 * different status vocabulary ('active'/'completed') that a naive `!= done`
 * test would read backwards.
 */
export const EXTERNAL_TRACKER_PROJECTS = new Set(['flow']);

/** Buckets are mutually exclusive and sum to the filed total. */
export const LEDGER_BUCKETS = [
  'open',                  // resolves in the owner tracker, live, not deleted
  'closed',                // resolves, done
  'soft-deleted',          // deleted_at set — a payer must never spawn a lane for one
  'unresolvable',          // fid-shaped, no row in the OWNER tracker
  'external-tracker',      // owner project does not use pib-db
  'unregistered-project',  // project absent from the registry, or its path is gone
  'tracker-unreadable',    // db missing, unmigrated, or throws — caught PER PROJECT
];

const DEFAULT_REGISTRY = join(homedir(), '.claude', 'cc-registry.json');

/**
 * Every fid a verdict cites, deduped, in first-appearance order.
 *
 * Re-validates the anchored shape at READ time even though the write gate now
 * enforces it: the 364 verdicts stored before that gate are never re-validated,
 * and they contain inbox ids, prose, and disposition sentinels. Anything that
 * is not fid-shaped is not a lookup — it is discarded here and counted by the
 * caller, never interpolated into a query.
 */
export function citedFids(verdict) {
  if (!verdict || typeof verdict !== 'object') return [];
  const out = [];
  const seen = new Set();
  const take = (arr) => {
    if (!Array.isArray(arr)) return;
    for (const entry of arr) {
      const raw = entry && typeof entry.filed_as === 'string' ? entry.filed_as.trim() : '';
      if (!QA_FILED_AS_FID_RE.test(raw) || seen.has(raw)) continue;
      seen.add(raw);
      out.push({ fid: raw, kind: QA_GAP_KINDS.includes(entry.kind) ? entry.kind : null });
    }
  };
  take(verdict.filed_gaps);
  take(verdict.confessed_gap && verdict.confessed_gap.deferred);
  take(verdict.coverage_posture && verdict.coverage_posture.gap_filed);
  take(verdict.class_sweep && verdict.class_sweep.filed);
  return out;
}

/** Read the cc-registry into name → path. Absent registry is empty, never a throw. */
export async function loadRegistry(registryPath = DEFAULT_REGISTRY) {
  try {
    const parsed = JSON.parse(await readFile(registryPath, 'utf8'));
    const map = new Map();
    for (const p of parsed.projects || []) {
      if (p && typeof p.name === 'string' && typeof p.path === 'string') map.set(p.name, p.path);
    }
    return map;
  } catch {
    return new Map();
  }
}

/**
 * Open one project's tracker read-only.
 *
 * Read-only is not a preference. A default-flags open of a missing file CREATES
 * it (measured: a wrong PIB_DB_PATH materialized an empty pib.db + -wal + -shm
 * before failing), and the CLI's own read-only helper calls process.exit on
 * failure, which a loop cannot catch. So the ledger opens its own handle and
 * returns a typed failure instead of dying.
 */
function openTracker(dbPath) {
  try {
    const require = createRequire(import.meta.url);
    const Database = require('better-sqlite3');
    const db = new Database(dbPath, { readonly: true, fileMustExist: true });
    db.prepare('SELECT fid FROM actions LIMIT 1').get();  // unmigrated dbs throw here, not later
    return { db, error: null };
  } catch (err) {
    return { db: null, error: err.message.split('\n')[0] };
  }
}

/**
 * Assemble the ledger.
 *
 * @param {object} [opts]
 * @param {string} [opts.registryPath]
 * @param {Set<string>} [opts.externalTrackers]
 * @param {Function} [opts.listItemsFn] injectable for tests
 * @param {Function} [opts.openTrackerFn] injectable for tests
 */
export async function buildLedger(opts = {}) {
  const {
    registryPath = DEFAULT_REGISTRY,
    externalTrackers = EXTERNAL_TRACKER_PROJECTS,
    listItemsFn = listItems,
    openTrackerFn = openTracker,
  } = opts;

  const unreadableSources = [];
  const items = listItemsFn({
    category: 'qa-handoff',
    onUnreadable: (name) => unreadableSources.push(name),
  });

  // fid → {fid, kind, project, item_id, resolved_at}. First citation wins: the
  // same fid cited by two drains is ONE debt, and the earlier one owns it.
  const cited = new Map();
  let verdictsRead = 0;
  const nonFidCitations = [];
  for (const item of items) {
    const v = item.qa_verdict;
    if (!v) continue;
    verdictsRead++;
    for (const { fid, kind } of citedFids(v)) {
      if (cited.has(fid)) continue;
      cited.set(fid, {
        fid, kind,
        project: item.project || null,
        item_id: item.id,
        resolved_at: item.resolved_at || item.filed_at || null,
      });
    }
    for (const arr of [v.filed_gaps, v.confessed_gap && v.confessed_gap.deferred,
                       v.coverage_posture && v.coverage_posture.gap_filed,
                       v.class_sweep && v.class_sweep.filed]) {
      if (!Array.isArray(arr)) continue;
      for (const e of arr) {
        const raw = e && typeof e.filed_as === 'string' ? e.filed_as.trim() : '';
        if (raw && !QA_FILED_AS_FID_RE.test(raw)) nonFidCitations.push({ item_id: item.id, value: raw });
      }
    }
  }

  const registry = await loadRegistry(registryPath);
  const byProject = new Map();
  for (const rec of cited.values()) {
    const key = rec.project || '(unattributed)';
    if (!byProject.has(key)) byProject.set(key, []);
    byProject.get(key).push(rec);
  }

  const entries = [];
  const projects = {};
  for (const [project, recs] of byProject) {
    const bucketFor = (b) => recs.map((r) => ({ ...r, bucket: b, status: null, age_days: null }));

    if (externalTrackers.has(project)) {
      entries.push(...bucketFor('external-tracker'));
      projects[project] = { tracker: 'external', filed: recs.length };
      continue;
    }
    const path = registry.get(project);
    if (!path || !existsSync(path)) {
      entries.push(...bucketFor('unregistered-project'));
      projects[project] = { tracker: path ? 'path-missing' : 'unregistered', filed: recs.length };
      continue;
    }
    const { db, error } = openTrackerFn(join(path, 'pib.db'));
    if (!db) {
      entries.push(...bucketFor('tracker-unreadable'));
      projects[project] = { tracker: 'unreadable', error, filed: recs.length };
      continue;
    }
    try {
      // Bound as a parameter, after the anchored re-validation above. Raw
      // verdict text never reaches SQL.
      // `needs` is the ACTION's current answer; the verdict's kind is what the
      // drain judged at filing. The column WINS when set — it is re-judgeable,
      // the verdict is immutable provenance. Older trackers have no column at
      // all, so the read is guarded rather than assumed.
      const hasNeeds = db.prepare('PRAGMA table_info(actions)').all().some((c) => c.name === 'needs');
      const stmt = db.prepare(`SELECT fid, status, completed, created, deleted_at${hasNeeds ? ', needs' : ''} FROM actions WHERE fid = ?`);
      let open = 0, closed = 0;
      for (const rec of recs) {
        const row = stmt.get(rec.fid);
        let bucket, status = null, ageDays = null;
        if (!row) bucket = 'unresolvable';
        else if (row.deleted_at) { bucket = 'soft-deleted'; status = row.status; }
        else if (row.completed || row.status === 'done') { bucket = 'closed'; status = row.status; closed++; }
        else {
          bucket = 'open'; status = row.status; open++;
          if (row.created) ageDays = Math.round((Date.now() - Date.parse(row.created)) / 86400000);
        }
        const columnNeeds = row && row.needs ? row.needs : null;
        entries.push({
          ...rec,
          bucket, status, age_days: ageDays,
          verdict_kind: rec.kind,
          kind: columnNeeds || rec.kind,   // the column wins
          kind_source: columnNeeds ? 'column' : (rec.kind ? 'verdict' : null),
        });
      }
      projects[project] = { tracker: 'pib-db', filed: recs.length, open, closed };
    } finally {
      db.close();
    }
  }

  const counts = Object.fromEntries(LEDGER_BUCKETS.map((b) => [b, 0]));
  for (const e of entries) counts[e.bucket]++;

  // THE CONVERGENCE DETECTOR. Two homes for one fact is how this codebase's
  // dead fields were born (`sort_order` at 0 on all 361 rows; `actions.area`
  // rotted to 'dev' on 141 of 255), and every unconverged "eventually" here
  // became permanent because nothing counted it. So convergence gets a number.
  //
  //   untyped_open  — open items with no kind ANYWHERE. Before Phase 2 this is
  //                   the backlog left to type; it should fall to zero.
  //   verdict_only  — the drain judged it but the column does not carry it.
  //                   After Phase 2 (the drain writes both) this must be zero;
  //                   a non-zero value means the dual write is broken.
  //   disagreements — the two sources differ. Always worth surfacing: either
  //                   someone re-judged (legitimate) or a writer is wrong.
  //
  // Phase 3 (the ledger reads the column alone) is gated on these holding at
  // zero — not on anyone remembering. Plan: qa-debt-payer-build.md.
  const openEntries = entries.filter((e) => e.bucket === 'open');
  const convergence = {
    untyped_open: openEntries.filter((e) => !e.kind).length,
    verdict_only: openEntries.filter((e) => e.verdict_kind && e.kind_source !== 'column').length,
    disagreements: openEntries
      .filter((e) => e.verdict_kind && e.kind_source === 'column' && e.kind !== e.verdict_kind)
      .map((e) => ({ fid: e.fid, verdict: e.verdict_kind, column: e.kind })),
  };

  return {
    filed_total: entries.length,
    counts,
    convergence,
    entries,
    projects,
    // Honesty channels. Each is its own outcome, never folded into a bucket and
    // never silently zero: an empty result must say WHICH sources were readable.
    sources: {
      qa_handoff_items: items.length,
      verdicts_read: verdictsRead,
      unreadable_queue_files: unreadableSources.length,
      non_fid_citations: nonFidCitations.length,
    },
  };
}

/** The one-line ledger the payer prints. Never says "owed". */
export function renderLedgerLine(ledger) {
  const open = ledger.counts.open;
  const oldest = ledger.entries.reduce((m, e) => (e.age_days != null && e.age_days > m ? e.age_days : m), 0);
  if (!open) return `Filed by drains, still open: none (${ledger.filed_total} filed all-time).`;
  return `Filed by drains, still open: ${open}, oldest ${oldest}d (of ${ledger.filed_total} filed all-time).`;
}

const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].split('/').pop());
if (isMain) {
  const ledger = await buildLedger();
  if (process.argv.includes('--json')) {
    console.log(JSON.stringify(ledger, null, 2));
  } else {
    console.log(renderLedgerLine(ledger));
    console.log('');
    for (const [b, n] of Object.entries(ledger.counts)) if (n) console.log(`  ${b.padEnd(22)} ${n}`);
    console.log('');
    for (const [p, info] of Object.entries(ledger.projects)) {
      console.log(`  ${p.padEnd(26)} ${info.tracker.padEnd(14)} filed ${info.filed}${info.open != null ? `, open ${info.open}` : ''}${info.error ? ` — ${info.error}` : ''}`);
    }
    const s = ledger.sources;
    const c = ledger.convergence;
    console.log(`\n  convergence: ${c.untyped_open} open untyped, ${c.verdict_only} verdict-only, ${c.disagreements.length} disagreeing`);
    console.log(`  sources: ${s.qa_handoff_items} qa-handoff items, ${s.verdicts_read} with a verdict, ${s.unreadable_queue_files} unreadable, ${s.non_fid_citations} non-fid citations`);
  }
}

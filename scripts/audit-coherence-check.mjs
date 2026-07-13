#!/usr/bin/env node
// Audit persistence coherence check (workflow-cop-0001, act:faf23e4c).
//
// The failure mode: a session runs /audit, creates a reviews/<date>/<time>/
// run directory, but the orchestrator dies (or forgets phase 5) before the
// findings are ingested into pib-db — so the run directory exists on disk but
// there is no matching `audit_runs` row. Suppression stays permanently empty
// and the enforcement-pipeline promotion signal is blind. This detector is the
// single home (the /validate `audit-coherence` validator) that surfaces it.
//
// Match rule: merge-findings.js keys a run as `run-<basename(runDir)>`, i.e.
// the leaf directory name (the HH-MM-SS component). So a reviews dir
// `reviews/<date>/<time>/` is "ingested" iff `audit_runs` holds id
// `run-<time>`. We flag the NEWEST reviews dir only when it has no matching
// row — that is the actionable "the audit you just ran did not persist"
// signal, and it does not cry wolf over the many pre-persistence historical
// dirs (audit_runs was empty until 2026-07-03).
//
// Usage:
//   node scripts/audit-coherence-check.mjs           # check + warn (exit 0)
// The check is a WARNING, not a gate: a reviews dir newer than the latest run
// may be an audit still in progress, so failing /validate would cry wolf.

import { readdirSync, existsSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

// ---------------------------------------------------------------------------
// Pure core (unit-tested): given the reviews run dirs and the set of known
// audit_runs ids, decide whether the newest run dir was ingested.
// ---------------------------------------------------------------------------

/**
 * @param {Array<{date:string,time:string}>} reviewDirs leaf run dirs
 * @param {Iterable<string>} auditRunIds  audit_runs.id values
 * @returns {{coherent:boolean, uningested:Array<{date:string,time:string,expectedId:string}>}}
 */
export function assessRunCoherence(reviewDirs, auditRunIds) {
  const ids = auditRunIds instanceof Set ? auditRunIds : new Set(auditRunIds || []);
  if (!Array.isArray(reviewDirs) || reviewDirs.length === 0) {
    return { coherent: true, uningested: [] };
  }
  // newest first by (date, time) — both are zero-padded and lexically sortable
  const sorted = [...reviewDirs].sort((a, b) =>
    `${b.date}/${b.time}`.localeCompare(`${a.date}/${a.time}`));
  const newest = sorted[0];
  // Run ids are date-full since act:4ec70792 (run-<date>-<time>, minted by
  // merge-findings); rows ingested before the change keep the legacy
  // run-<time> shape, so both spellings count as ingested.
  const expectedId = `run-${newest.date}-${newest.time}`;
  const legacyId = `run-${newest.time}`;
  if (ids.has(expectedId) || ids.has(legacyId)) {
    return { coherent: true, uningested: [] };
  }
  return { coherent: false, uningested: [{ date: newest.date, time: newest.time, expectedId }] };
}

// ---------------------------------------------------------------------------
// CLI wiring (fail-open: any IO/db error → skip silently, exit 0)
// ---------------------------------------------------------------------------

function readReviewDirs(reviewsRoot) {
  // reviews/<date>/<time>/ — return the leaf run dirs as {date,time}
  const out = [];
  for (const date of readdirSync(reviewsRoot)) {
    const dateDir = join(reviewsRoot, date);
    let s;
    try { s = statSync(dateDir); } catch { continue; }
    if (!s.isDirectory()) continue;
    for (const time of readdirSync(dateDir)) {
      let ts;
      try { ts = statSync(join(dateDir, time)); } catch { continue; }
      if (ts.isDirectory()) out.push({ date, time });
    }
  }
  return out;
}

function readAuditRunIds(scriptsDir) {
  // Reuse the pib-db CLI (it owns path resolution + native-module ABI heal).
  const pibDb = join(scriptsDir, 'pib-db.mjs');
  if (!existsSync(pibDb)) return null; // no pib-db → fail open
  const raw = execFileSync('node', [pibDb, 'query', 'SELECT id FROM audit_runs'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'],
  });
  const rows = JSON.parse(raw);
  return new Set(rows.map(r => r.id));
}

function main() {
  const scriptsDir = dirname(fileURLToPath(import.meta.url));
  const reviewsRoot = 'reviews';
  if (!existsSync(reviewsRoot)) {
    console.log('audit-coherence: no reviews/ directory — skipping');
    return 0;
  }
  let reviewDirs, ids;
  try {
    reviewDirs = readReviewDirs(reviewsRoot);
    ids = readAuditRunIds(scriptsDir);
  } catch (e) {
    console.log(`audit-coherence: skipped (${e.message})`);
    return 0;
  }
  if (ids === null) {
    console.log('audit-coherence: no pib-db — skipping');
    return 0;
  }
  const { coherent, uningested } = assessRunCoherence(reviewDirs, ids);
  if (coherent) {
    console.log('audit-coherence: latest reviews/ run has a matching audit_runs row.');
    return 0;
  }
  const u = uningested[0];
  console.log('');
  console.log(`⚠ audit-coherence: reviews/${u.date}/${u.time}/ has no matching audit_runs row`);
  console.log(`  (expected id "${u.expectedId}"). An /audit run reached the run directory but`);
  console.log('  its findings were never ingested — suppression stays blind until they are.');
  console.log('  If the run is complete, ingest it: node scripts/merge-findings.js '
    + `reviews/${u.date}/${u.time} --db`);
  return 0; // warning, not a gate
}

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exit(main());
}

#!/usr/bin/env node
// Merge cabinet member JSON outputs into a unified run-summary.json.
//
// Usage:
//   node scripts/merge-findings.js <run-dir>            # Merge only
//   node scripts/merge-findings.js <run-dir> --db       # Merge + ingest to pib-db
//
// Reads all *.json files in <run-dir> (one per cabinet member), validates
// against finding-schema.json, deduplicates by finding ID, and writes
// run-summary.json with merged findings and metadata.
//
// When the deliberative-audit workflow ran, <run-dir>/deliberation-report.json
// carries the Stage-2 outcome (annotations + challenged/upheld status) that the
// per-member files — written by each Stage-1 agent BEFORE the critics ran —
// cannot have. This script UNIONS the two (act:8e1fa16f): per-member files are
// the base, the report overlays its verdicts, and anything present on only one
// side is kept. See "Deliberation overlay" below for why neither side alone is
// sufficient.
//
// Environment:
//   PIB_DB_PATH  — path to SQLite file (for --db mode)

import { readdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join, dirname, basename, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
// deliberation-report.json is written by the deliberative-audit workflow
// itself (act:faf23e4c) — it is the ranked+annotated salvage artifact, not a
// per-member finding file; skip it in the member scan so it is not re-read as
// a member. It is read separately, by name, as the deliberation overlay
// (act:8e1fa16f) — skipping it here is NOT the same as ignoring it.
const DELIBERATION_FILE = 'deliberation-report.json';
const SKIP_FILES = new Set(['run-summary.json', 'day-summary.json', 'layer1-results.json', 'triage.json', DELIBERATION_FILE]);

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
const runDir = args.find(a => !a.startsWith('--'));
const useDb = args.includes('--db');

if (!runDir) {
  console.log(`Usage: merge-findings.js <run-dir> [--db]

Merges per-cabinet-member JSON files into run-summary.json.
  --db    Also ingest findings into the pib-db database.`);
  process.exit(1);
}

if (!existsSync(runDir)) {
  console.error(`Directory not found: ${runDir}`);
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Read and merge
// ---------------------------------------------------------------------------
const files = readdirSync(runDir).filter(f =>
  f.endsWith('.json') && !SKIP_FILES.has(f)
);

if (files.length === 0) {
  console.error(`No cabinet member JSON files found in ${runDir}`);
  process.exit(1);
}

const allFindings = [];
const byId = new Map();
const memberCounts = {};
const severityCounts = { critical: 0, warn: 0, info: 0, idea: 0 };
let positiveCount = 0;

// One accounting path for every finding that enters the merged set, whether it
// arrived from a per-member file or from the deliberation overlay below — so an
// overlay-only finding can never land in `findings` while missing from `counts`.
function addFinding(f, member) {
  if (byId.has(f.id)) return false;
  byId.set(f.id, f);
  allFindings.push(f);

  if (f.type === 'positive') {
    positiveCount++;
  } else {
    severityCounts[f.severity] = (severityCounts[f.severity] || 0) + 1;
    memberCounts[member] = (memberCounts[member] || 0) + 1;
  }
  return true;
}

for (const file of files) {
  try {
    const data = JSON.parse(readFileSync(join(runDir, file), 'utf-8'));
    const findings = data.findings || [];
    const member = data.meta?.['cabinet-member'] || basename(file, '.json');

    for (const f of findings) addFinding(f, member);

    console.log(`  ${member}: ${findings.length} findings`);
  } catch (err) {
    console.error(`  Error reading ${file}: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// Deliberation overlay (act:8e1fa16f)
// ---------------------------------------------------------------------------
// The two artifacts in a run dir are complementary, and merging either one
// alone loses real work:
//
//   per-member <member>.json — written by each Stage-1 agent the moment it
//     finished, BEFORE any critic ran. Guaranteed to exist even if the
//     orchestrator dies, but carries no annotations and no status. It can hold
//     findings the workflow itself dropped (a member whose returned text failed
//     to parse still wrote its file).
//   deliberation-report.json — the workflow's own ranked+annotated set. Holds
//     the entire Stage-2/3 outcome, and can hold findings whose per-member
//     Write failed. Excludes findings withdrawn in rebuttal (carried
//     separately as `withdrawnFindings`).
//
// So: union. Base = per-member files; the report overlays its verdict fields
// onto matching ids and contributes any finding the base is missing. Merging
// only the base was the original defect — it silently discarded 85 annotations
// and 16 challenges from a production-readiness audit and reported success.
const reportPath = join(runDir, DELIBERATION_FILE);
const deliberationPresent = existsSync(reportPath);
let reportAnnotated = 0;

if (deliberationPresent) {
  let report;
  try {
    report = JSON.parse(readFileSync(reportPath, 'utf-8'));
  } catch (err) {
    // A present-but-unreadable report is precisely the silent-loss shape this
    // guard exists for: merging past it would produce a clean-looking summary
    // with the whole deliberation missing. Refuse.
    console.error(`\n✗ ${DELIBERATION_FILE} is present but unreadable: ${err.message}`);
    console.error('  Refusing to merge — the Stage-2 deliberation would be silently dropped.');
    console.error('  Fix or remove the report, then re-run.');
    process.exit(1);
  }

  const reportFindings = [
    ...(Array.isArray(report.findings) ? report.findings : []),
    ...(Array.isArray(report.withdrawnFindings) ? report.withdrawnFindings : []),
  ];

  if (reportFindings.length === 0) {
    console.error(`\n✗ ${DELIBERATION_FILE} is present but carries no findings array.`);
    console.error('  Refusing to merge — the Stage-2 deliberation would be silently dropped.');
    process.exit(1);
  }

  reportAnnotated = reportFindings.filter(f => f.annotations && f.annotations.length > 0).length;

  let overlaid = 0;
  let added = 0;
  const addedIds = [];
  for (const rf of reportFindings) {
    if (!rf || !rf.id) continue;
    const base = byId.get(rf.id);
    if (base) {
      // Only the deliberation-owned fields cross over. Everything the Stage-1
      // member authored stays as that member wrote it.
      if (rf.annotations) base.annotations = rf.annotations;
      if (rf.status) base.status = rf.status;
      if (rf.rebuttal) base.rebuttal = rf.rebuttal;
      if (rf.triageParked) base.triageParked = rf.triageParked;
      overlaid++;
    } else if (addFinding(rf, rf['cabinet-member'] || 'unknown')) {
      added++;
      addedIds.push(rf.id);
    }
  }

  const mergedAnnotated = allFindings.filter(f => f.annotations && f.annotations.length > 0).length;

  console.log(`\nDeliberation overlay: ${overlaid} finding(s) matched ${DELIBERATION_FILE} ` +
    `(${reportAnnotated} carry annotations)` +
    (added > 0 ? `; ${added} present only in the report and added (${addedIds.join(', ')})` : ''));

  // Post-condition. The union above makes annotation loss structurally
  // impossible, so this should never fire — which is exactly why it stays: the
  // defect being fixed was a merge that lost everything and reported success.
  // A silent drop must become a loud stop, not a quieter number.
  if (mergedAnnotated < reportAnnotated) {
    console.error(`\n✗ Annotation loss: ${DELIBERATION_FILE} carries ${reportAnnotated} annotated ` +
      `finding(s) but the merged set carries only ${mergedAnnotated}.`);
    console.error('  Refusing to write run-summary.json — the Stage-2 deliberation is the point ' +
      'of the deliberative path.');
    process.exit(1);
  }

  if (reportAnnotated === 0) {
    console.warn(`\n⚠ ${DELIBERATION_FILE} is present but carries ZERO annotations — Stage 2 ` +
      'produced no critique.');
    console.warn('  This is legitimate only if no critics were selected or every critic stayed ' +
      'silent. If critics ran, the run is incomplete.');
  }
}

// ---------------------------------------------------------------------------
// Write run-summary.json
// ---------------------------------------------------------------------------
const timestamp = new Date().toISOString();
// Date-full run ids (act:4ec70792): a bare run-<HH-MM-SS> id collides with a
// same-clock-second run on another day. In the canonical reviews/<date>/<time>
// layout the parent dir carries the date — fold it in. Any other layout keeps
// the leaf-name fallback.
const runParent = basename(dirname(resolve(runDir)));
const runId = /^\d{4}-\d{2}-\d{2}$/.test(runParent)
  ? `run-${runParent}-${basename(runDir)}`
  : `run-${basename(runDir)}`;

const meta = {
  runId,
  timestamp,
  trigger: 'manual',
  members: Object.keys(memberCounts),
  counts: {
    total: allFindings.length,
    findings: allFindings.length - positiveCount,
    positive: positiveCount,
    ...severityCounts,
  },
  byMember: memberCounts,
};

// Reachable through the documented flow since act:8e1fa16f (the overlay above
// is what puts annotations on the merged findings). It also fires for a run dir
// whose per-member files were annotated by hand, and for a deliberative run
// whose critics all stayed silent — a Stage 2 that ran and found nothing is
// still a fact worth recording, so the report's presence alone is enough.
if (deliberationPresent || allFindings.some(f => f.annotations && f.annotations.length > 0)) {
  meta.deliberation = {
    annotatedCount: allFindings.filter(f => f.annotations && f.annotations.length > 0).length,
    challengedCount: allFindings.filter(f => f.status === 'challenged' || f.status === 'rebutted').length,
    upheldCount: allFindings.filter(f => f.status === 'upheld').length,
    withdrawnCount: allFindings.filter(f => f.status === 'withdrawn').length,
    modifiedCount: allFindings.filter(f => f.status === 'modified').length,
  };
}

const summary = { findings: allFindings, meta };

const summaryPath = join(runDir, 'run-summary.json');
writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
console.log(`\nMerged ${allFindings.length} findings → ${summaryPath}`);
console.log(`  critical: ${severityCounts.critical}, warn: ${severityCounts.warn}, info: ${severityCounts.info}, idea: ${severityCounts.idea}, positive: ${positiveCount}`);
if (meta.deliberation) {
  const d = meta.deliberation;
  console.log(`  deliberation: ${d.annotatedCount} annotated, ${d.challengedCount} challenged, ` +
    `${d.upheldCount} upheld, ${d.withdrawnCount} withdrawn, ${d.modifiedCount} modified`);
}

// ---------------------------------------------------------------------------
// Optional: ingest to pib-db
// ---------------------------------------------------------------------------
if (useDb) {
  try {
    const pibDb = join(__dirname, 'pib-db.mjs');
    execSync(`node "${pibDb}" ingest-findings "${runDir}"`, { stdio: 'inherit' });
  } catch (err) {
    console.error(`DB ingest failed: ${err.message}`);
    console.error('Findings are still saved in run-summary.json.');
  }
}

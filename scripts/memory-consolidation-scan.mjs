#!/usr/bin/env node

// One-time, human-gated memory consolidation scan (act:421a8ab2, N5 of
// grp:retro-remeaning). Feeds the `consolidate` verb designed in
// memory-lifecycle-contract.md (Stage 2, gated on spill-rate data — not yet
// built): this scan IS the evidence that verb has real work to do. Reuses
// the (now overlap-coefficient, dual-scored) fold detector over an ACTUAL
// memory directory's files — a DIFFERENT corpus than proposeFolds' usual
// caller (the pending inbox queue), same recipe, since a memory file shaped
// as {id, title, draft_artifact} fits the existing function exactly.
//
// PROPOSES candidate consolidation pairs; never merges, never writes to the
// memory dir. A pure read + report — the operator reviews and decides by
// hand. Each candidate carries a `consolidated_from` pointer (both sides'
// filenames) so a future consolidate-verb build has a lossless provenance
// shape to start from, per the contract's "always keeps a lossless
// consolidated_from: provenance pointer" requirement.
//
// CORPUS-SCALE CALIBRATION (empirical, from the first real run against the
// maginnis memory dir, 1074 files): FOLD_SIMILARITY_THRESHOLD (0.22) is
// calibrated for the INBOX's small pending pile, where an O(n²) compare is
// dozens of pairs. Over a 1000+-file memory corpus the same bar produced
// 114,640 candidates — 98% in the 0.22-0.49 band, overwhelmingly
// coincidental domain-vocabulary overlap between UNRELATED entries, not
// duplicates. CONSOLIDATION_THRESHOLD (0.8) is the corpus-scale bar,
// verified against that same run: >=0.8 yielded 102 candidates, and a
// manual read of every one found them either exact bulk-write/curated
// twins (the wtx_* outage-recovery duplicates this whole program targets)
// or genuine same-topic restatements — zero false positives observed at
// that tier. This is retrieval, not adjudication (the operator still
// confirms every proposal) — the threshold exists so the proposal LIST
// itself is reviewable, not to hide anything: pass `threshold` to widen it.

import { readFileSync, readdirSync, existsSync, writeFileSync } from 'fs';
import { join, basename } from 'path';
import { pathToFileURL } from 'url';
import { proposeFolds } from './watchtower-queue.mjs';

export const CONSOLIDATION_THRESHOLD = 0.8;

// The index files themselves are never candidates.
const NON_MEMORY_FILES = new Set(['MEMORY.md', 'MEMORY-archive.md']);

// session_*/session_summary_* files are NARRATIVE HISTORY (what happened,
// in what order), not atomic memory claims — they share a lot of generic
// procedural vocabulary ("investigation", "lane", "audit", "remediation")
// purely from being the SAME KIND of record, which produced systematic
// false positives in the same calibration run (e.g. two unrelated sessions
// about different topics scoring 0.80 on shared procedural words alone).
// Session-summary consolidation is a DIFFERENT lifecycle path already
// (memory-lifecycle-contract.md's "Cross-store fold", to a committed
// methodology gist) — excluded here by default; pass
// `{ includeSessionFiles: true }` for the raw, unfiltered view.
function isSessionNarrativeFile(name) {
  return /^session[_-]/.test(name);
}

// Same title-extraction rule as loadRegionPointerTitles in
// watchtower-ring3-close.mjs (act:421a8ab2) — a memory file written by
// writeMemoryFile can carry two H1 headers (a generic slug-title, then the
// real descriptive one from embedded draft_artifact content); the LAST
// header found near the top is the more descriptive of the two.
function extractTitle(content, maxLines = 10) {
  if (typeof content !== 'string') return null;
  let last = null;
  for (const line of content.split('\n').slice(0, maxLines)) {
    const m = line.match(/^#\s+(.+)/);
    if (m) last = m[1].trim();
  }
  return last;
}

/**
 * Scan a memory directory for fold candidates. Pure read — no writes.
 * Fails open: a missing directory or an unreadable file degrades the scan,
 * never throws.
 * @param {string} memDir
 * @param {object} [opts]
 * @param {number} [opts.threshold] - defaults to CONSOLIDATION_THRESHOLD
 *   (corpus-scale), NOT proposeFolds' inbox-scale default.
 * @param {boolean} [opts.includeSessionFiles] - include session-narrative
 *   files (session_ / session_summary_ prefixed; excluded by default —
 *   see the header comment for why).
 * @returns {{memDir: string, files_scanned: number, candidates: Array<{a: string, b: string, similarity: number, consolidated_from: string[]}>}}
 */
export function scanMemoryDir(memDir, { threshold = CONSOLIDATION_THRESHOLD, includeSessionFiles = false } = {}) {
  if (!memDir || !existsSync(memDir)) {
    return { memDir, files_scanned: 0, candidates: [] };
  }
  let files;
  try {
    files = readdirSync(memDir).filter((f) => f.endsWith('.md') && !NON_MEMORY_FILES.has(f)
      && (includeSessionFiles || !isSessionNarrativeFile(f)));
  } catch {
    return { memDir, files_scanned: 0, candidates: [] };
  }
  const items = [];
  for (const f of files) {
    try {
      const content = readFileSync(join(memDir, f), 'utf8');
      const title = extractTitle(content) || basename(f, '.md');
      items.push({ id: f, title, draft_artifact: content });
    } catch {
      // one unreadable file skips, never aborts the scan
    }
  }
  const pairs = proposeFolds(items, { threshold });
  const candidates = pairs
    .map(({ a, b, similarity }) => ({ a, b, similarity, consolidated_from: [a, b].sort() }))
    .sort((x, y) => y.similarity - x.similarity);
  return { memDir, files_scanned: items.length, candidates };
}

// --- CLI: node memory-consolidation-scan.mjs <memDir> [--out report.json]
//          [--threshold N] [--include-sessions] ---

const isMain = (() => {
  try {
    return process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
  } catch {
    return false;
  }
})();

if (isMain) {
  const args = process.argv.slice(2);
  const memDir = args[0];
  const outIdx = args.indexOf('--out');
  const outPath = outIdx >= 0 ? args[outIdx + 1] : null;
  const thresholdIdx = args.indexOf('--threshold');
  const threshold = thresholdIdx >= 0 ? parseFloat(args[thresholdIdx + 1]) : undefined;
  const includeSessionFiles = args.includes('--include-sessions');

  if (!memDir || memDir.startsWith('-')) {
    process.stderr.write('usage: memory-consolidation-scan.mjs <memory-dir> [--out report.json] [--threshold N] [--include-sessions]\n');
    process.exit(2);
  }

  const result = scanMemoryDir(memDir, { threshold, includeSessionFiles });
  console.log(`Scanned ${result.files_scanned} memory file(s) in ${result.memDir}`);
  console.log(`${result.candidates.length} consolidation candidate(s) — PROPOSALS ONLY, nothing merged or written:`);
  for (const c of result.candidates) {
    console.log(`  ${c.similarity.toFixed(3)}  ${c.a}  <->  ${c.b}`);
  }
  if (outPath) {
    writeFileSync(outPath, JSON.stringify(result, null, 2) + '\n');
    console.log(`\nWrote full report to ${outPath}`);
  }
}

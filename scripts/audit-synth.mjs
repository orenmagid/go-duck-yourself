// Pure mechanical-merge helpers for the deliberative-audit synthesizer.
//
// The deliberative-audit.js workflow runs in the Workflow sandbox (no
// require/import, no filesystem), so it INLINES these functions. This module
// is the tested source-of-truth mirror; a parity test asserts the workflow's
// constants and behavior markers match. Keep the two in lockstep.
//
// Why this exists: at 60+ findings the old single-call synthesizer hung
// (act:3f2428b0). The fix is mechanical — findings pass through untouched,
// are ranked deterministically, and prose runs over bounded chunks so no
// single agent call ever carries the full set. These helpers are that
// mechanical core, split out so a synthetic large-input harness can prove the
// ranking/chunking without spawning the workflow.

export const SEVERITY_RANK = { critical: 0, warn: 1, info: 2, idea: 3 };
export const SYNTH_CHUNK_SIZE = 12;
// Unknown/missing severity sorts here (below 'idea') AND is logged — a
// non-canonical severity is a fail-toward-visibility signal, not a silent
// default (boundary-man-0002). Callers that want it treated as 'warn' pass a
// logger; the sort position is deliberately last so an unlabeled finding is
// never hidden among the worst.
export const UNKNOWN_SEVERITY_RANK = 4;

export const isContested = f => f.status !== 'upheld'; // challenged / rebutted / modified

// A finding's effective severity for ranking. Unknown → 'warn' (fail toward
// visibility), and the caller's onUnknown callback is invoked with the id so
// the drop is logged, never silent (boundary-man-0002).
export function effectiveSeverity(f, onUnknown) {
  const sev = f && f.severity;
  if (sev != null && Object.prototype.hasOwnProperty.call(SEVERITY_RANK, sev)) {
    return sev;
  }
  if (typeof onUnknown === 'function') onUnknown(f);
  return 'warn';
}

// A critic severity-suggestion, when present, overrides the finding's own
// severity in the rank key (boundary-man-0005). Annotations carry
// 'severity-suggestion'; the strongest (lowest-rank) suggestion wins.
export function suggestedSeverity(f) {
  if (!f || !Array.isArray(f.annotations)) return null;
  let best = null;
  let bestRank = Infinity;
  for (const a of f.annotations) {
    const s = a && a['severity-suggestion'];
    if (s != null && Object.prototype.hasOwnProperty.call(SEVERITY_RANK, s)) {
      if (SEVERITY_RANK[s] < bestRank) { bestRank = SEVERITY_RANK[s]; best = s; }
    }
  }
  return best;
}

// Rank surviving findings: by severity tier (critic suggestion overrides the
// finding's own; unknown → 'warn' + logged), then contested-before-confirmed,
// then original order. Stable and deterministic.
export function rankFindings(survivingFindings, opts = {}) {
  const onUnknown = opts.onUnknown;
  const rankOf = f => {
    const suggestion = suggestedSeverity(f);
    const sev = suggestion != null ? suggestion : effectiveSeverity(f, onUnknown);
    return SEVERITY_RANK[sev] ?? UNKNOWN_SEVERITY_RANK;
  };
  return survivingFindings
    .map((f, i) => ({ f, i, r: rankOf(f) }))
    .sort((a, b) => {
      if (a.r !== b.r) return a.r - b.r;
      const conA = isContested(a.f) ? 0 : 1;
      const conB = isContested(b.f) ? 0 : 1;
      if (conA !== conB) return conA - conB;
      return a.i - b.i;
    })
    .map(x => x.f);
}

// Split ranked findings into bounded chunks so no single synthesizer call
// carries the whole set.
export function chunkFindings(ranked, size = SYNTH_CHUNK_SIZE) {
  const chunks = [];
  for (let i = 0; i < ranked.length; i += size) {
    chunks.push(ranked.slice(i, i + size));
  }
  return chunks;
}

// Assemble the chunk-coverage result. Stage 4 keeps {idx,summary} pairs
// through the filter so a partial failure (e.g. 3 chunks, only chunk-2
// returns) is not conflated with "one chunk existed" (boundary-man-0001).
// Returns the surviving summaries in order plus, when some chunks produced no
// summary, an explicit caveat naming the missing chunk numbers.
export function assembleCoverage(chunkResults, chunkCount) {
  const pairs = (chunkResults || [])
    .map((r, idx) => ({ idx, summary: r && r.summary }))
    .filter(p => p.summary);
  const summaries = pairs.map(p => p.summary);
  let caveat = null;
  if (pairs.length < chunkCount) {
    const present = new Set(pairs.map(p => p.idx));
    const missing = [];
    for (let i = 0; i < chunkCount; i++) if (!present.has(i)) missing.push(i + 1);
    caveat = `Coverage caveat: ${pairs.length}/${chunkCount} synthesis chunks returned a `
      + `summary; chunk(s) ${missing.join(', ')} produced none — those findings are carried `
      + `in full mechanically but are unsummarized.`;
  }
  return { summaries, caveat, covered: pairs.length, total: chunkCount };
}

// Run-level triage budget (cabinet-organized-mind-0004): surface the top-N
// most-severe findings for triage while KEEPING the remainder (still ranked,
// still persisted, just parked). Returns { top, parked }. Non-positive or
// absent N → everything is "top" (no cap).
export function applyRunBudget(ranked, n) {
  if (!Number.isFinite(n) || n <= 0 || ranked.length <= n) {
    return { top: ranked, parked: [] };
  }
  return { top: ranked.slice(0, n), parked: ranked.slice(n) };
}

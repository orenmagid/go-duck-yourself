#!/usr/bin/env node

// Watchtower narrative corpus — the read-only substrate for the /briefing
// "decision lineage" drill-down (Plan 12, act:a2efc0ce).
//
// It answers, for ONE project: "what decisions does this project's history
// hold, and which causal edges are RECORDED vs merely associated?" — the raw
// material the /briefing drill-down synthesizes into the answer-first render
// ("what still constrains this project / is X reversible").
//
// This is a PROJECTION over existing stores, never a fifth store. It writes
// NOTHING: no state file, no inbox item, no memory write. The read-only
// invariant is structural — there is no write path in this module, and the
// hermetic test asserts the filesystem is untouched.
//
// Deliberately a SIBLING of watchtower-snapshot.mjs (same reader family, same
// hermetic-test shape), not folded into any ring. It covers the two
// watchtower-NATIVE stores — the inbox queue and threads — because those are
// the ones it can read without the auto-memory-dir resolver (which ships in
// the memory module and is a documented footgun: project-context.cjs throws on
// a literal `~`, honors settings.json, dashifies the slug). The memory
// `decision_*.md` files, methodology, and pib-db are read by the /briefing
// CONSUMER, which resolves those paths itself (the "punt to the consumer"
// decision). To keep the EDGE rule testable despite that punt, the consumer
// hands this module a memory DIRECTORY (already resolved) and this module reads
// the decision files from it and parses edges with one tested rule
// (parseDecisionEdges) — so the consumer owns path resolution, this module owns
// the parsing.
//
// CLI modes (additive; reads only):
//   --corpus --project-path <cwd>
//       Resolve the project the cwd belongs to (worktree-aware) and print the
//       inbox+threads corpus JSON.
//   --memory <dir>
//       Read decision_*.md from <dir> (the consumer-resolved memory dir) and
//       print each decision's title/date/edges JSON.
//
// Never throws on a missing/partial store — readers degrade to empty.

import { readFileSync, existsSync, readdirSync, realpathSync } from 'fs';
import { join } from 'path';
import { pathToFileURL } from 'url';
import {
  loadActiveThreads,
  threadMatchesProject,
  resolveProjectIdentity,
} from './watchtower-lib.mjs';
import { listItems } from './watchtower-queue.mjs';
// The 4-field cursor projection has ONE definition, in the cross-ring reader
// (a sibling non-ring-loaded file — watchtower-lib is soak-frozen). Both
// readers project the same shape; this import is what keeps them from
// drifting.
import { projectCursorTimeline } from './watchtower-cross-ring-reader.mjs';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME || '', '.claude-cabinet', 'watchtower');

export const NARRATIVE_CORPUS_SCHEMA_VERSION = 1;

// Decisions live in the inbox under this category (Ring 3's knowledge
// extraction files them here). Constraints/lessons/preferences ride the same
// category, distinguished by evidence.type — the consumer leads with
// decision+constraint for the "what still constrains this" answer.
const DECISION_CATEGORY = 'knowledge-extraction';

// A resolved knowledge-extraction item with this resolution_type WAS written
// into a memory decision_*.md on sign-off — reading both the inbox item and the
// memory file would double-count the SAME decision (and the inbox copy is the
// stale draft). Excluded here; the memory file is the canonical copy the
// consumer reads. (data-integrity Finding 1 / Edge Cases.)
const CAPTURED_TO_MEMORY = 'captured-to-memory';

// Inbox statuses worth narrating. `superseded` is INCLUDED deliberately — a
// decision that was later replaced is exactly what the supersession view shows;
// excluding it (the obvious ['pending','resolved']) would drop the feature's
// own subjects. (boundary-man Finding 2 / Edge Cases.)
const NARRATED_STATUSES = ['pending', 'resolved', 'superseded'];

function safeReadJSON(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

// --- Edge parsing (pure, tested) ---------------------------------------------

// parseDecisionEdges — extract causal edges from one memory decision file's
// markdown, with PROVENANCE. The distinction is load-bearing: a narrative is a
// trust surface, so a RECORDED edge (an explicit `**Supersedes:**` line) must
// never be conflated with a mere see-also `[[wikilink]]`. A wikilink is an
// undirected association, NOT a supersession — rendering it as one fabricates
// history. Returns { supersedes: [{target, provenance:'recorded'}],
// seeAlso: [{target, kind:'see-also'}] }. Pure: text in, edges out.
export function parseDecisionEdges(markdown) {
  const text = typeof markdown === 'string' ? markdown : '';
  const supersedes = [];
  const supersedeTargets = new Set();

  // `**Supersedes:** [[decision_x]]` or `**Supersedes:** decision_x, decision_y`
  // — the recorded, directional edge. Take the rest of the line after the
  // marker and pull targets. A BRACKETED `[[...]]` target is the author's
  // explicit "this is a link target" signal — accept it verbatim. A BARE token
  // must look like a memory slug or a fid to qualify: the bare branch exists
  // for `**Supersedes:** decision_x`, but a naive whitespace split would also
  // accept prose ("decision_x because it was wrong" → because/it/was/wrong),
  // FABRICATING recorded supersession from English. A narrative is a trust
  // surface, so the bare branch admits only slug/fid-shaped tokens.
  const reSupersede = /\*\*Supersedes:\*\*\s*(.+)/gi;
  let m;
  while ((m = reSupersede.exec(text)) !== null) {
    const tail = m[1];
    const targets = [];
    const bracket = /\[\[([^\]]+)\]\]/g;
    let b;
    while ((b = bracket.exec(tail)) !== null) {
      const t = b[1].trim();
      if (t) targets.push(t); // bracketed = explicit intent (incl. titles)
    }
    if (targets.length === 0) {
      // No bracketed targets — accept ONLY slug/fid-shaped bare tokens.
      for (const tok of tail.split(/[,\s]+/)) {
        const t = tok.trim().replace(/[.,;]+$/, '');
        if (t && looksLikeDecisionTarget(t)) targets.push(t);
      }
    }
    for (const t of targets) {
      if (!supersedeTargets.has(t)) {
        supersedeTargets.add(t);
        supersedes.push({ target: t, provenance: 'recorded' });
      }
    }
  }

  // All OTHER `[[wikilinks]]` are see-also associations — explicitly NOT
  // supersedes. Exclude any already claimed as a supersede target so the same
  // link isn't double-counted in both buckets. Empty/whitespace targets (a
  // malformed `[[ ]]`) are dropped.
  const seeAlso = [];
  const seen = new Set();
  const reLink = /\[\[([^\]]+)\]\]/g;
  let l;
  while ((l = reLink.exec(text)) !== null) {
    const target = l[1].trim();
    if (!target || supersedeTargets.has(target) || seen.has(target)) continue;
    seen.add(target);
    seeAlso.push({ target, kind: 'see-also' });
  }

  return { supersedes, seeAlso };
}

// A bare (unbracketed) supersede target must look like a memory slug
// (`decision_…`, `lesson_…`, etc.) or a fid (`act:abc12345`, `dec-…`) — NOT an
// English word — so prose after a `**Supersedes:**` marker can't masquerade as
// a recorded edge.
function looksLikeDecisionTarget(token) {
  return /^(?:decision|lesson|constraint|preference|pattern)_[a-z0-9][\w-]*$/i.test(token)
    || /^act:[0-9a-f]{8}$/i.test(token)
    || /^dec-[0-9a-z]+$/i.test(token);
}

// --- Memory decisions (resolver-free; consumer hands us the dir) -------------

// First markdown heading or the filename, as a display title.
function decisionTitle(markdown, fallback) {
  const m = /^#\s+(.+)$/m.exec(markdown || '');
  return (m && m[1].trim()) || fallback;
}

// Memory files date themselves in a prose `_Captured: 2026-06-11_` line (not
// frontmatter, not filename). Missing/garbage dates are tolerated — null, never
// a NaN that would poison a sort.
function decisionDate(markdown) {
  const m = /_Captured:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})/.exec(markdown || '');
  return (m && m[1]) || null;
}

// readMemoryDecisions — read decision_*.md from a CONSUMER-RESOLVED directory.
// No path resolution here (the consumer owns that, per the punt decision); we
// only read files from a dir we are handed, so this stays hermetic and free of
// the memory-dir resolver. Returns [] on a missing/unreadable dir — never
// throws.
export function readMemoryDecisions(dir) {
  if (!dir || !existsSync(dir)) return [];
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return [];
  }
  const out = [];
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    if (!/^decision_.*\.md$/.test(entry.name)) continue;
    let markdown = '';
    try {
      markdown = readFileSync(join(dir, entry.name), 'utf8');
    } catch {
      continue;
    }
    const edges = parseDecisionEdges(markdown);
    out.push({
      source: 'memory',
      file: entry.name,
      title: decisionTitle(markdown, entry.name.replace(/\.md$/, '')),
      date: decisionDate(markdown),
      supersedes: edges.supersedes,
      see_also: edges.seeAlso,
    });
  }
  out.sort((a, b) => (a.file < b.file ? -1 : a.file > b.file ? 1 : 0));
  return out;
}

// --- Corpus assembly (inbox + threads; current project only) -----------------

// assembleNarrativeCorpus — read the project the cwd belongs to (worktree-aware
// via resolveProjectIdentity) and gather its inbox decisions + thread cursor
// timelines. Current project ONLY — no free `project` arg, so the five-namespace
// keying footgun and the broken cross-project case can't arise (boundary-man
// Finding 1/3). Writes nothing. Degrades to an empty-but-honest corpus when the
// project can't be resolved, rather than throwing or going blank silently.
//
// NOTE on testing: WATCHTOWER_DIR here AND QUEUE_DIR in watchtower-queue.mjs are
// read at module load, so a hermetic test must set process.env.WATCHTOWER_DIR
// BEFORE importing (dynamic import). An `env` param can't redirect them.
export function assembleNarrativeCorpus({ cwd } = {}) {
  const config = safeReadJSON(join(WATCHTOWER_DIR, 'config.json'));

  let identity = null;
  try {
    if (config && cwd) identity = resolveProjectIdentity(cwd, config);
  } catch {
    identity = null;
  }

  const empty = (reason) => ({
    schema_version: NARRATIVE_CORPUS_SCHEMA_VERSION,
    project_identity: identity || null,
    reason,
    inbox_decisions: [],
    thread_cursors: [],
    per_store_counts: { inbox_total: 0, inbox_kept: 0, threads_matched: 0 },
    skipped_unresolved: { count: 0, sample: [] },
    time_span: { first: null, last: null },
  });

  if (!identity || !identity.name) {
    return empty('project-unresolved');
  }

  // Inbox — knowledge-extraction items for THIS project (exact name match in
  // listItems). Drop captured-to-memory duplicates; keep superseded.
  let rawItems = [];
  try {
    rawItems = listItems({
      project: identity.name,
      category: DECISION_CATEGORY,
      statuses: NARRATED_STATUSES,
    });
  } catch {
    rawItems = [];
  }
  const inboxDecisions = [];
  for (const item of rawItems) {
    if (item.status === 'resolved' && item.resolution_type === CAPTURED_TO_MEMORY) {
      continue; // canonical copy lives in memory; the consumer reads it there
    }
    inboxDecisions.push({
      source: 'inbox',
      id: item.id,
      title: item.title || '(untitled)',
      text: item.summary || '',
      date: item.filed_at || null,
      status: item.status,
      type: (item.evidence && item.evidence.type) || null,
    });
  }

  // Transparency: knowledge-extraction items repo-wide that the identity
  // resolver could not attribute to a project (filed before the resolver, or
  // under a worktree-basename phantom key). Some may belong HERE. We cannot
  // know which, so we never silently present a complete-looking history — we
  // surface the count + a sample. (data-integrity Finding 4 / no-silent-trunc.)
  let unresolvedSample = [];
  let unresolvedCount = 0;
  try {
    const all = listItems({ category: DECISION_CATEGORY, statuses: NARRATED_STATUSES });
    const orphans = all.filter((i) => i.project_unresolved === true);
    unresolvedCount = orphans.length;
    unresolvedSample = orphans.slice(0, 5).map((i) => ({ id: i.id, title: i.title || '(untitled)' }));
  } catch {
    unresolvedCount = 0;
  }

  // Threads — active threads belonging to this project (exact slug match), with
  // their full cursor_history timeline (NOT just the current cursor — the
  // evolution is the point).
  let matchedThreads = [];
  try {
    const all = loadActiveThreads(join(WATCHTOWER_DIR, 'state', 'threads'));
    matchedThreads = all.filter((t) => threadMatchesProject(t, identity.slug).match);
  } catch {
    matchedThreads = [];
  }
  const threadCursors = matchedThreads
    .map(projectCursorTimeline)
    .sort((a, b) => (a.thread < b.thread ? -1 : a.thread > b.thread ? 1 : 0));

  // Time span across whatever dated material we have (inbox + thread cursors).
  const dates = [];
  for (const d of inboxDecisions) if (d.date) dates.push(d.date);
  for (const t of threadCursors) for (const h of t.history) if (h.date) dates.push(h.date);
  dates.sort();

  return {
    schema_version: NARRATIVE_CORPUS_SCHEMA_VERSION,
    project_identity: { name: identity.name, slug: identity.slug, registered: identity.registered },
    reason: null,
    inbox_decisions: inboxDecisions,
    thread_cursors: threadCursors,
    per_store_counts: {
      inbox_total: rawItems.length,
      inbox_kept: inboxDecisions.length,
      threads_matched: matchedThreads.length,
    },
    skipped_unresolved: { count: unresolvedCount, sample: unresolvedSample },
    time_span: { first: dates[0] || null, last: dates[dates.length - 1] || null },
  };
}

// --- CLI ---------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = { mode: null, projectPath: null, memoryDir: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--corpus') out.mode = 'corpus';
    else if (a === '--memory' && args[i + 1]) { out.mode = 'memory'; out.memoryDir = args[++i]; }
    else if (a === '--project-path' && args[i + 1]) out.projectPath = args[++i];
  }
  return out;
}

function main() {
  const { mode, projectPath, memoryDir } = parseArgs(process.argv);

  if (mode === 'corpus') {
    const corpus = assembleNarrativeCorpus({ cwd: projectPath || process.cwd() });
    process.stdout.write(JSON.stringify(corpus, null, 2) + '\n');
    return;
  }

  if (mode === 'memory') {
    process.stdout.write(JSON.stringify(readMemoryDecisions(memoryDir), null, 2) + '\n');
    return;
  }

  process.stderr.write(
    'usage: watchtower-narrative-corpus.mjs (--corpus --project-path <cwd> | --memory <dir>)\n'
  );
}

// Entry guard — importing must NOT run the CLI (mirrors the ring scripts so
// tests import the pure functions).
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

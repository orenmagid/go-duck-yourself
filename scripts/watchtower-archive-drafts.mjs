#!/usr/bin/env node

// watchtower-archive-drafts — the one-time draft archive (Simplification
// Project 1, plan C1, act:93a52285; plan:
// .claude/plans/simplification-1-memory-retrieval.md).
//
// The knowledge-extraction producers are gone (Phase 2d, the 2g durable arm,
// Ring 2's synthesis), and what they left in the queue was never read: the
// pending drafts sat until the 30-day expiry took them. Decision (c): archive
// everything, delete nothing, no human pass. This script turns every pending
// AND expired knowledge-extraction item into a searchable markdown file and
// closes the item, in one run over the whole (portfolio-wide) queue.
//
// WHERE THE FILES GO — runtime state, deliberately OUTSIDE the platform's
// memory folder (decision (g): the platform's background consolidation must
// never read ~1,200 near-duplicate drafts as input):
//
//   <WATCHTOWER_DIR>/state/recall/<slug>/archive/extraction-drafts/<item id>.md
//
// which is exactly the directory watchtower-recall.mjs indexes as its
// `archive` source. <slug> is the watchtower config's REGISTERED project slug
// — the key the session records use under state/projects/<slug>/ — reached
// through resolveProjectIdentity, never by building a slug string from a name
// or a path. An item whose project resolves to no registered path goes to
//
//   <WATCHTOWER_DIR>/state/recall/unresolved-drafts/<item id>.md
//
// and is listed in the run report: never dropped, never guessed into a project.
//
// HOW AN ITEM CLOSES — through the queue's archiveItem() door (status
// 'resolved', resolution_type 'archived', resolution = the archive path, the
// prior status and note kept in resolution_notes). No new status value, so the
// title-dedup corpus already covers these and nothing refiles.
//
// WHAT IS NEVER DONE — no queue item file is deleted or renamed, no memory
// file, session record or transcript is touched, and --dry-run and --report
// write nothing at all (not even a directory).
//
// Modes:
//   --dry-run   print the disposition map (per project AND per status, the
//               unresolved items listed) and write NOTHING. This printout is
//               the plan the operator approves before the apply.
//   --apply     back the queue up first (queue/items-backup-<date>-draft-archive/,
//               top-level item files — the precedent backup's shape; refuses to
//               proceed when the backup does not hold what it should), then
//               archive + close each item, re-reading its status first.
//   --report    AC4's verb: the category's items by status and resolution_type,
//               and whether every archived item's resolution path exists.
//               Exit 0 only when nothing is left pending/expired and every
//               archived path exists.
//
// Flags: --json (machine output); --mux-projects <path> / --registry <path>
// override the two registries the resolvers read (tests).
//
// Idempotent: an already-archived item is not a candidate; an archive file
// that exists is not rewritten (a run that died between the file write and
// the close resumes by closing); an existing same-day backup is left as it is.

import {
  readFileSync, readdirSync, existsSync, mkdirSync, copyFileSync,
} from 'fs';
import { join, basename, dirname } from 'path';
import { pathToFileURL } from 'url';
import {
  loadConfig, getWatchtowerDir, atomicWrite,
  resolveProjectIdentity, resolveProjectFromTranscriptSlug,
} from './watchtower-lib.mjs';
import { getItem, listItems, archiveItem } from './watchtower-queue.mjs';

export const CATEGORY = 'knowledge-extraction';
export const OPEN_STATUSES = ['pending', 'expired'];
export const ARCHIVED_TYPE = 'archived';
const BUCKETS = ['pending_with_draft', 'pending_without_draft', 'expired_with_draft', 'expired_without_draft'];

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

export function queueItemsDir(watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'queue', 'items');
}

export function archiveDirFor(slug, watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', 'recall', slug, 'archive', 'extraction-drafts');
}

export function unresolvedDirFor(watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', 'recall', 'unresolved-drafts');
}

export function backupDirFor(now, watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'queue', `items-backup-${String(now).slice(0, 10)}-draft-archive`);
}

// ---------------------------------------------------------------------------
// Project resolution
// ---------------------------------------------------------------------------

// Which REGISTERED project does this item belong to? Two routes, both ending
// in the canonical resolver, neither building a slug by hand:
//   1. the item's recorded project is a watchtower config KEY (and Ring 3 did
//      not flag the name as a fallback with project_unresolved) → that key's
//      registered path → resolveProjectIdentity;
//   2. otherwise the transcript directory slug, for a session that ran in a
//      mux worktree since cleaned up → resolveProjectFromTranscriptSlug.
// Anything else — and anything that resolves to an UNREGISTERED repo — is
// null: the caller files it under unresolved-drafts/ and reports it.
export function resolveItemProject(item, config, opts = {}) {
  const projects = (config && config.projects) || {};
  const key = item && item.project;
  // Each resolver call shells out to git; a run over ~1,200 items asks the
  // same few questions, so the two routes are memoized separately (by config
  // key, by transcript dir slug) when the caller passes a cache.
  const { cache, ...resolverOpts } = opts;
  const memo = (k, fn) => {
    if (!cache) return fn();
    if (!cache.has(k)) cache.set(k, fn());
    return cache.get(k);
  };
  let identity = null;
  let via = 'config-key';
  if (typeof key === 'string' && item.project_unresolved !== true
      && Object.prototype.hasOwnProperty.call(projects, key) && projects[key] && projects[key].path) {
    identity = memo(`key:${key}`, () => resolveProjectIdentity(projects[key].path, config, resolverOpts));
  }
  if (!identity || !identity.registered) {
    via = 'transcript-slug';
    const tPath = item && item.transcript_ref && item.transcript_ref.path;
    identity = typeof tPath === 'string' && tPath
      ? memo(`slug:${basename(dirname(tPath))}`, () => resolveProjectFromTranscriptSlug(basename(dirname(tPath)), config, resolverOpts))
      : null;
  }
  if (!identity || !identity.registered) return null;
  // `via` names the route, so the map can list every attribution that did not
  // come straight from a registered name.
  return { name: identity.name, slug: identity.slug, path: identity.path, via };
}

// ---------------------------------------------------------------------------
// Rendering — pure
// ---------------------------------------------------------------------------

export function hasDraft(item) {
  return typeof item.draft_artifact === 'string' && item.draft_artifact.trim() !== '';
}

function yamlValue(v) {
  if (v === null || v === undefined || v === '') return 'null';
  if (v === true) return 'true';
  return JSON.stringify(String(v));
}

function sessionIdOf(item) {
  const fromEvidence = item.evidence && item.evidence.session_id;
  if (typeof fromEvidence === 'string' && fromEvidence) return fromEvidence;
  const m = typeof item.context_anchor === 'string'
    && item.context_anchor.match(/session ([0-9a-f]{8}-[0-9a-f-]{27})/);
  return m ? m[1] : null;
}

function proseValue(v) {
  if (v === null || v === undefined) return 'none';
  if (Array.isArray(v) && v.every((x) => typeof x === 'string' || typeof x === 'number')) return v.join(', ');
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

// The archive file. `project` is the registered config key, or — for an
// unresolved item — the project string the item recorded, unaltered.
export function renderArchive(item, { project, priorStatus, archivedAt, unresolved = false }) {
  const evidence = (item.evidence && typeof item.evidence === 'object') ? item.evidence : {};
  const fm = [
    '---',
    'source: extraction-draft',
    `queue_item: ${item.id}`,
    `session_id: ${yamlValue(sessionIdOf(item))}`,
    `project: ${yamlValue(project)}`,
    `filed_at: ${yamlValue(item.filed_at)}`,
    `home: ${yamlValue(evidence.home)}`,
    `type: ${yamlValue(evidence.type)}`,
    `prior_status: ${priorStatus}`,
    `archived_at: ${yamlValue(archivedAt)}`,
  ];
  if (unresolved) fm.push('project_unresolved: true');
  fm.push('---');

  const title = (typeof item.title === 'string' && item.title.trim())
    ? item.title.replace(/\s*\n\s*/g, ' ').trim()
    : '(untitled)';
  const parts = [fm.join('\n'), '', `# ${title}`, ''];
  if (hasDraft(item)) {
    parts.push(item.draft_artifact); // verbatim
  } else {
    if (typeof item.summary === 'string' && item.summary.trim()) parts.push(item.summary, '');
    const keys = Object.keys(evidence);
    parts.push('This item carried no composed draft. Its evidence, as recorded:', '');
    if (keys.length === 0) parts.push('- none recorded');
    for (const k of keys) parts.push(`- ${k}: ${proseValue(evidence[k])}`);
  }
  const text = parts.join('\n');
  return text.endsWith('\n') ? text : text + '\n';
}

// ---------------------------------------------------------------------------
// Collection — read-only
// ---------------------------------------------------------------------------

// One pass over the queue directory. Reads the files itself (rather than
// through listItems) because the run must also SEE what a normal read skips:
// a crashed writer's <id>.json.tmp residue and unparseable item files, each
// reported by path.
export function collect({ watchtowerDir = getWatchtowerDir() } = {}) {
  const dir = queueItemsDir(watchtowerDir);
  const candidates = [];
  const residue = [];
  const malformed = [];
  let files = 0;
  if (!existsSync(dir)) return { dir, files, candidates, residue, malformed };
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    if (!e.isFile()) continue;
    files += 1;
    const p = join(dir, e.name);
    if (e.name.endsWith('.tmp')) { residue.push(p); continue; }
    if (!e.name.endsWith('.json')) continue;
    let item;
    try {
      item = JSON.parse(readFileSync(p, 'utf8'));
    } catch (err) {
      malformed.push({ path: p, reason: `unparseable JSON: ${err.message}` });
      continue;
    }
    if (!item || typeof item !== 'object' || item.schema_version !== 1) {
      malformed.push({ path: p, reason: `unsupported schema_version ${item && item.schema_version}` });
      continue;
    }
    if (item.category !== CATEGORY || !OPEN_STATUSES.includes(item.status)) continue;
    if (`${item.id}.json` !== e.name) {
      malformed.push({ path: p, reason: `item id ${JSON.stringify(item.id)} does not match its filename` });
      continue;
    }
    candidates.push(item);
  }
  residue.sort();
  malformed.sort((a, b) => a.path.localeCompare(b.path));
  candidates.sort((a, b) => a.id.localeCompare(b.id));
  return { dir, files, candidates, residue, malformed };
}

const emptyCounts = () => ({ pending_with_draft: 0, pending_without_draft: 0, expired_with_draft: 0, expired_without_draft: 0, total: 0 });
const bucketOf = (item) => `${item.status}_${hasDraft(item) ? 'with' : 'without'}_draft`;

// The disposition map. `totals` counts every candidate by status and draft
// (resolved AND unresolved); `totals.unresolved` is how many of those have no
// registered project.
export function buildPlan({ watchtowerDir = getWatchtowerDir(), muxProjectsPath, registryPath, homeDir } = {}) {
  const config = loadConfig(watchtowerDir);
  const resolverOpts = {};
  if (muxProjectsPath) resolverOpts.muxProjectsPath = muxProjectsPath;
  if (homeDir) resolverOpts.homeDir = homeDir; // the slug resolver's anchor (act:5ebf992e; tests)
  if (registryPath) resolverOpts.registryPath = registryPath;

  const scan = collect({ watchtowerDir });
  const projects = {};
  const unresolved = { counts: emptyCounts(), items: [] };
  const attributed_by_transcript_slug = [];
  const totals = { pending_with_draft: 0, pending_without_draft: 0, expired_with_draft: 0, expired_without_draft: 0, unresolved: 0, total: 0 };
  const entries = [];
  const cache = new Map();

  for (const item of scan.candidates) {
    const project = resolveItemProject(item, config, { ...resolverOpts, cache });
    const bucket = bucketOf(item);
    totals[bucket] += 1;
    totals.total += 1;
    if (project) {
      const row = projects[project.name] || (projects[project.name] = { slug: project.slug, ...emptyCounts() });
      row[bucket] += 1;
      row.total += 1;
      if (project.via === 'transcript-slug') {
        attributed_by_transcript_slug.push({ id: item.id, recorded_project: item.project ?? null, project: project.name });
      }
      entries.push({ id: item.id, project, target: join(archiveDirFor(project.slug, watchtowerDir), `${item.id}.md`) });
    } else {
      unresolved.counts[bucket] += 1;
      unresolved.counts.total += 1;
      totals.unresolved += 1;
      unresolved.items.push({ id: item.id, project: item.project ?? null, status: item.status, has_draft: hasDraft(item) });
      entries.push({ id: item.id, project: null, target: join(unresolvedDirFor(watchtowerDir), `${item.id}.md`) });
    }
  }
  return {
    queue_dir: scan.dir, queue_files: scan.files,
    projects, unresolved, totals, attributed_by_transcript_slug,
    residue: scan.residue, malformed: scan.malformed,
    entries,
  };
}

// ---------------------------------------------------------------------------
// Backup
// ---------------------------------------------------------------------------

// Copy every top-level item file to the backup dir BEFORE anything changes,
// and refuse to proceed unless the backup holds exactly the files that were
// listed. An existing backup (a same-day re-run) is never overwritten — it is
// the true pre-run state — but it must hold every item this run would change.
function backupQueue({ itemsDir, backupDir, candidateIds, copyFile }) {
  if (existsSync(backupDir)) {
    const have = new Set(readdirSync(backupDir));
    const missing = candidateIds.filter((id) => !have.has(`${id}.json`));
    if (missing.length > 0) {
      throw new Error(`backup refused: ${backupDir} already exists and does not hold ${missing.length} of the items this run would change (first: ${missing[0]}.json). Nothing was changed. Move that directory aside and re-run.`);
    }
    return { path: backupDir, files: have.size, created: false };
  }
  const names = readdirSync(itemsDir, { withFileTypes: true }).filter((e) => e.isFile()).map((e) => e.name);
  mkdirSync(backupDir, { recursive: true });
  for (const n of names) copyFile(join(itemsDir, n), join(backupDir, n));
  const copied = readdirSync(backupDir).length;
  if (copied !== names.length) {
    throw new Error(`backup refused: ${backupDir} holds ${copied} files, the queue listed ${names.length}. Nothing was changed. Remove the incomplete backup directory and re-run.`);
  }
  return { path: backupDir, files: copied, created: true };
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

function report({ watchtowerDir }) {
  const unreadable = [];
  const items = listItems({ category: CATEGORY, onUnreadable: (name, err) => unreadable.push({ file: name, reason: err.message }) });
  const by_status = {};
  const by_resolution_type = {};
  const unresolvedRoot = unresolvedDirFor(watchtowerDir) + '/';
  const archived = { total: 0, into_projects: 0, unresolved: 0, paths_missing: [] };
  for (const it of items) {
    by_status[it.status] = (by_status[it.status] || 0) + 1;
    const rt = it.resolution_type || '(none)';
    by_resolution_type[rt] = (by_resolution_type[rt] || 0) + 1;
    if (it.status === 'resolved' && it.resolution_type === ARCHIVED_TYPE) {
      archived.total += 1;
      const p = typeof it.resolution === 'string' ? it.resolution : '';
      if (p.startsWith(unresolvedRoot)) archived.unresolved += 1; else archived.into_projects += 1;
      if (!p || !existsSync(p)) archived.paths_missing.push({ id: it.id, path: p || null });
    }
  }
  archived.paths_missing.sort((a, b) => a.id.localeCompare(b.id));
  const open = OPEN_STATUSES.reduce((n, s) => n + (by_status[s] || 0), 0);
  const cleared = open === 0;
  return {
    mode: 'report', category: CATEGORY, items: items.length,
    by_status, by_resolution_type, open, cleared, archived, unreadable,
    ok: cleared && archived.paths_missing.length === 0,
  };
}

/**
 * @param {object} opts
 * @param {'dry-run'|'apply'|'report'} opts.mode
 * @param {string} [opts.now] ISO timestamp (archived_at and the backup date)
 * @param {string} [opts.muxProjectsPath] / [opts.registryPath] / [opts.homeDir] resolver overrides (tests)
 * @param {Function} [opts.copyFile] backup copy (tests)
 * @param {Function} [opts.beforeEach] called with each item id before its re-read (tests)
 */
export function run(opts = {}) {
  const watchtowerDir = getWatchtowerDir();
  const { mode } = opts;
  if (mode === 'report') return report({ watchtowerDir });
  if (mode !== 'dry-run' && mode !== 'apply') throw new Error(`watchtower-archive-drafts: unknown mode ${JSON.stringify(mode)}`);

  const plan = buildPlan({ watchtowerDir, muxProjectsPath: opts.muxProjectsPath, registryPath: opts.registryPath, homeDir: opts.homeDir });
  const { entries, ...map } = plan;
  if (mode === 'dry-run') return { mode, ...map };

  const now = opts.now || new Date().toISOString();
  const applied = { archived: 0, unresolved: 0, files_written: 0, files_kept: 0, skipped_changed: [], errors: [] };
  if (entries.length === 0) return { mode, ...map, backup: null, applied };

  const backup = backupQueue({
    itemsDir: plan.queue_dir,
    backupDir: backupDirFor(now, watchtowerDir),
    candidateIds: entries.map((e) => e.id),
    copyFile: opts.copyFile || copyFileSync,
  });

  for (const entry of entries) {
    try {
      if (typeof opts.beforeEach === 'function') opts.beforeEach(entry.id);
      // Re-read: another session may have closed the item since the plan.
      const fresh = getItem(entry.id);
      if (!fresh || fresh.category !== CATEGORY || !OPEN_STATUSES.includes(fresh.status)) {
        applied.skipped_changed.push({ id: entry.id, status: fresh ? fresh.status : 'missing' });
        continue;
      }
      if (existsSync(entry.target)) {
        applied.files_kept += 1;
      } else {
        atomicWrite(entry.target, renderArchive(fresh, {
          project: entry.project ? entry.project.name : (fresh.project ?? null),
          priorStatus: fresh.status,
          archivedAt: now,
          unresolved: !entry.project,
        }));
        applied.files_written += 1;
      }
      const closed = archiveItem(entry.id, { archive_path: entry.target });
      if (!closed) {
        applied.skipped_changed.push({ id: entry.id, status: (getItem(entry.id) || {}).status || 'missing' });
        continue;
      }
      if (entry.project) applied.archived += 1; else applied.unresolved += 1;
    } catch (err) {
      applied.errors.push({ id: entry.id, error: err.message });
    }
  }
  return { mode, ...map, backup, applied };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function renderMap(res) {
  const lines = [];
  const head = ['project', 'pending+draft', 'pending-draft', 'expired+draft', 'expired-draft', 'total'];
  const rows = Object.entries(res.projects)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, c]) => [name, ...BUCKETS.map((b) => c[b]), c.total]);
  rows.push(['(unresolved)', ...BUCKETS.map((b) => res.unresolved.counts[b]), res.unresolved.counts.total]);
  rows.push(['TOTAL', ...BUCKETS.map((b) => res.totals[b]), res.totals.total]);
  const widths = head.map((h, i) => Math.max(h.length, ...rows.map((r) => String(r[i]).length)));
  const fmt = (r) => r.map((c, i) => (i === 0 ? String(c).padEnd(widths[i]) : String(c).padStart(widths[i]))).join('  ');
  lines.push(`knowledge-extraction drafts to archive — ${res.queue_dir} (${res.queue_files} files)`);
  lines.push('', fmt(head), fmt(widths.map((w) => '-'.repeat(w))), ...rows.map(fmt), '');
  lines.push(`unresolved (no registered project — these go to state/recall/unresolved-drafts/): ${res.unresolved.items.length}`);
  for (const u of res.unresolved.items) {
    lines.push(`  ${u.id}  ${u.status}  ${u.has_draft ? 'draft' : 'no draft'}  recorded project: ${JSON.stringify(u.project)}`);
  }
  lines.push(`attributed by transcript slug, not by the recorded project name: ${res.attributed_by_transcript_slug.length}`);
  for (const a of res.attributed_by_transcript_slug) {
    lines.push(`  ${a.id}  recorded project: ${JSON.stringify(a.recorded_project)}  →  ${a.project}`);
  }
  lines.push(`temp-file residue skipped: ${res.residue.length}`);
  for (const p of res.residue) lines.push(`  ${p}`);
  lines.push(`malformed item files skipped: ${res.malformed.length}`);
  for (const m of res.malformed) lines.push(`  ${m.path} — ${m.reason}`);
  return lines.join('\n');
}

function main(argv) {
  const flags = new Set(argv.filter((a) => a.startsWith('--')));
  const valueOf = (name) => { const i = argv.indexOf(name); return i >= 0 ? argv[i + 1] : undefined; };
  const modes = ['--dry-run', '--apply', '--report'].filter((m) => flags.has(m));
  if (modes.length !== 1) {
    console.error('usage: watchtower-archive-drafts.mjs (--dry-run | --apply | --report) [--json] [--mux-projects <path>] [--registry <path>]');
    return 2;
  }
  const mode = modes[0].slice(2);
  const res = run({ mode, muxProjectsPath: valueOf('--mux-projects'), registryPath: valueOf('--registry'), homeDir: valueOf('--home-dir') });
  if (flags.has('--json')) {
    console.log(JSON.stringify(res, null, 2));
  } else if (mode === 'report') {
    console.log(`knowledge-extraction items: ${res.items}`);
    console.log(`by status: ${JSON.stringify(res.by_status)}`);
    console.log(`by resolution_type: ${JSON.stringify(res.by_resolution_type)}`);
    console.log(`still pending or expired: ${res.open}${res.cleared ? ' — queue cleared' : ' — NOT cleared'}`);
    console.log(`archived: ${res.archived.total} (${res.archived.into_projects} into a project archive, ${res.archived.unresolved} into unresolved-drafts)`);
    console.log(`archived paths missing on disk: ${res.archived.paths_missing.length}`);
    for (const m of res.archived.paths_missing) console.log(`  ${m.id} → ${m.path}`);
    if (res.unreadable.length) console.log(`unreadable item files: ${res.unreadable.length}`);
  } else {
    console.log(renderMap(res));
    if (mode === 'dry-run') {
      console.log('\nDRY RUN — nothing was written.');
    } else {
      const a = res.applied;
      console.log(res.backup ? `\nbackup: ${res.backup.path} (${res.backup.files} files${res.backup.created ? '' : ', already present'})` : '\nnothing to archive — no backup taken.');
      console.log(`archived into a project: ${a.archived}; into unresolved-drafts: ${a.unresolved}; files written: ${a.files_written}; files already present: ${a.files_kept}`);
      console.log(`skipped (status changed since the plan): ${a.skipped_changed.length}`);
      for (const s of a.skipped_changed) console.log(`  ${s.id} is now ${s.status}`);
      console.log(`errors: ${a.errors.length}`);
      for (const e of a.errors) console.log(`  ${e.id}: ${e.error}`);
    }
  }
  if (mode === 'report') return res.ok ? 0 : 1;
  if (mode === 'apply') return res.applied.errors.length === 0 ? 0 : 1;
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    process.exit(main(process.argv.slice(2)));
  } catch (err) {
    console.error(`watchtower-archive-drafts: ${err.message}`);
    process.exit(1);
  }
}

#!/usr/bin/env node

// Watchtower cross-ring reader — the read-only synthesis SUBSTRATE for
// /briefing (act:f5814d6c, Stage 1). It reads all ring outputs together and
// assembles one normalized per-project view none of the rings holds alone:
//
//   Ring 1  — per-project state file (parsed contract-owned sections) +
//             git-attention facts
//   Ring 3  — recent session summaries + thread cursor timelines
//   Inbox   — pending counts + resolution history (what the operator actually
//             acts on vs dismisses — the resolution_mix signal)
//
// Ring 2 has NO dedicated block by design: its durable outputs already arrive
// through the two channels above — pattern-promotion items ride the inbox,
// and the recall canary is projected into Ring 1's Standing Issues. A
// dedicated block would double-report through this reader's own output.
// Enrichment BODIES stay behind the Enrichment Directory contract (/inbox
// reads them per-item).
//
// The script GATHERS; the consumer SYNTHESIZES. No convergence scoring, no
// cross-store "insights" computed here — a mechanical projection on a trust
// surface must not fabricate synthesis (pattern-intelligence-first). The
// consumer is /briefing Step 1 (the replacement gather path for its old 1d/1e
// reads); contract in cabinet/watchtower-contracts.md ("Cross-Ring Reader").
//
// This is a PROJECTION over existing stores, never a new store. It writes
// NOTHING — there is no write path in this module, and the hermetic test
// asserts the filesystem is untouched. Sibling of watchtower-narrative-corpus
// (which imports projectCursorTimeline from here — one definition of the
// cursor projection without touching ring-loaded watchtower-lib mid-soak).
//
// Per-store join keys (verified against the writers — the load-bearing table;
// name = config.projects key, slug = slugify(name)):
//
//   state/projects/<SLUG>.md            Ring 1 state file        SLUG
//   state/projects/<SLUG>/sessions/     Ring 3 session summaries SLUG
//   state/threads sessions[].project    threads                  SLUG
//   git-attention.json fact.project     git attention            NAME
//   queue items item.project            inbox                    NAME
//
// CLI modes (reads only):
//   --portfolio [--since <ISO|Ndays>]
//   --project <name> [--since ...]          (name = config.projects key)
//   --project-path <cwd> [--since ...]      (worktree-aware via resolver)
//
// Never throws on a missing/partial store — readers degrade to honest-empty
// with a reason, and every cap/truncation is counted, never silent.

import { readFileSync, existsSync, readdirSync, statSync, realpathSync } from 'fs';
import { join, basename } from 'path';
import { pathToFileURL } from 'url';
import {
  loadActiveThreads,
  threadMatchesProject,
  resolveProjectIdentity,
  slugify,
} from './watchtower-lib.mjs';
import { listItems } from './watchtower-queue.mjs';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME || '', '.claude-cabinet', 'watchtower');

export const CROSS_RING_SCHEMA_VERSION = 1;

// Default disposition/session window when --since is absent or garbage.
export const DEFAULT_SINCE_DAYS = 14;

// Recent-session cap per project view (applied AFTER the window filter; both
// totals are reported so the cap is never a silent truncation).
export const SESSION_CAP = 5;

// Output-size caps (CP3 finding: uncapped, a live portfolio view measured
// 1.37 MB — 920 KB of full thread histories + 104 KB of resolution events —
// which no consumer channel can carry). Every cap reports its total; the
// consumer sees "5 of 85", never a silently complete-looking 5. The thread
// cap is applied at the reader's CALL SITE, not inside projectCursorTimeline —
// narrative-corpus's lineage mode needs the full history (the evolution is
// the point there). 3 = the current cursor plus two prior snapshots — enough
// for a briefing's "how the thinking moved"; the full evolution is lineage
// mode's job (narrative-corpus, uncapped).
export const THREAD_HISTORY_CAP = 3;
export const RESOLUTION_EVENTS_CAP = 20;

// Ring 1's contract-owned section headers — the ownership table in
// cabinet/watchtower-contracts.md ("Project State Section Ownership") is the
// source of truth; these constants must match it, and the test fixture uses
// Ring 1's real output shape so a header respelling fails a test instead of
// silently nulling a field.
export const STATE_SECTIONS = {
  active_plans: 'Active Plans',
  last_session: 'Last Session',
  standing_issues: 'Standing Issues',
  tech_stack: 'Tech Stack',
};

// Resolution-history status sets. resolved/dismissed stamp resolved_at (a
// real disposition time); superseded/expired never do, so their `since`
// filter inside listItems falls back to filed_at — they are therefore
// reported as separate FILED-DATE-window counts, never blended into the
// disposition-time mix (expiry is >=30d old by construction, so a 14d
// disposition window would be structurally zero — the silent-status trap).
const DISPOSITIONED_STATUSES = ['resolved', 'dismissed'];
const FILED_WINDOW_STATUSES = ['superseded', 'expired'];

// resolution_type buckets. The field is unvalidated free text defaulting to
// null, so anything outside the documented enum lands in untyped_or_other —
// counted in the summary, never silently dropped. Exported so a sibling reader
// (watchtower-inbox-assessment.mjs) classifies against the SAME vocabulary
// instead of re-deriving the enum — one classifier, one source.
export const ENGAGED_TYPES = ['acted-on', 'captured-to-memory', 'deferred'];
export const DISCARDED_TYPES = ['stale', 'noise'];
// Machine acts, NOT human engagement (grp:wt-noise-immunity, binding
// cross-lane convention): every Ring 1 auto-retraction/reconciliation the
// program adds stamps resolution_type 'auto-reconciled' + evidence.actor
// 'ring1' (the producer is the program's Ring 1 lane — this spelling and
// that stamp must stay in lockstep, or the exclusion silently never fires;
// pre-convention auto-resolves carry no type and age out of the recent
// windows naturally). Machine acts are structurally EXCLUDED from
// ENGAGED_TYPES — cron activity masquerading as operator engagement would
// silently defeat the not-consumed detector (the assessment's worked_recent
// skips this bucket for the same reason).
export const MACHINE_TYPES = ['auto-reconciled'];

// bucketResolution — THE single resolution_type → bucket classifier. Returns
// 'engaged' | 'discarded' | 'machine' | 'other' ('other' = null/untyped or an
// out-of-enum value; 'machine' = a ring's own act, never operator
// engagement). readInboxView's resolution_mix and the sibling
// inbox-assessment reader both call this so the trust-ladder buckets are
// defined once. Pure; null/undefined-safe.
export function bucketResolution(resolution_type) {
  const rt = resolution_type || null;
  if (rt && ENGAGED_TYPES.includes(rt)) return 'engaged';
  if (rt && DISCARDED_TYPES.includes(rt)) return 'discarded';
  if (rt && MACHINE_TYPES.includes(rt)) return 'machine';
  return 'other';
}

function safeReadJSON(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function safeRealpath(p) {
  try {
    return realpathSync(p);
  } catch {
    return null;
  }
}

// --- Ring 1 state-file section parsing (pure, tested) -------------------------

// parseProjectStateSections — split one Ring 1 per-project state file into its
// contract-owned sections. Missing sections → null + a parse warning; unknown
// sections are listed, not lost. Never throws.
export function parseProjectStateSections(markdown) {
  const text = typeof markdown === 'string' ? markdown : '';
  const sections = {};
  const parse_warnings = [];
  const other_sections = [];

  // Header line: `# <name> — <timestamp>` (informational).
  const headerMatch = /^#\s+(.+)$/m.exec(text);
  const header = headerMatch ? headerMatch[1].trim() : null;

  // Split on `## ` headings; body = everything to the next `## ` or EOF.
  const found = {};
  const re = /^##\s+(.+)$/gm;
  const marks = [];
  let m;
  while ((m = re.exec(text)) !== null) {
    marks.push({ name: m[1].trim(), start: m.index, bodyStart: m.index + m[0].length });
  }
  for (let i = 0; i < marks.length; i++) {
    const end = i + 1 < marks.length ? marks[i + 1].start : text.length;
    found[marks[i].name] = text.slice(marks[i].bodyStart, end).trim();
  }

  const known = new Set(Object.values(STATE_SECTIONS));
  for (const [field, title] of Object.entries(STATE_SECTIONS)) {
    if (Object.prototype.hasOwnProperty.call(found, title)) {
      sections[field] = found[title];
    } else {
      sections[field] = null;
      parse_warnings.push(`missing section: ${title}`);
    }
  }
  for (const name of Object.keys(found)) {
    if (!known.has(name)) other_sections.push(name);
  }

  return { header, sections, parse_warnings, other_sections };
}

// --- Thread cursor projection (pure; the single definition) -------------------

// projectCursorTimeline — one thread → its projected cursor timeline. A legacy
// thread file carrying a lone `cursor` object (no cursor_history) becomes a
// single-entry history (the same rule /briefing documents for its old 1e
// read). watchtower-narrative-corpus imports this — one definition of the
// 4-field shape, in a non-ring-loaded file (watchtower-lib is soak-frozen).
export function projectCursorTimeline(thread) {
  const t = thread && typeof thread === 'object' ? thread : {};
  let history = Array.isArray(t.cursor_history) ? t.cursor_history : [];
  if (history.length === 0 && t.cursor && typeof t.cursor === 'object') {
    history = [{ date: t.last_updated || null, cursor: t.cursor }];
  }
  return {
    thread: t.thread || null,
    display_name: t.display_name || t.thread || null,
    history: history.map((h) => ({
      date: (h && h.date) || null,
      what: (h && h.cursor && h.cursor.what) || '',
      why: (h && h.cursor && h.cursor.why) || '',
      open_questions: (h && h.cursor && h.cursor.open_questions) || '',
    })),
  };
}

// --- Since-window handling -----------------------------------------------------

// parseSince — accept an ISO date (`2026-06-01` / full ISO) or a day count
// (`14` / `14days`/`14d`). Garbage → the default window plus a warning field;
// never a throw. All window math is UTC (session filenames are UTC-derived).
export function parseSince(raw, { defaultDays = DEFAULT_SINCE_DAYS } = {}) {
  const fallback = () => ({
    since: new Date(Date.now() - defaultDays * 86400000).toISOString(),
    since_warning: raw == null
      ? null
      : `unparseable --since ${JSON.stringify(String(raw))}; using default ${defaultDays}d`,
  });
  if (raw == null || raw === '') return { ...fallback(), since_warning: null };
  const s = String(raw).trim();
  const dayMatch = /^(\d+)\s*(d|day|days)?$/i.exec(s);
  if (dayMatch) {
    // An absurd day count overflows Date into Invalid → toISOString throws;
    // guard so the "never a throw" contract holds (CP3: through the CLI the
    // throw became exit-0-with-empty-stdout, indistinguishable from success).
    const d = new Date(Date.now() - Number(dayMatch[1]) * 86400000);
    if (Number.isFinite(d.getTime())) return { since: d.toISOString(), since_warning: null };
    return fallback();
  }
  const t = new Date(s).getTime();
  if (Number.isFinite(t) && /^\d{4}-\d{2}-\d{2}/.test(s)) {
    return { since: new Date(t).toISOString(), since_warning: null };
  }
  return fallback();
}

// --- Ring 3 sessions ------------------------------------------------------------

// Ring 3 writes `state/projects/<slug>/sessions/<YYYY-MM-DD>-<sessionId>.md`.
// The filename date is UTC-derived and same-day files sort randomly by uuid,
// so ordering uses each file's content `Date:` ISO line (mtime fallback);
// the filename is only the recognition pattern.
const SESSION_FILE_RE = /^(\d{4}-\d{2}-\d{2})-(.+)\.md$/;

function readProjectSessions(slug, sinceIso) {
  const dir = join(WATCHTOWER_DIR, 'state', 'projects', slug, 'sessions');
  const out = {
    sessions: [],
    sessions_total: 0,
    sessions_in_window: 0,
    sessions_included: 0,
    sessions_skipped_unrecognized: 0,
    sessions_unreadable: 0,
    present: existsSync(dir),
  };
  if (!out.present) return out;

  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return { ...out, present: false };
  }

  const sinceMs = new Date(sinceIso).getTime();
  const parsed = [];
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const m = SESSION_FILE_RE.exec(entry.name);
    if (!m) {
      out.sessions_skipped_unrecognized++;
      continue;
    }
    out.sessions_total++;
    const fp = join(dir, entry.name);
    let text = '';
    try {
      text = readFileSync(fp, 'utf8');
    } catch {
      out.sessions_unreadable++; // recognized name, unreadable content —
      continue;                  // distinct from the pattern-mismatch counter
    }
    // Real timestamp: the content `Date: <ISO>` line; mtime fallback.
    let ts = NaN;
    const dm = /^Date:\s*(\S+)/m.exec(text);
    if (dm) ts = new Date(dm[1]).getTime();
    if (!Number.isFinite(ts)) {
      try {
        ts = statSync(fp).mtimeMs;
      } catch {
        ts = 0;
      }
    }
    parsed.push({
      date: dm ? dm[1] : m[1],
      session_id: m[2],
      ts,
      bullets: text.split('\n').filter((l) => l.startsWith('- ')).map((l) => l.slice(2).trim()),
    });
  }

  const inWindow = parsed.filter((s) => s.ts >= sinceMs);
  out.sessions_in_window = inWindow.length;
  inWindow.sort((a, b) => b.ts - a.ts); // newest first, by real timestamp
  out.sessions = inWindow.slice(0, SESSION_CAP)
    .map(({ date, session_id, bullets }) => ({ date, session_id, bullets }));
  out.sessions_included = out.sessions.length;
  return out;
}

// --- Inbox (counts + resolution history) ----------------------------------------

function readInboxView(name, sinceIso) {
  const view = {
    pending_total: 0,
    pending_by_category: {},
    pending_urgent: 0,
    resolution_events: [],
    resolution_mix: { engaged: 0, discarded: 0, machine: 0, untyped_or_other: 0, total: 0 },
    superseded_by_filed_date: 0,
    expired_by_filed_date: 0,
    attributed_but_flagged: 0,
  };

  // Pending: COUNTS ONLY — /briefing Step 1c owns expiry + the full pending
  // load for dispositions; the reader must not become a second one.
  let pending = [];
  try {
    pending = listItems({ project: name, statuses: ['pending'] });
  } catch {
    pending = [];
  }
  view.pending_total = pending.length;
  for (const item of pending) {
    const cat = item.category || '(uncategorized)';
    view.pending_by_category[cat] = (view.pending_by_category[cat] || 0) + 1;
    if (item.urgency === 'urgent') view.pending_urgent++;
    if (item.project_unresolved === true) view.attributed_but_flagged++;
  }

  // Dispositioned items (resolved/dismissed): resolved_at exists, so the
  // listItems since-filter is a true disposition-time window.
  let dispositioned = [];
  try {
    dispositioned = listItems({ project: name, statuses: DISPOSITIONED_STATUSES, since: sinceIso });
  } catch {
    dispositioned = [];
  }
  for (const item of dispositioned) {
    if (item.project_unresolved === true) view.attributed_but_flagged++;
    view.resolution_events.push({
      id: item.id,
      title: item.title || '(untitled)',
      category: item.category || null,
      status: item.status,
      resolution_type: item.resolution_type || null,
      date: item.resolved_at || item.filed_at || null,
    });
    const bucket = bucketResolution(item.resolution_type);
    if (bucket === 'engaged') view.resolution_mix.engaged++;
    else if (bucket === 'discarded') view.resolution_mix.discarded++;
    // Machine acts get their own additive count — folding them into
    // untyped_or_other would inflate the "operator resolved without typing"
    // signal with cron acts (the masquerade this bucket exists to prevent).
    else if (bucket === 'machine') view.resolution_mix.machine++;
    else view.resolution_mix.untyped_or_other++;
    view.resolution_mix.total++;
  }
  // Cap the event LIST (the mix above already carries the full counts, so no
  // signal is lost). listItems returns newest-first; the total keeps the cap
  // honest. (CP3: 251 uncapped events = 104 KB on one live project.)
  view.resolution_events_total = view.resolution_events.length;
  view.resolution_events = view.resolution_events.slice(0, RESOLUTION_EVENTS_CAP);

  // Superseded/expired: no resolved_at → the since-filter falls back to
  // filed_at. Labeled as such; never blended into the disposition-time mix.
  let filedWindow = [];
  try {
    filedWindow = listItems({ project: name, statuses: FILED_WINDOW_STATUSES, since: sinceIso });
  } catch {
    filedWindow = [];
  }
  for (const item of filedWindow) {
    if (item.status === 'superseded') view.superseded_by_filed_date++;
    else if (item.status === 'expired') view.expired_by_filed_date++;
  }

  return view;
}

// --- Per-project view -------------------------------------------------------------

// assembleProjectView — one project's cross-ring view. `name` is the config
// key (inbox/git-attention join key); `slug` = slugify(name) (state file,
// sessions dir, thread join key). Writes nothing; degrades per-store.
export function assembleProjectView({ name, slug, path = null, since } = {}) {
  const resolvedSlug = slug || slugify(name || '');
  const { since: sinceIso } = parseSince(since);

  // Ring 1: state file (slug-keyed) + git-attention facts (name-keyed).
  const statePath = join(WATCHTOWER_DIR, 'state', 'projects', `${resolvedSlug}.md`);
  let ring1;
  if (existsSync(statePath)) {
    let text = '';
    try {
      text = readFileSync(statePath, 'utf8');
    } catch {
      text = '';
    }
    ring1 = { present: true, ...parseProjectStateSections(text) };
  } else {
    ring1 = { present: false, header: null, sections: null, parse_warnings: [], other_sections: [] };
  }
  const gitAttention = safeReadJSON(join(WATCHTOWER_DIR, 'state', 'git-attention.json'));
  ring1.git_attention_present = gitAttention !== null;
  ring1.git_attention = Array.isArray(gitAttention && gitAttention.facts)
    ? gitAttention.facts.filter((f) => f && f.project === name)
    : [];

  // Ring 3: sessions (slug-keyed) + threads (slug-keyed via exact match).
  const ring3 = readProjectSessions(resolvedSlug, sinceIso);
  let matchedThreads = [];
  try {
    const all = loadActiveThreads(join(WATCHTOWER_DIR, 'state', 'threads'));
    matchedThreads = all.filter((t) => threadMatchesProject(t, resolvedSlug).match);
  } catch {
    matchedThreads = [];
  }
  ring3.threads = matchedThreads
    .map((t) => {
      // Cap here, not in projectCursorTimeline — narrative-corpus needs the
      // full history. cursor_history is append-only chronological, so the
      // newest entries are the LAST ones; the total makes the cap honest.
      const tl = projectCursorTimeline(t);
      return {
        ...tl,
        history_total: tl.history.length,
        history: tl.history.slice(-THREAD_HISTORY_CAP),
      };
    })
    .sort((a, b) => {
      const ka = a.thread || '';
      const kb = b.thread || '';
      return ka < kb ? -1 : ka > kb ? 1 : 0;
    });
  ring3.threads_matched = matchedThreads.length;

  // Inbox: pending counts + resolution history (name-keyed).
  const inbox = readInboxView(name, sinceIso);

  return {
    name: name || null,
    slug: resolvedSlug,
    path: path || null,
    path_exists: !!(path && safeRealpath(path)),
    ring1,
    ring3,
    inbox,
  };
}

// --- Portfolio view -----------------------------------------------------------------

const emptyPortfolio = (reason, sinceInfo = {}) => ({
  schema_version: CROSS_RING_SCHEMA_VERSION,
  generated_at_source: 'live-read',
  since_window: sinceInfo.since || null,
  since_warning: sinceInfo.since_warning || null,
  reason,
  projects: [],
  per_store_counts: { projects: 0, threads_active: 0, items_seen: 0 },
  unattributed_items: { count: 0, sample: [] },
  orphan_threads: { count: 0, sample: [] },
  orphaned_state_files: { count: 0, sample: [] },
  duplicate_path_warning: [],
});

// assemblePortfolioView — every configured project's view + the portfolio
// honesty fields (the complement of the config-driven walk: items, threads,
// and state files that belong to NO configured project are surfaced with
// counts + samples, never silently dropped between views).
export function assemblePortfolioView({ since } = {}) {
  const sinceInfo = parseSince(since);
  const config = safeReadJSON(join(WATCHTOWER_DIR, 'config.json'));
  if (!config) return emptyPortfolio('no-watchtower', sinceInfo);

  const entries = Object.entries(config.projects || {});
  if (entries.length === 0) return emptyPortfolio('no-projects', sinceInfo);

  const views = entries.map(([key, proj]) => assembleProjectView({
    name: key,
    slug: slugify(key),
    path: (proj && proj.path) || null,
    since: sinceInfo.since,
  }));

  const configNames = new Set(entries.map(([key]) => key));
  const configSlugs = new Set(entries.map(([key]) => slugify(key)));

  // Unattributed items: partition on config-key MEMBERSHIP, not on the
  // project_unresolved flag — the flag misses removed-project and
  // pre-resolver phantom keys, and wrongly trusts flagged items whose stored
  // basename collides with a real key (those are counted per-view instead).
  let allItems = [];
  try {
    allItems = listItems({});
  } catch {
    allItems = [];
  }
  const unattributed = allItems.filter((i) => !configNames.has(i.project));
  const unattributed_items = {
    count: unattributed.length,
    sample: unattributed.slice(0, 5).map((i) => ({
      id: i.id,
      title: i.title || '(untitled)',
      project: i.project || null,
    })),
  };

  // Orphan threads: active threads matching NO configured project's slug —
  // phantom-key history must not vanish portfolio-wide.
  let activeThreads = [];
  try {
    activeThreads = loadActiveThreads(join(WATCHTOWER_DIR, 'state', 'threads'));
  } catch {
    activeThreads = [];
  }
  const orphanThreads = activeThreads.filter(
    (t) => ![...configSlugs].some((s) => threadMatchesProject(t, s).match),
  );
  const orphan_threads = {
    count: orphanThreads.length,
    sample: orphanThreads.slice(0, 5).map((t) => t.thread || '(unnamed)'),
  };

  // Orphaned state files: removed-from-config residue, surfaced not hidden.
  let stateFiles = [];
  try {
    stateFiles = readdirSync(join(WATCHTOWER_DIR, 'state', 'projects'), { withFileTypes: true })
      .filter((e) => e.isFile() && e.name.endsWith('.md'))
      .map((e) => basename(e.name, '.md'));
  } catch {
    stateFiles = [];
  }
  const orphanStateFiles = stateFiles.filter((s) => !configSlugs.has(s));
  const orphaned_state_files = {
    count: orphanStateFiles.length,
    sample: orphanStateFiles.slice(0, 5),
  };

  // Duplicate paths: realpath-compared so symlinked duplicates count.
  const byReal = new Map();
  for (const [key, proj] of entries) {
    const real = proj && proj.path ? safeRealpath(proj.path) : null;
    if (!real) continue;
    if (!byReal.has(real)) byReal.set(real, []);
    byReal.get(real).push(key);
  }
  const duplicate_path_warning = [...byReal.entries()]
    .filter(([, keys]) => keys.length > 1)
    .map(([path, keys]) => ({ path, keys }));

  return {
    schema_version: CROSS_RING_SCHEMA_VERSION,
    generated_at_source: 'live-read',
    since_window: sinceInfo.since,
    since_warning: sinceInfo.since_warning,
    reason: null,
    projects: views,
    per_store_counts: {
      projects: views.length,
      threads_active: activeThreads.length,
      items_seen: allItems.length,
    },
    unattributed_items,
    orphan_threads,
    orphaned_state_files,
    duplicate_path_warning,
  };
}

// --- Single-project convenience (cwd-resolved, worktree-aware) ----------------------

export function assembleCrossRingView({ cwd, since } = {}) {
  const sinceInfo = parseSince(since);
  const config = safeReadJSON(join(WATCHTOWER_DIR, 'config.json'));
  if (!config) return { ...emptyPortfolio('no-watchtower', sinceInfo), project: null };

  let identity = null;
  try {
    if (cwd) identity = resolveProjectIdentity(cwd, config);
  } catch {
    identity = null;
  }
  if (!identity || !identity.name) {
    return { ...emptyPortfolio('project-unresolved', sinceInfo), project: null };
  }

  // A registered:false identity (untracked-but-known repo) still assembles —
  // Ring 3 and the inbox can legitimately hold data for it.
  const view = assembleProjectView({
    name: identity.name,
    slug: identity.slug,
    path: identity.path || null,
    since: sinceInfo.since,
  });
  return {
    schema_version: CROSS_RING_SCHEMA_VERSION,
    generated_at_source: 'live-read',
    since_window: sinceInfo.since,
    since_warning: sinceInfo.since_warning,
    reason: null,
    registered: identity.registered !== false,
    project: view,
  };
}

// --- CLI ------------------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = { mode: null, project: null, projectPath: null, since: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--portfolio') out.mode = 'portfolio';
    else if (a === '--project' && args[i + 1]) { out.mode = 'project'; out.project = args[++i]; }
    else if (a === '--project-path' && args[i + 1]) { out.mode = 'project-path'; out.projectPath = args[++i]; }
    else if (a === '--since' && args[i + 1]) out.since = args[++i];
  }
  return out;
}

function main() {
  const { mode, project, projectPath, since } = parseArgs(process.argv);

  if (mode === 'portfolio') {
    process.stdout.write(JSON.stringify(assemblePortfolioView({ since }), null, 2) + '\n');
    return;
  }

  if (mode === 'project') {
    const sinceInfo = parseSince(since);
    const config = safeReadJSON(join(WATCHTOWER_DIR, 'config.json'));
    if (!config) {
      process.stdout.write(JSON.stringify(
        { ...emptyPortfolio('no-watchtower', sinceInfo), project: null }, null, 2,
      ) + '\n');
      return;
    }
    // Exact config-key match first; else a UNIQUE slug match (the /briefing
    // scope filter is matched against state/projects/ dir names, which are
    // SLUGS — without this fallback the instructed flow dead-ends on any
    // project whose key differs from its slug; CP3 finding 3).
    let key = null;
    if (config.projects && Object.prototype.hasOwnProperty.call(config.projects, project)) {
      key = project;
    } else if (config.projects) {
      const bySlug = Object.keys(config.projects)
        .filter((k) => slugify(k) === slugify(project || ''));
      if (bySlug.length === 1) key = bySlug[0];
    }
    if (key == null) {
      process.stdout.write(JSON.stringify(
        { ...emptyPortfolio('project-not-in-config', sinceInfo), project: null }, null, 2,
      ) + '\n');
      return;
    }
    const view = assembleProjectView({
      name: key,
      slug: slugify(key),
      path: (config.projects[key] && config.projects[key].path) || null,
      since: sinceInfo.since,
    });
    process.stdout.write(JSON.stringify({
      schema_version: CROSS_RING_SCHEMA_VERSION,
      generated_at_source: 'live-read',
      since_window: sinceInfo.since,
      since_warning: sinceInfo.since_warning,
      reason: null,
      project: view,
    }, null, 2) + '\n');
    return;
  }

  if (mode === 'project-path') {
    process.stdout.write(
      JSON.stringify(assembleCrossRingView({ cwd: projectPath, since }), null, 2) + '\n',
    );
    return;
  }

  process.stderr.write(
    'usage: watchtower-cross-ring-reader.mjs (--portfolio | --project <name> | --project-path <cwd>) [--since <ISO|Ndays>]\n',
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

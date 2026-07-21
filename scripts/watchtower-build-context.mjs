#!/usr/bin/env node

// Watchtower context builder.
// Called by the SessionStart hook to assemble ambient state for injection.
//
// Inputs:
//   --project-path <path>   CWD of the session (required)
//   WATCHTOWER_DIR env      Override watchtower directory (default ~/.claude-cabinet/watchtower/)
//
// Outputs a string to stdout. Empty output means "nothing to inject."
// Never crashes — all errors are caught and noted inline or skipped.

import { readFileSync, readdirSync, existsSync, statSync, mkdirSync, realpathSync } from 'fs';
import { execSync } from 'child_process';
import { pathToFileURL } from 'url';
import { join, resolve, basename } from 'path';
import { currentCursor, resolveProjectIdentity } from './watchtower-lib.mjs';
import { runAdvisoryPass } from './watchtower-advisories.mjs';
import { listPending } from './watchtower-queue.mjs';
import { buildPickupPrompt } from './watchtower-routines.mjs';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const MAX_OUTPUT_CHARS = 9500;
const STALENESS_THRESHOLD_MS = 24 * 60 * 60 * 1000; // 24 hours
const RINGS_WARNING_THRESHOLD_MS = 48 * 60 * 60 * 1000; // 48 hours
const PROJECT_STALENESS_DAYS = 7;

// Section truncation priority — LOWER survives, HIGHER is dropped first.
// `assembleSections` removes whole sections (never trims within one) in
// descending priority order until the output fits MAX_OUTPUT_CHARS;
// PRIORITY_NEVER is never dropped.
const PRIORITY_NEVER = 0;    // ack directive + summary + missed-routine directive — always kept
const PRIORITY_KEEP = 1;     // threads, inbox, advisories — try hard to keep
const PRIORITY_PROJECT = 2;  // per-project state
const PRIORITY_DOMAIN = 3;   // injected domain files
const PRIORITY_PATTERNS = 4; // enforcement patterns — nice-to-have, drop first

const MAX_PATTERN_LINES = 5;

// --- Argument parsing ---

function parseArgs() {
  const args = process.argv.slice(2);
  let projectPath = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--project-path' && args[i + 1]) {
      projectPath = resolve(args[i + 1]);
      i++;
    }
  }
  return { projectPath };
}

// --- Safe file readers ---

function safeReadFile(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return readFileSync(filePath, 'utf8');
  } catch {
    return null;
  }
}

function safeReadJSON(filePath) {
  const content = safeReadFile(filePath);
  if (content === null) return null;
  try {
    return JSON.parse(content);
  } catch {
    return null;
  }
}

function fileAge(filePath) {
  try {
    const stat = statSync(filePath);
    return Date.now() - stat.mtimeMs;
  } catch {
    return Infinity;
  }
}

// --- Cached git-attention re-verification (act:a136b362) ---
//
// Ring 1's summary.md is a cached snapshot — its "MERGE OR LOSE" worktree
// lines and "diverged from main" lines are git facts that may have gone stale
// (the branch merged, the worktree was cleaned) between the last ring tick and
// this SessionStart. Relaying them verbatim asserts a stale fact as live; the
// recurring false MERGE-OR-LOSE banner is exactly what trains the operator to
// ignore the attention block. So before relaying, re-verify each git-attention
// fact (from the structured sidecar Ring 1 writes alongside summary.md)
// against current git reality and rewrite the matching summary line:
//   - now-merged / branch-gone  → DROP the line (it's resolved)
//   - still genuinely ahead     → keep it verbatim
//   - git unreachable / no path  → STAMP "(unverified)" rather than assert it
//
// The git runner is injectable for hermetic tests; the default shells git.
function gitRunner(cmd, cwd) {
  try {
    return execSync(cmd, {
      cwd, encoding: 'utf8', timeout: 10_000,
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    return null;
  }
}

// Returns 'resolved' (drop), 'live' (keep), or 'unverified' (stamp) for one
// git-attention fact. A fact is resolved when its branch is gone or already an
// ancestor of compare_ref; live when it is still ahead; unverified when git
// can't answer (no project path on disk, fetch/command failure).
//
// `fetched` is an optional Set used to dedupe the per-repo fetch across many
// facts in one re-verify pass (SessionStart should not fetch the same repo N
// times). When absent (direct callers / tests), each fact fetches once.
function verifyGitFact(fact, exec, fetched) {
  const cwd = fact.project_path;
  if (!cwd || !existsSync(cwd)) return 'unverified';
  const compareRef = fact.compare_ref || 'origin/main';

  // Best-effort refresh so origin/<main> is current; tolerate failure, fetch
  // each repo at most once per pass.
  if (!fetched || !fetched.has(cwd)) {
    exec('git fetch origin --quiet', cwd);
    fetched?.add(cwd);
  }

  const compareExists = exec(`git rev-parse --verify --quiet ${compareRef}`, cwd);
  if (compareExists === null) return 'unverified'; // not even a git repo / no ref

  const branchRef = exec(`git rev-parse --verify --quiet ${fact.branch}`, cwd);
  if (branchRef === null) return 'resolved'; // branch gone → merged or deleted

  // merge-base --is-ancestor exits 0 (→ non-null trimmed '') when merged.
  const merged = exec(`git merge-base --is-ancestor ${fact.branch} ${compareRef}`, cwd) !== null;
  return merged ? 'resolved' : 'live';
}

// Rewrite summary.md text: drop lines for resolved facts, stamp unverified
// ones. Lines for facts that are still live, and any line NOT tracked by a
// fact (everything that isn't a git ahead-check), pass through untouched.
function reverifyGitAttention(summaryText, sidecar, exec = gitRunner) {
  if (!sidecar || !Array.isArray(sidecar.facts) || sidecar.facts.length === 0) {
    return summaryText;
  }

  // Build a map: cached line text → verdict. Fetch each repo at most once.
  const verdict = new Map();
  const fetched = new Set();
  for (const fact of sidecar.facts) {
    if (!fact || !fact.line) continue;
    verdict.set(fact.line, verifyGitFact(fact, exec, fetched));
  }

  const out = [];
  for (const rawLine of summaryText.split('\n')) {
    // Summary attention lines are rendered as "- <line>"; match the payload.
    const m = rawLine.match(/^(\s*-\s+)(.*)$/);
    const payload = m ? m[2] : null;
    const v = payload != null ? verdict.get(payload) : undefined;
    if (v === undefined) { out.push(rawLine); continue; }
    if (v === 'resolved') continue; // drop the stale banner entirely
    if (v === 'unverified') {
      out.push(`${m[1]}${payload} (unverified — git state could not be re-checked this session)`);
      continue;
    }
    out.push(rawLine); // 'live' → relay verbatim
  }
  return out.join('\n');
}

// --- Queue helpers ---

function countQueueItems() {
  const queueDir = join(WATCHTOWER_DIR, 'queue', 'items');
  if (!existsSync(queueDir)) return { total: 0, urgent: 0, byCategory: {}, draftsReady: 0 };

  let total = 0;
  let urgent = 0;
  let draftsReady = 0;
  const byCategory = {};

  try {
    const entries = readdirSync(queueDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
      const item = safeReadJSON(join(queueDir, entry.name));
      if (!item || item.status !== 'pending') continue;
      total++;
      if (item.urgency === 'urgent') urgent++;
      const cat = item.category || 'uncategorized';
      byCategory[cat] = (byCategory[cat] || 0) + 1;
      if (cat === 'knowledge-extraction' && item.draft_artifact) draftsReady++;
    }
  } catch {
    // Queue unreadable — degrade gracefully
  }

  return { total, urgent, byCategory, draftsReady };
}

// A bare count is a scary number; a category breakdown is a work plan.
// "33 knowledge-extraction (drafts ready), 9 worktree-unmerged, 6 routing-decision"
function renderCategoryBreakdown(byCategory, draftsReady) {
  const parts = Object.entries(byCategory)
    .sort((a, b) => b[1] - a[1])
    .map(([cat, n]) => {
      const annotation = cat === 'knowledge-extraction' && draftsReady > 0 ? ' (drafts ready)' : '';
      return `${n} ${cat}${annotation}`;
    });
  return parts.join(', ');
}

// --- Thread / focal zoom helpers ---

function readActiveThreads() {
  const threadsDir = join(WATCHTOWER_DIR, 'state', 'threads');
  if (!existsSync(threadsDir)) return [];
  const threads = [];
  try {
    for (const f of readdirSync(threadsDir)) {
      if (!f.endsWith('.json')) continue;
      const thread = safeReadJSON(join(threadsDir, f));
      if (thread && thread.status === 'active') threads.push(thread);
    }
  } catch { /* degrade gracefully */ }
  return threads;
}

function renderFocalZoom(threads, projectSlug) {
  if (threads.length === 0) return null;

  // Find threads that touched this project, sorted by recency
  const projectThreads = threads
    .filter(t => t.sessions?.some(s => s.project === projectSlug))
    .sort((a, b) => new Date(b.last_updated) - new Date(a.last_updated));

  // Other active threads not touching this project
  const otherThreads = threads
    .filter(t => !t.sessions?.some(s => s.project === projectSlug))
    .sort((a, b) => new Date(b.last_updated) - new Date(a.last_updated));

  if (projectThreads.length === 0 && otherThreads.length === 0) return null;

  const lines = ['--- Active Threads ---'];

  // Cursor level: primary thread for this project, full detail
  if (projectThreads.length > 0) {
    const primary = projectThreads[0];
    const c = currentCursor(primary);
    lines.push(`**${primary.thread}** (primary)`);
    if (c.what) lines.push(`  What: ${c.what}`);
    if (c.why) lines.push(`  Why: ${c.why}`);
    if (c.where_left_off) lines.push(`  Where left off: ${c.where_left_off}`);
    if (c.open_questions?.length > 0) {
      lines.push(`  Open questions: ${c.open_questions.join('; ')}`);
    }
    if (c.next_steps?.length > 0) {
      lines.push(`  Next: ${c.next_steps.join('; ')}`);
    }
    const sessCount = primary.sessions?.length || 0;
    lines.push(`  (${sessCount} session${sessCount !== 1 ? 's' : ''}, last updated ${primary.last_updated?.slice(0, 10) || '?'})`);
  }

  // Thread level: other project threads, one line each
  for (const t of projectThreads.slice(1)) {
    const what = currentCursor(t).what || '';
    const age = t.last_updated?.slice(0, 10) || '?';
    lines.push(`${t.thread}: ${what} (${age})`);
  }

  // Other active threads across portfolio, one line each
  if (otherThreads.length > 0) {
    lines.push('');
    lines.push('Other active threads:');
    for (const t of otherThreads.slice(0, 5)) {
      const what = currentCursor(t).what || '';
      const proj = t.sessions?.[t.sessions.length - 1]?.project || '?';
      lines.push(`  ${t.thread} [${proj}]: ${what}`);
    }
    if (otherThreads.length > 5) {
      lines.push(`  ... and ${otherThreads.length - 5} more`);
    }
  }

  return lines.join('\n');
}

// --- Enforcement patterns (.claude/memory/patterns/) ---

// Pull the project-root patterns directory (NOT the ~/.claude/projects/<slug>/
// memory tree) so captured enforcement lessons keep shaping behavior past
// orient (act:202e5934). Index-line style only — name + one-line description,
// budget-capped. A two-field line scan, NOT a YAML parser: there is no shared
// frontmatter parser in templates/scripts/ and a full YAML parse trips on flow
// sequences (see MEMORY.md lesson_parsefrontmatter_flow_sequences).
function extractField(content, field) {
  // Only look inside the leading --- frontmatter block.
  const m = content.match(/^---\n([\s\S]*?)\n---/);
  const block = m ? m[1] : content;
  for (const line of block.split('\n')) {
    const fm = line.match(new RegExp(`^${field}:\\s*(.+)$`));
    if (fm) return fm[1].trim().replace(/^["']|["']$/g, '');
  }
  return null;
}

function renderPatterns(projectPath) {
  if (!projectPath) return null;
  const patternsDir = join(projectPath, '.claude', 'memory', 'patterns');
  if (!existsSync(patternsDir)) return null;

  let files;
  try {
    files = readdirSync(patternsDir)
      .filter(f => f.endsWith('.md') && !f.startsWith('_'))
      .sort();
  } catch {
    return null;
  }
  if (files.length === 0) return null;

  const lines = [];
  for (const f of files.slice(0, MAX_PATTERN_LINES)) {
    let name = null, desc = null;
    try {
      const content = safeReadFile(join(patternsDir, f));
      if (content) {
        name = extractField(content, 'name');
        desc = extractField(content, 'description');
      }
    } catch {
      // fall through to basename fallback
    }
    if (!name) name = f.replace(/\.md$/, '');
    lines.push(desc ? `- ${name} — ${desc}` : `- ${name}`);
  }
  if (lines.length === 0) return null;

  const more = files.length > MAX_PATTERN_LINES
    ? `\n…and ${files.length - MAX_PATTERN_LINES} more`
    : '';
  return `--- Enforcement Patterns ---\n${lines.join('\n')}${more}`;
}

// --- Missed-routine re-delivery (act:4b4fa7d9) ---
//
// A routine that fired while the desk was closed or window 1 was busy queues
// as a pending 'routine' inbox item + ·badge, but nothing re-runs it. For
// routines (unlike qa-handoffs, which are deliberately operator-chosen), the
// intended UX is "if I missed the 8am briefing, it runs first thing when I
// open the desk." So at the project's MAIN session open, surface any pending
// routine item as a run-first directive — delivery becomes automatic the way
// firing already is. The directive reuses the dispatch `buildPickupPrompt`
// (single-sourced wording, including the resolve-on-run that clears the badge).

// Is this projectPath the project's MAIN checkout (not a linked worktree)?
// A worktree's --git-dir differs from its --git-common-dir; the main checkout
// has them equal. Non-git / unreadable → false (stay silent), so worktree
// windows and odd cwds never get the directive.
function isMainCheckout(projectPath) {
  if (!projectPath) return false;
  try {
    const opts = { cwd: projectPath, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 5000 };
    const gitDir = execSync('git rev-parse --git-dir', opts).trim();
    const commonDir = execSync('git rev-parse --git-common-dir', opts).trim();
    return realpathSync(resolve(projectPath, gitDir)) === realpathSync(resolve(projectPath, commonDir));
  } catch {
    return false;
  }
}

// Render the run-first directive for pending routine items, or null when there
// are none. qa-handoff items never reach here — the caller filters to
// category 'routine'. Caps the inline list at 3; the rest stay in the inbox.
// --- Session-start acknowledgment directive (act:058d00f0) ---
//
// The injection is invisible in the terminal, so a healthy quiet session is
// indistinguishable from a silently broken hook. This never-truncated
// directive tells the session to open its first reply with one visible
// plain-English line composed from the injected state — positive
// confirmation the context loaded (no-silent-failures), replacing the
// signal orient's briefing used to give. Prompt-layer convention only:
// nothing is blocked if the line is skipped.
function renderSessionAckSection() {
  return [
    '--- Session-Start Acknowledgment ---',
    'Open your first reply of this session with ONE plain-English line confirming this watchtower context loaded: where the operator left off, what needs attention in this project, and the inbox count — e.g. "Watchtower: picked up where you left off (merged the auth fix to main, 2h ago); 1 flagged action here; inbox 12 pending." If the state below carries a staleness or health warning, lead with that warning instead. A frontier-model warning above this block stays first. One line, then proceed with the operator\'s request. Skip it if the conversation already has replies (context re-injected on resume).',
  ].join('\n');
}

function renderMissedRoutineSection(pendingItems, projectName, projectPath) {
  if (!Array.isArray(pendingItems) || pendingItems.length === 0) return null;
  const n = pendingItems.length;
  const header = `⚡ ${n} scheduled routine${n === 1 ? '' : 's'} fired while you were away and ${n === 1 ? 'has' : 'have'} not run yet. Run ${n === 1 ? 'it' : 'them'} FIRST, before other work:`;
  const lines = ['--- Scheduled Routine Waiting ---', header];
  for (const item of pendingItems.slice(0, 3)) {
    const ev = item.evidence || {};
    const routine = { name: ev.routine_name || 'routine', script: ev.script || '', trigger: ev.trigger || {} };
    lines.push(`- ${buildPickupPrompt({ routine, projectName, projectPath, itemId: item.id })}`);
  }
  if (n > 3) lines.push(`…and ${n - 3} more (run /inbox)`);
  return lines.join('\n');
}

function renderRing3FailedSection() {
  // Outage-recovery visibility (act:6fb2b7d1): surface sessions Ring 3 left
  // UNMARKED because the API was down — so the loss is never silent. Reads the
  // GLOBAL ring3/failed/ worklist (the recovery drain is portfolio-wide).
  try {
    const failedDir = join(WATCHTOWER_DIR, 'ring3', 'failed');
    if (!existsSync(failedDir)) return null;
    const n = readdirSync(failedDir).filter((f) => f.endsWith('.json')).length;
    if (n === 0) return null;
    return [
      '--- Ring 3 Capture Recovery ---',
      `⚠ ${n} session(s) failed Ring 3 capture during an API outage and were NOT lost — their transcripts are queued for recovery. When the API is up, run \`watchtower-ring3-close.mjs --reprocess-failed\` from the main checkout to reprocess them.`,
    ].join('\n');
  } catch {
    return null;
  }
}

// --- Main ---

function main() {
  const { projectPath } = parseArgs();

  // Step 1: Read config.json. No config → empty output (hook exits silently).
  const configPath = join(WATCHTOWER_DIR, 'config.json');
  const config = safeReadJSON(configPath);
  if (!config) {
    // No config means watchtower not set up — output nothing.
    return;
  }

  // Find the project for --project-path via the canonical resolver. The old
  // exact-path match could never match a mux worktree cwd, so every worktree
  // session started with zero ambient project state. The resolver walks a
  // worktree back to its main repo before matching. Unresolved or untracked
  // (registered: false) → projectSlug stays null and we proceed exactly as
  // before — no regression for unknown projects, no crash (resolver returns
  // null rather than throwing).
  let projectSlug = null;
  let projectName = null;
  let projectConfig = null;
  if (config.projects && projectPath) {
    const identity = resolveProjectIdentity(projectPath, config);
    if (identity?.registered) {
      projectSlug = identity.slug;
      projectName = identity.name;
      projectConfig = config.projects[identity.name] || null;
    }
  }

  const sections = [];

  // Step 1b: Session-start acknowledgment directive — rendered first so the
  // visible-confirmation contract sits at the top of the injection
  // (act:058d00f0). The shell hook prepends any frontier-model warning above
  // the builder output, so that warning still comes first overall.
  sections.push({ key: 'session-ack', content: renderSessionAckSection(), priority: PRIORITY_NEVER });

  // Step 2: Read state/summary.md
  const summaryPath = join(WATCHTOWER_DIR, 'state', 'summary.md');
  const summaryContent = safeReadFile(summaryPath);
  let summarySection = '';

  if (summaryContent === null) {
    summarySection = '--- Watchtower State ---\nWatchtower installed but no state data yet. Rings may not have run.';
  } else {
    const summaryAge = fileAge(summaryPath);
    let stalenessWarning = '';
    if (summaryAge > RINGS_WARNING_THRESHOLD_MS) {
      stalenessWarning = 'WARNING: State data is >48h old — rings may not be running.\n';
    } else if (summaryAge > STALENESS_THRESHOLD_MS) {
      stalenessWarning = 'Note: State data is >24h old — may be stale.\n';
    }
    // Re-verify the cached git-attention lines (MERGE-OR-LOSE / diverged
    // branch) against current git before relaying — a since-merged branch
    // should not assert a live banner (act:a136b362).
    let relayedSummary = summaryContent.trim();
    try {
      const sidecar = safeReadJSON(join(WATCHTOWER_DIR, 'state', 'git-attention.json'));
      relayedSummary = reverifyGitAttention(relayedSummary, sidecar);
    } catch {
      // never block session start on re-verification; relay the cache as-is
    }
    summarySection = `--- Watchtower State ---\n${stalenessWarning}${relayedSummary}`;
  }

  // Summary is always included and never truncated
  sections.push({ key: 'summary', content: summarySection, priority: PRIORITY_NEVER });

  // Step 2b: Missed-routine re-delivery (act:4b4fa7d9). A routine that fired
  // while the desk was closed/busy queued as a pending 'routine' item but
  // never ran; surface it as a run-first directive at the project's MAIN
  // session open. Gated to the main checkout (worktree windows stay silent);
  // qa-handoff items are excluded by the category filter (operator-initiated
  // by design). Pushed right after summary so it renders at the top, and
  // never truncated.
  if (projectName && isMainCheckout(projectPath)) {
    try {
      const pendingRoutines = listPending({ project: projectName, category: 'routine' });
      const routineSection = renderMissedRoutineSection(pendingRoutines, projectName, projectPath);
      if (routineSection) {
        sections.push({ key: 'missed-routine', content: routineSection, priority: PRIORITY_NEVER });
      }
    } catch {
      // never block session start on routine re-delivery
    }
  }

  // Outage-recovery visibility (act:6fb2b7d1): sessions Ring 3 left UNMARKED
  // because the API was down. Main checkout only (worktree windows stay quiet);
  // never truncated — a silent capture loss is exactly what this surfaces.
  if (isMainCheckout(projectPath)) {
    try {
      const ring3FailedSection = renderRing3FailedSection();
      if (ring3FailedSection) {
        sections.push({ key: 'ring3-failed', content: ring3FailedSection, priority: PRIORITY_NEVER });
      }
    } catch {
      // never block session start on recovery visibility
    }
  }

  // Step 3: If project has inject_domains, read each state/<domain>.md
  const domainSections = [];
  if (projectConfig && Array.isArray(projectConfig.inject_domains)) {
    for (const domain of projectConfig.inject_domains) {
      const domainPath = join(WATCHTOWER_DIR, 'state', `${domain}.md`);
      const domainContent = safeReadFile(domainPath);
      if (domainContent) {
        domainSections.push({
          key: `domain:${domain}`,
          content: `--- ${domain} ---\n${domainContent.trim()}`,
          priority: PRIORITY_DOMAIN,
        });
      }
    }
  }
  sections.push(...domainSections);

  // Step 4: Read state/projects/<slug>.md if exists and <7d old
  if (projectSlug) {
    const projectStatePath = join(WATCHTOWER_DIR, 'state', 'projects', `${projectSlug}.md`);
    const projectStateContent = safeReadFile(projectStatePath);
    if (projectStateContent) {
      const projectStateAge = fileAge(projectStatePath);
      const projectStaleDays = projectStateAge / (24 * 60 * 60 * 1000);
      if (projectStaleDays < PROJECT_STALENESS_DAYS) {
        sections.push({
          key: `project:${projectSlug}`,
          content: `--- Project: ${projectSlug} ---\n${projectStateContent.trim()}`,
          priority: PRIORITY_PROJECT,
        });
      }
    }
  }

  // Step 5: Focal zoom — thread state
  const allThreads = readActiveThreads();
  if (allThreads.length > 0 && projectSlug) {
    const focalZoom = renderFocalZoom(allThreads, projectSlug);
    if (focalZoom) {
      sections.push({
        key: 'threads',
        content: focalZoom,
        priority: PRIORITY_KEEP,
      });
    }
  }

  // Step 6: Inbox summary — one number, decomposed by category
  const { total, urgent, byCategory, draftsReady } = countQueueItems();
  if (total > 0) {
    const breakdown = renderCategoryBreakdown(byCategory, draftsReady);
    const headline = urgent > 0
      ? `⚡ ${total} pending (${urgent} urgent) — run /inbox`
      : `${total} pending — run /inbox when ready`;
    sections.push({
      key: 'queue',
      content: `--- Inbox ---\n${headline}\n${breakdown}`,
      priority: PRIORITY_KEEP,
    });
  }

  // Step 6b: Enforcement patterns from the project-root patterns dir
  const patternsSection = renderPatterns(projectPath);
  if (patternsSection) {
    sections.push({
      key: 'patterns',
      content: patternsSection,
      priority: PRIORITY_PATTERNS,
    });
  }

  // Step 6c: Environment advisories (LSP / Railway-MCP / hookify / briefing
  // file / registry orphans) with per-project dismissal memory. The module
  // owns all logic + I/O and never throws; the builder only renders. This is
  // orient's advisory home now that orient is being retired (act:f9ea075d).
  try {
    const advisories = runAdvisoryPass({ projectPath });
    if (advisories.length > 0) {
      const body = advisories.map(a => `- ${a.action}`).join('\n');
      sections.push({
        key: 'advisories',
        content: `--- Advisories ---\n${body}`,
        priority: PRIORITY_KEEP,
      });
    }
  } catch {
    // never block session start on advisory rendering
  }

  // Untracked project mode — if no project match, just add a note
  if (!projectSlug && projectPath) {
    sections.push({
      key: 'untracked',
      content: '(This project is not tracked by watchtower. Only global state is shown.)',
      priority: PRIORITY_KEEP,
    });
  }

  // Step 7: Assemble and truncate if needed
  let output = assembleSections(sections);

  process.stdout.write(output);
}

function assembleSections(sections) {
  // Sort by priority (0 = never truncate, higher = truncate first)
  // Build full output first
  let fullOutput = sections.map(s => s.content).join('\n\n');

  if (fullOutput.length <= MAX_OUTPUT_CHARS) {
    return fullOutput;
  }

  // Need to truncate. Remove sections in reverse priority order (highest first).
  // PRIORITY_PATTERNS (4) = enforcement patterns (truncated first)
  // PRIORITY_DOMAIN (3)   = domain files
  // PRIORITY_PROJECT (2)  = project file
  // PRIORITY_KEEP (1)     = threads/queue/advisories/untracked (try to keep)
  // PRIORITY_NEVER (0)    = summary (never truncated)

  const sortedByTruncPriority = [...sections].sort((a, b) => b.priority - a.priority);

  let remaining = [...sections];
  for (const section of sortedByTruncPriority) {
    if (section.priority === PRIORITY_NEVER) break; // Never truncate summary

    const idx = remaining.findIndex(s => s.key === section.key);
    if (idx === -1) continue;

    remaining.splice(idx, 1);
    const candidate = remaining.map(s => s.content).join('\n\n');
    if (candidate.length <= MAX_OUTPUT_CHARS) {
      return candidate;
    }
  }

  // If still too long after removing all truncatable sections,
  // return what we have (summary only, possibly)
  return remaining.map(s => s.content).join('\n\n');
}

// Exports for hermetic tests (act:a136b362). Importing this module must NOT
// run main() — gate the entry on an argv/url match (matches the ring scripts).
export { reverifyGitAttention, verifyGitFact, renderMissedRoutineSection, renderSessionAckSection, renderRing3FailedSection, isMainCheckout };

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
    // Never crash — silent exit if something unexpected happens
    process.exit(0);
  }
}

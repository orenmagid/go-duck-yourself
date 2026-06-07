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

import { readFileSync, readdirSync, existsSync, statSync, mkdirSync } from 'fs';
import { join, resolve, basename } from 'path';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const MAX_OUTPUT_CHARS = 9500;
const STALENESS_THRESHOLD_MS = 24 * 60 * 60 * 1000; // 24 hours
const RINGS_WARNING_THRESHOLD_MS = 48 * 60 * 60 * 1000; // 48 hours
const PROJECT_STALENESS_DAYS = 7;

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
    const c = primary.cursor || {};
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
    const what = t.cursor?.what || '';
    const age = t.last_updated?.slice(0, 10) || '?';
    lines.push(`${t.thread}: ${what} (${age})`);
  }

  // Other active threads across portfolio, one line each
  if (otherThreads.length > 0) {
    lines.push('');
    lines.push('Other active threads:');
    for (const t of otherThreads.slice(0, 5)) {
      const what = t.cursor?.what || '';
      const proj = t.sessions?.[t.sessions.length - 1]?.project || '?';
      lines.push(`  ${t.thread} [${proj}]: ${what}`);
    }
    if (otherThreads.length > 5) {
      lines.push(`  ... and ${otherThreads.length - 5} more`);
    }
  }

  return lines.join('\n');
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

  // Find project matching --project-path
  let projectSlug = null;
  let projectConfig = null;
  if (config.projects && projectPath) {
    for (const [slug, proj] of Object.entries(config.projects)) {
      if (proj.path && resolve(proj.path) === projectPath) {
        projectSlug = slug;
        projectConfig = proj;
        break;
      }
    }
  }

  const sections = [];

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
    summarySection = `--- Watchtower State ---\n${stalenessWarning}${summaryContent.trim()}`;
  }

  // Summary is always included and never truncated
  sections.push({ key: 'summary', content: summarySection, priority: 0 });

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
          priority: 3, // Truncated first
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
          priority: 2, // Truncated second (after domains)
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
        priority: 1,
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
      priority: 1,
    });
  }

  // Untracked project mode — if no project match, just add a note
  if (!projectSlug && projectPath) {
    sections.push({
      key: 'untracked',
      content: '(This project is not tracked by watchtower. Only global state is shown.)',
      priority: 1,
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
  // Priority 3 = domain files (truncated first)
  // Priority 2 = project file (truncated second)
  // Priority 0 = summary (never truncated)
  // Priority 1 = queue/untracked (try to keep)

  const sortedByTruncPriority = [...sections].sort((a, b) => b.priority - a.priority);

  let remaining = [...sections];
  for (const section of sortedByTruncPriority) {
    if (section.priority === 0) break; // Never truncate summary

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

try {
  main();
} catch {
  // Never crash — silent exit if something unexpected happens
  process.exit(0);
}

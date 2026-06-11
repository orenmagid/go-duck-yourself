#!/usr/bin/env node

// Watchtower Ring 1 — Mechanical heartbeat.
// Zero Claude API calls. Runs on a cron (default 300s / 5min).
//
// For each tracked project:
//   - Git state (branch, last commit, branches ahead of main)
//   - pib-db queries (open actions, flagged, stale, completion candidates)
//   - Active sessions (recently-modified .jsonl files)
//   - Deployment detection (railway.toml, fly.toml, vercel.json)
//
// Assembles state/summary.md (hard cap 30 lines, atomic write)
// and per-project state/projects/<slug>.md files.
// Writes ring1-health.json with last-run timestamp.
//
// Consumer hooks: reads config.json hooks.ring1-post-collect, spawns
// each with 30s timeout, passes project state JSON on stdin.

import {
  readFileSync, readdirSync, existsSync, statSync,
  mkdirSync,
} from 'fs';
import { join, basename } from 'path';
import { execSync } from 'child_process';
import { homedir } from 'os';
import {
  atomicWrite, loadConfig, slugify, log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending, resolveItem, loadBetterSqlite3,
  preserveRing3LastSession,
} from './watchtower-lib.mjs';

const WATCHTOWER_DIR = getWatchtowerDir();

const CLAUDE_HOME = join(homedir(), '.claude');
const STALE_DAYS = 14;
const ACTIVE_SESSION_THRESHOLD_MS = 30 * 60 * 1000; // 30 minutes
const DEPLOY_MARKERS = ['railway.toml', 'fly.toml', 'vercel.json'];
const HOOK_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function log(msg) { _log('ring1', msg); }
function logError(msg) { _logError('ring1', msg); }

function ago(ms) {
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function safeExec(cmd, opts = {}) {
  try {
    // stderr is ignored, not inherited — otherwise expected failures
    // (e.g. non-git projects) bleed "fatal:" noise into the launchd log
    return execSync(cmd, {
      encoding: 'utf8', timeout: 10_000,
      stdio: ['ignore', 'pipe', 'ignore'],
      ...opts,
    }).trim();
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Git state collection
// ---------------------------------------------------------------------------

function collectGitState(projectPath) {
  if (!existsSync(join(projectPath, '.git'))) {
    return null;
  }

  const cwd = projectPath;
  const branch = safeExec('git rev-parse --abbrev-ref HEAD', { cwd });
  const lastCommitRaw = safeExec(
    'git log -1 --format="%H%x00%s%x00%aI"',
    { cwd }
  );

  let lastCommit = null;
  if (lastCommitRaw) {
    const [hash, message, timestamp] = lastCommitRaw.split('\0');
    lastCommit = { hash, message, timestamp };
  }

  // Branches ahead of main
  const branchesAhead = [];
  const mainBranch = safeExec('git rev-parse --verify main 2>/dev/null && echo main || echo master', { cwd });
  if (mainBranch) {
    const branchList = safeExec('git branch --no-merged ' + mainBranch + ' 2>/dev/null', { cwd });
    if (branchList) {
      for (const line of branchList.split('\n')) {
        const b = line.trim().replace(/^\* /, '');
        if (b && b !== mainBranch) branchesAhead.push(b);
      }
    }
  }

  return { branch, lastCommit, branchesAhead, mainBranch };
}

// ---------------------------------------------------------------------------
// pib-db queries
// ---------------------------------------------------------------------------

function collectPibState(projectPath) {
  const dbPath = join(projectPath, 'pib.db');
  if (!existsSync(dbPath)) {
    return null;
  }

  const Database = loadBetterSqlite3(projectPath);
  if (!Database) {
    return { error: 'better-sqlite3 not available' };
  }

  let db;
  try {
    db = new Database(dbPath, { readonly: true });
  } catch (e) {
    return { error: `Cannot open pib.db: ${e.message}` };
  }

  try {
    // Open action count
    const openActions = db.prepare(
      "SELECT COUNT(*) as count FROM actions WHERE status IN ('open', 'in-progress', 'blocked')"
    ).get();

    // Flagged count (user-prioritized, still-open actions)
    let flaggedCount = 0;
    try {
      flaggedCount = db.prepare(
        "SELECT COUNT(*) as count FROM actions WHERE flagged = 1 AND status IN ('open', 'in-progress', 'blocked') AND deleted_at IS NULL"
      ).get().count;
    } catch {
      // flagged column may not exist in older schemas
    }

    // Deferred actions waiting on triggers — standing state, not attention.
    // Orient's deferred-check evaluates these every session; reported in the
    // per-project deep file, never in the summary's attention list.
    let deferredTriggerCount = 0;
    try {
      deferredTriggerCount = db.prepare(
        "SELECT COUNT(*) as count FROM actions WHERE status = 'deferred' AND trigger_condition IS NOT NULL AND deleted_at IS NULL"
      ).get().count;
    } catch {
      // trigger_condition column may not exist in older schemas
    }

    // Stale projects (no action updated in 14 days)
    const staleThreshold = new Date(Date.now() - STALE_DAYS * 86400000).toISOString().slice(0, 10);
    let staleProjects = [];
    try {
      staleProjects = db.prepare(
        `SELECT DISTINCT p.fid, p.name FROM projects p
         WHERE p.status = 'active' AND p.deleted_at IS NULL
         AND NOT EXISTS (
           SELECT 1 FROM actions a
           WHERE a.project_fid = p.fid
           AND a.updated_at > ?
           AND a.status IN ('open', 'in-progress')
         )`
      ).all(staleThreshold);
    } catch {
      // best-effort
    }

    // Completion candidates (all actions done)
    let completionCandidates = [];
    try {
      completionCandidates = db.prepare(
        `SELECT p.fid, p.name FROM projects p
         WHERE p.status = 'active' AND p.deleted_at IS NULL
         AND EXISTS (SELECT 1 FROM actions a WHERE a.project_fid = p.fid AND a.deleted_at IS NULL)
         AND NOT EXISTS (
           SELECT 1 FROM actions a
           WHERE a.project_fid = p.fid
           AND a.deleted_at IS NULL
           AND a.status != 'done'
         )`
      ).all();
    } catch {
      // best-effort
    }

    // Per-project breakdown for portfolio pulse
    let projectBreakdown = [];
    try {
      projectBreakdown = db.prepare(
        `SELECT p.fid, p.name,
           SUM(CASE WHEN a.status IN ('open','in-progress','blocked') THEN 1 ELSE 0 END) as open_count,
           SUM(CASE WHEN a.status = 'blocked' THEN 1 ELSE 0 END) as blocked_count
         FROM projects p
         LEFT JOIN actions a ON a.project_fid = p.fid
         GROUP BY p.fid, p.name`
      ).all();
    } catch {
      // best-effort
    }

    return {
      openActions: openActions.count,
      flaggedCount,
      deferredTriggerCount,
      staleProjects,
      completionCandidates,
      projectBreakdown,
    };
  } finally {
    db.close();
  }
}

// ---------------------------------------------------------------------------
// Active session detection
// ---------------------------------------------------------------------------

function checkMemoryIntegrity(projectPath) {
  const encoded = projectPath.replace(/\//g, '-');
  const memDir = join(CLAUDE_HOME, 'projects', encoded, 'memory');
  const indexPath = join(memDir, 'MEMORY.md');

  if (!existsSync(memDir) || !existsSync(indexPath)) return null;

  try {
    const indexContent = readFileSync(indexPath, 'utf8');
    const files = readdirSync(memDir).filter(f => f.endsWith('.md') && f !== 'MEMORY.md');
    const orphans = files.filter(f => !indexContent.includes(f));
    const referenced = indexContent.match(/\(([^)]+\.md)\)/g) || [];
    const missing = referenced
      .map(r => r.slice(1, -1))
      .filter(f => f !== 'MEMORY.md' && !existsSync(join(memDir, f)));

    if (orphans.length === 0 && missing.length === 0) return null;

    return { orphans, missing };
  } catch {
    return null;
  }
}

function detectActiveSessions(projectPath) {
  // Claude Code stores session transcripts as .jsonl files under
  // ~/.claude/projects/<encoded-path>/
  const encoded = projectPath.replace(/\//g, '-');
  const sessionsDir = join(CLAUDE_HOME, 'projects', encoded);

  if (!existsSync(sessionsDir)) return [];

  const now = Date.now();
  const active = [];

  try {
    const entries = readdirSync(sessionsDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.endsWith('.jsonl')) continue;
      const filePath = join(sessionsDir, entry.name);
      try {
        const st = statSync(filePath);
        if (now - st.mtimeMs < ACTIVE_SESSION_THRESHOLD_MS) {
          active.push({
            file: entry.name,
            lastModified: st.mtime.toISOString(),
            agoMs: now - st.mtimeMs,
          });
        }
      } catch {
        // stat failure, skip
      }
    }
  } catch {
    // directory read failure, skip
  }

  return active;
}

// ---------------------------------------------------------------------------
// Deployment detection (cached per config cycle)
// ---------------------------------------------------------------------------

function detectDeployment(projectPath) {
  const found = [];
  for (const marker of DEPLOY_MARKERS) {
    if (existsSync(join(projectPath, marker))) {
      found.push(marker);
    }
  }
  return found;
}

// ---------------------------------------------------------------------------
// Consumer hooks
// ---------------------------------------------------------------------------

function runConsumerHooksSync(hooks, projectState) {
  if (!hooks || !Array.isArray(hooks) || hooks.length === 0) return [];

  const results = [];
  for (const hookCmd of hooks) {
    try {
      const output = execSync(hookCmd, {
        encoding: 'utf8',
        timeout: HOOK_TIMEOUT_MS,
        input: JSON.stringify(projectState),
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      try {
        const parsed = JSON.parse(output);
        if (parsed && parsed.additional_checks) {
          results.push(...parsed.additional_checks);
        }
      } catch {
        // non-JSON output, ignore
      }
    } catch (e) {
      // Hook failure — log, never abort
      results.push({
        hook: hookCmd,
        error: e.message,
        status: 'hook-failed',
      });
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Queue item creation for branch divergence
// ---------------------------------------------------------------------------

function createBranchDivergedItem(projectName, projectPath, branch) {
  try {
    // Check for existing branch-diverged item for this branch (dedup)
    const existingItems = listPending({ category: 'branch-diverged' });
    const isDuplicate = existingItems.some(item =>
      item.evidence &&
      item.evidence.branch === branch &&
      item.project_path === projectPath
    );

    if (isDuplicate) return;

    createItem({
      project: projectName,
      project_path: projectPath,
      filed_by: 'ring1',
      category: 'branch-diverged',
      urgency: 'normal',
      title: `Branch "${branch}" diverged from main`,
      summary: `Branch "${branch}" in ${projectName} is ahead of main with no active session on it. Consider merging or cleaning up.`,
      context_anchor: `git log main..${branch} in ${projectPath}`,
      evidence: { branch, project_path: projectPath },
    });
  } catch (e) {
    logError(`Failed to create branch-diverged queue item: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Worktree scan — find orphaned worktrees with unmerged work
// ---------------------------------------------------------------------------

// Count uncommitted changes in a worktree, excluding CC/mux session
// artifacts (.claude/, .mcp.json), node_modules, and package-manager
// lockfiles — all of which churn in mux worktrees without representing
// work to lose. node_modules is untracked in every worktree; lockfiles
// regenerate with worktree-relative `file:` dependency paths (a worktree
// sits deeper than the main repo, so any install rewrites resolved tarball
// paths, e.g. ../../ → ../../../../). Both produced false "unmerged work"
// alarms for fully-merged branches. A genuine dependency change also
// dirties package.json, which is still counted, so real work still alarms.
// safeExec trims output, which can shift porcelain column offsets —
// match artifact patterns anywhere in the line instead.
function countRealUncommitted(wtPath) {
  const uncommitted = safeExec('git status --porcelain', { cwd: wtPath });
  if (!uncommitted) return 0;
  return uncommitted.split('\n').filter(l => {
    if (!l.trim()) return false;
    if (/\s\.claude$/.test(l) || /\s\.claude\//.test(l)) return false;
    if (/\s\.mcp\.json$/.test(l)) return false;
    if (/\snode_modules$/.test(l) || /\snode_modules\//.test(l)) return false;
    if (/(?:^|[\s/])package-lock\.json$/.test(l)) return false;
    if (/(?:^|[\s/])npm-shrinkwrap\.json$/.test(l)) return false;
    if (/(?:^|[\s/])yarn\.lock$/.test(l)) return false;
    if (/(?:^|[\s/])pnpm-lock\.yaml$/.test(l)) return false;
    if (/(?:^|[\s/])bun\.lockb$/.test(l)) return false;
    if (isMainShadow(l, wtPath)) return false;
    return true;
  }).length;
}

// A worktree whose HEAD predates a file committed to main shows that file as
// untracked ("?? path") even though main already owns it — a stale-worktree
// shadow, not work to lose. (The mux/devex false-alarm: scripts/skill-usage.mjs
// was added to main after devex branched, so it counted as a "real" uncommitted
// change and blocked the merged branch from ever auto-resolving.) Exclude an
// untracked file only when main holds the SAME path with byte-identical content
// — a genuinely new untracked file, or a modified shadow, still counts, so real
// work-to-lose still alarms.
function isMainShadow(porcelainLine, wtPath) {
  const m = porcelainLine.match(/^\?\?\s+(.+)$/);
  if (!m) return false;
  let p = m[1].trim();
  if (p.startsWith('"') && p.endsWith('"')) p = p.slice(1, -1);
  const mainBlob = safeExec(`git rev-parse --verify --quiet "main:${p}"`, { cwd: wtPath });
  if (!mainBlob) return false;
  const wtBlob = safeExec(`git hash-object "${p}"`, { cwd: wtPath });
  return !!wtBlob && wtBlob === mainBlob;
}

// Does a worktree-unmerged item belong to this project? Can't trust
// item.project_path alone — Ring 3 historically attributed items to the
// worktree path rather than the project root. Resolve ownership through
// git itself: a worktree's --git-common-dir points at the main repo.
function itemBelongsToProject(item, projectPath) {
  if (item.project_path === projectPath) return true;
  const ev = item.evidence || {};
  if (ev.worktree_path && existsSync(ev.worktree_path)) {
    const commonDir = safeExec(
      'git rev-parse --path-format=absolute --git-common-dir',
      { cwd: ev.worktree_path }
    );
    return !!commonDir && commonDir === join(projectPath, '.git');
  }
  // Worktree gone — claim only if the branch lives in this repo
  return !!(ev.branch && safeExec(`git rev-parse --verify ${ev.branch}`, { cwd: projectPath }));
}

// Auto-resolve pending worktree-unmerged items whose branch is now clean.
// Mechanical truth-checking, not judgment: if the branch has 0 commits
// ahead of main and no real uncommitted changes, the alarm is stale —
// and it will never self-heal otherwise, because dedup suppresses
// refiling but nothing retracts the original. Stale alarms train the
// operator to ignore real ones.
function autoResolveWorktreeItems(projectName, projectPath) {
  let pending;
  try {
    pending = listPending({ category: 'worktree-unmerged' });
  } catch {
    return;
  }

  for (const item of pending) {
    if (!itemBelongsToProject(item, projectPath)) continue;
    const ev = item.evidence || {};
    if (!ev.branch) continue;

    const branchRef = safeExec(`git rev-parse --verify ${ev.branch}`, { cwd: projectPath });

    let staleReason = null;
    if (!branchRef) {
      // Branch is gone entirely — merged or deliberately deleted;
      // either way there is nothing left to lose.
      staleReason = `branch "${ev.branch}" no longer exists`;
    } else {
      const ahead = safeExec(`git log --oneline main..${ev.branch}`, { cwd: projectPath });
      const aheadCount = ahead ? ahead.split('\n').filter(l => l.trim()).length : 0;
      const uncommittedCount = (ev.worktree_path && existsSync(ev.worktree_path))
        ? countRealUncommitted(ev.worktree_path)
        : 0;
      if (aheadCount === 0 && uncommittedCount === 0) {
        staleReason = '0 commits ahead of main, no uncommitted changes (session artifacts excluded)';
      }
    }

    if (!staleReason) continue;

    try {
      resolveItem(item.id, {
        resolution: 'merged',
        resolution_notes: `Auto-resolved by Ring 1: ${staleReason}.`,
      });
      log(`Auto-resolved stale worktree-unmerged item ${item.id} (${ev.branch})`);
    } catch (e) {
      logError(`Failed to auto-resolve ${item.id}: ${e.message}`);
    }
  }
}

function scanWorktrees(projectName, projectPath) {
  if (!existsSync(join(projectPath, '.git'))) return [];

  // Retract stale alarms before looking for new ones
  autoResolveWorktreeItems(projectName, projectPath);

  const wtList = safeExec('git worktree list --porcelain', { cwd: projectPath });
  if (!wtList) return [];

  const worktrees = [];
  let current = {};
  for (const line of wtList.split('\n')) {
    if (line.startsWith('worktree ')) {
      if (current.path) worktrees.push(current);
      current = { path: line.slice(9) };
    } else if (line.startsWith('branch ')) {
      current.branch = line.slice(7).replace('refs/heads/', '');
    } else if (line === 'bare') {
      current.bare = true;
    } else if (line.trim() === '') {
      if (current.path) worktrees.push(current);
      current = {};
    }
  }
  if (current.path) worktrees.push(current);

  // Filter to mux worktrees (under ~/.mux/worktrees/) that aren't the main worktree
  const muxDir = join(homedir(), '.mux', 'worktrees');
  const orphaned = [];

  for (const wt of worktrees) {
    if (wt.bare || !wt.path.startsWith(muxDir)) continue;
    if (wt.path === projectPath) continue;

    // Check if branch has unmerged commits
    const mainRef = safeExec('git rev-parse --verify main', { cwd: projectPath });
    if (!mainRef) continue;

    const ahead = safeExec(`git log --oneline main..${wt.branch}`, { cwd: projectPath });
    const aheadCount = ahead ? ahead.split('\n').filter(l => l.trim()).length : 0;

    // Also check for uncommitted changes in the worktree
    // (artifact exclusions live in countRealUncommitted)
    const uncommittedCount = countRealUncommitted(wt.path);

    if (aheadCount === 0 && uncommittedCount === 0) continue;

    // Check if there's an active tmux window for this worktree
    const windowName = basename(wt.path).replace(/^[^-]+-/, '');
    const tmuxWindows = safeExec('tmux list-windows -a -F "#{window_name}" 2>/dev/null');
    const hasWindow = tmuxWindows && tmuxWindows.split('\n').some(w => w.trim() === windowName);

    if (hasWindow) continue; // Active window exists, not orphaned

    orphaned.push({
      path: wt.path,
      branch: wt.branch,
      ahead: aheadCount,
      uncommitted: uncommittedCount,
    });
  }

  // Create inbox items for orphaned worktrees
  for (const wt of orphaned) {
    try {
      const existing = listPending({ category: 'worktree-unmerged' });
      const isDuplicate = existing.some(item =>
        item.evidence?.branch === wt.branch &&
        item.evidence?.worktree_path === wt.path
      );
      if (isDuplicate) continue;

      const detail = [];
      if (wt.ahead > 0) detail.push(`${wt.ahead} unmerged commit(s)`);
      if (wt.uncommitted > 0) detail.push(`${wt.uncommitted} uncommitted change(s)`);

      createItem({
        project: projectName,
        project_path: projectPath,
        filed_by: 'ring1',
        category: 'worktree-unmerged',
        urgency: 'urgent',
        title: `Orphaned worktree "${wt.branch}" has unmerged work`,
        summary: `Worktree at ${wt.path} has ${detail.join(' and ')} with no active tmux window. Merge to main or the work may be lost.`,
        context_anchor: `git log main..${wt.branch} in ${wt.path}`,
        evidence: { branch: wt.branch, worktree_path: wt.path, ahead: wt.ahead, uncommitted: wt.uncommitted },
        options: [
          { key: 'merge', label: 'Merge to main now' },
          { key: 'keep', label: 'Keep branch for later' },
          { key: 'dismiss', label: 'Dismiss (already handled)' },
        ],
      });
    } catch (e) {
      logError(`Failed to create worktree-unmerged item: ${e.message}`);
    }
  }

  return orphaned;
}

// ---------------------------------------------------------------------------
// Summary assembly (30-line hard cap)
// ---------------------------------------------------------------------------

function assembleSummary(projectStates, config) {
  const now = new Date().toISOString();
  const lines = [];

  lines.push(`# Watchtower — ${now}`);
  lines.push('');

  // --- Where You Left Off ---
  lines.push('## Where You Left Off');
  let mostRecent = null;
  let mostRecentTime = 0;
  for (const ps of projectStates) {
    if (ps.git && ps.git.lastCommit) {
      const commitTime = new Date(ps.git.lastCommit.timestamp).getTime();
      if (commitTime > mostRecentTime) {
        mostRecentTime = commitTime;
        mostRecent = ps;
      }
    }
    // Also check active sessions
    if (ps.activeSessions && ps.activeSessions.length > 0) {
      for (const s of ps.activeSessions) {
        const t = new Date(s.lastModified).getTime();
        if (t > mostRecentTime) {
          mostRecentTime = t;
          mostRecent = ps;
        }
      }
    }
  }
  if (mostRecent) {
    const desc = mostRecent.git?.lastCommit?.message || 'unknown activity';
    lines.push(`${mostRecent.name}: ${desc} (${ago(Date.now() - mostRecentTime)})`);
  } else {
    lines.push('No recent activity detected.');
  }
  lines.push('');

  // --- What Needs Attention ---
  lines.push('## What Needs Attention');
  const attention = [];
  for (const ps of projectStates) {
    if (ps.pib && ps.pib.flaggedCount > 0) {
      attention.push(`${ps.name}: ${ps.pib.flaggedCount} flagged action(s)`);
    }
    if (ps.pib && ps.pib.staleProjects && ps.pib.staleProjects.length > 0) {
      for (const sp of ps.pib.staleProjects) {
        attention.push(`${ps.name}/${sp.name}: stale (no activity in ${STALE_DAYS}d)`);
      }
    }
    if (ps.divergedBranches && ps.divergedBranches.length > 0) {
      for (const b of ps.divergedBranches) {
        attention.push(`${ps.name}: branch "${b}" diverged from main`);
      }
    }
    if (ps.pib && ps.pib.completionCandidates && ps.pib.completionCandidates.length > 0) {
      for (const c of ps.pib.completionCandidates) {
        attention.push(`${ps.name}/${c.name}: all actions done — close?`);
      }
    }
    if (ps.orphanedWorktrees && ps.orphanedWorktrees.length > 0) {
      for (const wt of ps.orphanedWorktrees) {
        attention.unshift(`⚠ ${ps.name}: worktree "${wt.branch}" has unmerged work — MERGE OR LOSE`);
      }
    }
    if (ps.memoryIntegrity) {
      const mi = ps.memoryIntegrity;
      if (mi.orphans.length > 0) {
        attention.push(`${ps.name}: ${mi.orphans.length} orphaned memory file(s) — not indexed in MEMORY.md`);
      }
      if (mi.missing.length > 0) {
        attention.push(`${ps.name}: ${mi.missing.length} broken memory reference(s) — indexed but file missing`);
      }
    }
  }
  if (attention.length === 0) {
    lines.push('Nothing urgent.');
  } else {
    // Cap at 8 items (urgent first via natural ordering, then newest)
    const maxAttention = 8;
    const shown = attention.slice(0, maxAttention);
    for (const a of shown) {
      lines.push(`- ${a}`);
    }
    if (attention.length > maxAttention) {
      lines.push(`... and ${attention.length - maxAttention} more`);
    }
  }
  lines.push('');

  // --- Portfolio Pulse ---
  lines.push('## Portfolio Pulse');
  const pulseLines = [];
  for (const ps of projectStates) {
    const openCount = ps.pib?.openActions ?? '?';
    const hasActive = ps.activeSessions && ps.activeSessions.length > 0;
    const isBlocked = ps.pib?.projectBreakdown?.some(p => p.blocked_count > 0);
    const status = hasActive ? 'hot' : (isBlocked ? 'blocked' : 'quiet');
    pulseLines.push(`- ${ps.name}: ${openCount} open, ${status}`);
  }
  // Sort: hot first, then blocked, then quiet
  const order = { hot: 0, blocked: 1, quiet: 2 };
  pulseLines.sort((a, b) => {
    const sa = a.includes('hot') ? 0 : a.includes('blocked') ? 1 : 2;
    const sb = b.includes('hot') ? 0 : b.includes('blocked') ? 1 : 2;
    return sa - sb;
  });
  for (const pl of pulseLines) lines.push(pl);
  lines.push('');

  // --- Inbox ---
  lines.push('## Inbox');
  const pendingItems = listPending();
  if (pendingItems.length === 0) {
    lines.push('No pending items.');
  } else {
    const oldest = pendingItems.reduce((a, b) =>
      new Date(a.filed_at) < new Date(b.filed_at) ? a : b
    );
    lines.push(`${pendingItems.length} pending (oldest: ${ago(Date.now() - new Date(oldest.filed_at).getTime())})`);
  }
  lines.push('');

  // --- Health ---
  lines.push('## Health');
  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring1-health.json');
  if (existsSync(healthPath)) {
    try {
      const health = JSON.parse(readFileSync(healthPath, 'utf8'));
      const lastRun = health.last_run ? ago(Date.now() - new Date(health.last_run).getTime()) : 'never';
      lines.push(`Ring 1: last ran ${lastRun}, ${health.status || 'unknown'}`);
    } catch {
      lines.push('Ring 1: health file corrupt');
    }
  } else {
    lines.push('Ring 1: first run');
  }
  lines.push('');
  lines.push('Deep files: ~/.claude-cabinet/watchtower/state/projects/');

  // --- Truncation to 30 lines ---
  if (lines.length > 30) {
    // Truncation order: Health detail first, then quiet Portfolio projects
    // Find Health section and reduce
    const healthIdx = lines.findIndex(l => l === '## Health');
    if (healthIdx >= 0) {
      // Replace health content with one-line summary
      const nextSection = lines.findIndex((l, i) => i > healthIdx && l.startsWith('## '));
      const end = nextSection > 0 ? nextSection : lines.length;
      const healthContent = lines.slice(healthIdx + 1, end).filter(l => l.trim()).join('; ');
      lines.splice(healthIdx + 1, end - healthIdx - 1, healthContent || 'OK');
    }

    // If still too long, remove quiet projects from Portfolio Pulse
    if (lines.length > 30) {
      const pulseIdx = lines.findIndex(l => l === '## Portfolio Pulse');
      if (pulseIdx >= 0) {
        const nextSection = lines.findIndex((l, i) => i > pulseIdx && l.startsWith('## '));
        const end = nextSection > 0 ? nextSection : lines.length;
        const kept = [];
        for (let i = pulseIdx + 1; i < end; i++) {
          if (!lines[i].includes('quiet')) kept.push(lines[i]);
        }
        lines.splice(pulseIdx + 1, end - pulseIdx - 1, ...kept);
      }
    }

    // Final hard truncation
    if (lines.length > 30) {
      lines.length = 29;
      lines.push('(truncated to 30 lines)');
    }
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Per-project state file
// ---------------------------------------------------------------------------

function assembleProjectState(ps) {
  const now = new Date().toISOString();
  const lines = [];

  lines.push(`# ${ps.name} — ${now}`);
  lines.push('');

  // Active Plans
  lines.push('## Active Plans');
  if (ps.pib && ps.pib.openActions != null) {
    if (ps.pib.openActions > 0) {
      lines.push(`${ps.pib.openActions} open action(s)`);
    } else {
      lines.push('No open actions.');
    }
  } else {
    lines.push('No pib-db data.');
  }
  lines.push('');

  // Last Session
  lines.push('## Last Session');
  if (ps.activeSessions && ps.activeSessions.length > 0) {
    const latest = ps.activeSessions.reduce((a, b) =>
      new Date(a.lastModified) > new Date(b.lastModified) ? a : b
    );
    lines.push(`Active: ${ago(latest.agoMs)}`);
  } else if (ps.git && ps.git.lastCommit) {
    lines.push(`Last commit: ${ps.git.lastCommit.message} (${ps.git.lastCommit.timestamp})`);
  } else {
    lines.push('No session data.');
  }
  lines.push('');

  // Standing Issues
  lines.push('## Standing Issues');
  const issues = [];
  if (ps.pib && ps.pib.flaggedCount > 0) {
    issues.push(`${ps.pib.flaggedCount} flagged action(s)`);
  }
  if (ps.pib && ps.pib.deferredTriggerCount > 0) {
    issues.push(`${ps.pib.deferredTriggerCount} deferred action(s) waiting on triggers`);
  }
  if (ps.divergedBranches && ps.divergedBranches.length > 0) {
    issues.push(`Diverged branches: ${ps.divergedBranches.join(', ')}`);
  }
  if (issues.length === 0) {
    lines.push('None.');
  } else {
    for (const i of issues) lines.push(`- ${i}`);
  }
  lines.push('');

  // Tech Stack
  lines.push('## Tech Stack');
  const stack = [];
  if (ps.deployment && ps.deployment.length > 0) {
    stack.push(`Deploy: ${ps.deployment.join(', ')}`);
  }
  if (ps.git) {
    stack.push(`Branch: ${ps.git.branch}`);
  }
  if (stack.length === 0) {
    lines.push('Not detected.');
  } else {
    for (const s of stack) lines.push(`- ${s}`);
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
  const startTime = Date.now();
  let status = 'success';
  let errorMessage = null;

  try {
    const config = loadConfig();
    const projects = config.projects || {};
    const projectNames = Object.keys(projects);

    if (projectNames.length === 0) {
      log('no projects configured. Run /watchtower install to add projects.');
      status = 'no-projects';
    }

    const projectStates = [];

    for (const name of projectNames) {
      const projectPath = projects[name].path || projects[name];
      if (!existsSync(projectPath)) {
        logError(`project path ${projectPath} not found, skipping ${name}`);
        continue;
      }

      const ps = {
        name,
        path: projectPath,
        git: collectGitState(projectPath),
        pib: collectPibState(projectPath),
        activeSessions: detectActiveSessions(projectPath),
        deployment: detectDeployment(projectPath),
        memoryIntegrity: checkMemoryIntegrity(projectPath),
        divergedBranches: [],
        hookResults: [],
      };

      // Branch divergence detection (feature-flagged)
      if (config.defaults?.branch_orphan_detection !== false && ps.git && ps.git.branchesAhead) {
        for (const branch of ps.git.branchesAhead) {
          // Check if there's an active session on this branch
          const hasActiveSession = ps.activeSessions.length > 0 &&
            ps.git.branch === branch;
          if (!hasActiveSession) {
            ps.divergedBranches.push(branch);
            createBranchDivergedItem(name, projectPath, branch);
          }
        }
      }

      // Worktree scan — find orphaned worktrees with unmerged work
      ps.orphanedWorktrees = scanWorktrees(name, projectPath);

      // Consumer hooks
      const hooks = config.hooks?.['ring1-post-collect'] || [];
      ps.hookResults = runConsumerHooksSync(hooks, ps);

      projectStates.push(ps);
    }

    // Ensure output directories exist
    const stateDir = join(WATCHTOWER_DIR, 'state');
    const projectsDir = join(stateDir, 'projects');
    mkdirSync(projectsDir, { recursive: true });

    // Write summary.md
    const summary = assembleSummary(projectStates, config);
    atomicWrite(join(stateDir, 'summary.md'), summary);

    // Write per-project state files.
    // Section ownership (watchtower-contracts.md §Project State Section
    // Ownership): Ring 3 owns "## Last Session" once it has authored a rich
    // session summary there (marked by its `_<date> (<session-id>)_`
    // attribution line). Ring 1 rebuilds every OTHER section from scratch,
    // but must carry a Ring 3-authored Last Session forward verbatim —
    // otherwise this rebuild deterministically clobbers Ring 3's summary
    // within one cron tick.
    for (const ps of projectStates) {
      const slug = slugify(ps.name);
      const statePath = join(projectsDir, `${slug}.md`);
      let projectMd = assembleProjectState(ps);
      if (existsSync(statePath)) {
        try {
          projectMd = preserveRing3LastSession(projectMd, readFileSync(statePath, 'utf8'));
        } catch (e) {
          logError(`could not merge existing state for ${slug}: ${e.message} — writing fresh`);
        }
      }
      atomicWrite(statePath, projectMd);
    }

    log(`collected state for ${projectStates.length} project(s)`);
  } catch (e) {
    status = 'error';
    errorMessage = e.message;
    logError(`${e.message}`);
  }

  // Write ring health
  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring1-health.json');
  mkdirSync(join(WATCHTOWER_DIR, 'state'), { recursive: true });
  const health = {
    schema_version: 1,
    last_run: new Date().toISOString(),
    duration_ms: Date.now() - startTime,
    status,
    error: errorMessage,
  };
  atomicWrite(healthPath, JSON.stringify(health, null, 2));
}

try {
  main();
} catch (e) {
  logError(`fatal: ${e.message}`);
  process.exit(1);
}

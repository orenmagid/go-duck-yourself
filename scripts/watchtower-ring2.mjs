#!/usr/bin/env node

// Watchtower Ring 2 — Periodic intelligence.
// Two tiers: --fast (every 5 min) and --slow (every 30 min).
// Uses Claude API via @anthropic-ai/sdk for AI-powered evaluation.
//
// Fast tier (--fast):
//   - Deferred trigger evaluation (cap 5 items/run)
//   - Queue enrichment for bare items >10min old (cap 2/run)
//   - Queue escalation (14d [AGING], 30d expire)
//   - Event-driven: checks lock/ring2-fast-trigger file
//
// Slow tier (--slow):
//   - Memory hygiene via validate-memory.mjs
//   - Cross-project memory synthesis (cap 3 evals/run)
//   - Stale work detection (14d → stale-project, all-done → project-completion)
//   - Quality pattern detection — routes Ring 3 patterns to cabinet members via inbox
//   - Consumer hooks (subshell, 60s timeout)
//   - Writes state/memory-health.md, state/work-items.md
//   - Triggers Ring 1 re-eval (lock/ring1-trigger)

import {
  readFileSync, writeFileSync, readdirSync, existsSync, statSync,
  mkdirSync, unlinkSync,
} from 'fs';
import { join, resolve } from 'path';
import { execSync } from 'child_process';
import { createRequire } from 'module';
import { homedir } from 'os';
import {
  atomicWrite, loadConfig, slugify,
  log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending, loadBetterSqlite3,
} from './watchtower-lib.mjs';
import { expireItem } from './watchtower-queue.mjs';

const require = createRequire(import.meta.url);

const WATCHTOWER_DIR = getWatchtowerDir();

const CLAUDE_MODEL = 'claude-sonnet-4-6';
const FAST_TRIGGER_EVAL_CAP = 5;
const FAST_ENRICHMENT_CAP = 2;
const FAST_ENRICHMENT_MIN_AGE_MS = 10 * 60 * 1000; // 10 minutes
const ESCALATION_WARN_DAYS = 14;
const ESCALATION_EXPIRE_DAYS = 30;
// Urgency = value decay, not importance (decision_ring3_urgency_value_based).
// An urgent item whose decay window has passed is by definition no longer
// urgent — de-escalate so the SessionStart urgent count stays trustworthy.
const URGENT_DECAY_DAYS = 7;
const SLOW_STALE_DAYS = 14;
const SLOW_CROSS_PROJECT_CAP = 3;
const HOOK_TIMEOUT_MS = 60_000;
const SLOW_PATTERN_SCAN_CAP = 3;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function log(msg) { _log('ring2', msg); }
function logError(msg) { _logError('ring2', msg); }

/** Read all queue items (any status) — used for enrichment context lookup. */
function readAllQueueItems() {
  const queueDir = join(WATCHTOWER_DIR, 'queue', 'items');
  if (!existsSync(queueDir)) return [];
  const items = [];
  try {
    for (const entry of readdirSync(queueDir, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
      try {
        items.push(JSON.parse(readFileSync(join(queueDir, entry.name), 'utf8')));
      } catch { /* skip */ }
    }
  } catch { /* skip */ }
  return items;
}

// ---------------------------------------------------------------------------
// Claude API helper
// ---------------------------------------------------------------------------

let _anthropicClient = null;

async function getAnthropicClient() {
  if (_anthropicClient) return _anthropicClient;

  // ESM import() ignores NODE_PATH; use createRequire for global installs
  let Anthropic;
  try {
    const mod = require('@anthropic-ai/sdk');
    Anthropic = mod.default || mod.Anthropic || mod;
  } catch (e) {
    throw new Error(
      'Ring 2 requires @anthropic-ai/sdk. Install it: npm install -g @anthropic-ai/sdk\n' +
      `Import error: ${e.message}`
    );
  }

  if (!process.env.ANTHROPIC_API_KEY) {
    throw new Error('ANTHROPIC_API_KEY environment variable is not set. Ring 2 requires it for Claude API calls.');
  }

  _anthropicClient = new Anthropic();
  return _anthropicClient;
}

async function askClaude(systemPrompt, userPrompt, maxTokens = 1024) {
  const client = await getAnthropicClient();
  const response = await client.messages.create({
    model: CLAUDE_MODEL,
    max_tokens: maxTokens,
    system: systemPrompt,
    messages: [{ role: 'user', content: userPrompt }],
  });

  // Extract text from response
  const textBlocks = response.content.filter(b => b.type === 'text');
  return textBlocks.map(b => b.text).join('\n');
}

// ---------------------------------------------------------------------------
// pib-db helper
// ---------------------------------------------------------------------------

function openPibDb(projectPath) {
  const dbPath = join(projectPath, 'pib.db');
  if (!existsSync(dbPath)) return null;

  const Database = loadBetterSqlite3(projectPath);
  if (!Database) return null;

  try {
    return new Database(dbPath, { readonly: true });
  } catch {
    return null;
  }
}

// ===================================================================
// FAST TIER (--fast, every 5 minutes)
// ===================================================================

// ---------------------------------------------------------------------------
// 1. Deferred trigger evaluation
// ---------------------------------------------------------------------------

async function evaluateDeferredTriggers(config) {
  const projects = config.projects || {};
  const projectNames = Object.keys(projects);
  const triggerItems = [];

  // Collect all trigger_condition items across projects
  for (const name of projectNames) {
    const projectPath = projects[name].path || projects[name];
    if (!existsSync(projectPath)) continue;

    const db = openPibDb(projectPath);
    if (!db) continue;

    try {
      let rows = [];
      try {
        rows = db.prepare(
          "SELECT fid, text, trigger_condition, notes FROM actions WHERE status = 'deferred' AND trigger_condition IS NOT NULL AND trigger_condition != ''"
        ).all();
      } catch {
        // trigger_condition column may not exist
      }

      for (const row of rows) {
        triggerItems.push({
          projectName: name,
          projectPath,
          fid: row.fid,
          text: row.text,
          triggerCondition: row.trigger_condition,
          notes: row.notes || '',
        });
      }
    } finally {
      db.close();
    }
  }

  if (triggerItems.length === 0) {
    log('Fast: no deferred triggers to evaluate');
    return;
  }

  // Rotate by least-recently-checked using state file
  const checkedPath = join(WATCHTOWER_DIR, 'state', 'trigger-checks.json');
  let checked = {};
  if (existsSync(checkedPath)) {
    try {
      checked = JSON.parse(readFileSync(checkedPath, 'utf8'));
    } catch {
      checked = {};
    }
  }

  // Sort by least-recently-checked
  triggerItems.sort((a, b) => {
    const aTime = checked[a.fid] || 0;
    const bTime = checked[b.fid] || 0;
    return aTime - bTime;
  });

  // Cap at FAST_TRIGGER_EVAL_CAP
  const batch = triggerItems.slice(0, FAST_TRIGGER_EVAL_CAP);
  log(`Fast: evaluating ${batch.length} of ${triggerItems.length} deferred triggers`);

  for (const item of batch) {
    try {
      // Gather evidence: recent git activity, project state
      let evidence = `Project: ${item.projectName}\nAction: ${item.text}\nTrigger condition: ${item.triggerCondition}`;
      if (item.notes) {
        evidence += `\nNotes: ${item.notes}`;
      }

      // Check for recent git activity as context
      try {
        const recentCommits = execSync(
          'git log --oneline -5 --since="7 days ago"',
          { cwd: item.projectPath, encoding: 'utf8', timeout: 5000 }
        ).trim();
        if (recentCommits) {
          evidence += `\nRecent commits:\n${recentCommits}`;
        }
      } catch {
        // git not available or not a repo
      }

      const systemPrompt = 'You evaluate whether deferred action trigger conditions have been met. Respond with exactly one of: TRIGGERED, STILL_WAITING, or CONDITION_OBSOLETE. Then on the next line, provide a brief reason (one sentence).';
      const userPrompt = `Evaluate this deferred action's trigger condition:\n\n${evidence}\n\nHas the trigger condition been met, is it still waiting, or has the condition become obsolete?`;

      const result = await askClaude(systemPrompt, userPrompt, 256);
      const firstLine = result.split('\n')[0].trim().toUpperCase();
      const reasoning = result.split('\n').slice(1).join('\n').trim() || 'No reasoning provided.';

      checked[item.fid] = Date.now();

      if (firstLine.includes('TRIGGERED')) {
        log(`Fast: trigger FIRED for ${item.fid} — ${item.text}`);

        // Check for existing pending item with same fid to avoid duplicates
        const existing = listPending({ category: 'deferred-trigger' }).some(qi =>
          qi.evidence?.fid === item.fid
        );

        if (!existing) {
          createItem({
            project: item.projectName,
            project_path: item.projectPath,
            filed_by: 'ring2-fast',
            category: 'deferred-trigger',
            urgency: 'normal',
            title: `Trigger fired: ${item.text.slice(0, 60)}`,
            summary: `Deferred action "${item.text}" trigger condition appears met: ${item.triggerCondition}`,
            context_anchor: `pib-db action ${item.fid}`,
            evidence: { fid: item.fid, trigger_condition: item.triggerCondition, reasoning },
          });
        }
      } else if (firstLine.includes('OBSOLETE')) {
        log(`Fast: trigger OBSOLETE for ${item.fid} — marking checked`);
        // Just mark as checked — human decides what to do
      } else {
        log(`Fast: trigger still waiting for ${item.fid}`);
      }
    } catch (e) {
      logError(`Trigger eval failed for ${item.fid}: ${e.message}`);
    }
  }

  // Save checked state
  atomicWrite(checkedPath, JSON.stringify(checked, null, 2));
}

// ---------------------------------------------------------------------------
// 2. Queue enrichment
// ---------------------------------------------------------------------------

async function enrichQueueItems() {
  const allPending = listPending();

  // Recovery: reset stale in-progress items (enrichment started >10 min ago but never completed)
  const staleInProgress = allPending.filter(i => i.enrichment_status === 'in-progress');
  for (const stale of staleInProgress) {
    const enrichDir = join(WATCHTOWER_DIR, 'queue', 'items', stale.id, 'enrichment');
    let isStale = false;
    if (existsSync(enrichDir)) {
      try {
        const dirStat = statSync(enrichDir);
        isStale = (Date.now() - dirStat.mtimeMs) > 10 * 60 * 1000;
      } catch { isStale = true; }
    } else {
      isStale = true;
    }
    if (isStale) {
      const fp = join(WATCHTOWER_DIR, 'queue', 'items', `${stale.id}.json`);
      if (existsSync(fp)) {
        const updated = JSON.parse(readFileSync(fp, 'utf8'));
        updated.enrichment_status = 'bare';
        atomicWrite(fp, updated);
        log(`Fast: reset stale in-progress item ${stale.id} to bare`);
      }
    }
  }

  const items = allPending.filter(i => i.enrichment_status === 'bare');

  const now = Date.now();
  const eligible = items.filter(i => {
    const filedAt = new Date(i.filed_at).getTime();
    return (now - filedAt) >= FAST_ENRICHMENT_MIN_AGE_MS;
  });

  if (eligible.length === 0) {
    log('Fast: no bare queue items eligible for enrichment');
    return;
  }

  // Sort by oldest first, cap at FAST_ENRICHMENT_CAP
  eligible.sort((a, b) => new Date(a.filed_at) - new Date(b.filed_at));
  const batch = eligible.slice(0, FAST_ENRICHMENT_CAP);
  log(`Fast: enriching ${batch.length} of ${eligible.length} eligible queue items`);

  for (const item of batch) {
    try {
      await enrichSingleItem(item);
    } catch (e) {
      logError(`Enrichment failed for ${item.id}: ${e.message}`);
    }
  }
}

async function enrichSingleItem(item) {
  // Gather context for enrichment
  let projectContext = '';

  // Read project state if available
  if (item.project_path && existsSync(item.project_path)) {
    // Check for recent resolved decisions in the same project
    const relatedDecisions = readAllQueueItems().filter(qi =>
      qi.project === item.project &&
      qi.status === 'resolved' &&
      qi.id !== item.id
    ).slice(0, 5);

    if (relatedDecisions.length > 0) {
      projectContext += '\nRelated resolved decisions:\n';
      for (const d of relatedDecisions) {
        projectContext += `- ${d.title} — ${d.resolution || 'resolved'}\n`;
      }
    }

    // Check memory entries
    const memoryDir = findMemoryDir(item.project_path);
    if (memoryDir) {
      const memoryIndex = join(memoryDir, 'MEMORY.md');
      if (existsSync(memoryIndex)) {
        try {
          const memContent = readFileSync(memoryIndex, 'utf8');
          // Extract relevant lines (first 30 lines as context)
          const lines = memContent.split('\n').slice(0, 30).join('\n');
          projectContext += `\nMemory index (excerpt):\n${lines}\n`;
        } catch {
          // skip
        }
      }
    }
  }

  const systemPrompt = `You are enriching an inbox item with background context. Produce four sections, each starting with the section header on its own line:

## Code Context
Relevant code areas and technical context for this decision.

## Related Decisions
Prior decisions that bear on this one.

## Memory References
Relevant project memory entries.

## Options Analysis
Pros and cons of the available options.

Be concise and factual. Each section should be 2-5 lines.`;

  const userPrompt = `Decision item to enrich:
Title: ${item.title}
Summary: ${item.summary}
Category: ${item.category}
Project: ${item.project || 'unknown'}
Context anchor: ${item.context_anchor || 'none'}
Evidence: ${JSON.stringify(item.evidence || {}, null, 2)}
${projectContext}`;

  // Mark in-progress before Claude call
  const inProgressPath = join(WATCHTOWER_DIR, 'queue', 'items', `${item.id}.json`);
  if (existsSync(inProgressPath)) {
    const ipUpdate = JSON.parse(readFileSync(inProgressPath, 'utf8'));
    ipUpdate.enrichment_status = 'in-progress';
    atomicWrite(inProgressPath, ipUpdate);
  }

  const result = await askClaude(systemPrompt, userPrompt, 1024);

  // Parse sections from the result
  const sections = {
    'code-context': '',
    'related-decisions': '',
    'memory-refs': '',
    'options-analysis': '',
  };

  const sectionMap = {
    'code context': 'code-context',
    'related decisions': 'related-decisions',
    'memory references': 'memory-refs',
    'options analysis': 'options-analysis',
  };

  let currentSection = null;
  for (const line of result.split('\n')) {
    const headerMatch = line.match(/^##\s+(.+)/);
    if (headerMatch) {
      const headerLower = headerMatch[1].toLowerCase();
      currentSection = sectionMap[headerLower] || null;
      continue;
    }
    if (currentSection && sections[currentSection] !== undefined) {
      sections[currentSection] += line + '\n';
    }
  }

  // Write enrichment files
  const enrichDir = join(WATCHTOWER_DIR, 'queue', 'items', item.id, 'enrichment');
  mkdirSync(enrichDir, { recursive: true });

  atomicWrite(join(enrichDir, 'code-context.md'), sections['code-context'].trim() || 'No code context available.');
  atomicWrite(join(enrichDir, 'related-decisions.md'), sections['related-decisions'].trim() || 'No related decisions found.');
  atomicWrite(join(enrichDir, 'memory-refs.md'), sections['memory-refs'].trim() || 'No memory references found.');
  atomicWrite(join(enrichDir, 'options-analysis.md'), sections['options-analysis'].trim() || 'No options analysis available.');

  // Update item enrichment_status
  const itemPath = join(WATCHTOWER_DIR, 'queue', 'items', `${item.id}.json`);
  if (existsSync(itemPath)) {
    const updated = JSON.parse(readFileSync(itemPath, 'utf8'));
    updated.enrichment_status = 'complete';
    updated.enrichment_dir = enrichDir;
    atomicWrite(itemPath, updated);
  }

  log(`Fast: enriched item ${item.id}`);
}

function findMemoryDir(projectPath) {
  // Claude Code encodes paths as dash-separated with leading dash preserved
  const encoded = projectPath.replace(/\//g, '-');
  const memDir = join(homedir(), '.claude', 'projects', encoded, 'memory');
  return existsSync(memDir) ? memDir : null;
}

// ---------------------------------------------------------------------------
// 3. Queue escalation
// ---------------------------------------------------------------------------

async function escalateQueueItems() {
  const now = Date.now();
  const items = listPending();

  let escalated = 0;
  let expired = 0;

  let deescalated = 0;

  for (const item of items) {
    const ageMs = now - new Date(item.filed_at).getTime();
    const ageDays = ageMs / (24 * 60 * 60 * 1000);

    if (ageDays >= ESCALATION_EXPIRE_DAYS && item.category !== 'qa-handoff') {
      // 30+ days: expire. qa-handoff items are exempt — they never expire
      // (recipient-gate contract: expireItem THROWS on them); they fall
      // through to the [AGING] path below and stay surfaced as QA debt.
      expireItem(item.id);
      expired++;
      log(`Fast: expired queue item ${item.id} (${Math.floor(ageDays)}d old)`);
      continue;
    }

    if (ageDays >= ESCALATION_WARN_DAYS) {
      // 14+ days: add [AGING] prefix if not already there
      if (!item.title.startsWith('[AGING]')) {
        const itemPath = join(WATCHTOWER_DIR, 'queue', 'items', `${item.id}.json`);
        if (existsSync(itemPath)) {
          const updated = JSON.parse(readFileSync(itemPath, 'utf8'));
          updated.title = `[AGING] ${updated.title}`;
          atomicWrite(itemPath, updated);
          escalated++;
          log(`Fast: marked item ${item.id} as [AGING] (${Math.floor(ageDays)}d old)`);
        }
      }
    }

    // De-escalation: an urgent item that sat past its decay window is no
    // longer urgent — if the value really decayed, the urgency is spent;
    // if it didn't decay, it was never urgent (act:3f3f9a31).
    if (item.urgency === 'urgent' && ageDays >= URGENT_DECAY_DAYS) {
      const itemPath = join(WATCHTOWER_DIR, 'queue', 'items', `${item.id}.json`);
      if (existsSync(itemPath)) {
        const updated = JSON.parse(readFileSync(itemPath, 'utf8'));
        updated.urgency = 'normal';
        atomicWrite(itemPath, updated);
        deescalated++;
        log(`Fast: de-escalated item ${item.id} urgent → normal (${Math.floor(ageDays)}d old, decay window passed)`);
      }
    }
  }

  if (escalated > 0 || expired > 0 || deescalated > 0) {
    log(`Fast: escalation — ${escalated} aged, ${expired} expired, ${deescalated} de-escalated`);
  }
}

// ---------------------------------------------------------------------------
// 4. Event-driven trigger check
// ---------------------------------------------------------------------------

function checkFastTrigger() {
  const triggerFile = join(WATCHTOWER_DIR, 'lock', 'ring2-fast-trigger');
  if (existsSync(triggerFile)) {
    try {
      unlinkSync(triggerFile);
      log('Fast: event-driven trigger detected, running full cycle');
      return true;
    } catch {
      // race condition with another process, ignore
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Fast tier main
// ---------------------------------------------------------------------------

async function runFast() {
  log('Ring 2 fast tier starting');
  const config = loadConfig();

  // Check for event-driven trigger (re-run immediately marker)
  checkFastTrigger();

  // 1. Deferred trigger evaluation
  try {
    await evaluateDeferredTriggers(config);
  } catch (e) {
    logError(`Deferred trigger evaluation failed: ${e.message}`);
  }

  // 2. Queue enrichment
  try {
    await enrichQueueItems();
  } catch (e) {
    logError(`Queue enrichment failed: ${e.message}`);
  }

  // 3. Queue escalation
  try {
    await escalateQueueItems();
  } catch (e) {
    logError(`Queue escalation failed: ${e.message}`);
  }

  log('Ring 2 fast tier complete');
}

// ===================================================================
// SLOW TIER (--slow, every 30 minutes)
// ===================================================================

// ---------------------------------------------------------------------------
// 1. Memory hygiene
// ---------------------------------------------------------------------------

async function runMemoryHygiene(config) {
  const projects = config.projects || {};
  const projectNames = Object.keys(projects);
  const results = [];

  for (const name of projectNames) {
    const projectPath = projects[name].path || projects[name];
    if (!existsSync(projectPath)) continue;

    // Run validate-memory.mjs as child process (may not exist in all projects)
    // CC installs scripts/ at project root, not .claude/scripts/
    const validateScript = join(projectPath, 'scripts', 'validate-memory.mjs');
    if (!existsSync(validateScript)) {
      results.push({ project: name, status: 'skipped', reason: 'validate-memory.mjs not found' });
      continue;
    }

    try {
      const output = execSync(
        `node "${validateScript}" --quiet`,
        { cwd: projectPath, encoding: 'utf8', timeout: 30_000, stdio: ['pipe', 'pipe', 'pipe'] }
      );
      results.push({ project: name, status: 'pass', output: output.trim() });
    } catch (e) {
      // Exit code 1 means violations, 2 means bad usage
      const stderr = e.stderr || '';
      const stdout = e.stdout || '';

      // Parse violations from output
      const violations = [];
      const lines = (stdout + '\n' + stderr).split('\n');
      for (const line of lines) {
        if (line.startsWith('FAIL:')) {
          violations.push(line.replace('FAIL:', '').trim());
        }
      }

      results.push({
        project: name,
        status: 'violations',
        violations,
        exitCode: e.status,
      });
    }
  }

  // Write state/memory-health.md
  const healthLines = [`# Memory Health — ${new Date().toISOString()}`, ''];

  const violationProjects = results.filter(r => r.status === 'violations');
  const passProjects = results.filter(r => r.status === 'pass');
  const skippedProjects = results.filter(r => r.status === 'skipped');

  if (violationProjects.length === 0) {
    healthLines.push('All projects pass memory validation.');
  } else {
    healthLines.push(`${violationProjects.length} project(s) with memory issues:`);
    for (const p of violationProjects) {
      healthLines.push(`\n### ${p.project}`);
      for (const v of p.violations) {
        healthLines.push(`- ${v}`);
      }
    }
  }

  healthLines.push('');
  healthLines.push(`## Summary`);
  healthLines.push(`- Pass: ${passProjects.length}`);
  healthLines.push(`- Violations: ${violationProjects.length}`);
  healthLines.push(`- Skipped: ${skippedProjects.length}`);

  const stateDir = join(WATCHTOWER_DIR, 'state');
  mkdirSync(stateDir, { recursive: true });
  atomicWrite(join(stateDir, 'memory-health.md'), healthLines.join('\n'));

  // Silent detection ≈ no detection (no-silent-failures rule, act:3f3f9a31):
  // a project persistently over budget gets ONE inbox item, not a log line
  // nobody reads. Streaks tracked across runs; re-file at most every 7 days.
  try {
    surfacePersistentViolations(violationProjects, projects, stateDir);
  } catch (e) {
    logError(`Slow: memory violation surfacing failed: ${e.message}`);
  }

  log(`Slow: memory hygiene — ${passProjects.length} pass, ${violationProjects.length} violations, ${skippedProjects.length} skipped`);
  return results;
}

const MEMORY_VIOLATION_STREAK_THRESHOLD = 3; // consecutive slow runs
const MEMORY_VIOLATION_REFILE_DAYS = 7;

function surfacePersistentViolations(violationProjects, projects, stateDir) {
  const streakPath = join(stateDir, 'memory-violation-streaks.json');
  let streaks = {};
  if (existsSync(streakPath)) {
    try { streaks = JSON.parse(readFileSync(streakPath, 'utf8')); } catch { streaks = {}; }
  }

  const violatingNames = new Set(violationProjects.map(p => p.project));

  // Reset streaks for projects that now pass
  for (const name of Object.keys(streaks)) {
    if (!violatingNames.has(name)) delete streaks[name];
  }

  for (const p of violationProjects) {
    const entry = streaks[p.project] || { count: 0, last_filed: null };
    entry.count += 1;

    if (entry.count >= MEMORY_VIOLATION_STREAK_THRESHOLD) {
      const pending = listPending({ project: p.project, category: 'watchtower-health' });
      const alreadyPending = pending.some(i => i.title.startsWith('Memory budget violation'));
      const daysSinceFiled = entry.last_filed
        ? (Date.now() - new Date(entry.last_filed).getTime()) / 86400000
        : Infinity;

      if (!alreadyPending && daysSinceFiled >= MEMORY_VIOLATION_REFILE_DAYS) {
        const projectPath = projects[p.project]?.path || projects[p.project] || '';
        createItem({
          project: p.project,
          project_path: projectPath,
          filed_by: 'ring2-slow',
          category: 'watchtower-health',
          urgency: 'normal',
          title: `Memory budget violation: ${p.project}`,
          summary: `MEMORY.md has failed validation for ${entry.count} consecutive Ring 2 runs:\n${(p.violations || []).map(v => `- ${v}`).join('\n')}\n\nOver-budget MEMORY.md only partially loads at session start — memories past the cap are invisible.`,
          context_anchor: 'state/memory-health.md',
          evidence: { violations: p.violations || [], streak: entry.count },
          options: [
            { key: 'trim', label: 'Trim MEMORY.md now' },
            { key: 'defer', label: 'Defer — file a pib-db action' },
            { key: 'dismiss', label: 'Dismiss' },
          ],
        });
        entry.last_filed = new Date().toISOString();
        log(`Slow: filed memory-budget violation item for ${p.project} (streak ${entry.count})`);
      }
    }

    streaks[p.project] = entry;
  }

  atomicWrite(streakPath, JSON.stringify(streaks, null, 2));
}

// ---------------------------------------------------------------------------
// 2. Cross-project memory synthesis
// ---------------------------------------------------------------------------

async function crossProjectMemorySynthesis(config) {
  const projects = config.projects || {};
  const projectNames = Object.keys(projects);

  // Collect memory indices from all projects
  const projectMemories = [];
  for (const name of projectNames) {
    const projectPath = projects[name].path || projects[name];
    if (!existsSync(projectPath)) continue;

    const memoryDir = findMemoryDir(projectPath);
    if (!memoryDir) continue;

    const indexPath = join(memoryDir, 'MEMORY.md');
    if (!existsSync(indexPath)) continue;

    try {
      const content = readFileSync(indexPath, 'utf8');
      // Extract memory entries (lines with links or bold references)
      const entries = [];
      for (const line of content.split('\n')) {
        if (line.match(/\[.+\]\(.+\.md\)/) || line.match(/\*\*.+\.md\*\*/)) {
          entries.push(line.trim());
        }
      }
      if (entries.length >= 5) {
        projectMemories.push({ project: name, entries, path: projectPath });
      }
    } catch {
      // skip
    }
  }

  if (projectMemories.length < 2) {
    log('Slow: cross-project synthesis skipped (need at least 2 projects with memory)');
    return;
  }

  // Rotate evaluations using state file
  const synthStatePath = join(WATCHTOWER_DIR, 'state', 'cross-project-synth.json');
  let synthState = { last_pairs: [], last_run: null };
  if (existsSync(synthStatePath)) {
    try {
      synthState = JSON.parse(readFileSync(synthStatePath, 'utf8'));
    } catch {
      // reset
    }
  }

  // Generate pairs to compare, skip recently evaluated
  const pairs = [];
  for (let i = 0; i < projectMemories.length; i++) {
    for (let j = i + 1; j < projectMemories.length; j++) {
      const pairKey = `${projectMemories[i].project}:${projectMemories[j].project}`;
      const lastEval = (synthState.last_pairs || []).find(p => p.key === pairKey);
      const daysSinceEval = lastEval
        ? (Date.now() - new Date(lastEval.checked_at).getTime()) / (24 * 60 * 60 * 1000)
        : Infinity;
      // Re-evaluate if not checked in 7 days
      if (daysSinceEval >= 7) {
        pairs.push({ a: projectMemories[i], b: projectMemories[j], key: pairKey });
      }
    }
  }

  const batch = pairs.slice(0, SLOW_CROSS_PROJECT_CAP);
  if (batch.length === 0) {
    log('Slow: no cross-project pairs due for synthesis');
    return;
  }

  log(`Slow: evaluating ${batch.length} cross-project memory pairs`);

  const newPairChecks = [];
  for (const pair of batch) {
    try {
      const systemPrompt = 'You compare memory entries from two projects to find cross-cutting patterns, shared lessons, or transferable decisions. Be concise. If nothing interesting, say "No cross-project insights."';

      const userPrompt = `Project A (${pair.a.project}) memories:\n${pair.a.entries.join('\n')}\n\nProject B (${pair.b.project}) memories:\n${pair.b.entries.join('\n')}\n\nAre there shared patterns, contradictory decisions, or transferable lessons?`;

      const result = await askClaude(systemPrompt, userPrompt, 512);
      newPairChecks.push({ key: pair.key, checked_at: new Date().toISOString() });

      if (!result.toLowerCase().includes('no cross-project insights')) {
        log(`Slow: cross-project insight found for ${pair.key}`);
        // Route insights to the inbox
        createItem({
          project: pair.a.project,
          project_path: pair.a.path,
          filed_by: 'ring2-slow',
          category: 'knowledge-extraction',
          urgency: 'low',
          title: `Cross-project insight: ${pair.a.project} + ${pair.b.project}`,
          summary: result.trim(),
          context_anchor: `cross-project memory synthesis: ${pair.key}`,
          evidence: { pair_key: pair.key, projects: [pair.a.project, pair.b.project] },
        });
      }
    } catch (e) {
      logError(`Cross-project synthesis failed for ${pair.key}: ${e.message}`);
      newPairChecks.push({ key: pair.key, checked_at: new Date().toISOString() });
    }
  }

  // Update synthesis state (tracks when pairs were last checked)
  const existingPairs = (synthState.last_pairs || []).filter(p =>
    !newPairChecks.some(np => np.key === p.key)
  );
  synthState.last_pairs = [...existingPairs, ...newPairChecks];
  synthState.last_run = new Date().toISOString();
  atomicWrite(synthStatePath, JSON.stringify(synthState, null, 2));
}

// ---------------------------------------------------------------------------
// 3. Stale work detection
// ---------------------------------------------------------------------------

async function detectStaleWork(config) {
  const projects = config.projects || {};
  const projectNames = Object.keys(projects);
  const workItems = [];

  for (const name of projectNames) {
    const projectPath = projects[name].path || projects[name];
    if (!existsSync(projectPath)) continue;

    const db = openPibDb(projectPath);
    if (!db) continue;

    try {
      // Check for stale projects (no action completion in 14+ days)
      const staleThreshold = new Date(Date.now() - SLOW_STALE_DAYS * 86400000).toISOString().slice(0, 10);

      let activeActions = [];
      try {
        activeActions = db.prepare(
          `SELECT fid, text, status, updated_at FROM actions
           WHERE status IN ('open', 'in-progress')
           AND project_fid IN (SELECT fid FROM projects WHERE status = 'active')
           ORDER BY updated_at ASC`
        ).all();
      } catch {
        // best-effort
      }

      // Check for active projects with no recent completion
      let recentCompletions = [];
      try {
        recentCompletions = db.prepare(
          `SELECT COUNT(*) as count FROM actions
           WHERE completed_at > ? AND status = 'done'`
        ).all(staleThreshold);
      } catch {
        // best-effort
      }

      const hasRecentCompletion = recentCompletions.length > 0 && recentCompletions[0].count > 0;

      if (activeActions.length > 0 && !hasRecentCompletion) {
        // Stale project: has active work but no completions in 14+ days
        // Dedup: check if a pending stale-project item already exists for this project
        const existingStale = listPending({ category: 'stale-project' }).some(qi =>
          qi.project === name
        );

        if (!existingStale) {
          createItem({
            project: name,
            project_path: projectPath,
            filed_by: 'ring2-slow',
            category: 'stale-project',
            urgency: 'low',
            title: `Stale project: ${name} — no completions in ${SLOW_STALE_DAYS}+ days`,
            summary: `${activeActions.length} active action(s) but no completions in the last ${SLOW_STALE_DAYS} days. Consider reviewing priorities.`,
            context_anchor: `pib-db actions in ${name}`,
            evidence: { active_action_count: activeActions.length, stale_days: SLOW_STALE_DAYS },
          });
          log(`Slow: queued stale-project for ${name}`);
        }

        workItems.push({
          project: name,
          type: 'stale',
          activeActions: activeActions.length,
          detail: `No completions in ${SLOW_STALE_DAYS}+ days`,
        });
      }

      // Check for all-done projects (project-completion candidates)
      let allDoneProjects = [];
      try {
        allDoneProjects = db.prepare(
          `SELECT p.fid, p.name FROM projects p
           WHERE p.status = 'active'
           AND EXISTS (SELECT 1 FROM actions a WHERE a.project_fid = p.fid)
           AND NOT EXISTS (
             SELECT 1 FROM actions a
             WHERE a.project_fid = p.fid
             AND a.status != 'done'
           )`
        ).all();
      } catch {
        // best-effort
      }

      for (const proj of allDoneProjects) {
        // Dedup: check for existing pending project-completion item
        const existingCompletion = listPending({ category: 'project-completion' }).some(qi =>
          qi.project === name &&
          qi.evidence?.project_fid === proj.fid
        );

        if (!existingCompletion) {
          createItem({
            project: name,
            project_path: projectPath,
            filed_by: 'ring2-slow',
            category: 'project-completion',
            urgency: 'low',
            title: `All done: ${proj.name} in ${name}`,
            summary: `All actions in project "${proj.name}" are marked done. Consider closing the project.`,
            context_anchor: `pib-db project ${proj.fid}`,
            evidence: { project_fid: proj.fid, project_name: proj.name },
          });
          log(`Slow: queued project-completion for ${proj.name} in ${name}`);
        }

        workItems.push({
          project: name,
          type: 'completion-candidate',
          subproject: proj.name,
        });
      }
    } finally {
      db.close();
    }
  }

  // Write state/work-items.md
  const wiLines = [`# Work Items — ${new Date().toISOString()}`, ''];

  const staleItems = workItems.filter(w => w.type === 'stale');
  const completionItems = workItems.filter(w => w.type === 'completion-candidate');

  if (staleItems.length > 0) {
    wiLines.push('## Stale Projects');
    for (const w of staleItems) {
      wiLines.push(`- ${w.project}: ${w.activeActions} active actions, ${w.detail}`);
    }
    wiLines.push('');
  }

  if (completionItems.length > 0) {
    wiLines.push('## Completion Candidates');
    for (const w of completionItems) {
      wiLines.push(`- ${w.project}/${w.subproject}: all actions done`);
    }
    wiLines.push('');
  }

  if (workItems.length === 0) {
    wiLines.push('No stale projects or completion candidates detected.');
  }

  const stateDir = join(WATCHTOWER_DIR, 'state');
  mkdirSync(stateDir, { recursive: true });
  atomicWrite(join(stateDir, 'work-items.md'), wiLines.join('\n'));

  log(`Slow: work items — ${staleItems.length} stale, ${completionItems.length} completion candidates`);
}

// ---------------------------------------------------------------------------
// 4. Thread absorption
// ---------------------------------------------------------------------------

// Pruning safety gate (act:ef5a3842): pruning is the LOSSY step in the
// memory-consolidation gradient — once session detail files are gone, every
// recovery costs a full transcript replay. Until the re-enrichment ascent
// (on-demand re-sharpening of the absorbed past) exists, keep the cheap
// middle tier around for 90 days instead of 7.
const ABSORPTION_SESSION_AGE_DAYS = 90;
const DORMANT_THREAD_AGE_DAYS = 30;

async function absorbThreadSessions(config) {
  const threadsDir = join(WATCHTOWER_DIR, 'state', 'threads');
  if (!existsSync(threadsDir)) {
    log('Slow: no threads directory, skipping absorption');
    return;
  }

  const now = Date.now();
  const sessionAgeCutoff = now - ABSORPTION_SESSION_AGE_DAYS * 86400000;
  const dormantCutoff = now - DORMANT_THREAD_AGE_DAYS * 86400000;
  let pruned = 0;
  let dormant = 0;

  const threadFiles = readdirSync(threadsDir).filter(f => f.endsWith('.json'));

  for (const f of threadFiles) {
    const threadPath = join(threadsDir, f);
    let thread;
    try {
      thread = JSON.parse(readFileSync(threadPath, 'utf8'));
    } catch { continue; }

    if (thread.status !== 'active') continue;

    // Mark threads dormant if untouched for 30+ days
    const lastUpdated = new Date(thread.last_updated || 0).getTime();
    if (lastUpdated < dormantCutoff) {
      thread.status = 'dormant';
      atomicWrite(threadPath, JSON.stringify(thread, null, 2));
      dormant++;
      log(`Slow: thread ${thread.thread} marked dormant (${Math.floor((now - lastUpdated) / 86400000)}d inactive)`);
      continue;
    }

    // Prune old session detail files that are already captured in the thread's
    // cursor_history. The session entry in the thread's sessions array (with
    // transcript pointer) survives.
    if (!thread.sessions || thread.sessions.length === 0) continue;

    const projects = config.projects || {};
    for (const sess of thread.sessions) {
      const sessDate = new Date(sess.date || 0).getTime();
      if (sessDate >= sessionAgeCutoff) continue;

      // Find the session detail file and prune it if it exists
      const projectSlug = sess.project;
      const sessionsDir = join(WATCHTOWER_DIR, 'state', 'projects', projectSlug, 'sessions');
      if (!existsSync(sessionsDir)) continue;

      // Match session files by session ID
      const sessFilePattern = `${sess.date}-${sess.id}.md`;
      const sessFilePath = join(sessionsDir, sessFilePattern);

      if (existsSync(sessFilePath)) {
        try {
          unlinkSync(sessFilePath);
          pruned++;
        } catch { /* best effort */ }
      }
    }
  }

  if (pruned > 0 || dormant > 0) {
    log(`Slow: absorption — ${pruned} session files pruned, ${dormant} threads marked dormant`);
  } else {
    log('Slow: absorption — nothing to prune');
  }
}

// ---------------------------------------------------------------------------
// 5. Audit pattern detection
// ---------------------------------------------------------------------------

function simpleHash(text) {
  let h = 0;
  for (let i = 0; i < text.length; i++) {
    h = ((h << 5) - h) + text.charCodeAt(i);
    h = h & h;
  }
  return 'ph_' + Math.abs(h).toString(36);
}

function parseAuditPatterns(content) {
  const patterns = [];
  let currentDate = null;
  const lines = content.split('\n');

  for (let i = 0; i < lines.length; ) {
    const dateMatch = lines[i].match(/^### (\d{4}-\d{2}-\d{2})/);
    if (dateMatch) { currentDate = dateMatch[1]; i++; continue; }

    const patternMatch = lines[i].match(/^## (.+)/);
    if (patternMatch && currentDate) {
      const title = patternMatch[1];
      const bodyLines = [];
      i++;
      while (i < lines.length && !lines[i].match(/^##[# ]/) && lines[i].trim() !== '---') {
        bodyLines.push(lines[i]);
        i++;
      }
      const body = bodyLines.join('\n').trim();
      if (body) patterns.push({ title, body, date: currentDate, hash: simpleHash(title + body) });
      continue;
    }
    i++;
  }
  return patterns;
}

function collectCabinetMembers(config) {
  const members = [];
  for (const [name, projConfig] of Object.entries(config.projects || {})) {
    const projectPath = projConfig.path || projConfig;
    if (!existsSync(projectPath)) continue;

    const indexPath = join(projectPath, '.claude', 'skills', '_index.json');
    if (!existsSync(indexPath)) continue;

    try {
      const index = JSON.parse(readFileSync(indexPath, 'utf8'));
      for (const skill of (index.skills || [])) {
        if (skill.type === 'cabinet' && skill.name) {
          members.push({
            name: skill.name.replace(/^cabinet-/, ''),
            description: skill.description || '',
            project: name,
            project_path: projectPath,
          });
        }
      }
    } catch { /* skip */ }
  }
  return members;
}

async function routePatternToMember(pattern, members) {
  const seen = new Set();
  const unique = members.filter(m => { if (seen.has(m.name)) return false; seen.add(m.name); return true; });
  const memberList = unique.map(m => `- ${m.name}: ${m.description.slice(0, 120)}`).join('\n');

  const result = await askClaude(
    'You route audit patterns to the cabinet member whose portfolio best matches. Respond with EXACTLY one line: the member name (no "cabinet-" prefix). If no member is a good match, respond "NONE".',
    `Pattern:\nTitle: ${pattern.title}\n${pattern.body.slice(0, 500)}\n\nCabinet members:\n${memberList}\n\nWhich member should own this pattern?`,
    64,
  );

  const memberName = result.trim().split('\n')[0].trim().toLowerCase().replace(/^cabinet-/, '');
  if (memberName === 'none' || !memberName) return null;
  return members.find(m => m.name === memberName) || null;
}

async function scanAuditPatterns(config) {
  const patternsPath = join(WATCHTOWER_DIR, 'state', 'audit-patterns.md');
  if (!existsSync(patternsPath)) {
    log('Slow: no audit-patterns.md, skipping pattern detection');
    return;
  }

  const statePath = join(WATCHTOWER_DIR, 'state', 'pattern-detection.json');
  let state = { last_scan: null, audit_patterns_mtime: 0, processed_hashes: [] };
  if (existsSync(statePath)) {
    try { state = JSON.parse(readFileSync(statePath, 'utf8')); } catch { /* reset */ }
  }

  const currentMtime = statSync(patternsPath).mtimeMs;
  if (currentMtime <= (state.audit_patterns_mtime || 0)) {
    log('Slow: audit-patterns.md unchanged since last scan');
    return;
  }

  const patterns = parseAuditPatterns(readFileSync(patternsPath, 'utf8'));
  const processedSet = new Set(state.processed_hashes || []);
  const newPatterns = patterns.filter(p => !processedSet.has(p.hash));

  if (newPatterns.length === 0) {
    state.audit_patterns_mtime = currentMtime;
    atomicWrite(statePath, JSON.stringify(state, null, 2));
    log('Slow: all audit patterns already processed');
    return;
  }

  const members = collectCabinetMembers(config);
  if (members.length === 0) {
    log('Slow: no cabinet members found, skipping pattern routing');
    return;
  }

  const batch = newPatterns.slice(0, SLOW_PATTERN_SCAN_CAP);
  log(`Slow: routing ${batch.length} of ${newPatterns.length} new audit patterns`);

  for (const pattern of batch) {
    try {
      const routing = await routePatternToMember(pattern, members);
      if (!routing) { processedSet.add(pattern.hash); continue; }

      const existing = listPending({ category: 'pattern-promotion' }).some(qi =>
        qi.evidence?.pattern_hash === pattern.hash
      );

      if (!existing) {
        createItem({
          project: routing.project,
          project_path: routing.project_path,
          filed_by: 'ring2-slow',
          category: 'pattern-promotion',
          urgency: 'low',
          title: `Pattern for cabinet-${routing.name}: ${pattern.title.slice(0, 50)}`,
          summary: pattern.body.slice(0, 300),
          context_anchor: `audit-patterns.md, ${pattern.date}`,
          evidence: {
            pattern_hash: pattern.hash,
            target_member: routing.name,
            target_project: routing.project,
            target_project_path: routing.project_path,
            pattern_title: pattern.title,
            pattern_text: pattern.body,
            source_date: pattern.date,
          },
          options: [
            { value: 'write', label: 'Write to member', description: `Add to cabinet-${routing.name}/patterns-project.md` },
            { value: 'promote', label: 'Promote upstream', description: 'File as upstream feedback for CC maintainer' },
            { value: 'dismiss', label: 'Dismiss', description: 'Not worth capturing as a pattern' },
          ],
        });
        log(`Slow: filed pattern-promotion for cabinet-${routing.name}: ${pattern.title.slice(0, 40)}`);
      }
      processedSet.add(pattern.hash);
    } catch (e) {
      logError(`Pattern routing failed: ${e.message}`);
      processedSet.add(pattern.hash);
    }
  }

  state.audit_patterns_mtime = currentMtime;
  state.last_scan = new Date().toISOString();
  state.processed_hashes = [...processedSet];
  atomicWrite(statePath, JSON.stringify(state, null, 2));
}

// ---------------------------------------------------------------------------
// 6. Consumer hooks (renumbered from 5 — audit pattern detection inserted)
// ---------------------------------------------------------------------------

function runSlowConsumerHooks(config, slowState) {
  const hooks = config.hooks?.['ring2-slow-post'] || [];
  if (!hooks || hooks.length === 0) return;

  const results = [];
  for (const hookCmd of hooks) {
    try {
      const output = execSync(hookCmd, {
        encoding: 'utf8',
        timeout: HOOK_TIMEOUT_MS,
        input: JSON.stringify(slowState),
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: true,
      });

      try {
        const parsed = JSON.parse(output);
        results.push({ hook: hookCmd, status: 'success', output: parsed });
      } catch {
        results.push({ hook: hookCmd, status: 'success', output: output.trim() });
      }
    } catch (e) {
      // Hook failure — log, never abort
      logError(`Slow consumer hook failed: ${hookCmd} — ${e.message}`);
      results.push({ hook: hookCmd, status: 'failed', error: e.message });
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// 7. Trigger Ring 1 re-eval
// ---------------------------------------------------------------------------

function triggerRing1Reeval() {
  const triggerFile = join(WATCHTOWER_DIR, 'lock', 'ring1-trigger');
  try {
    mkdirSync(join(WATCHTOWER_DIR, 'lock'), { recursive: true });
    writeFileSync(triggerFile, new Date().toISOString());
    log('Slow: triggered Ring 1 re-evaluation');
  } catch (e) {
    logError(`Failed to trigger Ring 1: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Slow tier main
// ---------------------------------------------------------------------------

async function runSlow() {
  log('Ring 2 slow tier starting');
  const config = loadConfig();
  const slowState = { started_at: new Date().toISOString() };

  // 1. Memory hygiene (feature-flagged)
  if (config.defaults?.memory_hygiene !== false) {
    try {
      const memResults = await runMemoryHygiene(config);
      slowState.memory_hygiene = memResults;
    } catch (e) {
      logError(`Memory hygiene failed: ${e.message}`);
      slowState.memory_hygiene = { error: e.message };
    }
  }

  // 2. Cross-project memory synthesis
  try {
    await crossProjectMemorySynthesis(config);
  } catch (e) {
    logError(`Cross-project synthesis failed: ${e.message}`);
  }

  // 3. Stale work detection (feature-flagged)
  if (config.defaults?.stale_work_detection !== false) {
    try {
      await detectStaleWork(config);
    } catch (e) {
      logError(`Stale work detection failed: ${e.message}`);
    }
  }

  // 4. Thread absorption
  try {
    await absorbThreadSessions(config);
  } catch (e) {
    logError(`Thread absorption failed: ${e.message}`);
  }

  // 5. Audit pattern detection (feature-flagged)
  if (config.defaults?.audit_pattern_detection !== false) {
    try {
      await scanAuditPatterns(config);
    } catch (e) {
      logError(`Audit pattern detection failed: ${e.message}`);
    }
  }

  // 6. Consumer hooks
  try {
    const hookResults = runSlowConsumerHooks(config, slowState);
    slowState.hook_results = hookResults;
  } catch (e) {
    logError(`Consumer hooks failed: ${e.message}`);
  }

  // 7. Trigger Ring 1 re-eval
  triggerRing1Reeval();

  slowState.completed_at = new Date().toISOString();
  log('Ring 2 slow tier complete');
}

// ===================================================================
// Main entry point
// ===================================================================

async function main() {
  const args = process.argv.slice(2);
  const tier = args.find(a => a === '--fast' || a === '--slow');
  const startTime = Date.now();
  let status = 'success';
  let errorMessage = null;
  let tierName = 'unknown';

  if (!tier) {
    console.error('Usage: watchtower-ring2.mjs --fast | --slow');
    process.exit(1);
  }

  tierName = tier === '--fast' ? 'fast' : 'slow';

  try {
    if (tier === '--fast') {
      await runFast();
    } else {
      await runSlow();
    }
  } catch (e) {
    status = 'error';
    errorMessage = e.message;
    logError(`Ring 2 ${tierName} error: ${e.message}`);
  }

  // Write ring health
  const healthPath = join(WATCHTOWER_DIR, 'state', `ring2-${tierName}-health.json`);
  mkdirSync(join(WATCHTOWER_DIR, 'state'), { recursive: true });
  const health = {
    schema_version: 1,
    last_run: new Date().toISOString(),
    duration_ms: Date.now() - startTime,
    tier: tierName,
    status,
    error: errorMessage,
  };
  atomicWrite(healthPath, JSON.stringify(health, null, 2));
}

main().catch(e => {
  logError(`fatal: ${e.message}`);
  process.exit(1);
});

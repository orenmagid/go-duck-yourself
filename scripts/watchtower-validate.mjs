#!/usr/bin/env node

// Watchtower structural validator.
// Checks schema integrity of watchtower state and queue files.
// Grows as schemas are added by later plans.

import { readFileSync, readdirSync, existsSync, statSync } from 'fs';
import { join, resolve } from 'path';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const errors = [];
const warnings = [];

function error(msg) { errors.push(msg); }
function warn(msg) { warnings.push(msg); }

// --- config.json ---

const configPath = join(WATCHTOWER_DIR, 'config.json');
if (existsSync(configPath)) {
  try {
    const config = JSON.parse(readFileSync(configPath, 'utf8'));
    if (config.schema_version !== 1) {
      error(`config.json: unsupported schema_version ${config.schema_version} (expected 1)`);
    }
    if (typeof config.projects !== 'object' || config.projects === null) {
      error('config.json: projects must be an object');
    }
  } catch (e) {
    error(`config.json: parse error — ${e.message}`);
  }
} else {
  warn('config.json not found — watchtower may not be installed');
}

// --- state/summary.md ---

const summaryPath = join(WATCHTOWER_DIR, 'state', 'summary.md');
if (existsSync(summaryPath)) {
  const lines = readFileSync(summaryPath, 'utf8').trimEnd().split('\n');
  if (lines.length > 30) {
    error(`state/summary.md: ${lines.length} lines (hard cap is 30)`);
  }
} else {
  warn('state/summary.md not found — rings may not have run yet');
}

// --- queue/items/*.json ---

const VALID_STATUSES = ['pending', 'resolved', 'expired', 'superseded', 'dismissed'];
const VALID_CATEGORIES = [
  'deferred-trigger', 'routing-decision', 'knowledge-extraction', 'methodology-capture',
  'upstream-friction', 'project-completion', 'completion-review', 'branch-diverged',
  'stale-project', 'pattern-promotion', 'watchtower-health', 'worktree-unmerged',
];
const VALID_ENRICHMENT = ['bare', 'in-progress', 'complete'];
const VALID_URGENCY = ['urgent', 'normal', 'low'];

const queueDir = join(WATCHTOWER_DIR, 'queue', 'items');
if (existsSync(queueDir)) {
  const entries = readdirSync(queueDir, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
    const itemPath = join(queueDir, entry.name);
    try {
      const item = JSON.parse(readFileSync(itemPath, 'utf8'));
      const id = entry.name.replace('.json', '');

      if (item.schema_version !== 1) {
        error(`queue/${entry.name}: unsupported schema_version ${item.schema_version}`);
        continue;
      }
      if (!VALID_STATUSES.includes(item.status)) {
        error(`queue/${entry.name}: invalid status '${item.status}'`);
      }
      if (!VALID_CATEGORIES.includes(item.category)) {
        error(`queue/${entry.name}: invalid category '${item.category}'`);
      }
      if (!VALID_ENRICHMENT.includes(item.enrichment_status)) {
        error(`queue/${entry.name}: invalid enrichment_status '${item.enrichment_status}'`);
      }
      if (!VALID_URGENCY.includes(item.urgency)) {
        error(`queue/${entry.name}: invalid urgency '${item.urgency}'`);
      }
      if (!item.context_anchor) {
        error(`queue/${entry.name}: missing context_anchor (required)`);
      }
      const REQUIRED_STRINGS = ['project', 'project_path', 'filed_at', 'filed_by', 'title', 'summary'];
      for (const field of REQUIRED_STRINGS) {
        if (!item[field]) {
          error(`queue/${entry.name}: missing required field '${field}'`);
        }
      }

      // Check for orphaned enrichment directories
      const enrichDir = join(queueDir, id, 'enrichment');
      if (item.enrichment_status === 'bare' && existsSync(enrichDir)) {
        warn(`queue/${entry.name}: enrichment_status is 'bare' but enrichment/ directory exists`);
      }
    } catch (e) {
      error(`queue/${entry.name}: parse error — ${e.message}`);
    }
  }

  // Check for orphaned enrichment dirs without a parent item
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const itemFile = join(queueDir, entry.name + '.json');
    if (!existsSync(itemFile)) {
      warn(`queue/${entry.name}/: orphaned enrichment directory (no ${entry.name}.json)`);
    }
  }
}

// --- Report ---

const label = 'watchtower-validate';
if (errors.length === 0 && warnings.length === 0) {
  console.log(`${label}: PASS`);
  process.exit(0);
}

if (warnings.length > 0) {
  for (const w of warnings) console.log(`  WARN: ${w}`);
}
if (errors.length > 0) {
  for (const e of errors) console.log(`  ERROR: ${e}`);
  console.log(`${label}: FAIL (${errors.length} error(s), ${warnings.length} warning(s))`);
  process.exit(1);
}

console.log(`${label}: PASS (${warnings.length} warning(s))`);

#!/usr/bin/env node

// Watchtower shared library.
// Canonical implementations of utilities used across all ring scripts.
// Ring scripts import from here instead of maintaining local copies.

import {
  readFileSync, writeFileSync, existsSync,
  mkdirSync, renameSync,
} from 'fs';
import { join, dirname } from 'path';
import { homedir } from 'os';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);

// Re-export queue operations so ring scripts have a single import source.
export { createItem, listPending, getItem, resolveItem } from './watchtower-queue.mjs';

// ---------------------------------------------------------------------------
// getWatchtowerDir — resolve watchtower root directory
// ---------------------------------------------------------------------------

export function getWatchtowerDir() {
  return process.env.WATCHTOWER_DIR
    || join(homedir(), '.claude-cabinet', 'watchtower');
}

// ---------------------------------------------------------------------------
// atomicWrite — mkdir -p dirname, write to .tmp, rename
// ---------------------------------------------------------------------------

export function atomicWrite(filePath, content) {
  const dir = dirname(filePath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const tmp = filePath + '.tmp';
  const data = typeof content === 'string'
    ? content
    : JSON.stringify(content, null, 2);
  writeFileSync(tmp, data);
  renameSync(tmp, filePath);
}

// ---------------------------------------------------------------------------
// loadConfig — read and validate config.json with schema_version check
// ---------------------------------------------------------------------------

export function loadConfig(watchtowerDir) {
  const dir = watchtowerDir || getWatchtowerDir();
  const configPath = join(dir, 'config.json');
  if (!existsSync(configPath)) {
    throw new Error('Watchtower config.json not found. Run /watchtower install first.');
  }
  const config = JSON.parse(readFileSync(configPath, 'utf8'));
  if (config.schema_version !== 1) {
    throw new Error(`Unsupported config schema_version ${config.schema_version} (expected 1)`);
  }
  return config;
}

// ---------------------------------------------------------------------------
// slugify — lowercase, replace non-alphanum with dash, trim dashes
// ---------------------------------------------------------------------------

export function slugify(text) {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

// ---------------------------------------------------------------------------
// loadBetterSqlite3 — resolve better-sqlite3 from wherever it actually lives
// ---------------------------------------------------------------------------
// The ring scripts run from ~/.claude-cabinet/watchtower/scripts/ with no
// node_modules nearby. NODE_PATH resolution only sees TOP-LEVEL packages in
// each NODE_PATH entry — it does not descend into a package's own
// node_modules, so better-sqlite3 nested under the globally-installed CC
// package is invisible to a bare require(). Resolution candidates, in order:
//   1. Normal resolution from this script's location (works if a top-level
//      install exists)
//   2. The project's own node_modules — any project with a pib.db has
//      better-sqlite3 installed, since pib-db depends on it
//   3. Nested under create-claude-cabinet in each NODE_PATH entry
// Returns the Database constructor, or null if no candidate resolves.

export function loadBetterSqlite3(projectPath) {
  const candidates = [() => require('better-sqlite3')];
  if (projectPath) {
    candidates.push(
      () => createRequire(join(projectPath, 'package.json'))('better-sqlite3')
    );
  }
  for (const entry of (process.env.NODE_PATH || '').split(':').filter(Boolean)) {
    candidates.push(
      () => createRequire(join(entry, 'create-claude-cabinet', 'package.json'))('better-sqlite3')
    );
  }
  for (const load of candidates) {
    try {
      return load();
    } catch {
      // try the next candidate
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// log / logError — structured logging with ring name and ISO timestamp
// ---------------------------------------------------------------------------

export function log(ring, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ring} ${ts}] ${msg}`);
}

export function logError(ring, msg) {
  const ts = new Date().toISOString();
  console.error(`[${ring} ${ts}] ERROR: ${msg}`);
}

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
// Thread cursor history (schema v2)
// ---------------------------------------------------------------------------
// A thread's cursor is no longer a single overwritten object; it is an
// append-only `cursor_history` array of point-in-time snapshots, one per
// session that advanced the thread. Each entry is
//   { date, session_id, cursor: { what, why, where_left_off, open_questions,
//     next_steps } }
// The "current" cursor is always the last entry. Overwriting threw away the
// journey (symptom → diagnosis → solution → abstraction); the history keeps it.
// `cursor_history` is the thread's first sibling timeline — the QA-handoff
// protocol (.claude/plans/qa-handoff-protocol.md) adds a parallel
// point-in-time event sibling rather than nesting inside the cursor.

// migrateThreadCursor — convert a legacy (schema v1) thread object that has a
// single `cursor` field into the v2 `cursor_history` shape, in place.
// Idempotent: a thread already carrying `cursor_history` is left alone (beyond
// stripping any stale `cursor` field and bumping the version). The single
// migrated entry inherits the date/session_id of the thread's most recent
// session, since the legacy cursor reflected the latest understanding.
export function migrateThreadCursor(threadData) {
  if (!threadData || typeof threadData !== 'object') return threadData;
  if (threadData.schema_version === undefined) threadData.schema_version = 1;

  if (Array.isArray(threadData.cursor_history)) {
    delete threadData.cursor; // drop any stale legacy field
    if (threadData.schema_version < 2) threadData.schema_version = 2;
    return threadData;
  }

  const legacy = threadData.cursor;
  const sessions = Array.isArray(threadData.sessions) ? threadData.sessions : [];
  const last = sessions.length ? sessions[sessions.length - 1] : null;
  threadData.cursor_history = legacy
    ? [{
        date: last?.date || (threadData.last_updated || '').slice(0, 10) || null,
        session_id: last?.id || null,
        cursor: legacy,
      }]
    : [];
  delete threadData.cursor;
  threadData.schema_version = 2;
  return threadData;
}

// currentCursor — the most recent cursor snapshot for a thread. Reads the last
// `cursor_history` entry, falling back to a legacy `cursor` field so consumers
// stay correct against any un-migrated thread file. Always returns an object.
export function currentCursor(thread) {
  const hist = thread?.cursor_history;
  if (Array.isArray(hist) && hist.length) return hist[hist.length - 1].cursor || {};
  return thread?.cursor || {};
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

#!/usr/bin/env node

// Watchtower inbox queue CRUD library.
// All writes use atomic temp+rename per watchtower-contracts.md.
// Queue uses directory listing, not index files (no-index convention).

import { readFileSync, writeFileSync, readdirSync, existsSync, mkdirSync, renameSync } from 'fs';
import { join } from 'path';
import { randomBytes } from 'crypto';

const WATCHTOWER_DIR = process.env.WATCHTOWER_DIR
  || join(process.env.HOME, '.claude-cabinet', 'watchtower');

const QUEUE_DIR = join(WATCHTOWER_DIR, 'queue', 'items');

// --- Helpers ---

function ensureDir(dir) {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

function itemPath(id) {
  return join(QUEUE_DIR, `${id}.json`);
}

function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp';
  writeFileSync(tmp, JSON.stringify(data, null, 2) + '\n');
  renameSync(tmp, filePath);
}

function readItem(filePath) {
  const raw = readFileSync(filePath, 'utf8');
  const item = JSON.parse(raw);
  if (item.schema_version !== 1) {
    throw new Error(`Unsupported schema version ${item.schema_version} in ${filePath}`);
  }
  return item;
}

function generateId() {
  return 'dec-' + randomBytes(4).toString('hex');
}

// Urgency sort order: urgent < normal < low (urgent first)
const URGENCY_ORDER = { urgent: 0, normal: 1, low: 2 };

// --- Exports ---

/**
 * Create a new inbox item.
 * @param {object} params
 * @returns {string} The generated item id
 */
export function createItem({
  project,
  project_path,
  category,
  urgency = 'normal',
  title,
  summary,
  context_anchor,
  evidence = {},
  options = [],
  draft_artifact = null,
  transcript_ref = null,
  filed_by = 'manual',
  plan_fid = null,
  thread_ids = [],
  confidence = null,
}) {
  ensureDir(QUEUE_DIR);
  const id = generateId();
  const item = {
    schema_version: 1,
    id,
    project,
    project_path,
    filed_at: new Date().toISOString(),
    filed_by,
    status: 'pending',
    enrichment_status: 'bare',
    category,
    urgency,
    title,
    summary,
    context_anchor,
    evidence,
    options,
    draft_artifact,
    transcript_ref,
    plan_fid,
    thread_ids,
    confidence,
    enrichment_dir: null,
    resolved_at: null,
    resolution: null,
    resolution_type: null,
    resolution_notes: null,
  };
  atomicWrite(itemPath(id), item);
  return id;
}

/**
 * Resolve an inbox item.
 * @param {string} id
 * @param {object} params
 * @returns {object} The updated item
 */
export function resolveItem(id, { resolution, resolution_notes = null, resolution_type = null }) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  item.status = 'resolved';
  item.resolved_at = new Date().toISOString();
  item.resolution = resolution;
  item.resolution_type = resolution_type;
  item.resolution_notes = resolution_notes;
  atomicWrite(fp, item);
  return item;
}

/**
 * Dismiss an inbox item.
 * @param {string} id
 * @param {object} params
 * @returns {object} The updated item
 */
export function dismissItem(id, { notes = null, resolution_type = null } = {}) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  item.status = 'dismissed';
  item.resolved_at = new Date().toISOString();
  item.resolution_type = resolution_type;
  item.resolution_notes = notes;
  atomicWrite(fp, item);
  return item;
}

/**
 * Mark an inbox item as superseded.
 * @param {string} id
 * @param {object} params
 * @returns {object} The updated item
 */
export function supersedeItem(id, { reason = null } = {}) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  item.status = 'superseded';
  item.resolution_notes = reason;
  atomicWrite(fp, item);
  return item;
}

/**
 * Mark an inbox item as expired.
 * @param {string} id
 * @returns {object} The updated item
 */
export function expireItem(id) {
  const fp = itemPath(id);
  const item = readItem(fp);
  if (item.status !== 'pending') return null;
  item.status = 'expired';
  item.resolution_notes = 'Auto-expired by age policy';
  atomicWrite(fp, item);
  return item;
}

/**
 * List pending inbox items with optional filters.
 * @param {object} filters
 * @returns {Array} Sorted array of pending items
 */
export function listPending({ project, category, urgency, maxAge } = {}) {
  if (!existsSync(QUEUE_DIR)) return [];
  const entries = readdirSync(QUEUE_DIR, { withFileTypes: true });
  const items = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
    try {
      const item = readItem(join(QUEUE_DIR, entry.name));
      if (item.status !== 'pending') continue;
      if (project && item.project !== project) continue;
      if (category && item.category !== category) continue;
      if (urgency && item.urgency !== urgency) continue;
      if (maxAge) {
        const filed = new Date(item.filed_at);
        const cutoff = new Date(Date.now() - maxAge);
        if (filed < cutoff) continue;
      }
      items.push(item);
    } catch {
      // Skip unparseable items
    }
  }
  // Sort: urgent first, then by filed_at descending (newest first)
  items.sort((a, b) => {
    const urgDiff = (URGENCY_ORDER[a.urgency] ?? 99) - (URGENCY_ORDER[b.urgency] ?? 99);
    if (urgDiff !== 0) return urgDiff;
    return new Date(b.filed_at) - new Date(a.filed_at);
  });
  return items;
}

/**
 * Get a single inbox item by id.
 * @param {string} id
 * @returns {object|null} The item, or null if not found
 */
export function getItem(id) {
  const fp = itemPath(id);
  if (!existsSync(fp)) return null;
  return readItem(fp);
}

/**
 * Get enrichment data for an inbox item.
 * Returns {code_context, related_decisions, memory_refs, options_analysis}
 * with nulls for missing files. Never throws on missing files.
 * @param {string} id
 * @returns {object}
 */
export function getEnrichment(id) {
  const enrichDir = join(QUEUE_DIR, id, 'enrichment');
  const files = {
    code_context: 'code-context.md',
    related_decisions: 'related-decisions.md',
    memory_refs: 'memory-refs.md',
    options_analysis: 'options-analysis.md',
  };
  const result = {};
  for (const [key, filename] of Object.entries(files)) {
    const fp = join(enrichDir, filename);
    try {
      result[key] = existsSync(fp) ? readFileSync(fp, 'utf8') : null;
    } catch {
      result[key] = null;
    }
  }
  return result;
}

/**
 * Run expiry check on pending items.
 * Items older than expireDays are marked expired with conservative defaults.
 * Items older than warnDays (but not yet expireDays) are flagged for warning.
 * @param {object} params
 * @returns {object} { warned: [], expired: [] }
 */
export function runExpiry({ warnDays = 14, expireDays = 30 } = {}) {
  const now = Date.now();
  const warnMs = warnDays * 24 * 60 * 60 * 1000;
  const expireMs = expireDays * 24 * 60 * 60 * 1000;
  const pending = listPending();
  const warned = [];
  const expired = [];

  for (const item of pending) {
    const age = now - new Date(item.filed_at).getTime();
    if (age >= expireMs) {
      item.status = 'expired';
      item.resolution_notes = `Auto-expired after ${expireDays} days. If still relevant, re-file with updated context.`;
      atomicWrite(itemPath(item.id), item);
      expired.push(item);
    } else if (age >= warnMs) {
      warned.push(item);
    }
  }

  return { warned, expired };
}

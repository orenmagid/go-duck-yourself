// Process-in-a-Box shared library
//
// All database operations as importable functions.
// Both the CLI (pib-db.mjs) and MCP server (pib-db-mcp-server.mjs)
// import from here. Schema changes update one place.
//
// Every function takes (db, params) and returns a result object.
// None of them do console.log — callers decide how to present output.

import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

// sql-constants lives in the installed engagement runtime (.claude/engagement/),
// which is gitignored in the CC source repo — absent on every fresh checkout/CI,
// where npm test must still pass (act:d64feaac). Fall back to the committed
// template source of truth. Same code in template and installed copies — no fork.
const { ENGAGEMENT_EVENTS_CREATE, ENGAGEMENT_EVENTS_INDEXES } =
  await import('../.claude/engagement/sql-constants.mjs').catch((err) => {
    if (err.code !== 'ERR_MODULE_NOT_FOUND') throw err;
    return import('../templates/engagement/sql-constants.mjs');
  });

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function generateFid(prefix) {
  return `${prefix}:${randomUUID().replace(/-/g, '').slice(0, 8)}`;
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Migrations — gated by PRAGMA user_version
// ---------------------------------------------------------------------------
// SCHEMA_VERSION history:
//   1 — added actions.status CHECK constraint
//   2 — added actions.tags
//   3 — added trigger_condition on actions/projects + trigger_checks history
//   4 — composite index on trigger_checks(target_fid, checked_at DESC) for listTriggered
//   5 — added engagement_events append-only log + 3 indexes (engagement mgmt)
//   6 — added projects.tags (symmetric with actions.tags at v2)
//   7 — added client-facing copy columns on actions + projects (4 each)
//   8 — added engagement_events.visibility (client|internal, default internal)
//   9 — audit finding ids run-prefixed (run_id || '/' || id) — same change
//       as BASE v7 (act:4ec70792); numbered 9 here because the patch owns
//       7-8. Keep base and patch bumped in the same release
//       (engagement-setup's base-ahead guard enforces it).
export const SCHEMA_VERSION = 9;

// Each entry: { version, sql }. A single version may have multiple SQL
// statements (e.g. column add + index). Statements run in array order;
// each is wrapped in try/catch so re-running on a DB that already has
// the column/table is a no-op. The user_version pragma is the primary
// gate — try/catch is a safety net for pre-pragma DBs.
//
// NOTE on version numbering (base vs patch):
//   Base (work-tracking): v1-v7 (v6 = client columns, v7 = audit id prefix)
//   Patch (engagement):   v1-v9 (v5 = engagement_events, v6 = projects.tags, v7 = client columns, v9 = audit id prefix)
//   The same physical columns appear at different version numbers because
//   base and patch have different migration histories. The try/catch on
//   "duplicate column" makes the overlap safe — a DB that already has
//   the columns from base v6 will silently skip patch v7's ALTERs.
const MIGRATIONS = [
  { version: 1, sql: "ALTER TABLE actions ADD COLUMN status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','in-progress','blocked','deferred','done'))" },
  { version: 2, sql: "ALTER TABLE actions ADD COLUMN tags TEXT NOT NULL DEFAULT ''" },
  { version: 3, sql: "ALTER TABLE actions ADD COLUMN trigger_condition TEXT" },
  { version: 3, sql: "ALTER TABLE projects ADD COLUMN trigger_condition TEXT" },
  { version: 3, sql: `CREATE TABLE IF NOT EXISTS trigger_checks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    target_table  TEXT NOT NULL CHECK(target_table IN ('actions','projects')),
    target_fid    TEXT NOT NULL,
    checked_at    TEXT NOT NULL,
    result        TEXT NOT NULL CHECK(result IN ('triggered','still-waiting','needs-info','condition-obsolete')),
    notes         TEXT
  )` },
  { version: 3, sql: "CREATE INDEX IF NOT EXISTS idx_trigger_checks_fid ON trigger_checks(target_fid)" },
  { version: 4, sql: "CREATE INDEX IF NOT EXISTS idx_trigger_checks_target_time ON trigger_checks(target_fid, checked_at DESC)" },
  { version: 5, sql: ENGAGEMENT_EVENTS_CREATE },
  ...ENGAGEMENT_EVENTS_INDEXES.map(sql => ({ version: 5, sql })),
  { version: 6, sql: "ALTER TABLE projects ADD COLUMN tags TEXT NOT NULL DEFAULT ''" },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_title TEXT" },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_body TEXT" },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_generated_at TEXT" },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_generated_status TEXT" },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_title TEXT" },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_body TEXT" },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_generated_at TEXT" },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_generated_status TEXT" },
  { version: 8, sql: "ALTER TABLE engagement_events ADD COLUMN visibility TEXT NOT NULL DEFAULT 'internal' CHECK(visibility IN ('client','internal'))" },
  // v9: prefix legacy audit finding ids with their run id (base v7 twin —
  // act:4ec70792). Idempotent via the NOT LIKE guard. skipOn: a db that
  // predates the audit tables has nothing to prefix.
  { version: 9, sql: "UPDATE audit_findings SET id = run_id || '/' || id WHERE run_id IS NOT NULL AND run_id != '' AND id NOT LIKE run_id || '/%'", skipOn: /no such table: audit_findings/i },
];

export function migrate(db) {
  const current = db.pragma('user_version', { simple: true });
  if (current >= SCHEMA_VERSION) return { from: current, to: current, applied: 0 };

  // Wrap in a transaction so a real mid-migration failure (disk full,
  // locked DB, constraint violation) rolls back user_version along with
  // the partial DDL. Only swallow "already exists" errors from legacy
  // pre-pragma DBs where columns may have been added before versioning.
  const tx = db.transaction(() => {
    let applied = 0;
    for (const m of MIGRATIONS) {
      if (m.version <= current) continue;
      try { db.exec(m.sql); applied++; }
      catch (e) {
        const msg = e.message || '';
        // Per-entry skip condition (explicit, never a general widening):
        // e.g. the v9 audit-id prefix on a db that predates audit tables.
        if (m.skipOn && m.skipOn.test(msg)) continue;
        if (!/already exists|duplicate column/i.test(msg)) throw e;
      }
    }
    db.pragma(`user_version = ${SCHEMA_VERSION}`);
    return applied;
  });
  const applied = tx();
  return { from: current, to: SCHEMA_VERSION, applied };
}

// ---------------------------------------------------------------------------
// Init — create tables from schema, then migrate
// ---------------------------------------------------------------------------
export function init(db, { schemaPath }) {
  const schema = readFileSync(schemaPath, 'utf-8');
  db.exec(schema);
  migrate(db);
  return { message: `Database initialized` };
}

// ---------------------------------------------------------------------------
// Data migration — extract client-facing copy from notes into columns
// ---------------------------------------------------------------------------
const CLIENT_COPY_RE = /<!--\s*client-facing\s*\n([\s\S]*?)-->/;
const CC_GENERATED_RE = /<!--\s*cc-generated:(\S+)\s+status:(\S+)\s*-->/;

export function migrateClientCopy(db) {
  const rows = db.prepare(
    "SELECT fid, notes FROM actions WHERE notes LIKE '%client-facing%' AND client_title IS NULL AND client_body IS NULL AND client_generated_at IS NULL"
  ).all();
  const projRows = db.prepare(
    "SELECT fid, notes FROM projects WHERE notes LIKE '%client-facing%' AND client_title IS NULL AND client_body IS NULL AND client_generated_at IS NULL"
  ).all();

  let migrated = 0;
  const update = db.prepare(
    "UPDATE actions SET client_title = ?, client_body = ?, client_generated_at = ?, client_generated_status = ? WHERE fid = ?"
  );
  const updateProj = db.prepare(
    "UPDATE projects SET client_title = ?, client_body = ?, client_generated_at = ?, client_generated_status = ? WHERE fid = ?"
  );

  for (const { fid, notes } of [...rows, ...projRows]) {
    if (!notes) continue;
    const copyMatch = notes.match(CLIENT_COPY_RE);
    const genMatch = notes.match(CC_GENERATED_RE);
    if (!copyMatch && !genMatch) continue;

    let title = null, body = null, genAt = null, genStatus = null;

    if (copyMatch) {
      const lines = copyMatch[1].split('\n').map(l => l.trim()).filter(Boolean);
      title = lines[0] || null;
      body = lines.slice(1).join('\n').trim() || null;
    }
    if (genMatch) {
      genAt = genMatch[1] || null;
      genStatus = genMatch[2] || null;
    }

    const stmt = rows.some(r => r.fid === fid) ? update : updateProj;
    stmt.run(title, body, genAt, genStatus, fid);
    migrated++;
  }
  return { migrated };
}

// ---------------------------------------------------------------------------
// Query — run arbitrary SQL
// ---------------------------------------------------------------------------
export function query(db, { sql }) {
  let stmt;
  try {
    stmt = db.prepare(sql);
  } catch (e) {
    return { error: { message: e.message } };
  }
  // SECURITY (security-0002): pib_query is READ-ONLY. better-sqlite3's
  // stmt.reader is the authoritative "does this statement return rows?" gate —
  // true for SELECT / WITH…SELECT / read PRAGMA / EXPLAIN, false for
  // INSERT/UPDATE/DELETE/DDL/write-PRAGMA. A non-reader is rejected WITHOUT
  // executing (prepare compiles but never runs it), so arbitrary writes can no
  // longer bypass the work-tracker guard and the surface-area/completion gates.
  // The old `else { db.exec(sql) }` branch was exactly that bypass — the guard's
  // own recommended escape hatch let you UPDATE/DELETE the actions table freely.
  if (!stmt.reader) {
    return {
      error: {
        message:
          'pib_query is read-only — only SELECT statements are allowed. To change data use pib_update_action / pib_complete_action / pib_create_action (they enforce the surface-area and completion gates).',
      },
    };
  }
  return { rows: stmt.all() };
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

/**
 * Validate that notes contain a ## Surface Area section with at least one
 * - files: or - dirs: line. Returns null if valid, or an error object if not.
 */
function validateSurfaceArea(notes) {
  if (!notes) {
    return {
      error: 'missing_surface_area',
      message: 'Action notes must contain a ## Surface Area section.',
      suggestedFormat: [
        '## Surface Area',
        '- files: path/to/file.js',
        '- files: path/to/other.js',
        '- dirs: src/components/',
      ].join('\n'),
    };
  }

  const hasSection = /^## Surface Area/m.test(notes);
  if (!hasSection) {
    return {
      error: 'missing_surface_area',
      message: 'Action notes must contain a ## Surface Area section.',
      suggestedFormat: [
        '## Surface Area',
        '- files: path/to/file.js',
        '- files: path/to/other.js',
        '- dirs: src/components/',
      ].join('\n'),
    };
  }

  // Extract everything after ## Surface Area until the next ## or end
  // Lookahead ends only at the next "## " header or absolute end-of-string.
  // A bare `\n*$` here terminated the match at a blank line right after the
  // header, yielding an empty capture for standard markdown spacing.
  const sectionMatch = notes.match(/^## Surface Area[^\n]*\n([\s\S]*?)(?=\n## |$(?![\s\S]))/m);
  const sectionBody = sectionMatch ? sectionMatch[1] : '';
  const hasEntry = /^- (?:files|dirs):/m.test(sectionBody);
  if (!hasEntry) {
    return {
      error: 'empty_surface_area',
      message: '## Surface Area section must contain at least one "- files:" or "- dirs:" line.',
      suggestedFormat: [
        '## Surface Area',
        '- files: path/to/file.js',
        '- dirs: src/components/',
      ].join('\n'),
    };
  }

  return null; // valid
}

export function createAction(db, { text, area, projectFid, due, notes }) {
  const validationError = validateSurfaceArea(notes);
  if (validationError) {
    return { error: validationError };
  }

  const fid = generateFid('act');
  db.prepare(`
    INSERT INTO actions (fid, text, area, project_fid, due, notes, created)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(fid, text, area || null, projectFid || null, due || null, notes || '', today());
  return { fid, text, message: `Created action ${fid}: ${text}` };
}

export function listActions(db, { status, project } = {}) {
  const conditions = ['a.deleted_at IS NULL'];
  const params = [];

  if (status) {
    conditions.push('a.status = ?');
    params.push(status);
  } else {
    conditions.push('a.completed = 0');
  }
  if (project) {
    conditions.push('a.project_fid = ?');
    params.push(project);
  }

  const rows = db.prepare(`
    SELECT a.fid, a.text, a.area, a.due, a.flagged, a.status, a.tags, p.name as project
    FROM actions a
    LEFT JOIN projects p ON a.project_fid = p.fid
    WHERE ${conditions.join(' AND ')}
    ORDER BY
      CASE WHEN a.due IS NOT NULL AND a.due <= date('now') THEN 0 ELSE 1 END,
      a.due,
      a.flagged DESC,
      a.created DESC
  `).all(...params);
  return { rows };
}

export function updateAction(db, { fid, status, text, tags, notes, due, flagged, projectFid, client_title, client_body, client_generated_at, client_generated_status }) {
  const sets = [];
  const params = [];

  if (status !== undefined) { sets.push('status = ?'); params.push(status); }
  if (text !== undefined) { sets.push('text = ?'); params.push(text); }
  if (tags !== undefined) { sets.push('tags = ?'); params.push(tags); }
  if (notes !== undefined) { sets.push('notes = ?'); params.push(notes); }
  if (due !== undefined) { sets.push('due = ?'); params.push(due); }
  if (flagged !== undefined) { sets.push('flagged = ?'); params.push(flagged === 'true' || flagged === '1' || flagged === true ? 1 : 0); }
  if (projectFid !== undefined) {
    // Reparent. The target project must exist — never silently accept a bogus
    // fid the way create-action's snake_case project_fid was silently dropped
    // (act:5a2f1f38), which is how actions ended up orphaned to begin with.
    const proj = db.prepare('SELECT fid FROM projects WHERE fid = ? AND deleted_at IS NULL').get(projectFid);
    if (!proj) {
      return { error: { message: `Cannot reparent ${fid}: project ${projectFid} does not exist (or is deleted). Pass an existing project fid.` } };
    }
    sets.push('project_fid = ?'); params.push(projectFid);
  }
  if (client_title !== undefined) { sets.push('client_title = ?'); params.push(client_title || null); }
  if (client_body !== undefined) { sets.push('client_body = ?'); params.push(client_body || null); }
  if (client_generated_at !== undefined) { sets.push('client_generated_at = ?'); params.push(client_generated_at || null); }
  if (client_generated_status !== undefined) { sets.push('client_generated_status = ?'); params.push(client_generated_status || null); }

  // If marking done, also set completed fields; conversely, reopening a done
  // action to any non-done status must CLEAR the completed flag/timestamp, or
  // the row is left half-completed (completed=1 with an active status).
  if (status === 'done') {
    sets.push('completed = 1', 'completed_at = ?');
    params.push(new Date().toISOString());
  } else if (status !== undefined) {
    sets.push('completed = 0', 'completed_at = NULL');
  }

  if (sets.length === 0) {
    return { error: { message: 'No fields to update. Use status, text, tags, notes, due, flagged, projectFid, client_title, client_body, client_generated_at, or client_generated_status.' } };
  }

  params.push(fid);
  // Guard the write: a nonexistent/typo'd fid — or a soft-deleted row — touches
  // zero rows, and a write announced from call success is not a confirmed write
  // (verify-your-writes). deleted_at IS NULL keeps updates off tombstoned rows.
  const result = db.prepare(`UPDATE actions SET ${sets.join(', ')} WHERE fid = ? AND deleted_at IS NULL`).run(...params);
  if (result.changes === 0) {
    return { error: { message: `Action ${fid} not found (no row updated)` } };
  }
  return { fid, message: `Updated ${fid}` };
}

export function completeAction(db, { fid }) {
  const fidError = validateFid(fid);
  if (fidError) return fidError;

  const row = db.prepare(
    `SELECT fid, text, completed FROM actions WHERE fid = ? AND deleted_at IS NULL`
  ).get(fid);
  if (!row) return { error: 'not_found', message: `No action with fid ${fid}` };
  if (row.completed === 1) return { error: 'already_done', message: `${fid} already completed` };

  const result = db.prepare(`
    UPDATE actions SET completed = 1, completed_at = ?, status = 'done' WHERE fid = ?
  `).run(new Date().toISOString(), fid);

  if (result.changes === 0) {
    return { error: 'update_failed', message: `UPDATE matched no rows for ${fid}` };
  }
  return { fid, text: row.text, message: `Completed ${fid}: ${row.text}` };
}

export function getAction(db, { fid }) {
  if (!fid) return { error: 'fid is required' };
  const row = db.prepare(`
    SELECT a.*, p.name as project_name
    FROM actions a
    LEFT JOIN projects p ON a.project_fid = p.fid
    WHERE a.fid = ? AND a.deleted_at IS NULL
  `).get(fid);
  if (!row) return { error: `No action found with fid: ${fid}` };
  return row;
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------
export function createProject(db, { name, area, notes, due, tags }) {
  const fid = generateFid('prj');
  db.prepare(`
    INSERT INTO projects (fid, name, area, notes, due, tags, created)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(fid, name, area || null, notes || '', due || null, tags || '', today());
  return { fid, name, message: `Created project ${fid}: ${name}` };
}

export function updateProject(db, { fid, tags, name, status, notes, client_title, client_body, client_generated_at, client_generated_status }) {
  const sets = [];
  const params = [];
  if (tags !== undefined) { sets.push('tags = ?'); params.push(tags); }
  if (name !== undefined) { sets.push('name = ?'); params.push(name); }
  if (status !== undefined) { sets.push('status = ?'); params.push(status); }
  if (notes !== undefined) { sets.push('notes = ?'); params.push(notes); }
  if (client_title !== undefined) { sets.push('client_title = ?'); params.push(client_title || null); }
  if (client_body !== undefined) { sets.push('client_body = ?'); params.push(client_body || null); }
  if (client_generated_at !== undefined) { sets.push('client_generated_at = ?'); params.push(client_generated_at || null); }
  if (client_generated_status !== undefined) { sets.push('client_generated_status = ?'); params.push(client_generated_status || null); }
  // Mirror updateAction: a project moving to done stamps completed_at; any move
  // OFF done clears it, so a reopened project isn't left with a stale
  // completion timestamp (data-integrity-0005 — 23 done projects had NULL).
  if (status === 'done') {
    sets.push('completed_at = ?');
    params.push(new Date().toISOString());
  } else if (status !== undefined) {
    sets.push('completed_at = NULL');
  }
  if (sets.length === 0) {
    return { error: { message: 'No fields to update. Use tags, name, status, notes, client_title, client_body, client_generated_at, or client_generated_status.' } };
  }
  params.push(fid);
  const result = db.prepare(`UPDATE projects SET ${sets.join(', ')} WHERE fid = ?`).run(...params);
  if (result.changes === 0) {
    return { error: { message: `Project ${fid} not found` } };
  }
  return { fid, message: `Updated ${fid}` };
}

export function listProjects(db) {
  const rows = db.prepare(`
    SELECT p.fid, p.name, p.area, p.status, p.due, p.notes, p.tags, p.deleted_at,
      (SELECT COUNT(*) FROM actions a WHERE a.project_fid = p.fid AND a.completed = 0 AND a.deleted_at IS NULL) as open_actions
    FROM projects p
    WHERE p.status = 'active' AND p.deleted_at IS NULL
    ORDER BY p.created DESC
  `).all();
  return { rows };
}

// ---------------------------------------------------------------------------
// Audit — ingest findings from a run directory
// ---------------------------------------------------------------------------
export function ingestFindings(db, { runDir }) {
  const summaryPath = join(runDir, 'run-summary.json');
  if (!existsSync(summaryPath)) {
    return { error: { message: `No run-summary.json found in ${runDir}` } };
  }
  const data = JSON.parse(readFileSync(summaryPath, 'utf-8'));
  const runId = data.meta?.runId || `run-${Date.now()}`;
  const timestamp = data.meta?.timestamp || new Date().toISOString();
  const dateStr = timestamp.slice(0, 10);

  // Same-run re-ingest refreshes the run row; cross-run id collisions are
  // structurally impossible now that run ids carry the date
  // (run-<YYYY-MM-DD>-<HH-MM-SS>, minted by merge-findings).
  db.prepare(`
    INSERT INTO audit_runs (id, date, timestamp, trigger, finding_count)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      date = excluded.date, timestamp = excluded.timestamp,
      trigger = excluded.trigger, finding_count = excluded.finding_count
  `).run(runId, dateStr, timestamp, data.meta?.trigger || 'manual', data.findings?.length || 0);

  // Stored finding ids are RUN-PREFIXED (<runId>/<member-NNNN>): member ids
  // restart every run, and the old bare-id INSERT OR REPLACE silently
  // destroyed prior runs' rows AND their triage columns on collision
  // (act:4ec70792, demonstrated live 2026-07-13). The upsert updates
  // CONTENT columns only — the four triage columns (triage_status,
  // triage_notes, triaged_at, fix_description) are never in the SET list,
  // so a same-run re-ingest can never reset an operator's dispositions.
  // Try the full shape with deliberation columns first; fall back to the
  // legacy shape for existing databases that haven't added them yet.
  let insert;
  let hasDeliberationCols = true;
  try {
    insert = db.prepare(`
      INSERT INTO audit_findings
        (id, run_id, cabinet_member, severity, title, description, assumption,
         evidence, question, file, line, suggested_fix, auto_fixable, type,
         status, annotations, rebuttal)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        run_id = excluded.run_id, cabinet_member = excluded.cabinet_member,
        severity = excluded.severity, title = excluded.title,
        description = excluded.description, assumption = excluded.assumption,
        evidence = excluded.evidence, question = excluded.question,
        file = excluded.file, line = excluded.line,
        suggested_fix = excluded.suggested_fix,
        auto_fixable = excluded.auto_fixable, type = excluded.type,
        status = excluded.status, annotations = excluded.annotations,
        rebuttal = excluded.rebuttal
    `);
  } catch {
    hasDeliberationCols = false;
    insert = db.prepare(`
      INSERT INTO audit_findings
        (id, run_id, cabinet_member, severity, title, description, assumption,
         evidence, question, file, line, suggested_fix, auto_fixable, type)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        run_id = excluded.run_id, cabinet_member = excluded.cabinet_member,
        severity = excluded.severity, title = excluded.title,
        description = excluded.description, assumption = excluded.assumption,
        evidence = excluded.evidence, question = excluded.question,
        file = excluded.file, line = excluded.line,
        suggested_fix = excluded.suggested_fix,
        auto_fixable = excluded.auto_fixable, type = excluded.type
    `);
  }

  let count = 0;
  for (const f of (data.findings || [])) {
    const storedId = String(f.id).startsWith(`${runId}/`) ? f.id : `${runId}/${f.id}`;
    const base = [
      storedId, runId, f['cabinet-member'], f.severity, f.title,
      f.description || null, f.assumption || null, f.evidence || null,
      f.question || null, f.file || null, f.line || null,
      f.suggestedFix || null, f.autoFixable ? 1 : 0, f.type || 'finding'
    ];
    if (hasDeliberationCols) {
      base.push(
        f.status || null,
        f.annotations ? JSON.stringify(f.annotations) : null,
        f.rebuttal ? JSON.stringify(f.rebuttal) : null
      );
    }
    insert.run(...base);
    count++;
  }
  return { count, runId, message: `Ingested ${count} findings from ${runDir} (run: ${runId})` };
}

// ---------------------------------------------------------------------------
// Triage
// ---------------------------------------------------------------------------
export function triage(db, { findingId, status, notes }) {
  const result = db.prepare(`
    UPDATE audit_findings
    SET triage_status = ?, triage_notes = ?, triaged_at = ?
    WHERE id = ?
  `).run(status, notes || null, new Date().toISOString(), findingId);
  // A typo'd/nonexistent findingId touches zero rows; do not announce a false
  // 'Triaged' (verify-your-writes — same class as updateAction above).
  if (result.changes === 0) {
    return { error: { message: `No audit finding with id ${findingId} (no row triaged)` } };
  }
  return { findingId, status, message: `Triaged ${findingId} → ${status}` };
}

export function triageHistory(db) {
  const rejected = db.prepare(`
    SELECT id, cabinet_member, title FROM audit_findings
    WHERE triage_status = 'rejected'
  `).all();

  const deferred = db.prepare(`
    SELECT id, cabinet_member, title FROM audit_findings
    WHERE triage_status = 'deferred'
  `).all();

  return {
    rejectedIds: rejected.map(r => r.id),
    rejectedFingerprints: rejected.map(r => ({ 'cabinet-member': r.cabinet_member, title: r.title })),
    deferredIds: deferred.map(r => r.id),
    deferredFingerprints: deferred.map(r => ({ 'cabinet-member': r.cabinet_member, title: r.title })),
  };
}

// ---------------------------------------------------------------------------
// Deferred triggers
// ---------------------------------------------------------------------------
// Items (actions or projects) with a trigger_condition are waiting on a
// specific condition. The orient skill re-evaluates them each session and
// records each check in trigger_checks (append-only history).

export const TRIGGER_RESULT_VOCABULARY = ['triggered', 'still-waiting', 'needs-info', 'condition-obsolete'];
export const FID_PATTERN = /^(act|prj):[a-f0-9]{8}$/;
const TRIGGER_CONDITION_MAX_LENGTH = 2000;

function validateFid(fid) {
  if (!fid || typeof fid !== 'string') {
    return { error: 'missing_fid', message: 'fid is required' };
  }
  if (!FID_PATTERN.test(fid)) {
    return { error: 'invalid_fid_format', message: `fid must match ${FID_PATTERN}, got "${fid}"` };
  }
  return null;
}

function tableForFid(fid) {
  return fid.startsWith('prj:') ? 'projects' : 'actions';
}

export function deferWithTrigger(db, { fid, triggerCondition, cascade = false } = {}) {
  const fidError = validateFid(fid);
  if (fidError) return { error: fidError };
  if (!triggerCondition || typeof triggerCondition !== 'string' || triggerCondition.trim() === '') {
    return { error: { error: 'missing_trigger_condition', message: 'triggerCondition must be a non-empty string' } };
  }
  if (triggerCondition.length > TRIGGER_CONDITION_MAX_LENGTH) {
    return { error: { error: 'trigger_condition_too_long', message: `triggerCondition must be ≤${TRIGGER_CONDITION_MAX_LENGTH} chars, got ${triggerCondition.length}` } };
  }

  const table = tableForFid(fid);
  const row = db.prepare(`SELECT status, ${table === 'actions' ? 'completed' : "'0' as completed"} FROM ${table} WHERE fid = ? AND deleted_at IS NULL`).get(fid);
  if (!row) return { error: { error: 'not_found', message: `No ${table} row with fid ${fid}` } };
  if (row.status === 'done' || row.completed === 1) {
    return { error: { error: 'already_done', message: `${fid} is already done; cannot defer` } };
  }

  let cascaded = 0;
  if (table === 'projects') {
    // Children with their own trigger_condition already carry their own
    // return condition; cascade leaves them alone so the parent's trigger
    // doesn't overwrite their independent wait state.
    const openChildren = db.prepare(`SELECT fid FROM actions WHERE project_fid = ? AND status NOT IN ('done','deferred') AND trigger_condition IS NULL AND deleted_at IS NULL`).all(fid);
    if (openChildren.length > 0 && !cascade) {
      return {
        error: {
          error: 'has_open_children',
          message: `Project ${fid} has ${openChildren.length} open action(s) without their own trigger. Pass cascade: true to defer them alongside.`,
          openChildren: openChildren.map(c => c.fid),
        },
      };
    }
    if (cascade) {
      const appendNote = `\n\n_Deferred alongside parent ${fid} (trigger: ${triggerCondition})_`;
      const stmt = db.prepare(`UPDATE actions SET status = 'deferred', notes = notes || ? WHERE fid = ?`);
      for (const child of openChildren) stmt.run(appendNote, child.fid);
      cascaded = openChildren.length;
    }
  }

  const newStatus = table === 'projects' ? 'someday' : 'deferred';
  db.prepare(`UPDATE ${table} SET status = ?, trigger_condition = ? WHERE fid = ?`).run(newStatus, triggerCondition, fid);

  return { fid, table, triggerCondition, status: newStatus, cascaded, message: `Deferred ${fid} with trigger` };
}

export function listTriggered(db, { includeDone = false } = {}) {
  const actionsWhere = includeDone
    ? 'a.trigger_condition IS NOT NULL AND a.deleted_at IS NULL'
    : "a.trigger_condition IS NOT NULL AND a.deleted_at IS NULL AND a.status != 'done' AND (a.completed IS NULL OR a.completed = 0)";
  const projectsWhere = includeDone
    ? 'p.trigger_condition IS NOT NULL AND p.deleted_at IS NULL'
    : "p.trigger_condition IS NOT NULL AND p.deleted_at IS NULL AND p.status != 'done'";

  const actions = db.prepare(`
    SELECT a.fid, a.text, a.trigger_condition, a.status, p.name AS project_name,
      (SELECT checked_at FROM trigger_checks WHERE target_fid = a.fid ORDER BY checked_at DESC LIMIT 1) AS last_checked,
      (SELECT result FROM trigger_checks WHERE target_fid = a.fid ORDER BY checked_at DESC LIMIT 1) AS last_result
    FROM actions a
    LEFT JOIN projects p ON a.project_fid = p.fid
    WHERE ${actionsWhere}
    ORDER BY last_checked IS NOT NULL, last_checked ASC
  `).all();

  const projects = db.prepare(`
    SELECT p.fid, p.name, p.trigger_condition, p.status,
      (SELECT checked_at FROM trigger_checks WHERE target_fid = p.fid ORDER BY checked_at DESC LIMIT 1) AS last_checked,
      (SELECT result FROM trigger_checks WHERE target_fid = p.fid ORDER BY checked_at DESC LIMIT 1) AS last_result
    FROM projects p
    WHERE ${projectsWhere}
    ORDER BY last_checked IS NOT NULL, last_checked ASC
  `).all();

  return { actions, projects };
}

export function markTriggerChecked(db, { fid, result, notes } = {}) {
  const fidError = validateFid(fid);
  if (fidError) return { error: fidError };
  if (!TRIGGER_RESULT_VOCABULARY.includes(result)) {
    return {
      error: {
        error: 'invalid_result',
        message: `result must be one of: ${TRIGGER_RESULT_VOCABULARY.join(', ')}`,
        got: result,
      },
    };
  }
  const table = tableForFid(fid);
  const row = db.prepare(`SELECT status, ${table === 'actions' ? 'completed' : "'0' as completed"} FROM ${table} WHERE fid = ? AND deleted_at IS NULL`).get(fid);
  if (!row) return { error: { error: 'not_found', message: `No ${table} row with fid ${fid}` } };
  if (row.status === 'done' || row.completed === 1) {
    return { error: { error: 'already_done', message: `${fid} is already done; recording a trigger check on a completed item is not allowed` } };
  }

  const checkedAt = new Date().toISOString();
  db.prepare(`
    INSERT INTO trigger_checks (target_table, target_fid, checked_at, result, notes)
    VALUES (?, ?, ?, ?, ?)
  `).run(table, fid, checkedAt, result, notes || null);

  return { fid, checkedAt, result, message: `Recorded trigger check for ${fid}: ${result}` };
}

// ---------------------------------------------------------------------------
// Engagement events (schema v5) — append-only log for consulting engagements
// ---------------------------------------------------------------------------
// All writes route through addEngagementEvent (CLI and MCP both call it; no
// raw INSERT). The library stays config-agnostic: it never reads engagement
// yaml — the caller passes allowedAuthors (recipient ids + 'consultant')
// derived from config, and the author allowlist is enforced fail-closed.
export const ENGAGEMENT_EVENT_KINDS = ['client_feedback', 'status_push', 'delegation', 'approval', 'note', 'packet_sent'];
// STORAGE verdicts. Duplicated (no single SoT across JS/SQL) in: the
// engagement_events CHECK in pib-db-schema.sql AND the v5 MIGRATIONS entry
// above. The client-side superset is FEEDBACK_VERDICTS in engagement.mjs.
// Keep all four in sync if you add/rename a verdict.
export const ENGAGEMENT_VERDICTS = ['approve', 'object', 'comment', 'none'];
// Kinds that must carry a meaningful verdict (not NULL, not 'none').
const ENGAGEMENT_VERDICT_REQUIRED_KINDS = ['client_feedback', 'approval'];
const ENGAGEMENT_MEANINGFUL_VERDICTS = ['approve', 'object', 'comment'];
const ENGAGEMENT_BODY_MAX_LENGTH = 10000;

/**
 * Append an engagement event. Returns the inserted row's id (or the existing
 * id when a dedup match is found). Validates the engagement exists, the
 * author is in the allowlist (fail-closed), the kind/verdict enums, and the
 * body length before touching the DB.
 *
 * Dedup is advisory, not a hard constraint: when packet_id, target_fid, and
 * verdict are all non-null (a real feedback/approval response), an identical
 * (packet_id, target_fid, verdict) triple is treated as a duplicate and not
 * re-inserted. Distinct verdicts for the same (packet_id, target_fid) are
 * preserved as separate rows. packet_sent rows (target_fid/verdict NULL) are
 * never deduped — duplicates there are harmless (file rename is primary state).
 */
export function addEngagementEvent(db, { engagement, target_fid, packet_id, kind, author, verdict, body, visibility = 'internal' } = {}, allowedAuthors) {
  // engagement must be a project fid that exists and is not soft-deleted.
  const engError = validateFid(engagement);
  if (engError) return { error: { error: 'invalid_engagement', message: `engagement must be a valid fid: ${engError.message}` } };
  if (!engagement.startsWith('prj:')) {
    return { error: { error: 'invalid_engagement', message: `engagement must be a project fid (prj:*), got "${engagement}"` } };
  }
  const engRow = db.prepare(`SELECT fid FROM projects WHERE fid = ? AND deleted_at IS NULL`).get(engagement);
  if (!engRow) return { error: { error: 'engagement_not_found', message: `No engagement (project) with fid ${engagement}` } };

  // target_fid is optional; when present it must be an action fid (no
  // existence check — append-only audit, action may be deleted later).
  if (target_fid !== undefined && target_fid !== null) {
    const tgtError = validateFid(target_fid);
    if (tgtError) return { error: { error: 'invalid_target_fid', message: `target_fid must be a valid fid or null: ${tgtError.message}` } };
    if (!target_fid.startsWith('act:')) {
      return { error: { error: 'invalid_target_fid', message: `target_fid must be an action fid (act:*) or null, got "${target_fid}"` } };
    }
  }

  if (!ENGAGEMENT_EVENT_KINDS.includes(kind)) {
    return { error: { error: 'invalid_kind', message: `kind must be one of: ${ENGAGEMENT_EVENT_KINDS.join(', ')}`, got: kind } };
  }

  if (verdict !== undefined && verdict !== null && !ENGAGEMENT_VERDICTS.includes(verdict)) {
    return { error: { error: 'invalid_verdict', message: `verdict must be one of: ${ENGAGEMENT_VERDICTS.join(', ')} (or null)`, got: verdict } };
  }
  if (ENGAGEMENT_VERDICT_REQUIRED_KINDS.includes(kind) && !ENGAGEMENT_MEANINGFUL_VERDICTS.includes(verdict)) {
    return { error: { error: 'verdict_required', message: `kind "${kind}" requires a meaningful verdict (${ENGAGEMENT_MEANINGFUL_VERDICTS.join(', ')}), got "${verdict ?? 'null'}"` } };
  }

  if (visibility !== 'client' && visibility !== 'internal') {
    return { error: { error: 'invalid_visibility', message: `visibility must be 'client' or 'internal', got "${visibility}"` } };
  }

  // Author allowlist — fail closed. Missing/empty/non-array allowedAuthors
  // rejects every author rather than silently accepting any.
  if (!Array.isArray(allowedAuthors) || allowedAuthors.length === 0) {
    return { error: { error: 'no_allowed_authors', message: 'allowedAuthors must be a non-empty array (fail-closed)' } };
  }
  if (typeof author !== 'string' || !allowedAuthors.includes(author)) {
    return { error: { error: 'author_not_allowed', message: `author "${author}" is not in allowedAuthors`, allowedAuthors } };
  }

  if (body !== undefined && body !== null) {
    if (typeof body !== 'string') {
      return { error: { error: 'invalid_body', message: 'body must be a string or null' } };
    }
    if (body.length > ENGAGEMENT_BODY_MAX_LENGTH) {
      return { error: { error: 'body_too_long', message: `body must be ≤${ENGAGEMENT_BODY_MAX_LENGTH} chars, got ${body.length}` } };
    }
  }

  // Advisory dedup: only when all three core key parts are present. Body is
  // part of the key so that two responses mapping to the same verdict but
  // carrying distinct content (e.g. a 'provided' value and a 'credential_sent'
  // envelope_id, both stored as verdict='comment') don't collapse — and so a
  // client can correct a value by re-sending with new content. Identical
  // re-feeds (same body) still dedup, preserving idempotence.
  if (packet_id != null && target_fid != null && verdict != null) {
    const existing = db.prepare(
      `SELECT id FROM engagement_events
       WHERE packet_id = ? AND target_fid = ? AND verdict = ? AND IFNULL(body,'') = IFNULL(?,'') LIMIT 1`
    ).get(packet_id, target_fid, verdict, body ?? null);
    if (existing) {
      return { id: existing.id, deduped: true, message: `Duplicate event skipped (id ${existing.id})` };
    }
  }

  const createdAt = new Date().toISOString();
  const result = db.prepare(`
    INSERT INTO engagement_events (engagement, target_fid, packet_id, kind, author, verdict, body, visibility, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(engagement, target_fid ?? null, packet_id ?? null, kind, author, verdict ?? null, body ?? null, visibility, createdAt);

  return {
    id: Number(result.lastInsertRowid),
    engagement,
    kind,
    created_at: createdAt,
    message: `Recorded engagement event ${result.lastInsertRowid} (${kind}) for ${engagement}`,
  };
}

/**
 * List engagement events, newest first. Filters:
 * - engagement: scope to one engagement (project fid)
 * - target_fid: scope to one action's events
 * - unaddressedOnly: only events with addressed = 0
 * - excludeSoftDeleted: drop events whose target action is soft-deleted.
 *   Engagement-level events (target_fid IS NULL) are ALWAYS kept — the
 *   LEFT JOIN produces a NULL deleted_at for them, and the WHERE clause
 *   explicitly retains them.
 */
export function listEngagementEvents(db, { engagement, target_fid, unaddressedOnly = false, excludeSoftDeleted = false } = {}) {
  const conditions = [];
  const params = [];

  if (engagement) { conditions.push('e.engagement = ?'); params.push(engagement); }
  if (target_fid) { conditions.push('e.target_fid = ?'); params.push(target_fid); }
  if (unaddressedOnly) { conditions.push('e.addressed = 0'); }
  if (excludeSoftDeleted) {
    // Keep engagement-level events (no target) and events whose target
    // action is not soft-deleted. a.deleted_at is NULL both when the action
    // is live AND when there is no joined action row (target_fid NULL) —
    // the explicit "e.target_fid IS NULL" guards the engagement-level case.
    conditions.push('(e.target_fid IS NULL OR a.deleted_at IS NULL)');
  }

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const join = excludeSoftDeleted ? 'LEFT JOIN actions a ON e.target_fid = a.fid' : '';

  const rows = db.prepare(`
    SELECT e.id, e.engagement, e.target_fid, e.packet_id, e.kind, e.author,
           e.verdict, e.body, e.visibility, e.addressed, e.created_at
    FROM engagement_events e
    ${join}
    ${where}
    ORDER BY e.created_at DESC, e.id DESC
  `).all(...params);

  return { rows };
}

/** Mark an engagement event addressed (consultant has triaged it). */
export function markEventAddressed(db, { id } = {}) {
  const numId = Number(id);
  if (!Number.isInteger(numId) || numId <= 0) {
    return { error: { error: 'invalid_id', message: `id must be a positive integer, got "${id}"` } };
  }
  const result = db.prepare(`UPDATE engagement_events SET addressed = 1 WHERE id = ?`).run(numId);
  if (result.changes === 0) {
    return { error: { error: 'not_found', message: `No engagement event with id ${numId}` } };
  }
  return { id: numId, addressed: 1, message: `Marked engagement event ${numId} addressed` };
}

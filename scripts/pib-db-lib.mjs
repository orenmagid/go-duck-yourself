// ENGAGEMENT OVERLAY COPY — downstream, not the source of truth.
//
// lib/engagement-setup.js REPLACES the installed pib-db files with these when
// the engagement module is enabled, so this file must be a strict SUPERSET of
// templates/scripts/pib-db-lib.mjs: anything added to base and not ported here
// simply does not exist for an engagement consumer — no install error, no
// runtime error, the capability is just absent. That has happened for real
// (act:404b1628: the reparent verb, found the hard way twice).
// test/engagement-pib-drift/ fails the build on it now.
//
// The ONE axis on which this file may differ is the MIGRATION NUMBERING. This
// tier runs ahead of base because it owns extra versions, so a base migration
// entry must be RENUMBERED here, never copy-pasted: a number at or below an
// existing database's user_version is skipped forever, and migrate() then
// stamps user_version to head anyway, so the change never lands and nothing
// says so. Edit base first, port here second, renumber deliberately.
//
// Process-in-a-Box shared library
//
// All database operations as importable functions.
// Both the CLI (pib-db.mjs) and MCP server (pib-db-mcp-server.mjs)
// import from here. Schema changes update one place.
//
// Every function takes (db, params) and returns a result object.
// None of them do console.log — callers decide how to present output.

import { existsSync, readFileSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';
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
//  10 — action_dependencies: dependency as a real edge between actions —
//       same change as BASE v8 (act:d46f3b48); numbered 10 here because the
//       patch runs two ahead. COPYING base's `version: 8` here would be
//       silently fatal: migrate() skips `if (m.version <= current)`, so every
//       engagement db (already at user_version 9) would skip it forever and
//       then be stamped 10 by the unconditional pragma at the end — the table
//       would simply never exist, on the exact installs that carry this file.
//       The drift guard cannot see that (it normalizes the version number
//       away before comparing), so test/engagement-pib-drift/ now also
//       asserts max(migration version) === SCHEMA_VERSION in each tier.
//  11 — the middle type: projects.kind + `area` column dropped from both
//       tables with values archived (base v9 twin — act:14876288; plan
//       §7/§13/§14). Numbered 11 here because the patch runs two ahead.
//  12 — completion propagation: actions.resolution + propagation_events
//       (base v10 twin — act:08c563ae; plan §16). Numbered 12 here because
//       the patch runs two ahead — RENUMBERED, never copy-pasted.
//  13 — container nesting: projects.parent_fid, so an AREA contains PROJECTs
//       (base v11 twin — act:e567d724; plan §17). Numbered 13 here because
//       the patch runs two ahead — RENUMBERED, never copy-pasted. This is the
//       tier that matters most for this migration: the live Maginnis tracker
//       sits at user_version 12 on THIS lineage, so v13 is the number that
//       actually reaches it.
export const SCHEMA_VERSION = 13;

// ---------------------------------------------------------------------------
// Schema lineage — the fail-closed guard (act:b7d81b52)
// ---------------------------------------------------------------------------
// TWO numbering lineages stamp the same PRAGMA user_version integer: the base
// library, and this engagement overlay whose numbers run two ahead
// (base 9 === overlay 11 in content). A database numbered on one lineage
// meeting code from the other reads the bare integer, skips migrations whose
// numbers fall at or below it, and stamps itself fully migrated anyway. That
// fired on two live consumers during the v0.56.0 walk (reflow and the
// maginnis tracker: user_version 10, no projects.kind, dead trackers, the
// installer reporting success). The defenses here are:
//
//   1. SHAPE VERIFICATION BEFORE TRUST. Every migration entry that produces
//      a durable structural object declares it as `expect`. migrate()
//      verifies the post-condition of every migration the stamp claims was
//      applied — including on the current >= SCHEMA_VERSION early-return
//      path — and REFUSES loudly on contradiction instead of no-opping into
//      a broken shape. Coverage starts at v5, the first version where the
//      two lineages' numbering diverges in content; below that the lineages
//      are identical and a collision cannot manifest.
//   2. THE LINEAGE MARKER. Every user_version stamp also records WHICH
//      lineage stamped it (schema_lineage row), so a bare integer is never
//      the only signal. The base library refuses to renumber a stamp the
//      engagement lineage wrote; the overlay may adopt a base stamp (its
//      higher-numbered entries are a superset guarded by duplicate-swallow —
//      that is the legitimate add-engagement-to-an-existing-consumer path,
//      and it REQUIRES engagement_events to exist before migrate() runs:
//      lib/engagement-setup.js ensures the table before it migrates).
//   3. verifySchema() — the full-shape check the installer runs post-install
//      (`pib-db.mjs verify-schema`), so "install succeeded" is a claim about
//      the schema on disk, not about the copy loop exiting zero.
export const SCHEMA_LINEAGE = 'engagement';

export const SCHEMA_LINEAGE_CREATE = `CREATE TABLE IF NOT EXISTS schema_lineage (
  id              INTEGER PRIMARY KEY CHECK(id = 1),
  lineage         TEXT NOT NULL CHECK(lineage IN ('base','engagement')),
  stamped_version INTEGER NOT NULL,
  stamped_at      TEXT NOT NULL
)`;

function tableExists(db, name) {
  return db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name) !== undefined;
}

function columnExists(db, table, column) {
  if (!tableExists(db, table)) return false;
  return db.pragma(`table_info(${table})`).some((c) => c.name === column);
}

// One declared post-condition: { table: 'name' } | { column: 'tbl.col' } |
// { gone: 'tbl.col' }. Returns a problem string, or null when satisfied.
function checkExpectation(db, expect) {
  if (expect.table) return tableExists(db, expect.table) ? null : `table ${expect.table} is missing`;
  const [tbl, col] = (expect.column || expect.gone).split('.');
  if (expect.column) return columnExists(db, tbl, col) ? null : `column ${expect.column} is missing`;
  return columnExists(db, tbl, col) ? `column ${expect.gone} should have been dropped but is still present` : null;
}

export function readSchemaLineage(db) {
  if (!tableExists(db, 'schema_lineage')) return null;
  return db.prepare('SELECT lineage, stamped_version, stamped_at FROM schema_lineage WHERE id = 1').get() || null;
}

// The action_dependencies DDL, single-sourced. This body must stay identical
// to the declaration in pib-db-schema.sql — a table that exists in one and not
// the other reaches either fresh installs or existing databases, never both.
// test/pib-db-deps/ asserts the parity (the pattern comes from
// test/pib-db-engagement/sql-constants-drift.test.js). The full rationale for
// NOT NULL and for the deliberately absent ON DELETE clause lives in the
// schema file beside the declaration.
export const ACTION_DEPENDENCIES_CREATE = `CREATE TABLE IF NOT EXISTS action_dependencies (
  blocker_fid  TEXT NOT NULL REFERENCES actions(fid),
  blocked_fid  TEXT NOT NULL REFERENCES actions(fid),
  created      TEXT NOT NULL CHECK(created GLOB '????-??-??'),
  PRIMARY KEY (blocker_fid, blocked_fid)
)`;

export const ACTION_DEPENDENCIES_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_action_deps_blocked ON action_dependencies(blocked_fid)',
  'CREATE INDEX IF NOT EXISTS idx_action_deps_blocker ON action_dependencies(blocker_fid)',
];

// The propagation_events DDL, single-sourced on the same pattern as
// ACTION_DEPENDENCIES_CREATE: this body must stay identical to the
// declaration in pib-db-schema.sql. The design rationale (no FK — history
// must survive its subject, per trigger_checks' precedent; project_fid NOT
// unique — a reopen-then-re-walk appends a second row) lives with the schema
// declaration.
export const PROPAGATION_EVENTS_CREATE = `CREATE TABLE IF NOT EXISTS propagation_events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  project_fid  TEXT NOT NULL,
  walked_at    TEXT NOT NULL,
  map          TEXT NOT NULL
)`;

export const PROPAGATION_EVENTS_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_propagation_events_project ON propagation_events(project_fid)',
];

// The containment index (v13, base v11; §17). Every rolled-up read resolves an
// area's child projects first, so this is on the hot path of the scope helper,
// the container cards, and the completion gate's live-child-container check —
// not speculative. Same single-sourcing rule as the two above: the body must
// stay identical to the declaration in pib-db-schema.sql.
export const CONTAINER_PARENT_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_projects_parent ON projects(parent_fid)',
];

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
  { version: 5, sql: ENGAGEMENT_EVENTS_CREATE, expect: { table: 'engagement_events' } },
  ...ENGAGEMENT_EVENTS_INDEXES.map(sql => ({ version: 5, sql })),
  { version: 6, sql: "ALTER TABLE projects ADD COLUMN tags TEXT NOT NULL DEFAULT ''", expect: { column: 'projects.tags' } },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_title TEXT", expect: { column: 'actions.client_title' } },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_body TEXT", expect: { column: 'actions.client_body' } },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_generated_at TEXT", expect: { column: 'actions.client_generated_at' } },
  { version: 7, sql: "ALTER TABLE actions ADD COLUMN client_generated_status TEXT", expect: { column: 'actions.client_generated_status' } },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_title TEXT", expect: { column: 'projects.client_title' } },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_body TEXT", expect: { column: 'projects.client_body' } },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_generated_at TEXT", expect: { column: 'projects.client_generated_at' } },
  { version: 7, sql: "ALTER TABLE projects ADD COLUMN client_generated_status TEXT", expect: { column: 'projects.client_generated_status' } },
  { version: 8, sql: "ALTER TABLE engagement_events ADD COLUMN visibility TEXT NOT NULL DEFAULT 'internal' CHECK(visibility IN ('client','internal'))", expect: { column: 'engagement_events.visibility' } },
  // v9: prefix legacy audit finding ids with their run id (base v7 twin —
  // act:4ec70792). Idempotent via the NOT LIKE guard. skipOn: a db that
  // predates the audit tables has nothing to prefix.
  { version: 9, sql: "UPDATE audit_findings SET id = run_id || '/' || id WHERE run_id IS NOT NULL AND run_id != '' AND id NOT LIKE run_id || '/%'", skipOn: /no such table: audit_findings/i },
  // v10: dependency as a real edge (base v8 twin — act:d46f3b48).
  { version: 10, sql: ACTION_DEPENDENCIES_CREATE, expect: { table: 'action_dependencies' } },
  ...ACTION_DEPENDENCIES_INDEXES.map(sql => ({ version: 10, sql })),
  // v11: the middle type (base v9 twin — act:14876288). Every existing
  // container defaults to 'area' — the DEFAULT is the whole classification
  // story, deliberately: reclassifying to 'project' is per-container operator
  // judgment (§7.2) and no migration may do it. Archive INSERTs run BEFORE
  // the drops, OR IGNORE for pre-pragma re-runs; skipOn covers databases
  // where `area` is already gone.
  { version: 11, sql: "ALTER TABLE projects ADD COLUMN kind TEXT NOT NULL DEFAULT 'area' CHECK(kind IN ('area','project'))", expect: { column: 'projects.kind' } },
  { version: 11, sql: `CREATE TABLE IF NOT EXISTS legacy_area_values (
  tbl    TEXT NOT NULL CHECK(tbl IN ('actions','projects')),
  fid    TEXT NOT NULL,
  value  TEXT NOT NULL,
  PRIMARY KEY (tbl, fid)
)`, expect: { table: 'legacy_area_values' } },
  { version: 11, sql: "INSERT OR IGNORE INTO legacy_area_values (tbl, fid, value) SELECT 'projects', fid, area FROM projects WHERE area IS NOT NULL", skipOn: /no such column: area/i },
  { version: 11, sql: "INSERT OR IGNORE INTO legacy_area_values (tbl, fid, value) SELECT 'actions', fid, area FROM actions WHERE area IS NOT NULL", skipOn: /no such column: area/i },
  { version: 11, sql: "ALTER TABLE projects DROP COLUMN area", skipOn: /no such column/i, expect: { gone: 'projects.area' } },
  { version: 11, sql: "ALTER TABLE actions DROP COLUMN area", skipOn: /no such column/i, expect: { gone: 'actions.area' } },
  // v12: completion propagation (base v10 twin — act:08c563ae; §16). The
  // resolution column is the per-row typed-closure claim; NO backfill — a
  // closed row with NULL resolution honestly predates the feature. The CHECK
  // closes the vocabulary at the schema layer.
  { version: 12, sql: "ALTER TABLE actions ADD COLUMN resolution TEXT CHECK(resolution IN ('verified','parent-outcome'))", expect: { column: 'actions.resolution' } },
  { version: 12, sql: PROPAGATION_EVENTS_CREATE, expect: { table: 'propagation_events' } },
  ...PROPAGATION_EVENTS_INDEXES.map(sql => ({ version: 12, sql })),
  // v13 (base v11): container nesting (act:e567d724; §17). RENUMBERED from
  // base's 11 — copying base's number here would be silently fatal on exactly
  // the installs that carry this file. NO backfill and no DEFAULT: every
  // existing container stays a root, because nesting is operator judgment and
  // no migration may do it. Measured on SQLite 3.51.3: the added CHECK is a
  // ROW SCAN, so a violating row rolls the migration back rather than
  // half-applying it; a re-run raises "duplicate column name", which
  // migrate()'s existing swallow already matches — no skipOn needed.
  { version: 13, sql: "ALTER TABLE projects ADD COLUMN parent_fid TEXT REFERENCES projects(fid) CHECK(parent_fid IS NULL OR kind = 'project')", expect: { column: 'projects.parent_fid' } },
  ...CONTAINER_PARENT_INDEXES.map(sql => ({ version: 13, sql })),
];

// The post-condition of every migration the stamp claims was applied.
// Returns problem strings; empty means the stamp and the shape agree.
function verifyStampedShape(db, current) {
  const problems = [];
  for (const m of MIGRATIONS) {
    if (m.version > current || !m.expect) continue;
    const p = checkExpectation(db, m.expect);
    if (p) problems.push(`v${m.version}: ${p}`);
  }
  return problems;
}

function schemaMismatchMessage(db, current, problems) {
  const marker = readSchemaLineage(db);
  const engagementInSchema = tableExists(db, 'engagement_events');
  return [
    'PIB SCHEMA MISMATCH — refusing to run migrations (fail-closed; act:b7d81b52).',
    `This database's version stamp (user_version ${current}) contradicts the schema shape on disk,`,
    'so the stamp cannot be trusted: migrations keyed on it would be skipped or misapplied, and',
    'the database would then be stamped fully migrated anyway. That silent path is what killed',
    'two consumers during the v0.56.0 walk; this refusal replaces it.',
    '',
    `  library lineage:    ${SCHEMA_LINEAGE} (head v${SCHEMA_VERSION})`,
    `  db lineage marker:  ${marker ? `${marker.lineage} (stamped v${marker.stamped_version} at ${marker.stamped_at})` : 'absent (stamp predates lineage recording)'}`,
    `  engagement_events:  ${engagementInSchema ? 'present in schema' : 'not in schema'}`,
    '  contradictions:',
    ...problems.map((p) => `    - ${p}`),
    '',
    engagementInSchema && SCHEMA_LINEAGE === 'base'
      ? 'This database was numbered on the ENGAGEMENT overlay lineage (its versions run two ahead of base), but the installed library is the BASE one — the module list in .ccrc.json is not the tell, the schema is. Fix: reinstall with the engagement module enabled so the overlay library, which understands this numbering, runs the migration. If engagement was removed deliberately, repair the schema by hand first (act:b7d81b52 holds the verified recipe).'
      : 'Fix: restore the pre-upgrade snapshot if one exists, or repair the schema by hand so the shape matches the stamp (act:b7d81b52 holds the verified recipe), then re-run.',
  ].join('\n');
}

// Full-shape check against THIS library's head — the post-install self-check
// (`pib-db.mjs verify-schema`). Read-only; never migrates.
export function verifySchema(db) {
  const version = db.pragma('user_version', { simple: true });
  const problems = [];
  for (const m of MIGRATIONS) {
    if (!m.expect) continue;
    const p = checkExpectation(db, m.expect);
    if (p) problems.push(`v${m.version}: ${p}`);
  }
  if (version < SCHEMA_VERSION) {
    problems.push(`user_version ${version} is behind this library's head v${SCHEMA_VERSION} — migrate() has not completed`);
  }
  return {
    ok: problems.length === 0,
    version,
    head: SCHEMA_VERSION,
    lineage: SCHEMA_LINEAGE,
    marker: readSchemaLineage(db),
    problems,
  };
}

export function migrate(db) {
  const current = db.pragma('user_version', { simple: true });

  // FAIL-CLOSED GUARD (act:b7d81b52) — runs BEFORE the early return, so a
  // stamp at or past head cannot silently no-op over a broken shape. A fresh
  // or pre-pragma database (current 0) has claimed nothing, so there is
  // nothing to verify.
  if (current > 0) {
    const marker = readSchemaLineage(db);
    // Which lineages' stamps may this library renumber and advance? The
    // overlay may adopt a base stamp (that is the legitimate add-engagement
    // path — its higher-numbered entries are a superset, guarded by
    // duplicate-swallow). The base library must NEVER renumber an
    // engagement stamp: the integer means something else there.
    const adoptable = SCHEMA_LINEAGE === 'engagement' ? ['base', 'engagement'] : ['base'];
    if (marker && current < SCHEMA_VERSION && !adoptable.includes(marker.lineage)) {
      throw new Error(schemaMismatchMessage(db, current, [
        `user_version ${current} was stamped by the '${marker.lineage}' lineage, which this '${SCHEMA_LINEAGE}' library cannot renumber`,
      ]));
    }
    const problems = verifyStampedShape(db, current);
    if (problems.length) throw new Error(schemaMismatchMessage(db, current, problems));
  }

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
    // Verify the whole declared shape BEFORE stamping — a skipOn that
    // wrongly swallowed a real failure must roll the stamp back with it,
    // never certify a shape that is not there.
    const post = verifyStampedShape(db, SCHEMA_VERSION);
    if (post.length) throw new Error(schemaMismatchMessage(db, current, post));
    // Stamp the lineage beside the version, atomically: a bare integer is
    // never again the only signal of which numbering wrote it.
    db.exec(SCHEMA_LINEAGE_CREATE);
    db.prepare(`INSERT INTO schema_lineage (id, lineage, stamped_version, stamped_at) VALUES (1, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET lineage = excluded.lineage, stamped_version = excluded.stamped_version, stamped_at = excluded.stamped_at`)
      .run(SCHEMA_LINEAGE, SCHEMA_VERSION, today());
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
/**
 * Run one read-only SQL statement. Returns `{ rows }` or `{ error: { message } }`
 * — never throws, and never writes.
 *
 * TWO CONTRACT NOTES, because this is a library shipped into every consuming
 * project and both are invisible from the call site:
 *
 * 1. **Reads see COMMITTED data only.** A file-backed database is read through a
 *    separate connection (below), so a caller that wrapped `query()` inside its
 *    own `db.transaction()` would read the PRE-transaction snapshot — in WAL,
 *    silently: stale rows, not an error. No caller in this repo does that (the
 *    pib_query MCP tool, the CLI `query` subcommand and audit-coherence-check
 *    all run at top level, and the gate structurally refuses BEGIN), but a
 *    consumer script could.
 *
 * 2. **Connection-state PRAGMA READS describe the scratch connection, not
 *    yours.** `PRAGMA foreign_keys` through pib_query reports the throwaway
 *    connection's value, so it answers 1 regardless of the long-lived caller
 *    connection — a false all-clear for precisely the condition act:afc7fd08
 *    exists to prevent. This is the one thing the fix made WORSE, so it is named
 *    here rather than discovered later: to audit a live connection's pragma
 *    state, read it on that connection, not through pib_query. File-level
 *    pragmas (user_version, table_info, database_list) are unaffected. Pinned by
 *    a test so it stays a known cost rather than a surprise.
 */
export function query(db, { sql }) {
  // WHERE the statement is prepared is itself a control (act:afc7fd08). SQLite
  // applies connection-level PRAGMAs during sqlite3_prepare(), so ANY gate that
  // runs after prepare can only refuse a statement that has ALREADY changed the
  // connection. `PRAGMA foreign_keys = OFF` was refused here and took effect
  // anyway — and the MCP server memoizes ONE connection for the life of the
  // process, so that flip outlived the refused call and left action_dependencies'
  // foreign keys unenforced for every later write. A flag check cannot fix an
  // ORDERING defect; only preparing somewhere else can.
  //
  // So a file-backed database is read through a THROWAWAY connection. Be precise
  // about which property does the work, because the two are separable:
  //
  //   SEPARATENESS closes act:afc7fd08. The prepare-time PRAGMA lands on a handle
  //     closed a few lines later instead of on the caller's long-lived one.
  //   READ-ONLY-NESS is defense in depth. SQLite refuses the write in its own
  //     engine, so a future regression of the flag gate is not a bypass. The gate
  //     below always fires FIRST, so nothing in normal operation ever reaches
  //     this layer — which is exactly why openScratchReadConnection is exported
  //     and separately tested. An untested second layer is a decorative one.
  //
  // Separateness also closes a hole the flag gate lets through and never claimed:
  // `PRAGMA locking_mode = EXCLUSIVE` is reader=true/readonly=true, so the gate
  // ALLOWS it. On the caller's memoized connection it would take an exclusive
  // file lock for the life of the process, shutting out the CLI, the work-tracker
  // server and every other client. On the scratch connection it dies with the
  // handle. Stated so it is not "simplified" away as an unclaimed win.
  //
  // Measured: prepare SUCCEEDS on a read-only connection and reports identical
  // reader/readonly flags, so callers keep one consistent, teaching error message
  // on both paths and the engine never becomes a second error surface.
  //
  // Isolation note: the scratch connection sees COMMITTED data only. Every caller
  // of query() (the pib_query MCP tool, the CLI `query` subcommand,
  // audit-coherence-check) runs at top level, never inside a transaction.
  let scratch = null;
  try {
    scratch = openScratchReadConnection(db);
  } catch (e) {
    // A file-backed database we SHOULD have reopened and could not. Refuse
    // rather than fall back: falling back would silently re-arm the very leak
    // this connection exists to close, and a silent re-arm is indistinguishable
    // from a working control. The caller's own handle is already open on this
    // file, so failing here is genuinely abnormal, not routine.
    return {
      error: {
        message: `pib_query could not open its read-only connection, so it refused rather than read through the caller's connection (which would silently reopen the connection-PRAGMA hole act:afc7fd08 closed): ${e.message}`,
      },
    };
  }
  try {
    return runReadOnlyStatement(scratch || db, sql);
  } finally {
    if (scratch) {
      try {
        scratch.close();
      } catch {
        /* the scratch handle is disposable — a failed close must not mask a result */
      }
    }
  }
}

/**
 * Open the throwaway read-only connection that query() reads a file-backed
 * database through (act:afc7fd08). Returns null when there is nothing to reopen
 * or when opening fails; query() then falls back to the caller's handle and the
 * flag gate — never to executing.
 *
 * `new db.constructor(...)` rather than importing better-sqlite3: this library
 * has a deliberate NO-NATIVE-MODULE contract — it accepts a handle and never
 * opens one (see templates/CLAUDE.md). Borrowing the class off the handle we
 * were already given keeps that contract intact.
 *
 * Returns null for NOTHING-TO-REOPEN; THROWS when a reopen was warranted and
 * failed. query() turns the throw into a refusal — the two cases must not share
 * an exit, or an open failure silently degrades to the pre-fix behavior.
 *
 * Nothing-to-reopen is three shapes:
 *
 *   already read-only — the caller's handle cannot leak a PRAGMA anyway.
 *   in-memory — cannot be reopened at all: a second ':memory:' connection is a
 *     different, empty database, so the prepare-time PRAGMA leak genuinely
 *     REMAINS for in-memory handles. Safe in practice, but note the honest
 *     scope: `resolvePibDbPath` returns an explicit PIB_DB_PATH VERBATIM and
 *     never shape-checks it, so "always a file path" is caller discipline (every
 *     setter in this repo passes a real path), NOT a guarantee the resolver
 *     enforces. Stating the stronger version is the mistake act:c4e5324f
 *     already paid for once.
 *   relative path — `db.name` is a re-resolution KEY, not the caller's open
 *     handle. A relative name resolves against process.cwd() AT CALL TIME, so a
 *     chdir between open and query would silently read a DIFFERENT database and
 *     return its rows with no error. Reading the right database with the leak
 *     beats reading the wrong one cleanly, so this falls back rather than throws.
 *
 * Two properties are verified rather than assumed. The read-only-ness is
 * post-checked because `new db.constructor(...)` borrows a class off the handle
 * and a subclass with a different signature could drop the options object,
 * yielding a separate-but-WRITABLE connection — the hole would stay closed
 * (separateness does that) while the documented second layer silently vanished.
 *
 * Exported for testability, not for callers: the flag gate always refuses before
 * execution, so nothing else can observe whether this connection is read-only.
 */
export function openScratchReadConnection(db) {
  if (!db || db.readonly || db.memory || !db.name) return null;
  if (!isAbsolute(db.name)) return null;
  const scratch = new db.constructor(db.name, { readonly: true });
  try {
    scratch.pragma('busy_timeout = 5000');
    if (!scratch.readonly) {
      throw new Error(
        'the scratch connection opened WRITABLE — db.constructor did not honour { readonly: true }'
      );
    }
  } catch (e) {
    try {
      scratch.close();
    } catch {
      /* already failing; the close is best-effort */
    }
    throw e;
  }
  return scratch;
}

function runReadOnlyStatement(db, sql) {
  let stmt;
  try {
    stmt = db.prepare(sql);
  } catch (e) {
    return { error: { message: e.message } };
  }
  // SECURITY (security-0002, act:fd2fc643; corrected act:c4e5324f). pib_query is
  // READ-ONLY, and it takes BOTH better-sqlite3 statement flags to say so. Each
  // half refuses what the other lets through — neither is a belt on the other's
  // braces, so do NOT "simplify" this to a single clause:
  //
  //   stmt.readonly — sqlite3_stmt_readonly(): "can this statement change the
  //     database?" The ONLY half that refuses INSERT/UPDATE/DELETE … RETURNING.
  //   stmt.reader   — COLUMN-COUNT based ("does this return rows?"), NOT a write
  //     gate. The ONLY half that refuses BEGIN/COMMIT/ROLLBACK/ATTACH/DETACH and
  //     the connection-level PRAGMAs — all of which are readonly=TRUE.
  //
  // MEASURED AGAINST THIS SCHEMA, 2026-08-09, better-sqlite3 12.8.0 / SQLite
  // 3.51.3, and pinned by a table-driven test in test/pib-db-query-readonly/ so
  // it cannot rot back into fiction. The version of this comment that stood until
  // act:c4e5324f claimed stmt.reader was "false for INSERT/UPDATE/DELETE/DDL" —
  // false for the RETURNING forms, which passed the guard, executed, and persisted.
  //
  //   ALLOWED — both flags true
  //     SELECT / WITH…SELECT / WITH RECURSIVE…SELECT      reader=true  readonly=true
  //     PRAGMA user_version | table_info (read forms)     reader=true  readonly=true
  //     EXPLAIN [QUERY PLAN] SELECT …                     reader=true  readonly=true
  //   REFUSED by the readonly half alone — reader is TRUE (the act:c4e5324f class)
  //     INSERT/UPDATE/DELETE … RETURNING, incl. CTE-wrapped  reader=true readonly=false
  //     PRAGMA journal_mode [= WAL] · PRAGMA optimize        reader=true readonly=false
  //     EXPLAIN <a write statement>                          reader=true readonly=false
  //   REFUSED by both halves
  //     INSERT / UPDATE / DELETE / DDL                    reader=false readonly=false
  //     PRAGMA user_version = N  (file-writing PRAGMA)    reader=false readonly=false
  //   REFUSED by the reader half alone — readonly is TRUE (why that half stays)
  //     PRAGMA foreign_keys = OFF  (connection-level)     reader=false readonly=true
  //     BEGIN / COMMIT / ROLLBACK / ATTACH / DETACH       reader=false readonly=true
  //
  // "write-PRAGMA" is three different shapes, which is why the table splits them:
  // connection-level pragmas are readonly=TRUE, file-writing ones readonly=false,
  // and journal_mode is a reader. A gate written as `!stmt.readonly` alone would
  // pass `PRAGMA foreign_keys = OFF` straight through to stmt.all().
  //
  // Three READS are refused as a deliberate, accepted cost. No caller in this repo
  // sends any of them, and each is locked by a test so it reads as CHOSEN:
  //   • `PRAGMA journal_mode` / `wal_checkpoint` in their bare read forms.
  //   • `EXPLAIN <a write statement>` — SQLite documents sqlite3_stmt_readonly()
  //     on an EXPLAIN as returning what it would with the keyword omitted. EXPLAIN
  //     never runs the statement, so allowing it would be safe; refusing it beats
  //     reintroducing SQL-prefix string sniffing.
  //   • `PRAGMA optimize` — and this one is not merely conservative. Its flags are
  //     SCHEMA-DEPENDENT: readonly=true on a toy index-less fixture, readonly=FALSE
  //     against this schema whether empty or populated, and it genuinely writes
  //     sqlite_stat1 once analysis is due. Measure it against the REAL schema or
  //     you will measure the wrong answer — that mistake is how the row above it
  //     was wrong in the first place.
  //
  // "Refused without executing" holds for DML/DDL/RETURNING, and — since
  // act:afc7fd08 routed file-backed reads through the scratch read-only
  // connection above — for connection-level PRAGMAs too, whose prepare-time
  // effect now lands on a handle that is immediately closed. At least 15 pragmas
  // take effect during sqlite3_prepare() (measured); the ones that matter are
  // foreign_keys, defer_foreign_keys, ignore_check_constraints,
  // recursive_triggers, query_only and trusted_schema. `PRAGMA query_only`
  // bracketing was measured and does NOT close this — it stops the write but not
  // the prepare-time flip, which is why the separate connection is the fix.
  // The ONE residue is an in-memory handle, which cannot be reopened; see the
  // header above. Both paths are pinned by tests.
  if (!stmt.reader || !stmt.readonly) {
    return {
      error: {
        message:
          'pib_query is read-only — this statement can modify the database (or SQLite reports it as able to), so it was refused. Note that INSERT/UPDATE/DELETE ... RETURNING return rows but are still writes. To change data use pib_update_action / pib_complete_action / pib_create_action (they enforce the surface-area and completion gates).',
      },
    };
  }
  // stmt.all() sits inside the try because a statement that PREPARES cleanly can
  // still fail at execution (malformed JSON literal, integer overflow, a blocked
  // load_extension). Those used to escape query() as an uncaught throw — a stack
  // trace out of the CLI — instead of this function's documented {error} shape.
  try {
    return { rows: stmt.all() };
  } catch (e) {
    return { error: { message: e.message } };
  }
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

// Titles that encode their position in a hierarchy are the measured tell that
// several actions are really one PROJECT (plan §4/§6: 37 open actions carried
// in-title hierarchy; no one ever improvised a parent project). The write
// boundary is the best interception point in the system — the caller still
// holds the context needed to fix it. Shared by createAction below and the
// action-quality-gate hook (the hook is the enforcement twin; this is the
// structural one that no matcher drift can silence).
export const HIERARCHY_TITLE_PATTERN = /\b(?:Phase|Lane|Child)\s+(?:[0-9]+|[A-Z][0-9]?)\b|TRACKER:|COORDINATOR|SEQUENCE/;

export function createAction(db, { text, projectFid, due, notes }) {
  const validationError = validateSurfaceArea(notes);
  if (validationError) {
    return { error: validationError };
  }

  // The enter-invariant, at its first writer (§16.3): an open action may not
  // ENTER a done container — a fresh child filed under a just-walked project
  // the next morning silently re-mints done-with-open-children (§4's
  // pathology). This guard also fixes the pre-existing silent acceptance of
  // soft-deleted parents (only the FK caught a missing one, with a raw
  // driver error rather than a teaching refusal).
  if (projectFid) {
    const parent = db.prepare('SELECT fid, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(projectFid);
    if (!parent) {
      return { error: { message: `Cannot create under ${projectFid}: that container does not exist (or is deleted). Pass an existing container fid, or omit projectFid.` } };
    }
    if (parent.status === 'done') {
      return { error: { message: `Cannot create under ${projectFid}: that container is done, and an open action may not enter a done container. If this work belongs to that outcome, reopen the container first (pib_update_project ${projectFid} status='active'); otherwise parent it where the work now belongs.` } };
    }
  }

  const fid = generateFid('act');
  db.prepare(`
    INSERT INTO actions (fid, text, project_fid, due, notes, created)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(fid, text, projectFid || null, due || null, notes || '', today());
  const result = { fid, text, message: `Created action ${fid}: ${text}` };
  // Teach-after-the-call (§6 lever 3): the write succeeded, but a
  // hierarchy-in-title pattern means this action is probably one step of a
  // larger outcome that deserves to be a real project.
  if (HIERARCHY_TITLE_PATTERN.test(text)) {
    result.suggestion = `This title encodes its position in a hierarchy. If several actions are steps toward one outcome, create a project for that outcome (pib_create_project with kind='project'), parent them to it via projectFid, and drop the position marker from each title — the project, not the title, carries the structure.`;
  }
  return result;
}

export function listActions(db, { status, project, scope } = {}) {
  const scopeError = validateScope(scope);
  if (scopeError) return scopeError;
  const conditions = ['a.deleted_at IS NULL'];
  const params = [];

  if (status) {
    conditions.push('a.status = ?');
    params.push(status);
  } else {
    conditions.push('a.completed = 0');
  }
  if (project) {
    const clause = containerScopeClause(project, scope);
    conditions.push(clause.sql);
    params.push(...clause.params);
  }

  const rows = db.prepare(`
    SELECT a.fid, a.text, a.due, a.flagged, a.status, a.completed, a.resolution, a.project_fid, p.name as project, p.kind as project_kind, a.tags
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
  // ONE closure path, enforced by construction (act:4e0de399). This function
  // used to treat status='done' as a full close — completed=1 + completed_at,
  // identical to completeAction — while the PreToolUse completion gate matches
  // only `mcp__pib-db__pib_complete_action`. So closing through here skipped
  // the verification-breadcrumb check entirely, and the guarantee "no action
  // closes without verified acceptance criteria" was false from the day the
  // gates were wired. (watchtower-contracts.md had already named "the ungated
  // pib_update_action --status done path" in passing; nobody had filed it.)
  //
  // Gating this path instead of refusing it would leave TWO closure paths and
  // re-open the hole the moment a matcher drifts — the same failure as
  // act:ff693d4c, where the gates' matchers silently stopped matching. Refusing
  // is structural: the wrong thing becomes impossible rather than detected.
  // Verified before landing that no shipped skill, script, or ring closes an
  // action this way, so nothing legitimate is broken. Reopen (done -> any
  // active status) is unaffected and still clears the completed columns.
  if (status === 'done') {
    return { error: { message: `Cannot close ${fid} via update-action: status='done' is refused here so that every closure passes the completion gate. Use pib_complete_action (CLI: complete-action ${fid}), which requires a verification breadcrumb proving the acceptance criteria were checked.` } };
  }

  // The current row gates two of the writes below: the reparent guard needs
  // the action's own openness (the enter-invariant is about OPEN actions),
  // and the reopen teaching needs to know whether this write is a genuine
  // reopen under a done parent.
  const row = db.prepare('SELECT fid, completed, status, project_fid FROM actions WHERE fid = ? AND deleted_at IS NULL').get(fid);
  if (!row) {
    return { error: { message: `Action ${fid} not found (no row updated)` } };
  }

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
    const proj = db.prepare('SELECT fid, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(projectFid);
    if (!proj) {
      return { error: { message: `Cannot reparent ${fid}: project ${projectFid} does not exist (or is deleted). Pass an existing project fid.` } };
    }
    // The enter-invariant, at its second writer (§16.3): an open action may
    // not ENTER a done container. Openness is judged as what this same call
    // leaves behind — any status write reopens (status='done' is refused
    // above), so only an untouched completed row may move into a done
    // container.
    const openAfterThisCall = status !== undefined || row.completed === 0;
    if (openAfterThisCall && proj.status === 'done') {
      return { error: { message: `Cannot reparent ${fid} into ${projectFid}: that container is done, and an open action may not enter a done container (it would re-mint done-with-open-children). Reopen the container first (pib_update_project ${projectFid} status='active'), or choose a live container.` } };
    }
    sets.push('project_fid = ?'); params.push(projectFid);
  }
  if (client_title !== undefined) { sets.push('client_title = ?'); params.push(client_title || null); }
  if (client_body !== undefined) { sets.push('client_body = ?'); params.push(client_body || null); }
  if (client_generated_at !== undefined) { sets.push('client_generated_at = ?'); params.push(client_generated_at || null); }
  if (client_generated_status !== undefined) { sets.push('client_generated_status = ?'); params.push(client_generated_status || null); }

  // Reopening a done action to any non-done status must CLEAR the completed
  // flag/timestamp — or the row is left half-completed (completed=1 with an
  // active status) — AND the typed resolution (§16.1): 'parent-outcome' or
  // 'verified' on an OPEN row would violate the column's own invariant. This
  // is one of THREE reopen-clearing sites; the tracker's action-edit branch
  // and the base tier's copies are the others, and each is its own AC
  // because the drift guard compares tool schemas, not clauses.
  // Closing is NOT done here — see the refusal above.
  if (status !== undefined) {
    sets.push('completed = 0', 'completed_at = NULL', 'resolution = NULL');
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

  const out = { fid, message: `Updated ${fid}` };
  // Child reopen under a done project gets a VOICE, never a refusal (§16.4):
  // reopening is an operator correction and must not be blocked, but it
  // re-mints done-project-with-open-children in place — and the container
  // surfaces filter to active, so the project vanishes while holding live
  // work. (A reparent in the same call was already validated against a live
  // target above, so the teaching keys on the parent the row keeps.)
  if (status !== undefined && row.completed === 1 && projectFid === undefined && row.project_fid) {
    const parent = db.prepare('SELECT fid, name, kind, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(row.project_fid);
    if (parent && parent.kind === 'project' && parent.status === 'done') {
      out.note = `Parent project ${parent.fid} (${parent.name}) is done — this reopen leaves live work under a completed project. Reparent this action to where the work now belongs (projectFid), or reopen the project (pib_update_project ${parent.fid} status='active').`;
    }
  }
  return out;
}

export function completeAction(db, { fid }) {
  const fidError = validateFid(fid);
  if (fidError) return fidError;

  const row = db.prepare(
    `SELECT fid, text, completed FROM actions WHERE fid = ? AND deleted_at IS NULL`
  ).get(fid);
  if (!row) return { error: 'not_found', message: `No action with fid ${fid}` };
  if (row.completed === 1) return { error: 'already_done', message: `${fid} already completed` };

  // resolution='verified' is the typed completion claim (§16.1): this closure
  // came through completeAction, whose every route is gated — the MCP tool's
  // PreToolUse breadcrumb hook, the tracker's delegation (act:cbf19c3f), and
  // the CLI's Bash-guard arm (act:f9173885, the fifth door, closed in the
  // same change that added this stamp — the token overclaimed while it stood).
  // The walk's typed closure is the ONLY other writer and structurally cannot
  // stamp this value.
  const result = db.prepare(`
    UPDATE actions SET completed = 1, completed_at = ?, status = 'done', resolution = 'verified' WHERE fid = ?
  `).run(new Date().toISOString(), fid);

  if (result.changes === 0) {
    return { error: 'update_failed', message: `UPDATE matched no rows for ${fid}` };
  }
  return { fid, text: row.text, message: `Completed ${fid}: ${row.text}` };
}

// The typed closure — the ONE other writer of the completed columns beside
// completeAction (§16.4). It lives HERE, in the library next to its sibling,
// never inline in the walk's verb handler: a second writer composed inside a
// handler is the exact shape act:4e0de399 and act:cbf19c3f each spent a build
// retiring. The resolution value is a SQL LITERAL, not a parameter — this
// function structurally CANNOT stamp 'verified'. That claim belongs to
// completeAction alone, because it is the claim the breadcrumb gate proves
// and a parent-outcome closure deliberately does not make (§10.2: the record
// never says acceptance criteria were verified when they were not).
//
// `completed = 0` in the WHERE arms the walk's skip-if-already-dispositioned
// rule a second time: a child completed by other hands mid-walk is never
// re-stamped.
function closeChildByParentOutcome(db, fid, walkedAt) {
  return db.prepare(`
    UPDATE actions
    SET completed = 1, completed_at = ?, status = 'done', resolution = 'parent-outcome'
    WHERE fid = ? AND deleted_at IS NULL AND completed = 0
  `).run(walkedAt, fid);
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
// Containers come in two kinds with opposite completion semantics (plan §7,
// §10.3). The vocabulary is closed here, at the one write boundary, so a typo
// can never mint a third kind.
export const CONTAINER_KINDS = ['area', 'project'];

function validateKind(kind) {
  if (!CONTAINER_KINDS.includes(kind)) {
    return { error: { message: `kind must be 'area' or 'project', got "${kind}". An area is a standing responsibility that never completes; a project is an outcome that completes.` } };
  }
  return null;
}

// A project's notes ARE its acceptance criteria — the outcome statement is the
// completion test (§10.3). ONE predicate, called by BOTH container writers.
//
// act:f9baa1c0: createProject enforced this from the start and updateProject's
// reclassify arm did not, so the gate stood on the path nobody takes. Every
// container on a migrated tracker is an area (the migration structurally cannot
// promote, §7.2 makes per-container operator judgment the only route), so
// reclassification is how EVERY project comes to exist — and an outcome-less
// project then completes instantly, because "every child closed" is trivially
// true at zero children. That is a completion with no test, which is the exact
// state the requirement exists to prevent.
//
// `notes` is the EFFECTIVE notes for the call: what it writes if it writes any,
// else what the row already carries. A reclassify may satisfy the gate with
// notes the container already had.
function validateProjectOutcome(kind, notes, { reclassifying = false } = {}) {
  if (kind !== 'project') return null;
  if (notes && notes.trim()) return null;
  const tail = reclassifying
    ? `Supply the outcome in the same call (notes: "done when …"), or leave it an area.`
    : `For a standing responsibility with no finish line, create an area instead (kind='area', the default).`;
  return { error: { message: `A project is an outcome that completes — its notes must state that outcome ("done when …"), because the outcome statement is the test its completion is judged against. ${tail}` } };
}

// The open-set predicate that gates a project's completion (§10.3) AND
// defines the propagation walk's open set (§16.3) — ONE definition, reused,
// never re-derived: if the walk's set and the gate's count could drift apart,
// a walked project could still refuse to close (or close over an unwalked
// child). Deliberately counts deferred/blocked/in-progress and
// trigger-carrying children as OPEN — an undone child is undone no matter
// what its status says, and no status is exempt from answering.
const OPEN_CHILD_PREDICATE_SQL = 'a.project_fid = ? AND a.completed = 0 AND a.deleted_at IS NULL';

// DELIBERATELY NOT parent-scoped, and this is load-bearing (§17.3). Nesting
// gives "in this container" two readings: membership for READING is rolled up
// (an area's lists and figures include its projects' actions — see
// containerScopeSql), and membership for COMPLETION is direct and stays
// direct. Scoping this predicate would make the walk recursive and give an
// area an open-children count it must never have, and it would break §16.3's
// guarantee that the gate's count and the walk's open set are ONE definition.
// A live CHILD CONTAINER is a separate concern with its own guard below —
// never a row in this count.
function openChildCount(db, fid) {
  return db.prepare(
    `SELECT COUNT(*) AS n FROM actions a WHERE ${OPEN_CHILD_PREDICATE_SQL}`
  ).get(fid).n;
}

// ---------------------------------------------------------------------------
// Containment — an AREA contains PROJECTs (v11; §17)
// ---------------------------------------------------------------------------
// The schema carries two of the three invariants: an area may not CARRY a
// parent (row-level CHECK, structural, fires in both directions), and the
// parent must EXIST (foreign key, pragma-gated). The third — **the parent
// must be an AREA** — is not expressible in a row-level CHECK because it
// reads another row, and it is the one that carries BOTH the two-level
// guarantee and acyclicity. Measured on SQLite 3.51.3: a project→project
// parent, and a self-parent, pass the CHECK and the FK alike. So it lives
// here, and both container writers run their parent/kind read-and-write in
// ONE immediate transaction — three processes hold concurrent write handles
// on this file, and two racing calls could otherwise each pass their pre-read
// and mint a third level with no error anywhere (§12.7's addDependency race,
// same shape, same fix).
//
// Every refusal returns this library's {error} shape BEFORE any SQL runs.
// The constraints are the backstop, not the teacher: a CHECK violation
// escapes as a thrown SqliteError, which the MCP server renders as a bare
// constraint string and the work-tracker server turns into a deliberately
// generic 500 — so an operator using the tracker's kind dropdown would get
// no information at all.

// Child CONTAINERS of a container. `liveOnly` excludes done ones, which is
// what every gate wants: a done child project is settled and constrains
// nothing. Soft-deleted children are always excluded — they are tombstones.
function childProjects(db, fid, { liveOnly = true } = {}) {
  return db.prepare(`
    SELECT fid, name, status FROM projects
    WHERE parent_fid = ? AND deleted_at IS NULL
      ${liveOnly ? "AND status != 'done'" : ''}
    ORDER BY name
  `).all(fid);
}

// Resolve and validate a proposed parent. Mirrors updateAction's projectFid
// guard and addDependency's resolveEndpoint (§12.5) — same refusals, same
// shape, so a reader who knows one knows all three.
function resolveParentTarget(db, parentFid, childFid) {
  const fidError = validateFid(parentFid);
  if (fidError) return { error: fidError };
  if (!parentFid.startsWith('prj:')) {
    return { error: { message: `A parent must be a container fid (prj:...), got ${parentFid}. Actions are parented with projectFid on the action, not here.` } };
  }
  // Self-parent gets its own message. It is NOT covered by the kind rule —
  // measured: the row satisfies the CHECK (it IS a project) and satisfies the
  // FK (the target DOES exist), exactly like the dependency self-edge.
  if (parentFid === childFid) {
    return { error: { message: `A container cannot be its own parent (${parentFid}).` } };
  }
  const row = db.prepare('SELECT fid, name, kind, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(parentFid);
  if (!row) {
    return { error: { message: `Parent ${parentFid} not found (or deleted). A project names the AREA it serves; that area must exist.` } };
  }
  if (row.kind !== 'area') {
    return { error: { message: `Cannot parent under ${parentFid} (${row.name}): it is a project, and a project cannot contain another project. Containers nest exactly one level — an area holds projects, a project holds actions. Parent this under an AREA, or leave it a root.` } };
  }
  // §16.3's enter-invariant, one level up: an open action may not ENTER a done
  // container, and neither may a live project. An area cannot normally reach
  // 'done' (the kind gate refuses it), but legacy done-areas exist in real
  // trackers — 40 on this repo's own when the kind column shipped.
  if (row.status === 'done') {
    return { error: { message: `Cannot parent under ${parentFid} (${row.name}): that area is marked done, and live work may not enter a done container. Reopen it first (pib_update_project ${parentFid} status='active'), or parent this elsewhere.` } };
  }
  return { row };
}

// Container statuses that hide a container from the active surfaces. Parking
// an area that still holds live projects makes their parent vanish from every
// container list while they hold live work — §4's pathology with the parent
// hidden rather than closed. `done` is handled by the completion gates.
const PARKED_CONTAINER_STATUSES = ['paused', 'someday', 'dropped'];

// Nesting gives "in this container" two readings, and they are NOT
// interchangeable (§17.3, §17.5). 'direct' means what the container filter has
// always meant — actions whose project_fid is exactly this container — and is
// the DEFAULT, so no existing caller's meaning changes. 'rolled-up' adds the
// actions of any project whose parent is this container, which is what
// "everything under Live Ops" means once areas hold projects.
//
// The rejected alternative, recorded: silently widening the filter so an area
// always rolled up. It changes the meaning of every existing call site at
// once, on the surface (`listStartable`) that is the payoff of the whole
// data-model rebuild. An explicit parameter costs one argument at the three
// call sites that want it and leaves every other caller provably untouched.
// (The design draft also rejected an explicit parameter by citing sort_order
// — a category error: sort_order rotted because it was a STORED COLUMN nobody
// wrote, and a call argument stores nothing and cannot rot.)
export const CONTAINER_SCOPES = ['direct', 'rolled-up'];

function containerScopeClause(fid, scope) {
  if (scope === 'rolled-up') {
    return {
      sql: '(a.project_fid = ? OR a.project_fid IN (SELECT c.fid FROM projects c WHERE c.parent_fid = ? AND c.deleted_at IS NULL))',
      params: [fid, fid],
    };
  }
  return { sql: 'a.project_fid = ?', params: [fid] };
}

function validateScope(scope) {
  if (scope === undefined || CONTAINER_SCOPES.includes(scope)) return null;
  return { error: { message: `scope must be one of ${CONTAINER_SCOPES.join(', ')}, got "${scope}". 'direct' counts only actions filed straight to this container; 'rolled-up' also counts the actions of projects inside it.` } };
}

// A rolled-up result must SAY it rolled up, or it is indistinguishable from a
// bug — §6's rule that a derived answer nothing displays is sort_order with
// extra steps. Computed from the rows already returned, never a second query.
//
// Two ways that sentence used to state a false quantity (act:f07b0f37), and it
// is the ONE sentence whose whole job is to be trusted as evidence:
//
//   - With no container named, it still said "under this container". There was
//     no container; `project_fid === undefined` matched only ORPHAN actions and
//     counted them as "filed directly", while every real container in the
//     result became a "project inside it". Measured: childProjects 4 on a
//     database holding 3 containers.
//   - The rows handed in are already LIMIT-capped, so the counts described the
//     PAGE while claiming to describe the container. Measured on one area: "5
//     under this container — 2 direct, 3 in 2 project(s)" unlimited, versus "2
//     ... 0 in 0 project(s)" at limit 2.
//
// Fixed by refusing to speak in the first case and naming the cap in the
// second. Still no second query: a capped view says it is capped rather than
// pretending its numbers are the whole.
function rollupSummary(rows, fid, scope, { capped = false } = {}) {
  if (scope !== 'rolled-up') return null;
  // A rollup is a claim ABOUT A CONTAINER. With none named there is nothing for
  // work to be "inside", so there is no honest sentence to write.
  if (!fid) return null;
  const direct = rows.filter((r) => r.project_fid === fid).length;
  const viaProjects = rows.length - direct;
  const containers = new Set(rows.filter((r) => r.project_fid !== fid).map((r) => r.project_fid));
  const head = capped
    ? `${rows.length} shown under this container (capped — more may exist)`
    : `${rows.length} under this container`;
  return {
    direct,
    viaProjects,
    childProjects: containers.size,
    capped,
    message: `${head} — ${direct} filed directly, ${viaProjects} in ${containers.size} project(s) inside it.`,
  };
}

// Thrown inside a container writer's transaction to abort it, then converted
// back to this library's documented {error} shape by the caller. Same device
// as the walk's WalkRefusal, and for the same reason: better-sqlite3 rolls
// back only on THROW, so a gate that merely RETURNED an error inside the
// transaction would let the write commit and then hand back the refusal
// (§16.4's flagship inversion). Anything else that throws mid-write
// propagates — a genuine defect must never be dressed up as a refusal.
class ContainmentRefusal extends Error {}

export function createProject(db, { name, kind, notes, due, tags, parentFid }) {
  const k = kind === undefined ? 'area' : kind;
  const kindError = validateKind(k);
  if (kindError) return kindError;
  // Containment (§17). An area is always a root; only a project may name the
  // area it serves. Refused here rather than left to the CHECK so the caller
  // gets the reason instead of a constraint string.
  if (parentFid !== undefined && parentFid !== null && parentFid !== '') {
    if (k !== 'project') {
      return { error: { message: `Only a project may name a parent — an area is always a root, because an area holds projects rather than sitting inside one. Create this as kind='project' (stating its outcome in notes), or drop parentFid.` } };
    }
  }
  // A project's notes ARE its acceptance criteria (§10.3). It needs no
  // ## Acceptance Criteria scaffold and never a ## Surface Area (a project
  // changes no files itself), but an outcome it must state. An area states
  // nothing: it has no outcome by definition, and demanding one would teach
  // callers to fabricate. Shared with updateProject's reclassify arm.
  const outcomeError = validateProjectOutcome(k, notes);
  if (outcomeError) return outcomeError;
  const fid = generateFid('prj');
  const parent = parentFid || null;

  // The parent check and the INSERT run in ONE immediate transaction. Separate,
  // a concurrent reclassification of the target area to 'project' between the
  // two would mint a project inside a project — and unlike a dangling parent,
  // an illegal third level has no report (§12.7's reasoning, verbatim).
  let parentRow = null;
  const tx = db.transaction(() => {
    if (parent) {
      const resolved = resolveParentTarget(db, parent, fid);
      if (resolved.error) throw new ContainmentRefusal(resolved.error.message);
      parentRow = resolved.row;
    }
    db.prepare(`
      INSERT INTO projects (fid, name, kind, notes, due, tags, created, parent_fid)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(fid, name, k, notes || '', due || null, tags || '', today(), parent);
  });
  try { tx.immediate(); }
  catch (e) {
    if (e instanceof ContainmentRefusal) return { error: { message: e.message } };
    throw e;
  }

  const result = { fid, name, kind: k, parent_fid: parent, message: `Created ${k} ${fid}: ${name}` };
  if (parentRow) result.message += ` (in ${parentRow.name})`;
  return result;
}

export function updateProject(db, { fid, tags, name, status, notes, due, kind, parentFid, client_title, client_body, client_generated_at, client_generated_status }) {
  // A caller may deliberately CLEAR the parent (parentFid: null), which is
  // different from not mentioning it — case 1 below turns on exactly that.
  const parentProvided = parentFid !== undefined;
  const parentTarget = parentProvided ? (parentFid || null) : undefined;

  let outcome;
  // EVERYTHING — the gating reads and the write — runs in ONE immediate
  // transaction (§17.1). Read-then-write with no transaction is what let two
  // concurrent callers each pass their own pre-read and mint a third
  // container level with no error anywhere; `.immediate()` because a deferred
  // transaction upgrading mid-call can die un-retried on a snapshot conflict
  // (§16.4). Refusals THROW so they unwind — a gate that merely returned
  // inside the transaction would let the write commit first (§16.4's
  // flagship inversion).
  const tx = db.transaction(() => {
  // Reads that gate the write need the current row (kind, status). Fetched
  // once, up front; the absent-row answer stays identical to the old path.
  const row = db.prepare('SELECT fid, name, kind, status, notes, parent_fid FROM projects WHERE fid = ? AND deleted_at IS NULL').get(fid);
  if (!row) {
    throw new ContainmentRefusal(`Project ${fid} not found`);
  }

  if (kind !== undefined) {
    const kindError = validateKind(kind);
    if (kindError) throw new ContainmentRefusal(kindError.error.message);
    // The outcome gate, on the arm that actually mints projects (act:f9baa1c0).
    // Effective notes: what this call writes if it writes any, else what the
    // row already carries — a container that already states its outcome does
    // not have to restate it to be promoted.
    const outcomeError = validateProjectOutcome(
      kind, notes !== undefined ? notes : row.notes, { reclassifying: true });
    if (outcomeError) throw new ContainmentRefusal(outcomeError.error.message);
    // Reclassification is the OPERATOR's verb (§7.2) — this function is its
    // one legal write path. Demoting a completed container to 'area' would
    // mint the incoherent state "a done thing that cannot be done"; reopen
    // first, then reclassify.
    const statusAfter = status !== undefined ? status : row.status;
    if (kind === 'area' && statusAfter === 'done') {
      throw new ContainmentRefusal(`Cannot reclassify ${fid} to an area while it is done — an area never completes, so a done area is a contradiction. Reopen it (status='active') first, or keep it a project.`);
    }
    // CONTAINMENT case 1 (§17.2): demoting a PARENTED project to an area.
    // An area is always a root, so the parent must go — but orphaning it
    // silently would destroy a stated fact (which area this served) with no
    // record. Refuse unless the SAME call clears it, which is the shape
    // validateProjectOutcome's reclassify arm already uses. The CHECK would
    // also catch this, with a message that teaches nothing.
    if (kind === 'area' && row.parent_fid && !(parentProvided && parentTarget === null)) {
      throw new ContainmentRefusal(`Cannot reclassify ${fid} to an area while it names a parent — an area is always a root. Clear the parent in the same call (kind='area' with parentFid=null) if this responsibility now stands on its own, or leave it a project inside its area.`);
    }
    // CONTAINMENT case 2 (§17.2): promoting an area that HOLDS projects.
    // Containers nest exactly one level, so this would leave live projects
    // inside a project. Symmetric to the reclassify-while-done gate below:
    // the completion gate fires on a STATUS write, so the gate that fires on
    // a KIND write has to be written separately or the path is ungated.
    // Counts done children too — the rule is about depth, not liveness.
    if (kind === 'project' && row.kind === 'area') {
      const held = childProjects(db, fid, { liveOnly: false });
      if (held.length > 0) {
        const names = held.slice(0, 3).map((c) => `${c.name} (${c.fid})`).join(', ');
        throw new ContainmentRefusal(`Cannot reclassify ${fid} (${row.name}) to a project: ${held.length} project(s) name it as their area — ${names}${held.length > 3 ? ', …' : ''} — and a project cannot contain another project. Move them out first (pib_update_project <child> parentFid=null, or point them at a different area), or leave this an area.`);
      }
    }
    // The symmetric gate (§16.0, checkpoint find): the completion gate below
    // fires only when status='done' is written IN THE CALL, so reclassifying a
    // STORED-done container to 'project' with no status write was ungated —
    // it minted a done project over open children through the one legal path,
    // and the §7.2 reclassification pass routes the operator through exactly
    // this verb (40 legacy done-areas in this repo alone when this shipped).
    if (kind === 'project' && status === undefined && row.status === 'done') {
      const open = openChildCount(db, fid);
      if (open > 0) {
        throw new ContainmentRefusal(`Cannot reclassify ${fid} to a project while it is done with ${open} open child action(s) — that would mint a completed project over open children. Reopen it first (status='active'), reclassify it, and when its outcome really is true, complete it through the gate (pib_complete_project walks the open children).`);
      }
    }
  }

  // CONTAINMENT cases 3 and 4 (§17.2): setting or changing the parent. The
  // kind this call is WRITING decides whether a parent is legal at all, so a
  // reclassify-and-parent call is judged as what it is becoming.
  if (parentProvided && parentTarget !== null) {
    const effectiveKind = kind !== undefined ? kind : row.kind;
    if (effectiveKind !== 'project') {
      throw new ContainmentRefusal(`Only a project may name a parent — an area is always a root, because an area holds projects rather than sitting inside one. Reclassify this to a project first (kind='project', stating its outcome in notes), or drop parentFid.`);
    }
    const resolved = resolveParentTarget(db, parentTarget, fid);
    if (resolved.error) throw new ContainmentRefusal(resolved.error.message);
  }

  // The two completion gates (§10.3). Evaluated against the kind this same
  // call is writing, if it writes one — a reclassify-and-complete call is
  // judged as what it is becoming, not what it was.
  if (status === 'done') {
    const effectiveKind = kind !== undefined ? kind : row.kind;
    if (effectiveKind === 'area') {
      throw new ContainmentRefusal(`Cannot complete ${fid}: it is an area, and an area never completes — it is a standing responsibility, not an outcome ("Is Watchtower done?" has no answer). If this container is really an outcome that finishes, reclassify it first (kind='project', stating the outcome in its notes), then complete it. To stop holding the responsibility, use status='dropped'.`);
    }
    const open = openChildCount(db, fid);
    if (open > 0) {
      throw new ContainmentRefusal(`Cannot complete ${fid}: ${open} child action(s) are still open. A project completes only when every child is closed or explicitly dispositioned. Complete each verified child individually (pib_complete_action), or run the walk: pib_complete_project ${fid} — it shows every open child, takes a per-child answer (done by this outcome / still needed, move it / not needed, delete it), and completes the project through this same gate.`);
    }
    // A live CHILD CONTAINER is not in openChildCount — that predicate counts
    // ACTIONS and deliberately stays direct (§17.3). Without this, a project
    // holding an open child project completes straight over it: measured at
    // the design checkpoint, and it is §4's Pre-Launch Readiness pathology one
    // level up, on the flagship verb. Only reachable if the two-level rule was
    // bypassed (raw SQL, or a future sub-project relaxation) — a report is
    // cheap insurance against a defense that lives in one function.
    const liveChildren = childProjects(db, fid, { liveOnly: true });
    if (liveChildren.length > 0) {
      const names = liveChildren.slice(0, 3).map((c) => `${c.name} (${c.fid})`).join(', ');
      throw new ContainmentRefusal(`Cannot complete ${fid}: ${liveChildren.length} container(s) still sit inside it — ${names}${liveChildren.length > 3 ? ', …' : ''}. Containers are meant to nest only one level, so this should not be reachable; close or move those out, and report how they got there.`);
    }
  }

  // CONTAINMENT case 5 (§17.2): parking an area that still holds live
  // projects. `listProjects` shows only active containers, so this hides the
  // parent from every container surface while its children hold live work —
  // §4's pathology with the parent hidden rather than closed. Note this
  // function's own completion refusal RECOMMENDS status='dropped', so the
  // route into this state is one the system itself suggests.
  if (status !== undefined && PARKED_CONTAINER_STATUSES.includes(status)) {
    const effectiveKind = kind !== undefined ? kind : row.kind;
    if (effectiveKind === 'area') {
      const liveChildren = childProjects(db, fid, { liveOnly: true });
      if (liveChildren.length > 0) {
        const names = liveChildren.slice(0, 3).map((c) => `${c.name} (${c.fid})`).join(', ');
        throw new ContainmentRefusal(`Cannot set ${fid} (${row.name}) to '${status}': ${liveChildren.length} live project(s) name it as their area — ${names}${liveChildren.length > 3 ? ', …' : ''}. Parking the area hides their parent from every container surface while they still hold live work. Finish or park those projects first, or move them to an area that stays active.`);
      }
    }
  }

  const sets = [];
  const params = [];
  if (tags !== undefined) { sets.push('tags = ?'); params.push(tags); }
  if (name !== undefined) { sets.push('name = ?'); params.push(name); }
  if (status !== undefined) { sets.push('status = ?'); params.push(status); }
  if (notes !== undefined) { sets.push('notes = ?'); params.push(notes); }
  if (due !== undefined) { sets.push('due = ?'); params.push(due || null); }
  if (kind !== undefined) { sets.push('kind = ?'); params.push(kind); }
  if (parentProvided) { sets.push('parent_fid = ?'); params.push(parentTarget); }
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
    throw new ContainmentRefusal('No fields to update. Use tags, name, status, notes, due, kind, parentFid, client_title, client_body, client_generated_at, or client_generated_status.');
  }
  params.push(fid);
  const result = db.prepare(`UPDATE projects SET ${sets.join(', ')} WHERE fid = ? AND deleted_at IS NULL`).run(...params);
  if (result.changes === 0) {
    throw new ContainmentRefusal(`Project ${fid} not found`);
  }
  outcome = { fid, message: `Updated ${fid}` };
  });

  try { tx.immediate(); }
  catch (e) {
    if (e instanceof ContainmentRefusal) return { error: { message: e.message } };
    throw e;
  }
  return outcome;
}

export function listProjects(db) {
  // The parent join deliberately does NOT filter on par.deleted_at. Adding
  // that filter is the obvious way to write this and it is wrong: a child
  // whose parent was soft-deleted would render as a ROOT, silently losing the
  // very fact this column exists to record. §12.3's rule — the pointer is the
  // durable fact, liveness is derived, and a suspension is reported LOUDLY,
  // never swallowed. So: join unconditionally, carry the tombstone out as
  // parent_deleted, and count them as their own line, exactly as
  // listDependencies reports danglingEdges.
  //
  // open_actions stays DIRECT (what it has always meant) and
  // open_actions_rolled adds the child projects' actions, because an area's
  // work is its own actions plus the actions of the projects inside it. A
  // direct-only figure would make carving a project out of an area LOWER the
  // area's number — reporting the reorganisation as loss, which is the exact
  // confusion nesting exists to end (§17.3).
  const rows = db.prepare(`
    SELECT p.fid, p.name, p.kind, p.status, p.due, p.notes, p.tags, p.deleted_at,
      p.parent_fid,
      par.name AS parent_name,
      (par.fid IS NOT NULL AND par.deleted_at IS NOT NULL) AS parent_deleted,
      (SELECT COUNT(*) FROM projects c WHERE c.parent_fid = p.fid AND c.deleted_at IS NULL) AS child_projects,
      (SELECT COUNT(*) FROM actions a WHERE a.project_fid = p.fid AND a.completed = 0 AND a.deleted_at IS NULL) as open_actions,
      (SELECT COUNT(*) FROM actions a
        WHERE a.completed = 0 AND a.deleted_at IS NULL
          AND (a.project_fid = p.fid
               OR a.project_fid IN (SELECT c.fid FROM projects c WHERE c.parent_fid = p.fid AND c.deleted_at IS NULL))
      ) as open_actions_rolled
    FROM projects p
    LEFT JOIN projects par ON par.fid = p.parent_fid
    WHERE p.status = 'active' AND p.deleted_at IS NULL
    ORDER BY p.created DESC
  `).all();
  const suspendedParents = rows.filter((r) => r.parent_deleted).map((r) => r.fid);
  const result = { rows };
  if (suspendedParents.length > 0) {
    result.suspendedParents = suspendedParents;
    result.message = `${suspendedParents.length} container(s) name a parent that has been deleted — their parent is suspended, not cleared: ${suspendedParents.join(', ')}`;
  }
  return result;
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
    //
    // The child scope is ROLLED UP (§17.2 case 6). Direct-only was the shape
    // before nesting existed, and after it the failure is silent and total:
    // an area whose work has all been carved into child projects has ZERO
    // direct actions, so it reports nothing to cascade, parks to 'someday',
    // and — since listProjects shows only active containers — disappears
    // while its projects hold live work. §4's pathology with the parent
    // hidden rather than closed.
    const scoped = containerScopeClause(fid, 'rolled-up');
    const openChildren = db.prepare(
      `SELECT a.fid FROM actions a WHERE ${scoped.sql} AND a.status NOT IN ('done','deferred') AND a.trigger_condition IS NULL AND a.deleted_at IS NULL`
    ).all(...scoped.params);
    if (openChildren.length > 0 && !cascade) {
      return {
        error: {
          error: 'has_open_children',
          message: `Project ${fid} has ${openChildren.length} open action(s) without their own trigger (counting any inside projects filed under it). Pass cascade: true to defer them alongside.`,
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
  if (table === 'projects') {
    // The container's STATUS write routes through updateProject — the one
    // legal write path for that column (§17.2 case 6). This function used to
    // compose its own UPDATE, making it a second writer of container status
    // that no kind, completion or parking gate ever saw: the shape
    // act:4e0de399 and act:cbf19c3f each spent a build retiring for actions.
    // trigger_condition stays this function's own write — it is that column's
    // only writer by design (§12.7) — and the two run in one transaction so a
    // refused park cannot leave a trigger stamped on an unparked container.
    let parked;
    const tx = db.transaction(() => {
      parked = updateProject(db, { fid, status: newStatus });
      if (parked.error) throw new ContainmentRefusal(parked.error.message);
      db.prepare('UPDATE projects SET trigger_condition = ? WHERE fid = ?').run(triggerCondition, fid);
    });
    try { tx.immediate(); }
    catch (e) {
      if (e instanceof ContainmentRefusal) return { error: { error: 'refused', message: e.message } };
      throw e;
    }
  } else {
    db.prepare(`UPDATE ${table} SET status = ?, trigger_condition = ? WHERE fid = ?`).run(newStatus, triggerCondition, fid);
  }

  return { fid, table, triggerCondition, status: newStatus, cascaded, message: `Deferred ${fid} with trigger` };
}

/**
 * Items carrying a trigger condition.
 *
 * DEFAULT = still WAITING, judged by the same `waitingOnTriggerSql` predicate
 * the Ready view uses (act:4eabb7e8). It previously returned everything with a
 * non-NULL trigger_condition, so an action whose trigger had already fired was
 * listed as "waiting" here while `listStartable` simultaneously offered it as
 * startable — two surfaces contradicting each other about one row. Released
 * items are not lost: pass `includeReleased` to see them, and every row now
 * carries `waiting_on_trigger` + `last_result` so a caller can render the
 * distinction rather than infer it from presence.
 */
export function listTriggered(db, { includeDone = false, includeReleased = false } = {}) {
  const notReleased = (alias) => (includeReleased ? '1=1' : waitingOnTriggerSql(alias));
  const actionsWhere = includeDone
    ? `a.trigger_condition IS NOT NULL AND a.deleted_at IS NULL AND ${notReleased('a')}`
    : `a.trigger_condition IS NOT NULL AND a.deleted_at IS NULL AND a.status != 'done' AND (a.completed IS NULL OR a.completed = 0) AND ${notReleased('a')}`;
  const projectsWhere = includeDone
    ? `p.trigger_condition IS NOT NULL AND p.deleted_at IS NULL AND ${notReleased('p')}`
    : `p.trigger_condition IS NOT NULL AND p.deleted_at IS NULL AND p.status != 'done' AND ${notReleased('p')}`;

  const actions = db.prepare(`
    SELECT a.fid, a.text, a.trigger_condition, a.status, p.name AS project_name, p.kind AS project_kind,
      (SELECT checked_at FROM trigger_checks WHERE target_fid = a.fid ORDER BY checked_at DESC LIMIT 1) AS last_checked,
      ${lastTriggerResultSql('a')} AS last_result,
      ${waitingOnTriggerSql('a')} AS waiting_on_trigger
    FROM actions a
    LEFT JOIN projects p ON a.project_fid = p.fid
    WHERE ${actionsWhere}
    ORDER BY last_checked IS NOT NULL, last_checked ASC
  `).all();

  const projects = db.prepare(`
    SELECT p.fid, p.name, p.kind, p.trigger_condition, p.status,
      (SELECT checked_at FROM trigger_checks WHERE target_fid = p.fid ORDER BY checked_at DESC LIMIT 1) AS last_checked,
      ${lastTriggerResultSql('p')} AS last_result,
      ${waitingOnTriggerSql('p')} AS waiting_on_trigger
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

// ---------------------------------------------------------------------------
// Dependency as a real edge (act:d46f3b48 — design in
// .claude/plans/work-tracker-data-model.md §12)
// ---------------------------------------------------------------------------
// The edge ROW is the durable fact ("B depends on A"). Whether B is blocked
// RIGHT NOW is a CONCLUSION recomputed on every read — never stored, never
// invalidated, never rotted. A blocker completing or reopening therefore
// requires no write at all.

// A trigger RESULT that releases the item. `triggered` means the condition
// fired; `condition-obsolete` means it no longer applies. Anything else —
// including never having been evaluated — leaves the item waiting.
//
// This is the whole reason the predicate reads trigger HISTORY rather than
// trigger EXISTENCE. `deferWithTrigger` is the only writer of
// trigger_condition and can only SET a non-empty string, so "has a trigger"
// is a stored conclusion nothing can ever clear — the identical pathology to
// the `pre-launch` tag this data model exists to abolish. It also hid 11 of
// the 15 items in the folder the feature was built to retire.
const TRIGGER_RELEASING_RESULTS = ['triggered', 'condition-obsolete'];

// SQL fragments, defined ONCE. Both `startable` and `blocked` are built from
// these, so the two predicates cannot drift apart; re-typing either at a second
// call site is how that happens.
//
// Parameterized by table alias (act:4eabb7e8) so the CONTAINER surfaces can use
// the identical rule: `listTriggered` renders both actions (`a`) and projects
// (`p`), and it previously keyed on trigger_condition's mere PRESENCE. That let
// one action be "startable" in the Ready view and "waiting" in the Waiting view
// simultaneously — the two views disagreeing about the same row, which is worse
// than either answer alone.
export const lastTriggerResultSql = (alias = 'a') => `(
  SELECT result FROM trigger_checks
  WHERE target_fid = ${alias}.fid
  ORDER BY checked_at DESC, id DESC LIMIT 1
)`;

export const waitingOnTriggerSql = (alias = 'a') => `(
  ${alias}.trigger_condition IS NOT NULL
  AND COALESCE(${lastTriggerResultSql(alias)}, '') NOT IN (${TRIGGER_RELEASING_RESULTS.map(r => `'${r}'`).join(', ')})
)`;

const LAST_TRIGGER_RESULT_SQL = lastTriggerResultSql('a');

const WAITING_ON_TRIGGER_SQL = waitingOnTriggerSql('a');

// An edge constrains only while it is LIVE (both endpoints present and not
// tombstoned) and its blocker is UNFINISHED. The INNER JOIN is deliberate: a
// blocker that is soft-deleted or absent stops blocking, because it will never
// complete and honouring it would strand its dependents forever — the exact
// 0-of-15 pathology. Those edges are not silently swallowed; listDanglingEdges
// reports them, and every startable/blocked result carries the count.
//
// `b.completed = 0`, never `b.status != 'done'`. The two columns disagree on
// real rows (6 in this repo carry completed=1 with status='open'), and
// `completed` is the trustworthy one — completeAction writes both together, so
// the divergence is legacy status drift. Keying on status would let already
// finished actions block their dependents permanently.
const HAS_UNMET_BLOCKER_SQL = `EXISTS (
  SELECT 1 FROM action_dependencies d
  JOIN actions b ON b.fid = d.blocker_fid
  WHERE d.blocked_fid = a.fid AND b.deleted_at IS NULL AND b.completed = 0
)`;

// The startable predicate (§12.4). Note what is NOT here: `status = 'blocked'`
// is deliberately not excluded, so an action whose blockers have all completed
// surfaces even if nobody remembered to flip the field — the derived answer
// CORRECTS the stale conclusion, which is the point of the whole model.
const STARTABLE_WHERE_SQL = `
  a.deleted_at IS NULL
  AND a.completed = 0
  AND a.status != 'done'
  AND NOT ${WAITING_ON_TRIGGER_SQL}
  AND NOT (a.status = 'deferred' AND a.trigger_condition IS NULL)
  AND NOT ${HAS_UNMET_BLOCKER_SQL}
`;

/**
 * Would adding blockerFid -> blockedFid close a cycle?
 *
 * Walks the existing graph forward from blockedFid (following blocker -> blocked)
 * and asks whether blockerFid is reachable. If it is, the new edge closes a loop
 * and neither end could ever start.
 *
 * WITH RECURSIVE is SQLite's own transitive-closure construct — the stock
 * answer, not a hand-rolled BFS. UNION (not UNION ALL) is load-bearing: it
 * dedups, so the walk terminates by construction even if the stored graph is
 * ALREADY cyclic (measured: the UNION ALL form did not terminate on a two-row
 * cycle, and the MCP server swallows uncaughtException, so a hung probe would
 * return no response at all). The node set is finite and each fid is visited
 * once, so no depth cap is needed to guarantee termination.
 *
 * The walk deliberately ignores deleted_at and completed: it asks whether the
 * GRAPH is acyclic, not whether the cycle is live today. Refusing an edge that
 * would only become a cycle after an undelete is conservative and loudly
 * correctable; admitting one is a silent permanent wait.
 */
function edgeWouldCycle(db, blockerFid, blockedFid) {
  const hit = db.prepare(`
    WITH RECURSIVE reach(fid) AS (
      SELECT ?
      UNION
      SELECT d.blocked_fid FROM action_dependencies d JOIN reach r ON d.blocker_fid = r.fid
    )
    SELECT 1 AS hit FROM reach WHERE fid = ? LIMIT 1
  `).get(blockedFid, blockerFid);
  return !!hit;
}

/** Shared endpoint validation: shape, then existence, mirroring updateAction's projectFid guard. */
function resolveEndpoint(db, fid, role) {
  const shapeError = validateFid(fid);
  if (shapeError) {
    return { error: { message: `${role} (${fid}): ${shapeError.message}` } };
  }
  if (!fid.startsWith('act:')) {
    return { error: { message: `${role} must be an action fid (act:...), got ${fid}. Dependencies are stated between actions, not projects.` } };
  }
  const row = db.prepare('SELECT fid, text, completed FROM actions WHERE fid = ? AND deleted_at IS NULL').get(fid);
  if (!row) {
    return { error: { message: `${role} ${fid} does not exist (or is deleted). Pass an existing action fid.` } };
  }
  return { row };
}

/**
 * State a dependency: blockerFid must finish before blockedFid can start.
 * Read from the other end, the same row says blockedFid depends on blockerFid.
 */
export function addDependency(db, { blockerFid, blockedFid } = {}) {
  if (!blockerFid || !blockedFid) {
    return { error: { message: 'Both blockerFid (must finish first) and blockedFid (waits) are required.' } };
  }
  if (blockerFid === blockedFid) {
    return { error: { message: `Cannot add ${blockerFid} as its own blocker — an action cannot wait for itself.` } };
  }

  const blocker = resolveEndpoint(db, blockerFid, 'blockerFid');
  if (blocker.error) return blocker;
  const blocked = resolveEndpoint(db, blockedFid, 'blockedFid');
  if (blocked.error) return blocked;

  const existing = db.prepare('SELECT 1 FROM action_dependencies WHERE blocker_fid = ? AND blocked_fid = ?').get(blockerFid, blockedFid);
  if (existing) {
    return { blockerFid, blockedFid, alreadyPresent: true, message: `${blockedFid} already depends on ${blockerFid}` };
  }

  // Check and insert in ONE transaction. Separately, two processes could each
  // pass the check and then insert opposite directions, producing exactly the
  // cycle this boundary exists to make impossible — and unlike a dangling edge
  // a cycle has no report, so it would be permanent and silent. The MCP server,
  // the CLI and the work-tracker server all hold concurrent write handles on
  // the same file, and this repo has already been bitten by a concurrent-lane
  // pib-db write race.
  let cycled = false;
  db.transaction(() => {
    if (edgeWouldCycle(db, blockerFid, blockedFid)) { cycled = true; return; }
    db.prepare('INSERT INTO action_dependencies (blocker_fid, blocked_fid, created) VALUES (?, ?, ?)')
      .run(blockerFid, blockedFid, today());
  })();

  if (cycled) {
    return { error: { message: `Refused: ${blockerFid} -> ${blockedFid} closes a dependency cycle (${blockedFid} already blocks ${blockerFid}, directly or through other actions). Neither action could ever start.` } };
  }

  return {
    blockerFid,
    blockedFid,
    message: `${blockedFid} now depends on ${blockerFid} ("${blocker.row.text}")`,
    blockerCompleted: blocker.row.completed === 1,
  };
}

export function removeDependency(db, { blockerFid, blockedFid } = {}) {
  if (!blockerFid || !blockedFid) {
    return { error: { message: 'Both blockerFid and blockedFid are required.' } };
  }
  const result = db.prepare('DELETE FROM action_dependencies WHERE blocker_fid = ? AND blocked_fid = ?').run(blockerFid, blockedFid);
  if (result.changes === 0) {
    return { error: { message: `No dependency ${blockerFid} -> ${blockedFid} to remove.` } };
  }
  return { blockerFid, blockedFid, message: `Removed dependency ${blockerFid} -> ${blockedFid}` };
}

/** Both directions for one action: what it waits on, and what waits on it. */
export function listDependencies(db, { fid } = {}) {
  const shapeError = validateFid(fid);
  if (shapeError) return { error: shapeError };

  const blockedBy = db.prepare(`
    SELECT d.blocker_fid AS fid, a.text, a.status, a.completed, a.deleted_at, d.created
    FROM action_dependencies d
    LEFT JOIN actions a ON a.fid = d.blocker_fid
    WHERE d.blocked_fid = ?
    ORDER BY d.created
  `).all(fid);

  const blocking = db.prepare(`
    SELECT d.blocked_fid AS fid, a.text, a.status, a.completed, a.deleted_at, d.created
    FROM action_dependencies d
    LEFT JOIN actions a ON a.fid = d.blocked_fid
    WHERE d.blocker_fid = ?
    ORDER BY d.created
  `).all(fid);

  const unmet = blockedBy.filter(r => r.deleted_at === null && r.completed === 0).length;
  return {
    fid,
    blockedBy,
    blocking,
    unmetBlockers: unmet,
    message: unmet === 0
      ? `${fid} has no unmet blockers`
      : `${fid} is waiting on ${unmet} unfinished action(s)`,
  };
}

/**
 * Edges whose endpoints no longer resolve — an endpoint soft-deleted or gone.
 * These stop constraining (see HAS_UNMET_BLOCKER_SQL) and must therefore be
 * REPORTED rather than silently swallowed: an action can become startable
 * because its blocker was tombstoned, and that is a fact the operator needs.
 */
export function listDanglingEdges(db) {
  const rows = db.prepare(`
    SELECT d.blocker_fid, d.blocked_fid, d.created,
      CASE WHEN b.fid IS NULL THEN 'blocker-missing'
           WHEN b.deleted_at IS NOT NULL THEN 'blocker-deleted'
           WHEN k.fid IS NULL THEN 'blocked-missing'
           ELSE 'blocked-deleted' END AS reason
    FROM action_dependencies d
    LEFT JOIN actions b ON b.fid = d.blocker_fid
    LEFT JOIN actions k ON k.fid = d.blocked_fid
    WHERE b.fid IS NULL OR k.fid IS NULL
       OR b.deleted_at IS NOT NULL OR k.deleted_at IS NOT NULL
    ORDER BY d.created
  `).all();
  return { rows, count: rows.length };
}

/**
 * "What can I start right now" — the derived next-actions list (§12.4).
 *
 * This is the payoff view the whole data model serves. It exists on three
 * surfaces (MCP tool, CLI, work-tracker Ready view) on purpose: sort_order
 * died because nothing wrote it and no view paid off for it, and it turned
 * out to be READ by five shipped surfaces while written by none.
 */
export function listStartable(db, { project, limit, scope } = {}) {
  const scopeError = validateScope(scope);
  if (scopeError) return scopeError;
  const params = [];
  let projectClause = '';
  if (project) {
    const clause = containerScopeClause(project, scope);
    projectClause = `AND ${clause.sql}`;
    params.push(...clause.params);
  }
  const cap = Number.isInteger(limit) && limit > 0 ? limit : null;

  const rows = db.prepare(`
    SELECT a.fid, a.text, a.status, a.due, a.flagged, a.project_fid, p.name AS project, p.kind AS project_kind,
      a.trigger_condition IS NOT NULL AS had_trigger,
      ${LAST_TRIGGER_RESULT_SQL} AS last_trigger_result,
      (a.status = 'blocked') AS status_says_blocked
    FROM actions a
    LEFT JOIN projects p ON a.project_fid = p.fid
    WHERE ${STARTABLE_WHERE_SQL} ${projectClause}
    ORDER BY
      CASE WHEN a.due IS NOT NULL AND a.due <= date('now') THEN 0 ELSE 1 END,
      a.due,
      a.flagged DESC,
      a.created DESC
    ${cap ? 'LIMIT ' + cap : ''}
  `).all(...params);

  const dangling = listDanglingEdges(db);
  const result = {
    rows,
    count: rows.length,
    // Startable despite a status that still says blocked — the stale
    // conclusion the derived answer corrects.
    staleBlockedStatus: rows.filter(r => r.status_says_blocked).map(r => r.fid),
    // Startable because a trigger was evaluated and found to have fired.
    releasedByTrigger: rows.filter(r => r.had_trigger).map(r => r.fid),
    danglingEdges: dangling.count,
  };
  // `capped` only when the limit was actually reached — a result that happens
  // to be shorter than the cap is complete, and saying otherwise would be its
  // own false note.
  const rollup = rollupSummary(rows, project, scope, { capped: Boolean(cap) && rows.length === cap });
  if (rollup) result.rollup = rollup;
  return result;
}

/**
 * What is waiting, and on what. This is the query a waiting-for FOLDER
 * actually collapses into — "show me everything blocked on the same gate" is a
 * group-by over this, not a container.
 *
 * NOT the exact complement of listStartable, and the difference is deliberate:
 * the open set partitions THREE ways, not two. An action that is bare-deferred
 * (status 'deferred' with no trigger condition) is an operator's explicit "not
 * now" — it is not startable, and it is not waiting on anything the system can
 * name, so it appears in neither view. It is reachable through the normal
 * action lists. Anything that IS waiting is here, whether the wait is an
 * unfinished blocker or an unfired trigger.
 */
export function listBlocked(db, { project, scope } = {}) {
  const scopeError = validateScope(scope);
  if (scopeError) return scopeError;
  const params = [];
  let projectClause = '';
  if (project) {
    const clause = containerScopeClause(project, scope);
    projectClause = `AND ${clause.sql}`;
    params.push(...clause.params);
  }

  const rows = db.prepare(`
    SELECT a.fid, a.text, a.status, a.project_fid, p.name AS project,
      (SELECT COUNT(*) FROM action_dependencies d JOIN actions b ON b.fid = d.blocker_fid
       WHERE d.blocked_fid = a.fid AND b.deleted_at IS NULL AND b.completed = 0) AS unmet_blockers,
      (SELECT GROUP_CONCAT(d.blocker_fid) FROM action_dependencies d JOIN actions b ON b.fid = d.blocker_fid
       WHERE d.blocked_fid = a.fid AND b.deleted_at IS NULL AND b.completed = 0) AS blocker_fids,
      ${WAITING_ON_TRIGGER_SQL} AS waiting_on_trigger,
      a.trigger_condition,
      ${LAST_TRIGGER_RESULT_SQL} AS last_trigger_result
    FROM actions a
    LEFT JOIN projects p ON a.project_fid = p.fid
    WHERE a.deleted_at IS NULL AND a.completed = 0 AND a.status != 'done'
      AND (${HAS_UNMET_BLOCKER_SQL} OR ${WAITING_ON_TRIGGER_SQL})
      ${projectClause}
    ORDER BY unmet_blockers DESC, a.created DESC
  `).all(...params);

  const result = { rows, count: rows.length };
  const rollup = rollupSummary(rows, project, scope);
  if (rollup) result.rollup = rollup;
  return result;
}

// Unanchored twin of FID_PATTERN, for finding fids EMBEDDED in prose. The
// anchored original cannot do this job, and the risk of a fresh regex is that
// it drifts wider — so every token this finds is re-validated against the
// anchored FID_PATTERN before it is used for anything.
const EMBEDDED_ACT_FID_RE = /\bact:[a-f0-9]{8}\b/g;

/**
 * PROPOSE dependency edges from prose. Writes NOTHING, by construction as well
 * as by intent — callers open the database read-only for this (the CLI does).
 *
 * Scope is `trigger_condition` ONLY. `text` was measured and dropped: every
 * one of the 10 act: citations in open action titles is provenance —
 * "absorbs", "follow-up to", "deferred from", "CC-side of" — not a blocker
 * claim, so scanning titles would propose ~10 non-dependencies. `notes` is out
 * for the same reason at ten times the volume (122 open actions cite a fid).
 *
 * Three buckets, not one. A bare 8-hex fid is not something a human can
 * confirm, so each proposal carries the target's title; and the two rejected
 * classes are surfaced rather than filtered away, because "the tracker doesn't
 * know that action" reads as a bug unless it is named.
 */
export function proposeDependencyBackfill(db) {
  const sources = db.prepare(`
    SELECT a.fid, a.text, a.trigger_condition, a.project_fid
    FROM actions a
    WHERE a.deleted_at IS NULL AND a.completed = 0 AND a.trigger_condition LIKE '%act:%'
    ORDER BY a.fid
  `).all();

  const proposals = [];
  const alreadySatisfied = [];
  const unresolvable = [];

  for (const src of sources) {
    const seen = new Set();
    for (const match of String(src.trigger_condition).matchAll(EMBEDDED_ACT_FID_RE)) {
      const target = match[0];
      // Re-validate against the ANCHORED pattern — the scanner is allowed to
      // be loose, the acceptance is not.
      if (!FID_PATTERN.test(target)) continue;
      if (target === src.fid || seen.has(target)) continue;
      seen.add(target);

      const row = db.prepare('SELECT fid, text, completed, deleted_at, project_fid FROM actions WHERE fid = ?').get(target);
      const entry = {
        blockedFid: src.fid,
        blockedText: src.text,
        blockerFid: target,
        blockerText: row ? row.text : null,
        crossesProject: row ? row.project_fid !== src.project_fid : null,
        triggerExcerpt: String(src.trigger_condition).slice(0, 200),
      };

      if (!row || row.deleted_at !== null) {
        // fids are minted per-database with no namespace, so a fid from
        // another tracker is a legitimate reference that simply cannot be an
        // edge here. Naming that is the whole point of this bucket.
        unresolvable.push({ ...entry, reason: row ? 'target-soft-deleted' : 'target-not-in-this-tracker' });
      } else if (row.completed === 1) {
        alreadySatisfied.push({ ...entry, reason: 'blocker-already-complete' });
      } else if (db.prepare('SELECT 1 FROM action_dependencies WHERE blocker_fid = ? AND blocked_fid = ?').get(target, src.fid)) {
        alreadySatisfied.push({ ...entry, reason: 'edge-already-stated' });
      } else if (edgeWouldCycle(db, target, src.fid)) {
        unresolvable.push({ ...entry, reason: 'would-close-a-cycle' });
      } else {
        proposals.push(entry);
      }
    }
  }

  return {
    proposals,
    alreadySatisfied,
    unresolvable,
    scanned: sources.length,
    message: `Scanned ${sources.length} open action(s) with a fid in trigger_condition: `
      + `${proposals.length} edge(s) to confirm, ${alreadySatisfied.length} already satisfied, `
      + `${unresolvable.length} not statable. Nothing was written — confirm each with add-dependency.`,
  };
}

// ---------------------------------------------------------------------------
// Completion propagation — the walk (act:08c563ae; design §16)
// ---------------------------------------------------------------------------
// The assisted path THROUGH updateProject's open-children refusal — never a
// parallel closure authority. Propose-then-apply: called with just a fid
// while open children exist, it mutates NOTHING and returns the walk sheet;
// called with a complete per-child disposition map, it validates, applies,
// records the walk, and ends by calling the same gate every other caller
// hits. The walk holds no completion logic of its own.

// Everything a per-child answer needs to be informed rather than blind
// (§16.2): status, trigger STATE (the latest trigger_checks result, never the
// condition's mere existence — §12.4's rule), and dependency degree with a
// cross-project flag, because closing a blocker releases its dependents and
// deleting one strands them loudly.
function walkSheetChildren(db, projectFid) {
  return db.prepare(`
    SELECT a.fid, a.text, a.status, a.trigger_condition,
      ${LAST_TRIGGER_RESULT_SQL} AS last_trigger_result,
      (SELECT COUNT(*) FROM action_dependencies d WHERE d.blocker_fid = a.fid) AS blocks,
      (SELECT COUNT(*) FROM action_dependencies d WHERE d.blocked_fid = a.fid) AS blocked_by,
      EXISTS(
        SELECT 1 FROM action_dependencies d
        JOIN actions o ON o.fid = CASE WHEN d.blocker_fid = a.fid THEN d.blocked_fid ELSE d.blocker_fid END
        WHERE (d.blocker_fid = a.fid OR d.blocked_fid = a.fid)
          AND o.project_fid IS NOT a.project_fid
      ) AS cross_project_edge
    FROM actions a
    WHERE ${OPEN_CHILD_PREDICATE_SQL}
    ORDER BY a.created, a.fid
  `).all(projectFid);
}

// Thrown inside the walk transaction to abort it; completeProject converts it
// back to this library's documented {error} shape. Anything else that throws
// mid-walk propagates — a genuine defect must not be dressed as a refusal.
class WalkRefusal extends Error {}

/**
 * Complete a project, walking its open children (§16.2–§16.5).
 *
 * - `{fid}` alone: zero open children → completes through the gate; otherwise
 *   returns the walk sheet and mutates nothing.
 * - `{fid, map}`: map is [{fid, seen_status, answer}] covering EVERY open
 *   child — answer is 'parent-outcome' | 'reparent:<prj-fid>' | 'delete',
 *   and seen_status must echo the status the sheet showed. Validation and
 *   application run in ONE immediate transaction that ends by calling
 *   updateProject(status='done') — the literal gate function — whose error
 *   RETURN is converted to a THROW so a refusal unwinds every per-child
 *   write (better-sqlite3 rolls back only on throw; the naive version would
 *   commit all closures and then hand back the refusal).
 *
 * "Verified complete" is deliberately INEXPRESSIBLE here (§16.3): the
 * breadcrumb gate is a PreToolUse hook on the pib_complete_action TOOL call,
 * so a walk answer claiming verification would silently bypass it. A child
 * whose acceptance criteria really were verified is completed FIRST through
 * pib_complete_action; the skip-if-already-dispositioned rule below is what
 * makes that two-step usable without a full resubmit.
 *
 * `deps.gate` is a TEST SEAM ONLY (defaults to updateProject): the
 * gate-refusal path is structurally unreachable from a single connection —
 * validation and gate share one transaction — so the rollback contract is
 * pinned by injecting a refusing gate double that returns the real error
 * shape. Production callers never pass it.
 */
export function completeProject(db, { fid, map } = {}, deps = {}) {
  const gate = deps.gate || updateProject;

  const fidError = validateFid(fid);
  if (fidError) return { error: fidError };
  if (!fid.startsWith('prj:')) {
    return { error: { message: `completeProject takes a container fid (prj:...), got ${fid}. To complete a single action, use pib_complete_action.` } };
  }

  const project = db.prepare('SELECT fid, name, kind, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(fid);
  if (!project) return { error: { message: `Project ${fid} not found` } };
  // The kind gate would refuse at the end anyway; the early check moves the
  // message BEFORE a 91-row walk instead of after it (§16.0). Propagation
  // never reclassifies — that is the operator's §7.2 judgment.
  if (project.kind === 'area') {
    return { error: { message: `Cannot complete ${fid} (${project.name}): it is an AREA — a standing responsibility that never completes, so there is nothing to walk. If it is really an outcome that finishes, reclassify it first (pib_update_project ${fid} kind='project', stating the outcome in its notes), then walk it. Propagation never reclassifies.` } };
  }
  // Mirror completeAction's already_done: updateProject re-stamps
  // completed_at on every done-write, so routing an already-done project
  // through the gate would silently rewrite its completion history.
  if (project.status === 'done') {
    return { error: 'already_done', message: `${fid} (${project.name}) is already done — refusing to re-stamp its completion history. To re-walk it, reopen it first (pib_update_project ${fid} status='active').` };
  }

  // ONE walk timestamp, minted at walk start (§16.4): every child closure,
  // every tombstone, and the event row carry it, so the walk reads as one
  // cluster. The project's own stamp comes from updateProject and may differ
  // by milliseconds — joins are by fid, never by time.
  const walkedAt = new Date().toISOString();

  if (map === undefined) {
    const children = walkSheetChildren(db, fid);
    if (children.length === 0) {
      // Nothing to walk — this is an ordinary completion through the gate.
      return gate(db, { fid, status: 'done' });
    }
    return {
      fid,
      name: project.name,
      kind: project.kind,
      status: project.status,
      proposed: true,
      count: children.length,
      openChildren: children.map((c) => ({
        fid: c.fid,
        text: c.text,
        status: c.status,
        trigger: c.trigger_condition
          ? { condition: c.trigger_condition, last_result: c.last_trigger_result || 'never-evaluated' }
          : null,
        blocks: c.blocks,
        blockedBy: c.blocked_by,
        crossProjectEdge: !!c.cross_project_edge,
      })),
      message: `${fid} (${project.name}) has ${children.length} open child action(s). NOTHING was changed — this is the walk sheet. To complete the project, answer for EVERY open child by re-calling with map: [{fid, seen_status, answer}], where answer is 'parent-outcome' (done by the fact of this outcome — a typed closure that never claims verification), 'reparent:<prj-fid>' (still needed; the work outlives the outcome and moves to a live container), or 'delete' (not needed, never happening — soft-delete). seen_status must echo the status shown on this sheet; an answer against changed state is refused. A child whose acceptance criteria really WERE verified is a different claim: complete it first through pib_complete_action (its gate), then re-apply the map — already-dispositioned children are skipped, not refused. There is no bulk default: every child answers.`,
    };
  }

  if (!Array.isArray(map)) {
    return { error: { message: `map must be an array of {fid, seen_status, answer} entries covering every open child. Call with just the fid to get the walk sheet first.` } };
  }

  const closed = [];
  const reparented = [];
  const deleted = [];
  const skipped = [];
  const eventEntries = [];
  const refuse = (message) => { throw new WalkRefusal(message); };

  const tx = db.transaction(() => {
    // Shape validation. No db reads yet; a malformed map refuses whole.
    const answered = new Set();
    for (const entry of map) {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
        refuse(`Every map entry must be a {fid, seen_status, answer} object; got ${JSON.stringify(entry)}.`);
      }
      const childFid = entry.fid;
      if (typeof childFid !== 'string' || !FID_PATTERN.test(childFid) || !childFid.startsWith('act:')) {
        refuse(`Map entry fid ${JSON.stringify(childFid)} is not an action fid (act:...).`);
      }
      if (answered.has(childFid)) {
        refuse(`The map answers ${childFid} twice — one answer per child.`);
      }
      answered.add(childFid);
      if (typeof entry.seen_status !== 'string' || entry.seen_status === '') {
        refuse(`Map entry for ${childFid} carries no seen_status echo. Every answer must echo the status the walk sheet showed for that child — a map generated without consulting the sheet is a bulk default, which the walk refuses by design (§16.3).`);
      }
      const a = entry.answer;
      if (typeof a !== 'string' || !(a === 'parent-outcome' || a === 'delete' || a.startsWith('reparent:'))) {
        refuse(`Map entry for ${childFid} has answer ${JSON.stringify(a)} — the vocabulary is exactly 'parent-outcome', 'reparent:<prj-fid>', and 'delete'.`);
      }
    }

    // Coverage: every open child — the gate's own predicate — must be
    // answered. A genuinely NEW child (filed mid-walk) fails the map here; a
    // human answers it or the walk does not happen.
    const openNow = db.prepare(`SELECT a.fid FROM actions a WHERE ${OPEN_CHILD_PREDICATE_SQL}`).all(fid);
    const unanswered = openNow.filter((c) => !answered.has(c.fid));
    if (unanswered.length > 0) {
      refuse(`${unanswered.length} open child(ren) of ${fid} have no answer: ${unanswered.map((c) => c.fid).join(', ')}. Every open child must be answered — re-fetch the sheet (call with just the fid) and answer each.`);
    }

    // Per-child: resolve, skip-if-already-dispositioned, echo-check, apply.
    for (const entry of map) {
      const row = db.prepare('SELECT fid, status, completed, deleted_at, project_fid FROM actions WHERE fid = ?').get(entry.fid);
      if (!row) {
        refuse(`The map answers ${entry.fid}, which does not exist in this tracker. Remove the entry (a fid from another tracker cannot be walked here).`);
      }
      // Skip-if-already-dispositioned (§16.3): the disposition it asked for
      // already happened by other hands — completed through the real gate
      // mid-conversation (the recommended two-step), deleted, or reparented
      // away. Skipped with a per-child note, never a whole-map refusal.
      const skip = (note) => {
        skipped.push({ fid: entry.fid, note });
        eventEntries.push({ fid: entry.fid, seen_status: entry.seen_status, answer: entry.answer, applied: 'skipped', note });
      };
      if (row.completed === 1) { skip('already completed (by the real gate) when the map applied'); continue; }
      if (row.deleted_at !== null) { skip('already deleted when the map applied'); continue; }
      if (row.project_fid !== fid) { skip(`already reparented away (now under ${row.project_fid || 'no container'}) when the map applied`); continue; }
      // The echo (§16.3, two birds): an answer given against stale state is
      // not an informed answer — and a mechanically generated map that never
      // consulted the sheet has no seen_status to echo.
      if (entry.seen_status !== row.status) {
        refuse(`${entry.fid} is now '${row.status}' but its answer was given against '${entry.seen_status}' — it changed since the sheet. Re-fetch the sheet and re-answer this child.`);
      }

      if (entry.answer === 'parent-outcome') {
        closeChildByParentOutcome(db, entry.fid, walkedAt);
        closed.push(entry.fid);
        eventEntries.push({ fid: entry.fid, seen_status: entry.seen_status, answer: entry.answer, applied: 'closed' });
      } else if (entry.answer === 'delete') {
        // Soft-delete: the disposition the gate already counts as
        // dispositioned. `completed` stays 0 — closing it would claim work
        // happened that never did. Edges suspend and report dangling (§12.3);
        // no general delete verb ships — the walk writes the tombstone
        // internally (act:9f380563's merge/delete scope stays separate).
        db.prepare('UPDATE actions SET deleted_at = ? WHERE fid = ? AND deleted_at IS NULL').run(walkedAt, entry.fid);
        deleted.push(entry.fid);
        eventEntries.push({ fid: entry.fid, seen_status: entry.seen_status, answer: entry.answer, applied: 'deleted' });
      } else {
        const target = entry.answer.slice('reparent:'.length);
        if (!FID_PATTERN.test(target) || !target.startsWith('prj:')) {
          refuse(`${entry.fid}: reparent target ${JSON.stringify(target)} is not a container fid (prj:...).`);
        }
        if (target === fid) {
          refuse(`${entry.fid}: cannot reparent into ${fid} — that is the project being completed. Pick the live container where the work now belongs.`);
        }
        // Target must be LIVE: exists, not soft-deleted, not done — the
        // enter-invariant (§16.3), enforced here as at the other writers.
        const dest = db.prepare('SELECT fid, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(target);
        if (!dest) {
          refuse(`${entry.fid}: reparent target ${target} does not exist (or is deleted). Pass a live container fid.`);
        }
        if (dest.status === 'done') {
          refuse(`${entry.fid}: reparent target ${target} is done, and an open action may not enter a done container. Reopen it first, or pick a live container.`);
        }
        // The child keeps its trigger and its edges — reparenting moves the
        // work, not the facts about it (§16.3).
        db.prepare('UPDATE actions SET project_fid = ? WHERE fid = ?').run(target, entry.fid);
        reparented.push({ fid: entry.fid, to: target });
        eventEntries.push({ fid: entry.fid, seen_status: entry.seen_status, answer: entry.answer, applied: 'reparented' });
      }
    }

    // The walk-level record (§16.5-B): ONE append-only row per walk carrying
    // the full map — answers, echoes, and apply-time skips — because the
    // resolution column clears on reopen by design and reparent/delete
    // answers never touch it; this row is what makes a later hand-reversal
    // informed. Written BEFORE the gate call so a gate refusal unwinds it
    // with everything else (an event row describing a walk that rolled back
    // would be a record of something that never happened).
    db.prepare('INSERT INTO propagation_events (project_fid, walked_at, map) VALUES (?, ?, ?)')
      .run(fid, walkedAt, JSON.stringify({ entries: eventEntries }));

    // End at the same gate everyone else hits (§16.4). updateProject signals
    // refusal by RETURNING an error object, and better-sqlite3 rolls back
    // only on THROW — swallowing this return would commit every closure
    // above and then hand back the refusal, the flagship guarantee inverted.
    const verdict = gate(db, { fid, status: 'done' });
    if (verdict && verdict.error) {
      refuse(typeof verdict.error === 'string' ? (verdict.message || verdict.error) : verdict.error.message);
    }
  });

  try {
    // .immediate(): three processes hold live write handles on this file, and
    // a deferred transaction upgrading mid-walk can die un-retried on a
    // snapshot conflict.
    tx.immediate();
  } catch (e) {
    if (e instanceof WalkRefusal) return { error: { message: e.message } };
    throw e;
  }

  return {
    fid,
    name: project.name,
    completed: true,
    walked_at: walkedAt,
    closedByParentOutcome: closed,
    reparented,
    deleted,
    skipped,
    message: `Completed ${fid} (${project.name}) through the gate. Walk: ${closed.length} closed by parent outcome, ${reparented.length} reparented, ${deleted.length} deleted, ${skipped.length} already dispositioned (skipped). The full disposition map is recorded in propagation_events (walked_at ${walkedAt}).`,
  };
}

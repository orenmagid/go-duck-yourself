-- Process-in-a-Box reference data layer
-- Local SQLite database for work tracking and audit findings.
-- This is the default persistence layer. Projects that outgrow it
-- override via phase files (pointing to their own API, DB, or service).
--
-- Initialize: node scripts/pib-db.mjs init
-- Query:      node scripts/pib-db.mjs query "SELECT ..."

-- CONTAINERS — two kinds in one table, one fid namespace (design:
-- .claude/plans/work-tracker-data-model.md §7, §13; act:14876288).
--
-- `kind` carries the completion semantics: an 'area' is a standing
-- responsibility that never completes ("Is Watchtower done?" has no answer);
-- a 'project' is an outcome that completes when every child action is closed
-- or dispositioned. The `prj:` fid prefix is the container NAMESPACE, not a
-- kind claim — both kinds mint and keep `prj:` fids forever, because the kind
-- is operator-mutable (reclassification) while fids are referenced from
-- notes, plan docs, and commit history that can never be rewritten (§13).
-- Every read surface renders `kind` beside the name so nobody ever infers
-- kind from the prefix.
--
-- The old free-text `area` column (a rotted proto-category field — 62% said
-- 'dev', nothing consumed it) was DROPPED in migration v11 (base v9); its
-- non-NULL values live in legacy_area_values below (§14).
--
-- `parent_fid` NESTS the two kinds (migration v13, base v11; design §17): an
-- AREA contains PROJECTs, which is the GTD relationship the vocabulary
-- borrows and the flat model did not provide. Only a kind='project' row may
-- carry a parent, and its target must be a kind='area' row — so container
-- depth is two, and project-under-project is refused. That refusal is
-- load-bearing, not tidiness: openChildCount (the ONE predicate shared by the
-- completion gate and the propagation walk's open set, §16.3) counts ACTIONS,
-- so a project holding an open child PROJECT would complete straight over it.
--
-- Read the enforcement honestly, because only half of it is here. The CHECK
-- below is structural and fires in BOTH directions (an area cannot be given a
-- parent; a parented project cannot be demoted to an area). The foreign key
-- refuses a parent that does not exist — FK enforcement is ON under
-- better-sqlite3 and OFF under the bare sqlite3 CLI, and there is NO ON
-- DELETE clause, following action_dependencies' precedent (soft-delete is the
-- retirement path; a soft-deleted parent RETAINS its children's pointers and
-- is reported as suspended rather than silently re-rooting them, §12.3). But
-- "the parent must be an AREA" — the rule carrying both the two-level
-- guarantee and acyclicity — is NOT expressible in a row-level CHECK, because
-- it reads another row. It lives in createProject and updateProject, inside
-- one immediate transaction (§12.7's addDependency race, verbatim).
CREATE TABLE IF NOT EXISTS projects (
  fid                TEXT PRIMARY KEY CHECK(fid GLOB 'prj:*'),
  name               TEXT NOT NULL,
  kind               TEXT NOT NULL DEFAULT 'area'
                       CHECK(kind IN ('area','project')),
  status             TEXT NOT NULL DEFAULT 'active'
                       CHECK(status IN ('active','paused','done','dropped','someday')),
  notes              TEXT NOT NULL DEFAULT '',
  created            TEXT NOT NULL CHECK(created GLOB '????-??-??'),
  completed_at       TEXT,
  due                TEXT,
  deleted_at         TEXT,
  trigger_condition  TEXT,
  tags               TEXT NOT NULL DEFAULT '',
  parent_fid         TEXT REFERENCES projects(fid)
                       CHECK(parent_fid IS NULL OR kind = 'project')
);
-- The containment index (idx_projects_parent) is created ONLY by migration v13
-- (CONTAINER_PARENT_INDEXES in pib-db-lib.mjs), deliberately NOT here.
--
-- init() is `exec(this file)` and THEN migrate(). On a pre-existing database
-- `CREATE TABLE IF NOT EXISTS projects` is a no-op, so parent_fid does not exist
-- yet, and a standalone CREATE INDEX over it throws before migrate() can add the
-- column — taking down every install on every existing consumer. That shipped
-- once and was caught in QA before release; see act:e1c0261d.
--
-- The general rule it taught: this file may only declare an index over a column
-- it creates ITSELF in the same run. An index over a MIGRATION-added column
-- belongs to the migration, which is the one path that runs on both fresh
-- databases (from user_version 0) and existing ones.
--
-- Note for any future migration that rebuilds or drops parent_fid: the index
-- must be dropped FIRST. SQLite refuses the column drop while an index depends
-- on it ("error in index idx_projects_parent after drop column"), which is not
-- a hypothetical — it is the order the v11 test fixture had to learn.

-- Archive of the dropped `area` column's non-NULL values (migration v11,
-- base v9 — §14). Pure history — no shipped code reads it; it exists because
-- the migration runs automatically on open, and silently destroying data in
-- an auto-migration would be a silent failure even when the data is measured
-- junk. Empty on fresh installs.
CREATE TABLE IF NOT EXISTS legacy_area_values (
  tbl    TEXT NOT NULL CHECK(tbl IN ('actions','projects')),
  fid    TEXT NOT NULL,
  value  TEXT NOT NULL,
  PRIMARY KEY (tbl, fid)
);

-- `resolution` is the typed completion claim (migration v12, base v10;
-- design §16.1). Exactly two writers: completeAction stamps 'verified'
-- (every route to it is gated — MCP hook, tracker delegation, the CLI's
-- Bash-guard arm), and the propagation walk's typed closure stamps
-- 'parent-outcome' (done by the fact of the parent project's outcome — a
-- materially different claim from verified acceptance criteria, and the
-- record must tell them apart forever, §10.2). NULL is honest: a closed row
-- with NULL resolution predates the feature ("unknown", never coerced); an
-- open row is NULL by definition — every reopen path clears it. The CHECK
-- closes the vocabulary so a typo cannot mint a third resolution.
CREATE TABLE IF NOT EXISTS actions (
  fid                TEXT PRIMARY KEY CHECK(fid GLOB 'act:*'),
  text               TEXT NOT NULL,
  project_fid        TEXT REFERENCES projects(fid) ON DELETE SET NULL,
  due                TEXT,
  flagged            INTEGER NOT NULL DEFAULT 0 CHECK(flagged IN (0, 1)),
  completed          INTEGER NOT NULL DEFAULT 0 CHECK(completed IN (0, 1)),
  completed_at       TEXT,
  status             TEXT NOT NULL DEFAULT 'open'
                       CHECK(status IN ('open','in-progress','blocked','deferred','done')),
  tags               TEXT NOT NULL DEFAULT '',
  sort_order         INTEGER NOT NULL DEFAULT 0,
  created            TEXT NOT NULL CHECK(created GLOB '????-??-??'),
  created_at         TEXT CHECK(created_at IS NULL OR created_at GLOB '????-??-??T??:??:??*'),
  notes              TEXT NOT NULL DEFAULT '',
  deleted_at         TEXT,
  trigger_condition  TEXT,
  resolution         TEXT CHECK(resolution IN ('verified','parent-outcome')),
  needs              TEXT CHECK(needs IS NULL OR needs IN ('code-fix','qa-verify','decision','operator','event-gated'))
);

CREATE TABLE IF NOT EXISTS audit_runs (
  id            TEXT PRIMARY KEY,
  date          TEXT NOT NULL,
  timestamp     TEXT NOT NULL,
  trigger       TEXT NOT NULL,
  finding_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit_findings (
  id                  TEXT PRIMARY KEY,
  run_id              TEXT NOT NULL REFERENCES audit_runs(id),
  cabinet_member      TEXT NOT NULL,  -- renamed from 'perspective' in v0.7; migration: ALTER TABLE audit_findings RENAME COLUMN perspective TO cabinet_member
  severity            TEXT NOT NULL CHECK(severity IN ('critical','warn','info','idea')),
  title               TEXT NOT NULL,
  description         TEXT,
  assumption          TEXT,
  evidence            TEXT,
  question            TEXT,
  file                TEXT,
  line                INTEGER,
  suggested_fix       TEXT,
  auto_fixable        INTEGER DEFAULT 0,
  type                TEXT DEFAULT 'finding' CHECK(type IN ('finding','positive')),
  triage_status       TEXT DEFAULT 'open'
                        CHECK(triage_status IN ('open','approved','rejected','deferred','fixed','archived')),
  triage_notes        TEXT,
  triaged_at          TEXT,
  fix_description     TEXT,
  -- Deliberation fields (two-stage audit). Absent for single-stage audits.
  status              TEXT CHECK(status IS NULL OR status IN ('upheld','challenged','modified','withdrawn','rebutted')),
  annotations         TEXT,  -- JSON array of Stage-2 critic annotations
  rebuttal            TEXT   -- JSON object: Stage-1 member's response to challenges
);

-- Action-to-action dependency, as a real edge (act:d46f3b48; design in
-- .claude/plans/work-tracker-data-model.md §12).
--
-- ONE directed row read from both ends: "A blocks B" and "B depends on A" are
-- the same fact spoken from opposite sides. Columns are named for their ROLES,
-- never from/to — a reversed edge is the likeliest defect in any dependency
-- feature, and from/to offers no defense in a review or a raw SQL read.
--
-- NOT NULL is load-bearing and is NOT implied by the composite PRIMARY KEY.
-- Verified on SQLite 3.51.3: a non-INTEGER PK on a rowid table accepts NULLs
-- (two rows with a NULL blocker landed in the same table), and SQLite skips
-- foreign-key validation entirely when the child key is NULL — so without
-- these declarations both the duplicate defense and the dangling-target
-- defense silently evaporate.
--
-- No ON DELETE clause, deliberately — the same call engagement_events makes
-- and documents. NO ACTION means a hard delete of a referenced action fails
-- loudly; soft delete (deleted_at) is the retirement path, and a soft-deleted
-- endpoint suspends its edges rather than destroying them. CASCADE would also
-- be client-dependent: foreign_keys defaults ON under better-sqlite3 and OFF
-- under the sqlite3 CLI, so one manual delete would erase edge history through
-- one client and orphan it through the other. Contrast trigger_checks below,
-- which takes no FK for the opposite reason (orphan history is wanted there).
--
-- The row is DURABLE. An endpoint completing, reopening, or being soft-deleted
-- never rewrites it: whether an action is blocked RIGHT NOW is derived on every
-- read (§12.3). A stored is_blocked flag is precisely the rot this model exists
-- to abolish — it is what happened to the `pre-launch` tag and to sort_order.
--
-- EXTENDING THIS: a different RELATION gets its own table, not a `kind` column
-- here. The column names are specific to THIS relation — a containment or
-- see-also edge would leave `blocker_fid` holding something that does not
-- block, which is exactly the misinformation the role-naming exists to prevent.
CREATE TABLE IF NOT EXISTS action_dependencies (
  blocker_fid  TEXT NOT NULL REFERENCES actions(fid),
  blocked_fid  TEXT NOT NULL REFERENCES actions(fid),
  created      TEXT NOT NULL CHECK(created GLOB '????-??-??'),
  PRIMARY KEY (blocker_fid, blocked_fid)
);
CREATE INDEX IF NOT EXISTS idx_action_deps_blocked ON action_dependencies(blocked_fid);
CREATE INDEX IF NOT EXISTS idx_action_deps_blocker ON action_dependencies(blocker_fid);

-- Append-only walk-level record of completion propagation (migration v12,
-- base v10; design §16.5 Shape B). ONE row per walk of pib_complete_project:
-- the project walked, the walk's shared ISO timestamp, and the FULL
-- disposition map as JSON — every answer with its seen_status echo, plus
-- apply-time skips — so the record shows what was decided AND what had
-- already happened by other hands.
--
-- This table is why §10.2's "distinguishable forever" survives: the
-- actions.resolution column CLEARS on reopen by design, so the column alone
-- carries the distinction only while the row stays closed, and reparent /
-- delete answers never touch the column at all. The event row is what makes
-- a project-reopen's hand-reversal informed.
--
-- project_fid is deliberately NOT unique: a reopen-then-re-walk legitimately
-- appends a second row. NO foreign key, following trigger_checks' documented
-- precedent — history must survive its subject.
CREATE TABLE IF NOT EXISTS propagation_events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  project_fid  TEXT NOT NULL,
  walked_at    TEXT NOT NULL,
  map          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_propagation_events_project ON propagation_events(project_fid);

-- Append-only history of trigger-condition evaluations.
-- No foreign key to actions/projects: if the target row is later deleted,
-- we want the historical record preserved (orphan rows are acceptable).
CREATE TABLE IF NOT EXISTS trigger_checks (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  target_table  TEXT NOT NULL CHECK(target_table IN ('actions','projects')),
  target_fid    TEXT NOT NULL,
  checked_at    TEXT NOT NULL,
  result        TEXT NOT NULL CHECK(result IN ('triggered','still-waiting','needs-info','condition-obsolete')),
  notes         TEXT
);
CREATE INDEX IF NOT EXISTS idx_trigger_checks_fid ON trigger_checks(target_fid);
CREATE INDEX IF NOT EXISTS idx_trigger_checks_target_time ON trigger_checks(target_fid, checked_at DESC);

-- Append-only engagement event log (schema v5). Records the multi-party
-- workflow state of a consulting engagement: client feedback, approvals,
-- status pushes, delegations, notes, and packet-sent markers.
--
-- engagement   -> projects(fid). FK enforced (foreign_keys = ON), so an
--                 event can never reference a non-existent engagement.
-- target_fid   -> an action fid, or NULL for engagement-level events. NO FK
--                 by design: like trigger_checks, this is append-only audit
--                 and a later soft/hard delete of the action must not erase
--                 history. listEngagementEvents({excludeSoftDeleted}) filters
--                 against actions.deleted_at at read time instead.
-- verdict      -> nullable; required (and meaningful, not 'none') for
--                 client_feedback and approval kinds via the table CHECK.
-- dedup index  -> non-UNIQUE lookup aid. Dedup is an application-layer
--                 SELECT-before-insert in addEngagementEvent (distinct
--                 verdicts for the same (packet_id,target_fid) are preserved;
--                 packet_sent rows with NULL target_fid/verdict may duplicate
--                 harmlessly). It is intentionally not a hard UNIQUE constraint.
-- SOURCE OF TRUTH: templates/engagement/sql-constants.mjs
-- This copy must stay in sync. Drift-guard test verifies it.
CREATE TABLE IF NOT EXISTS engagement_events (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  engagement    TEXT NOT NULL REFERENCES projects(fid),
  target_fid    TEXT,
  packet_id     TEXT,
  kind          TEXT NOT NULL
                  CHECK(kind IN ('client_feedback','status_push','delegation','approval','note','packet_sent')),
  author        TEXT NOT NULL,
  verdict       TEXT CHECK(verdict IS NULL OR verdict IN ('approve','object','comment','none')),
  body          TEXT CHECK(body IS NULL OR length(body) <= 10000),
  visibility    TEXT NOT NULL DEFAULT 'internal' CHECK(visibility IN ('client','internal')),
  addressed     INTEGER NOT NULL DEFAULT 0 CHECK(addressed IN (0,1)),
  created_at    TEXT NOT NULL CHECK(created_at GLOB '????-??-??T*'),
  -- client_feedback and approval must carry a meaningful verdict ('none'
  -- and NULL are both rejected for those kinds). The explicit IS NOT NULL
  -- is required: SQLite CHECK passes on NULL (3-valued logic), so without it
  -- a NULL verdict would slip through for these kinds.
  CHECK(kind NOT IN ('client_feedback','approval')
        OR (verdict IS NOT NULL AND verdict IN ('approve','object','comment')))
);
CREATE INDEX IF NOT EXISTS idx_engagement_events_eng ON engagement_events(engagement, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_engagement_events_tgt ON engagement_events(target_fid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_engagement_events_dedup ON engagement_events(packet_id, target_fid, verdict);

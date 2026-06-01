-- Process-in-a-Box reference data layer
-- Local SQLite database for work tracking and audit findings.
-- This is the default persistence layer. Projects that outgrow it
-- override via phase files (pointing to their own API, DB, or service).
--
-- Initialize: node scripts/pib-db.mjs init
-- Query:      node scripts/pib-db.mjs query "SELECT ..."

CREATE TABLE IF NOT EXISTS projects (
  fid                TEXT PRIMARY KEY CHECK(fid GLOB 'prj:*'),
  name               TEXT NOT NULL,
  area               TEXT,
  status             TEXT NOT NULL DEFAULT 'active'
                       CHECK(status IN ('active','paused','done','dropped','someday')),
  notes              TEXT NOT NULL DEFAULT '',
  created            TEXT NOT NULL CHECK(created GLOB '????-??-??'),
  completed_at       TEXT,
  due                TEXT,
  deleted_at         TEXT,
  trigger_condition  TEXT,
  tags               TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS actions (
  fid                TEXT PRIMARY KEY CHECK(fid GLOB 'act:*'),
  text               TEXT NOT NULL,
  area               TEXT,
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
  notes              TEXT NOT NULL DEFAULT '',
  deleted_at         TEXT,
  trigger_condition  TEXT
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

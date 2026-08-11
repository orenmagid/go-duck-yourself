#!/usr/bin/env node
// Process-in-a-Box reference data layer — CLI wrapper
//
// Thin CLI over pib-db-lib.mjs. All database operations live in the library;
// this file handles argument parsing and console output.
//
// Usage:
//   node scripts/pib-db.mjs init                        # Create/migrate DB
//   node scripts/pib-db.mjs query "SELECT * FROM ..."   # Run a query
//   node scripts/pib-db.mjs create-action "Do the thing" --projectFid prj:abc
//   node scripts/pib-db.mjs list-actions [--status X]   # Open actions (or filtered)
//   node scripts/pib-db.mjs update-action act:abc --status in-progress
//   node scripts/pib-db.mjs complete-action act:abc123
//   node scripts/pib-db.mjs create-project "My Area"                          # area (default): never completes
//   node scripts/pib-db.mjs create-project "Ship X" --kind project --notes "done when …"
//   node scripts/pib-db.mjs complete-project prj:abc                          # walk sheet (no mutation) or close if no open children
//   node scripts/pib-db.mjs complete-project prj:abc --map-file map.json      # apply the per-child disposition map
//   node scripts/pib-db.mjs list-projects               # Active projects
//   node scripts/pib-db.mjs defer-with-trigger act:abc --trigger "<text>" [--cascade]
//   node scripts/pib-db.mjs add-dependency act:blocker act:blocked
//   node scripts/pib-db.mjs next-actions [--project prj:abc] [--limit 20]
//   node scripts/pib-db.mjs list-blocked [--project prj:abc]
//   node scripts/pib-db.mjs propose-dependencies      # read-only backfill proposals
//   node scripts/pib-db.mjs list-triggered [--include-done]
//   node scripts/pib-db.mjs mark-trigger-checked act:abc --result <value> [--notes "<text>"]
//   node scripts/pib-db.mjs add-event --engagement prj:abc --kind note --author consultant --allowed-authors "consultant,ed"
//   node scripts/pib-db.mjs list-events [--engagement prj:abc] [--target act:xyz] [--unaddressed-only] [--exclude-soft-deleted]
//   node scripts/pib-db.mjs mark-event-addressed <id>
//   node scripts/pib-db.mjs ingest-findings <run-dir>   # Ingest audit findings
//   node scripts/pib-db.mjs triage <finding-id> <status> [notes]
//   node scripts/pib-db.mjs triage-history              # Suppression list JSON
//
// Environment:
//   PIB_DB_PATH  — path to SQLite file. Overrides everything. Otherwise the
//                  path resolver (pib-db-path.mjs) picks main's pib.db when in
//                  a linked worktree, else cwd/pib.db.

import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import * as lib from './pib-db-lib.mjs';
import { resolvePibDbPath } from './pib-db-path.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = resolvePibDbPath();

// ---------------------------------------------------------------------------
// SQLite setup — try better-sqlite3
// ---------------------------------------------------------------------------
let db;

function getDb() {
  if (db) return db;
  try {
    const require = createRequire(import.meta.url);
    const Database = require('better-sqlite3');
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    // Apply any pending migrations. Cheap: short-circuits when user_version
    // is already current. Skipped only for `init` which runs init() itself
    // (which also calls migrate internally).
    if (process.argv[2] !== 'init') lib.migrate(db);
    return db;
  } catch (err) {
    if (err.code === 'ERR_DLOPEN_FAILED') {
      console.error('Error: better-sqlite3 native module version mismatch.');
      console.error('  Rebuild it for your current Node version:');
      console.error('  npm rebuild better-sqlite3');
    } else {
      console.error('Error: better-sqlite3 not found. Install it:');
      console.error('  npm install better-sqlite3');
    }
    if (err.message) console.error(`  (${err.message.split('\n')[0]})`);
    process.exit(1);
  }
}

/**
 * A structurally read-only handle, for commands that must not be able to write
 * (today: propose-dependencies). "Proposes, never rewrites" is an acceptance
 * criterion, and better-sqlite3's readonly flag makes it impossible rather than
 * merely intended — a stray INSERT throws instead of landing.
 */
function getReadOnlyDb() {
  try {
    const require = createRequire(import.meta.url);
    const Database = require('better-sqlite3');
    return new Database(DB_PATH, { readonly: true });
  } catch (err) {
    console.error(`Error opening ${DB_PATH} read-only: ${err.message.split('\n')[0]}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
const [,, command, ...args] = process.argv;

function parseFlags(args) {
  const flags = {};
  const positional = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const key = args[i].slice(2);
      // `|| true` would coerce an explicit empty value (--tags "") to boolean
      // true, which better-sqlite3 refuses to bind. Empty string is a valid
      // value (e.g. stripping all tags); only a MISSING value means boolean flag.
      flags[key] = args[i + 1] !== undefined ? args[i + 1] : true;
      i++;
    } else {
      positional.push(args[i]);
    }
  }
  return { flags, positional };
}

/**
 * Print the WHOLE result object. Used by the dependency commands, whose
 * payloads carry meaning outside `rows` — `listDependencies` has no `rows` key
 * at all (its data is blockedBy/blocking), and listStartable/listBlocked carry
 * `danglingEdges`, `staleBlockedStatus` and `releasedByTrigger` beside theirs.
 * Routing those through printResult printed the summary sentence and silently
 * dropped the data, and dropping `danglingEdges` in particular would violate
 * the reporting rule that suspended edges are never swallowed (plan §12.3).
 */
function printFull(result) {
  if (result.error) {
    if (result.error.message) console.error(result.error.message);
    else console.error(JSON.stringify(result.error, null, 2));
    process.exit(1);
  }
  console.log(JSON.stringify(result, null, 2));
}

/** Print result — if it has rows, JSON-stringify them; otherwise print message. */
function printResult(result) {
  if (result.error) {
    if (result.error.message) console.error(result.error.message);
    else console.error(JSON.stringify(result.error, null, 2));
    process.exit(1);
  }
  if (result.rows !== undefined) {
    console.log(JSON.stringify(result.rows, null, 2));
  } else if (result.message) {
    console.log(result.message);
  } else {
    console.log(JSON.stringify(result, null, 2));
  }
}

switch (command) {
  case 'init': {
    const schemaPath = join(__dirname, 'pib-db-schema.sql');
    const result = lib.init(getDb(), { schemaPath });
    result.message = `Database initialized at ${DB_PATH}`;
    printResult(result);
    break;
  }
  case 'query':
    printResult(lib.query(getDb(), { sql: args.join(' ') }));
    break;
  case 'create-action': {
    const { flags, positional } = parseFlags(args);
    const result = lib.createAction(getDb(), { text: positional[0], ...flags });
    printResult(result);
    break;
  }
  case 'list-actions': {
    const { flags } = parseFlags(args);
    printResult(lib.listActions(getDb(), flags));
    break;
  }
  case 'update-action': {
    const { flags, positional } = parseFlags(args);
    printResult(lib.updateAction(getDb(), { fid: positional[0], ...flags }));
    break;
  }
  case 'complete-action':
    printResult(lib.completeAction(getDb(), { fid: args[0] }));
    break;
  case 'defer-with-trigger': {
    const { flags, positional } = parseFlags(args);
    printResult(lib.deferWithTrigger(getDb(), {
      fid: positional[0],
      triggerCondition: flags.trigger,
      cascade: flags.cascade === true,
    }));
    break;
  }
  case 'list-triggered': {
    const { flags } = parseFlags(args);
    printResult(lib.listTriggered(getDb(), { includeDone: flags['include-done'] === true }));
    break;
  }
  case 'mark-trigger-checked': {
    const { flags, positional } = parseFlags(args);
    printResult(lib.markTriggerChecked(getDb(), {
      fid: positional[0],
      result: flags.result,
      notes: typeof flags.notes === 'string' ? flags.notes : undefined,
    }));
    break;
  }
  case 'add-event': {
    const { flags } = parseFlags(args);
    // allowedAuthors comes from the caller (the engagement-sync skill reads
    // engagement.yaml and passes the recipient ids + 'consultant' here). The
    // CLI itself stays config-agnostic. Absent flag → undefined → fail-closed.
    const allowedAuthors = typeof flags['allowed-authors'] === 'string'
      ? flags['allowed-authors'].split(',').map(s => s.trim()).filter(Boolean)
      : undefined;
    printResult(lib.addEngagementEvent(getDb(), {
      engagement: flags.engagement,
      target_fid: typeof flags.target === 'string' ? flags.target : undefined,
      packet_id: typeof flags['packet-id'] === 'string' ? flags['packet-id'] : undefined,
      kind: flags.kind,
      author: flags.author,
      verdict: typeof flags.verdict === 'string' ? flags.verdict : undefined,
      body: typeof flags.body === 'string' ? flags.body : undefined,
    }, allowedAuthors));
    break;
  }
  case 'list-events': {
    const { flags } = parseFlags(args);
    printResult(lib.listEngagementEvents(getDb(), {
      engagement: typeof flags.engagement === 'string' ? flags.engagement : undefined,
      target_fid: typeof flags.target === 'string' ? flags.target : undefined,
      unaddressedOnly: flags['unaddressed-only'] === true,
      excludeSoftDeleted: flags['exclude-soft-deleted'] === true,
    }));
    break;
  }
  case 'mark-event-addressed': {
    const { positional } = parseFlags(args);
    printResult(lib.markEventAddressed(getDb(), { id: positional[0] }));
    break;
  }
  case 'create-project': {
    const { flags, positional } = parseFlags(args);
    printResult(lib.createProject(getDb(), { name: positional[0], ...flags }));
    break;
  }
  case 'update-project': {
    const fid = args[0];
    const { flags } = parseFlags(args.slice(1));
    printResult(lib.updateProject(getDb(), { fid, ...flags }));
    break;
  }
  case 'complete-project': {
    // The walk (plan §16). A disposition map for a large project cannot ride
    // argv, so it arrives as a JSON file (--map-file) or an inline JSON
    // string (--map). No flag = sheet mode, which mutates nothing.
    const { flags, positional } = parseFlags(args);
    let map;
    if (typeof flags['map-file'] === 'string') {
      let raw;
      try {
        raw = readFileSync(flags['map-file'], 'utf-8');
      } catch (e) {
        console.error(`Could not read --map-file ${flags['map-file']}: ${e.message}`);
        process.exit(1);
      }
      try {
        map = JSON.parse(raw);
      } catch (e) {
        console.error(`--map-file ${flags['map-file']} is not valid JSON: ${e.message}`);
        process.exit(1);
      }
    } else if (typeof flags.map === 'string') {
      try {
        map = JSON.parse(flags.map);
      } catch (e) {
        console.error(`--map is not valid JSON: ${e.message}`);
        process.exit(1);
      }
    }
    // printFull, not printResult: the sheet's openChildren and the apply
    // result's per-bucket lists are the payload — the summary sentence alone
    // would drop exactly the data a per-child answer needs.
    printFull(lib.completeProject(getDb(), { fid: positional[0], map }));
    break;
  }
  case 'list-projects':
    printResult(lib.listProjects(getDb()));
    break;
  case 'add-dependency': {
    const { positional } = parseFlags(args);
    printResult(lib.addDependency(getDb(), { blockerFid: positional[0], blockedFid: positional[1] }));
    break;
  }
  case 'remove-dependency': {
    const { positional } = parseFlags(args);
    printResult(lib.removeDependency(getDb(), { blockerFid: positional[0], blockedFid: positional[1] }));
    break;
  }
  case 'list-dependencies': {
    const { positional } = parseFlags(args);
    printFull(lib.listDependencies(getDb(), { fid: positional[0] }));
    break;
  }
  case 'next-actions': {
    const { flags } = parseFlags(args);
    printFull(lib.listStartable(getDb(), {
      project: typeof flags.project === 'string' ? flags.project : undefined,
      limit: flags.limit !== undefined ? parseInt(flags.limit, 10) : undefined,
    }));
    break;
  }
  case 'list-blocked': {
    const { flags } = parseFlags(args);
    printFull(lib.listBlocked(getDb(), { project: typeof flags.project === 'string' ? flags.project : undefined }));
    break;
  }
  case 'propose-dependencies':
    // Read-only handle by construction — this command proposes and never writes.
    printFull(lib.proposeDependencyBackfill(getReadOnlyDb()));
    break;
  case 'ingest-findings':
    printResult(lib.ingestFindings(getDb(), { runDir: args[0] }));
    break;
  case 'triage':
    printResult(lib.triage(getDb(), { findingId: args[0], status: args[1], notes: args.slice(2).join(' ') || undefined }));
    break;
  case 'triage-history':
    printResult(lib.triageHistory(getDb()));
    break;
  default:
    console.log(`Usage: pib-db.mjs <command>

Commands:
  init                              Create/migrate the database
  query "SQL"                       Run a SQL query
  create-action "text" [--projectFid X] [--due X] [--notes X]  Create an action
  list-actions [--status X] [--project X]  List actions (default: open)
  update-action <fid> [--status X] [--text X] [--tags X] [--notes X] [--projectFid prj:X]
                                    (--projectFid reparents; the target project must exist)
  complete-action <fid>             Mark action complete (status=done)
  defer-with-trigger <fid> --trigger "<text>" [--cascade]
                                    Defer with a return condition
  list-triggered [--include-done]   List items waiting on triggers
  mark-trigger-checked <fid> --result <value> [--notes "<text>"]
                                    Record trigger evaluation (triggered|still-waiting|needs-info|condition-obsolete)
  add-event --engagement prj:X --kind <kind> --author <a> --allowed-authors "a,b,c"
            [--target act:Y] [--packet-id P] [--verdict approve|object|comment|none] [--body "text"]
                                    Append an engagement event (kinds: client_feedback,status_push,delegation,approval,note,packet_sent)
  list-events [--engagement prj:X] [--target act:Y] [--unaddressed-only] [--exclude-soft-deleted]
                                    List engagement events (newest first)
  mark-event-addressed <id>         Mark an engagement event addressed
  add-dependency <blocker-fid> <blocked-fid>
                                    State that <blocker> must finish before <blocked> can start
  remove-dependency <blocker-fid> <blocked-fid>
                                    Remove that edge (a COMPLETED blocker needs no removal —
                                    blockedness is derived, so completing it already unblocks)
  list-dependencies <fid>           What this action waits on, and what waits on it
  next-actions [--project X] [--limit N]
                                    What can I start right now (no unfinished blocker,
                                    no unfired trigger) — the derived answer
  list-blocked [--project X]        What is waiting, and on what
  propose-dependencies              Propose edges from dependencies written as prose.
                                    Read-only: prints proposals, writes nothing
  create-project "name" [--kind area|project] [--notes X] [--tags X]
                                    Create a container. Default kind: area (never
                                    completes). kind=project is an outcome that
                                    completes — notes must state it
  update-project <fid> [--kind area|project] [--tags X] [--name X] [--status X] [--notes X] [--due X]
                                    (--kind reclassifies; done refused for areas
                                    and for projects with open children)
  complete-project <fid> [--map-file map.json | --map '<json>']
                                    Complete a project by walking its open children.
                                    No map: prints the walk sheet (mutates nothing;
                                    closes outright only when no children are open).
                                    Map: [{fid, seen_status, answer}] covering EVERY
                                    open child — answer is parent-outcome |
                                    reparent:<prj-fid> | delete. All-or-nothing,
                                    ends at the same completion gate
  list-projects                     List active containers (each row carries kind)
  ingest-findings <run-dir>         Ingest audit findings from a run directory
  triage <finding-id> <status>      Triage a finding (approved/rejected/deferred/fixed)
  triage-history                    Output suppression list as JSON

Environment:
  PIB_DB_PATH   Path to SQLite file (default: ./pib.db)`);
    if (command) process.exit(1);
}

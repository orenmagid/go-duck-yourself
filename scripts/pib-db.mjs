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
//   node scripts/pib-db.mjs list-actions --unfiled      # Actions with NO container
//   node scripts/pib-db.mjs update-action act:abc --status in-progress
//   node scripts/pib-db.mjs complete-action act:abc123
//   node scripts/pib-db.mjs write-breadcrumb act:abc123 --summary "<what you verified>"
//   node scripts/pib-db.mjs create-project "My Area"                          # area (default): never completes
//   node scripts/pib-db.mjs create-project "Ship X" --kind project --notes "done when …" --parent prj:abc
//                                                     # …and the AREA it serves
//   node scripts/pib-db.mjs complete-project prj:abc                          # walk sheet (no mutation) or close if no open children
//   node scripts/pib-db.mjs complete-project prj:abc --map-file map.json      # apply the per-child disposition map
//   node scripts/pib-db.mjs list-projects               # Active projects
//   node scripts/pib-db.mjs defer-with-trigger act:abc --trigger "<text>" [--cascade]
//   node scripts/pib-db.mjs add-dependency act:blocker act:blocked
//   node scripts/pib-db.mjs next-actions [--project prj:abc] [--limit 20]
//   node scripts/pib-db.mjs list-blocked [--project prj:abc]
//   node scripts/pib-db.mjs propose-dependencies      # read-only backfill proposals
//   node scripts/pib-db.mjs list-triggered [--include-done] [--include-released]
//   node scripts/pib-db.mjs mark-trigger-checked act:abc --result <value> [--notes "<text>"]
//   node scripts/pib-db.mjs add-event --engagement prj:abc --kind note --author consultant --allowed-authors "consultant,ed"
//   node scripts/pib-db.mjs list-events [--engagement prj:abc] [--target act:xyz] [--unaddressed-only] [--exclude-soft-deleted]
//   node scripts/pib-db.mjs mark-event-addressed <id>
//   node scripts/pib-db.mjs ingest-findings <run-dir>   # Ingest audit findings
//   node scripts/pib-db.mjs triage <finding-id> <status> [notes]
//   node scripts/pib-db.mjs triage-history              # Suppression list JSON
//   node scripts/pib-db.mjs verify-schema               # Shape vs stamp check (read-only)
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
import { resolvePibDbPath, resolveVerificationDir } from './pib-db-path.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = resolvePibDbPath();

// ---------------------------------------------------------------------------
// SQLite setup — try better-sqlite3
// ---------------------------------------------------------------------------
let db;

function getDb() {
  if (db) return db;
  // The CONNECTION, in its own try: this catch diagnoses better-sqlite3, so
  // only better-sqlite3 failures may reach it. The migration below is
  // deliberately OUTSIDE it — a schema refusal told to "npm install
  // better-sqlite3" sends the reader to fix a module that is already fine,
  // and burying the diagnosis is the exact failure mode act:b7d81b52 is
  // about.
  try {
    const require = createRequire(import.meta.url);
    const Database = require('better-sqlite3');
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
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

  // Apply any pending migrations. Cheap: short-circuits when user_version
  // is already current. Skipped only for `init` which runs init() itself
  // (which also calls migrate internally). A fail-closed refusal prints its
  // own full diagnosis verbatim — it already names the cause, the
  // contradictions, and the fix.
  if (process.argv[2] !== 'init') {
    try {
      lib.migrate(db);
    } catch (err) {
      console.error(err.message);
      console.error('');
      console.error(`  database: ${DB_PATH}`);
      console.error('  Inspect without migrating: node scripts/pib-db.mjs verify-schema');
      process.exit(1);
    }
  }
  return db;
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
 * The flags each verb accepts — the ONE allowlist (act:7497dd9b).
 *
 * parseFlags accepts any `--key value`, and each verb spreads the result into
 * a library call that destructures only the names it knows. Without this
 * table a misspelled flag vanished silently: `create-action … --project prj:X`
 * printed "Created action" and filed the action UNFILED (the real spelling is
 * --projectFid), invisible to every container view. A verb listed here
 * REFUSES any other flag by name before the database is opened, so nothing is
 * written. Every flag in the usage text must appear in its verb's list —
 * test/pib-db-cli-flags/ parses the usage block and fails on a gap, so a new
 * flag cannot ship without its entry. Names beyond the usage text (text,
 * flagged, parentFid, client_*) are spellings the spread has always accepted;
 * they stay so existing callers keep working. The engagement overlay carries
 * this table byte-identical, plus its own event verbs.
 */
const KNOWN_FLAGS = {
  'create-action': ['projectFid', 'due', 'notes', 'needs', 'text'],
  'get-action': [],
  'list-actions': ['status', 'project', 'scope', 'unfiled'],
  'update-action': ['status', 'text', 'tags', 'notes', 'due', 'flagged', 'projectFid', 'needs', 'client_title', 'client_body', 'client_generated_at', 'client_generated_status'],
  'complete-action': [],
  'write-breadcrumb': ['summary'],
  'defer-with-trigger': ['trigger', 'cascade'],
  'list-triggered': ['include-done', 'include-released'],
  'mark-trigger-checked': ['result', 'notes'],
  'create-project': ['kind', 'notes', 'tags', 'parent', 'due', 'parentFid'],
  'update-project': ['kind', 'tags', 'name', 'status', 'notes', 'due', 'parent', 'parentFid', 'client_title', 'client_body', 'client_generated_at', 'client_generated_status'],
  'complete-project': ['map-file', 'map'],
  'add-dependency': [],
  'remove-dependency': [],
  'list-dependencies': [],
  'next-actions': ['project', 'limit', 'scope'],
  'list-blocked': ['project', 'scope'],
  // Engagement overlay verbs (not in the base CLI).
  'add-event': ['engagement', 'target', 'packet-id', 'kind', 'author', 'verdict', 'body', 'allowed-authors'],
  'list-events': ['engagement', 'target', 'unaddressed-only', 'exclude-soft-deleted'],
  'mark-event-addressed': [],
};

function editDistance(a, b) {
  const row = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) {
    let prev = row[0];
    row[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const cur = row[j];
      row[j] = Math.min(row[j] + 1, row[j - 1] + 1, prev + (a[i - 1] === b[j - 1] ? 0 : 1));
      prev = cur;
    }
  }
  return row[b.length];
}

/**
 * The known flag an unknown one most likely meant, or null. Flag names are
 * single identifiers, so title-token matching does not fit them; a shared
 * prefix of 4+ characters (`project` → projectFid, `projet` → projectFid) or
 * an edit distance of 2 or less (`sumary` → summary) counts as a match, the
 * closest edit distance winning.
 */
function nearestFlag(name, known) {
  const n = name.toLowerCase();
  let best = null;
  for (const k of known) {
    const kl = k.toLowerCase();
    let prefix = 0;
    while (prefix < n.length && prefix < kl.length && n[prefix] === kl[prefix]) prefix++;
    const dist = editDistance(n, kl);
    if (prefix < 4 && dist > 2) continue;
    if (!best || dist < best.dist) best = { k, dist };
  }
  return best ? best.k : null;
}

/**
 * A due date must be a real calendar day in YYYY-MM-DD form. The column has
 * no CHECK at all, so `--due 2026-13-45` used to be stored as typed. An empty
 * value stays legal: it means "no due date" on create and clears it on update.
 */
function dueError(value) {
  if (value === undefined || value === '') return null;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return `--due must be a date written YYYY-MM-DD (got ${value === true ? 'no value' : `\`${value}\``})`;
  }
  const d = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== value) {
    return `--due ${value} is not a real calendar date`;
  }
  return null;
}

/**
 * parseFlags, then refuse — before any database is opened — a flag this verb
 * does not know, or a malformed --due. Exits 1; nothing is written.
 */
function parseVerbFlags(args) {
  const parsed = parseFlags(args);
  const known = KNOWN_FLAGS[command];
  if (known) {
    const unknown = Object.keys(parsed.flags).filter((k) => !known.includes(k));
    if (unknown.length) {
      for (const k of unknown) {
        const guess = nearestFlag(k, known);
        console.error(`Unknown flag --${k} for ${command}${guess ? ` (did you mean --${guess}?)` : ''}. Nothing was written.`);
      }
      console.error(known.length
        ? `  ${command} accepts: ${known.map((k) => `--${k}`).join(' ')}`
        : `  ${command} takes no flags.`);
      process.exit(1);
    }
  }
  const dueMsg = dueError(parsed.flags.due);
  if (dueMsg) {
    console.error(`${command}: ${dueMsg}. Nothing was written.`);
    process.exit(1);
  }
  return parsed;
}

/**
 * Read a BARE BOOLEAN switch straight from argv, before parseFlags sees it.
 *
 * parseFlags assigns the NEXT TOKEN as any flag's value (it has to — an empty
 * `--tags ""` is a legitimate value), so `--unfiled --project prj:x` yields
 * `{ unfiled: '--project' }` with `project` never set and the fid dropped into
 * positionals. The conflict refusal could not fire, and the operator would
 * silently get an unfiled list with their filter vanished.
 *
 * The house `=== 'true' || '1' || true` coercion does not rescue that call
 * either: it maps every non-matching value to FALSE and never refuses, so the
 * same command returns the FULL unfiltered list instead — the same silent
 * wrong answer, arguably worse. Hence reading argv directly and REFUSING
 * anything that is not a bare switch.
 *
 * Returns `{ value: true | undefined }` or `{ error: '<message>' }`.
 */
function readBareSwitch(argv, name) {
  const token = `--${name}`;
  const i = argv.indexOf(token);
  if (i === -1) return { value: undefined };
  const next = argv[i + 1];
  // Nothing after it, or another flag after it: a bare switch either way.
  if (next === undefined || next.startsWith('--')) return { value: true };
  if (next === 'true' || next === '1') return { value: true };
  return {
    error: `${token} is a switch, not a value — write \`${token}\` on its own (\`${token} true\` and \`${token} 1\` are also accepted). Got \`${token} ${next}\`. To turn it OFF, leave ${token} out entirely; there is no false form, because a silently-false switch is how a filter disappears without anyone noticing.`,
  };
}

/**
 * argv with a bare switch (and its explicit true/1 value) removed, so
 * parseFlags reads everything after it correctly instead of swallowing the
 * next flag as this one's value.
 */
function withoutBareSwitch(argv, name) {
  const token = `--${name}`;
  const i = argv.indexOf(token);
  if (i === -1) return argv;
  const next = argv[i + 1];
  const drop = next === 'true' || next === '1' ? 2 : 1;
  return [...argv.slice(0, i), ...argv.slice(i + drop)];
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
    const { flags, positional } = parseVerbFlags(args);
    const result = lib.createAction(getDb(), { text: positional[0], ...flags });
    printResult(result);
    break;
  }
  case 'get-action': {
    // Read-only ON PURPOSE, twice over. getDb() runs lib.migrate() on every
    // non-init command, so a "lookup" through it is a WRITE path; and a
    // default-flags open of a missing file CREATES it (measured: a wrong
    // PIB_DB_PATH materialised pib.db + -wal + -shm before failing). A lookup
    // must be able to say "not there" without changing anything.
    const { positional } = parseVerbFlags(args);
    const fid = positional[0];
    if (!fid) {
      console.error('get-action: a fid is required (e.g. get-action act:df8e90e7)');
      process.exit(1);
    }
    const result = lib.getAction(getReadOnlyDb(), { fid });
    if (result.error) {
      // NOT the usage banner. A miss is a real answer, and printing help for it
      // reads as "the command is wrong" rather than "the action is not there".
      console.error(`get-action: ${result.error}`);
      process.exit(1);
    }
    printResult(result);
    break;
  }
  case 'list-actions': {
    // --unfiled is read off argv FIRST and stripped, so `--unfiled --project
    // prj:x` reaches the library as { unfiled: true, project: 'prj:x' } and
    // gets the library's teaching refusal — instead of parseFlags eating the
    // --project token and dropping the fid into positionals.
    const unfiled = readBareSwitch(args, 'unfiled');
    if (unfiled.error) {
      console.error(`list-actions: ${unfiled.error}`);
      process.exit(1);
    }
    const { flags } = parseVerbFlags(withoutBareSwitch(args, 'unfiled'));
    printResult(lib.listActions(getDb(), { ...flags, ...(unfiled.value ? { unfiled: true } : {}) }));
    break;
  }
  case 'update-action': {
    const { flags, positional } = parseVerbFlags(args);
    printResult(lib.updateAction(getDb(), { fid: positional[0], ...flags }));
    break;
  }
  case 'complete-action': {
    const { positional } = parseVerbFlags(args);
    printResult(lib.completeAction(getDb(), { fid: positional[0] }));
    break;
  }
  case 'write-breadcrumb': {
    // The path is COMPUTED from the fid and resolved to the MAIN checkout —
    // the spelling the completion gate reads (act:c17510a8). The caller types
    // the fid and the summary, never the filename.
    const { flags, positional } = parseVerbFlags(args);
    printResult(lib.writeBreadcrumb(getDb(), {
      fid: positional[0],
      summary: typeof flags.summary === 'string' ? flags.summary : undefined,
      verifyDir: resolveVerificationDir(),
    }));
    break;
  }
  case 'defer-with-trigger': {
    const { flags, positional } = parseVerbFlags(args);
    printResult(lib.deferWithTrigger(getDb(), {
      fid: positional[0],
      triggerCondition: flags.trigger,
      cascade: flags.cascade === true,
    }));
    break;
  }
  case 'list-triggered': {
    const { flags } = parseVerbFlags(args);
    printResult(lib.listTriggered(getDb(), { includeDone: flags['include-done'] === true, includeReleased: flags['include-released'] === true }));
    break;
  }
  case 'mark-trigger-checked': {
    const { flags, positional } = parseVerbFlags(args);
    printResult(lib.markTriggerChecked(getDb(), {
      fid: positional[0],
      result: flags.result,
      notes: typeof flags.notes === 'string' ? flags.notes : undefined,
    }));
    break;
  }
  case 'add-event': {
    const { flags } = parseVerbFlags(args);
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
    const { flags } = parseVerbFlags(args);
    printResult(lib.listEngagementEvents(getDb(), {
      engagement: typeof flags.engagement === 'string' ? flags.engagement : undefined,
      target_fid: typeof flags.target === 'string' ? flags.target : undefined,
      unaddressedOnly: flags['unaddressed-only'] === true,
      excludeSoftDeleted: flags['exclude-soft-deleted'] === true,
    }));
    break;
  }
  case 'mark-event-addressed': {
    const { positional } = parseVerbFlags(args);
    printResult(lib.markEventAddressed(getDb(), { id: positional[0] }));
    break;
  }
  case 'create-project': {
    const { flags, positional } = parseVerbFlags(args);
    // --parent is the operator-facing spelling of parentFid (the AREA this
    // project serves). Spread alone would only accept --parentFid.
    const parentFid = typeof flags.parent === 'string' ? flags.parent : undefined;
    printResult(lib.createProject(getDb(), { name: positional[0], ...flags, ...(parentFid !== undefined ? { parentFid } : {}) }));
    break;
  }
  case 'update-project': {
    const fid = args[0];
    const { flags } = parseVerbFlags(args.slice(1));
    // --parent <area-fid> names the area; --parent "" CLEARS it. The empty
    // string must stay distinguishable from "not mentioned", because clearing
    // the parent is exactly what demoting a nested project back to an area
    // requires in the same call.
    const parentFid = typeof flags.parent === 'string' ? flags.parent : undefined;
    printResult(lib.updateProject(getDb(), { fid, ...flags, ...(parentFid !== undefined ? { parentFid: parentFid || null } : {}) }));
    break;
  }
  case 'complete-project': {
    // The walk (plan §16). A disposition map for a large project cannot ride
    // argv, so it arrives as a JSON file (--map-file) or an inline JSON
    // string (--map). No flag = sheet mode, which mutates nothing.
    const { flags, positional } = parseVerbFlags(args);
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
    const { positional } = parseVerbFlags(args);
    printResult(lib.addDependency(getDb(), { blockerFid: positional[0], blockedFid: positional[1] }));
    break;
  }
  case 'remove-dependency': {
    const { positional } = parseVerbFlags(args);
    printResult(lib.removeDependency(getDb(), { blockerFid: positional[0], blockedFid: positional[1] }));
    break;
  }
  case 'list-dependencies': {
    const { positional } = parseVerbFlags(args);
    printFull(lib.listDependencies(getDb(), { fid: positional[0] }));
    break;
  }
  case 'next-actions': {
    const { flags } = parseVerbFlags(args);
    printFull(lib.listStartable(getDb(), {
      project: typeof flags.project === 'string' ? flags.project : undefined,
      limit: flags.limit !== undefined ? parseInt(flags.limit, 10) : undefined,
      scope: typeof flags.scope === 'string' ? flags.scope : undefined,
    }));
    break;
  }
  case 'list-blocked': {
    const { flags } = parseVerbFlags(args);
    printFull(lib.listBlocked(getDb(), {
      project: typeof flags.project === 'string' ? flags.project : undefined,
      scope: typeof flags.scope === 'string' ? flags.scope : undefined,
    }));
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
  case 'verify-schema': {
    // The post-install self-check (act:b7d81b52). Deliberately NOT routed
    // through getDb(): getDb() runs migrate(), and this command's whole job
    // is to inspect a database the migration runner might refuse. Read-only
    // by construction; exits 1 when the shape and the stamp disagree.
    const res = lib.verifySchema(getReadOnlyDb());
    console.log(JSON.stringify(res, null, 2));
    if (!res.ok) process.exit(1);
    break;
  }
  default:
    console.log(`Usage: pib-db.mjs <command>

Commands:
  init                              Create/migrate the database
  query "SQL"                       Run a SQL query
  create-action "text" [--projectFid X] [--due X] [--notes X] [--needs X]
                                    Create an action. --needs types the work at
                                    filing time, when the type is actually known
                                    (see update-action for the vocabulary).
  get-action <fid>                  Print one action (read-only; exit 1 if absent)
  list-actions [--status X] [--project X] [--scope direct|rolled-up] [--unfiled]
                                    List actions (default: open). --scope rolled-up
                                    reads an AREA as including its projects' actions.
                                    --unfiled lists the actions with NO container —
                                    the ones no container view can reach. It is a bare
                                    switch and cannot be combined with --project
                                    (the pair is refused, never silently empty).
  update-action <fid> [--status X] [--text X] [--tags X] [--notes X] [--projectFid prj:X] [--needs X]
                                    (--projectFid reparents; the target project must exist)
                                    --needs is a CLOSED vocabulary — one of
                                    code-fix | qa-verify | decision | operator | event-gated
                                    (or null to clear). Anything else is refused.
  complete-action <fid>             Mark action complete (status=done)
  write-breadcrumb <fid> --summary "<what you verified>"
                                    Write the verification breadcrumb the completion
                                    gate reads, at the path computed from the fid
                                    (MAIN checkout's .claude/verification/<fid>.json —
                                    never type it). Without --summary it records
                                    ac_verified:false and the gate still refuses.
                                    Refuses an unknown fid and a done action
  defer-with-trigger <fid> --trigger "<text>" [--cascade]
                                    Defer with a return condition
  list-triggered [--include-done] [--include-released]
                                    Items STILL waiting on a trigger (the last recorded
                                    check did not release them). --include-released also
                                    shows those whose trigger already fired
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
  next-actions [--project X] [--limit N] [--scope direct|rolled-up]
                                    What can I start right now (no unfinished blocker,
                                    no unfired trigger) — the derived answer
  list-blocked [--project X] [--scope direct|rolled-up]
                                    What is waiting, and on what
  propose-dependencies              Propose edges from dependencies written as prose.
                                    Read-only: prints proposals, writes nothing
  create-project "name" [--kind area|project] [--notes X] [--tags X] [--parent prj:X]
                                    Create a container. Default kind: area (never
                                    completes). kind=project is an outcome that
                                    completes — notes must state it
  update-project <fid> [--kind area|project] [--tags X] [--name X] [--status X] [--notes X] [--due X] [--parent prj:X | --parent ""]
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
  list-projects                     List active containers (kind, the area each sits
                                    in, and both open counts: direct and rolled up)
  ingest-findings <run-dir>         Ingest audit findings from a run directory
  triage <finding-id> <status>      Triage a finding (approved/rejected/deferred/fixed)
  triage-history                    Output suppression list as JSON
  verify-schema                     Check the schema shape against this library's
                                    expectations (read-only; exits 1 on mismatch)

Environment:
  PIB_DB_PATH   Path to SQLite file (default: ./pib.db)`);
    if (command) process.exit(1);
}

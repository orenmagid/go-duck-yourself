/**
 * Work tracker server — serves the work tracker UI and provides
 * read/write access to pib.db (projects and actions).
 *
 * Usage: node work-tracker-server.mjs [--port 3458] [--db path/to/pib.db]
 */

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { basename, dirname, join, resolve } from 'node:path';
import Database from 'better-sqlite3';
import * as lib from './pib-db-lib.mjs';
import { resolvePibDbPath } from './pib-db-path.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PREFERRED_PORT = parseInt(process.env.PORT || process.argv.find((_, i, a) => a[i - 1] === '--port') || '3458');
// --db is the explicit override; otherwise route through the SINGLE pib.db
// resolver (act:5380d35b) so a linked worktree opens the main checkout's db,
// exactly like the CLI and MCP server. PIB_DB_PATH is honored inside it.
const dbFlag = process.argv.find((_, i, a) => a[i - 1] === '--db');
const DB_PATH = dbFlag ? resolve(dbFlag) : resolvePibDbPath({ cwd: process.cwd(), env: process.env });

if (!existsSync(DB_PATH)) {
  console.error(`Database not found: ${DB_PATH}`);
  console.error('Run: node scripts/pib-db.mjs init');
  process.exit(1);
}

// Derive project name from package.json name, else the directory name.
function getProjectName() {
  try {
    const data = JSON.parse(readFileSync(resolve('package.json'), 'utf-8'));
    if (data.name) return data.name;
  } catch {}
  return basename(resolve('.'));
}
const PROJECT_NAME = getProjectName();

const db = new Database(DB_PATH, { readonly: false });
db.pragma('journal_mode = WAL');
lib.migrate(db);

// ---------------------------------------------------------------------------
// The completion contract, enforced HERE (act:cbf19c3f)
//
// act:4e0de399 closed the `pib_update_action --status done` side door and was
// titled "ONE closure path, enforced by construction." A second one survived:
// this server's PATCH route composed its own UPDATE and called no library
// function, so the work tracker's checkbox closed actions through neither
// completeAction nor any gate — and stamped a bare `YYYY-MM-DD` completed_at
// where completeAction writes a full ISO timestamp, making UI closures
// distinguishable from gated ones by accident rather than design. Measured
// across three trackers before the fix: 37 date-only closures, all in April
// 2026, against 1549 gated ones.
//
// The gate cannot be reused as-is: `action-completion-gate.sh` is a PreToolUse
// hook on `mcp__pib-db__pib_complete_action`. It fires on an MCP TOOL CALL. An
// HTTP request is not a tool call, so the hook never sees this route and never
// will. Operator ruling 2026-08-09 chose delegation over refusal: the checkbox
// keeps working, and starts requiring what every other closure requires. So the
// contract is re-stated here in the only process that can enforce it, and the
// hook is its SIBLING — the two must stay in step.
//
// Deliberately NO WEAKER than the hook: a plan carrying a `grp:` tag is refused
// outright rather than half-checked, because its extra proof (the group
// Completion Report verdict) belongs to /execute-group, which marks its own
// plans done.
//
// Breadcrumbs are resolved BESIDE THE DATABASE, not from git. resolvePibDbPath
// already sends a linked worktree to its main checkout's db, so dirname(DB_PATH)
// IS the main checkout in every real invocation — and unlike a fourth copy of
// the MAIN_RESOLVE git-truth, it keeps the verification record and the rows it
// makes claims about in the same place by construction, including under an
// explicit --db.
//
// Do NOT "fix" this toward the hook's git resolution. The two can only diverge
// under an explicit --db / PIB_DB_PATH pointing outside the repo, and they
// diverge FAIL-CLOSED: the server refuses a close whose breadcrumb exists only
// beside the git main checkout. Keying on git instead would make the server
// gate on breadcrumbs belonging to a database it is not writing.
const VERIFY_DIR = join(dirname(DB_PATH), '.claude', 'verification');

// The only two spellings of a completion intent this route accepts. Everything
// else is refused rather than coerced: a `Number(x) === 1` test reads "true",
// 2, null, "" and [] as NOT-a-close, and the reopen clause below would then
// treat them as a REOPEN — wiping an unrecoverable completed_at and answering
// 200 to a caller who meant the opposite.
function completionIntent(value) {
  if (value === undefined) return 'none';
  if (value === 1 || value === '1' || value === true) return 'close';
  if (value === 0 || value === '0' || value === false) return 'reopen';
  return 'invalid';
}

// Null when the close may proceed; otherwise the refusal to send back. Every
// message routes the caller to the gated path rather than only saying no —
// `pib_defer_with_trigger`'s proven routing-description pattern. Paths are
// named RELATIVE: security-0005 keeps absolute filesystem paths out of response
// bodies, and the relative form is the actionable half anyway.
function closureRefusal(fid) {
  const rel = `.claude/verification/${fid}.json`;

  const action = lib.getAction(db, { fid });
  if (action.error) {
    return { error: 'not_found', message: `No action with fid ${fid}.` };
  }

  const grp = String(action.tags || '')
    .split(',')
    .map((t) => t.trim())
    .find((t) => t.startsWith('grp:'));
  if (grp) {
    const label = grp.slice(4);
    return {
      error: 'grouped_plan',
      message: `${fid} carries ${grp}, so it is completed by /execute-group — that workflow writes the Completion Report proving its checkpoints ran, and marks merged plans done itself. ${label ? `Run /execute-group ${label}, or remove` : 'Remove'} the ${grp} tag if this plan is being finished outside the group flow.`,
    };
  }

  let record;
  try {
    record = JSON.parse(readFileSync(join(VERIFY_DIR, `${fid}.json`), 'utf-8'));
  } catch {
    return {
      error: 'no_verification_record',
      message: `Cannot close ${fid} from the work tracker: there is no verification record at ${rel}. Every closure has to pass the completion gate — read the full spec, check each acceptance criterion against what shipped, then close it through Claude (pib_complete_action) or the CLI (complete-action ${fid}), which writes that record for you.`,
    };
  }
  // `null` and `7` are valid JSON. A breadcrumb that parses to a non-object is
  // not a verification record, and must produce the routing refusal rather than
  // a TypeError caught by the generic 500 handler.
  if (typeof record !== 'object' || record === null || Array.isArray(record)) {
    return {
      error: 'no_verification_record',
      message: `Cannot close ${fid}: ${rel} exists but does not contain a verification record. Close it through Claude (pib_complete_action) or the CLI (complete-action ${fid}), which writes a well-formed one.`,
    };
  }
  if (record.spec_read !== true) {
    return {
      error: 'spec_not_read',
      message: `Cannot close ${fid}: ${rel} exists but records spec_read as unread. Read the action's full notes, then complete it through Claude or the CLI.`,
    };
  }
  if (record.ac_verified !== true) {
    return {
      error: 'ac_not_verified',
      message: `Cannot close ${fid}: its spec was read but the acceptance criteria are not recorded as verified in ${rel}. Verify each criterion against the implementation, then complete it through Claude or the CLI.`,
    };
  }
  return null;
}

function json(res, data, status = 200) {
  // SECURITY (security-0001): no wildcard CORS. The UI is served from this same
  // origin, so same-origin requests need no CORS headers; omitting
  // Access-Control-Allow-Origin means a cross-origin site (any page open in the
  // operator's browser) cannot read pib.db responses or issue a cross-origin
  // PATCH/POST — its preflight finds no allow-origin to match.
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

async function readBody(req) {
  let body = '';
  for await (const chunk of req) body += chunk;
  return JSON.parse(body);
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${server.address()?.port || PREFERRED_PORT}`);

  if (req.method === 'OPTIONS') {
    // No cross-origin support (security-0001): same-origin requests don't
    // preflight, and a cross-origin preflight gets no allow headers back.
    res.writeHead(204);
    return res.end();
  }

  try {
    // Serve HTML
    if (req.method === 'GET' && url.pathname === '/') {
      const html = await readFile(join(__dirname, 'work-tracker-ui.html'), 'utf-8');
      res.writeHead(200, { 'Content-Type': 'text/html' });
      return res.end(html);
    }

    // GET /api/projects — list projects with action counts
    if (req.method === 'GET' && url.pathname === '/api/projects') {
      const status = url.searchParams.get('status') || 'active';
      const rows = db.prepare(`
        SELECT p.*,
          (SELECT COUNT(*) FROM actions a WHERE a.project_fid = p.fid AND a.completed = 0 AND a.deleted_at IS NULL) as open_actions,
          (SELECT COUNT(*) FROM actions a WHERE a.project_fid = p.fid AND a.completed = 1) as done_actions,
          (SELECT COUNT(*) FROM actions a WHERE a.project_fid = p.fid AND a.deleted_at IS NULL) as total_actions
        FROM projects p
        WHERE p.deleted_at IS NULL AND p.status = ?
        ORDER BY p.created DESC
      `).all(status);
      return json(res, rows);
    }

    // GET /api/actions — list actions, optionally filtered
    if (req.method === 'GET' && url.pathname === '/api/actions') {
      const project = url.searchParams.get('project');
      const status = url.searchParams.get('status'); // open, done, all
      let query = `
        SELECT a.*, p.name as project_name
        FROM actions a
        LEFT JOIN projects p ON a.project_fid = p.fid
        WHERE a.deleted_at IS NULL
      `;
      const params = [];

      if (project) {
        query += ' AND a.project_fid = ?';
        params.push(project);
      }
      if (status === 'open') {
        query += ' AND a.completed = 0';
      } else if (status === 'done') {
        query += ' AND a.completed = 1';
      }

      query += ' ORDER BY a.completed ASC, a.flagged DESC, a.sort_order ASC, a.created DESC';
      return json(res, db.prepare(query).all(...params));
    }

    // GET /api/action/:fid — single action with full notes
    if (req.method === 'GET' && url.pathname.startsWith('/api/action/')) {
      const fid = decodeURIComponent(url.pathname.slice('/api/action/'.length));
      const row = db.prepare(`
        SELECT a.*, p.name as project_name
        FROM actions a
        LEFT JOIN projects p ON a.project_fid = p.fid
        WHERE a.fid = ?
      `).get(fid);
      if (!row) return json(res, { error: 'Not found' }, 404);
      return json(res, row);
    }

    // PATCH /api/action/:fid — update action fields
    if (req.method === 'PATCH' && url.pathname.startsWith('/api/action/')) {
      const fid = decodeURIComponent(url.pathname.slice('/api/action/'.length));
      const body = await readBody(req);

      // Validate before anything reaches SQL or a path join, matching
      // /api/trigger-history/:fid. The closure branch below builds a filename
      // from this value.
      if (!lib.FID_PATTERN.test(fid)) {
        return json(res, { error: 'invalid_fid_format', message: `fid must match ${lib.FID_PATTERN}, got "${fid}"` }, 400);
      }

      const intent = completionIntent(body.completed);
      if (intent === 'invalid') {
        return json(res, {
          error: 'ambiguous_completed',
          message: `completed must be 1 (close) or 0 (reopen); got ${JSON.stringify(body.completed)}. Refused rather than guessed — guessing here silently reopened closed actions and reported success.`,
        }, 400);
      }
      // A body cannot say both things at once. Without this, `{status:'done',
      // completed:0}` would close on the status arm while the caller's other
      // arm asked for the opposite.
      if ((body.status === 'done' && intent === 'reopen') ||
          (intent === 'close' && body.status !== undefined && body.status !== 'done')) {
        return json(res, {
          error: 'contradictory_completion',
          message: `status "${body.status}" and completed ${JSON.stringify(body.completed)} disagree about whether ${fid} is being closed. Send one intent.`,
        }, 400);
      }

      // CLOSURE is its own branch and never reaches the generic UPDATE below.
      // It runs the completion contract above, then delegates to the library's
      // ONE closure path — which is also what makes completed_at an ISO
      // timestamp here, identical to every other closure in the system.
      const wantsClose = body.status === 'done' || intent === 'close';
      if (wantsClose) {
        // Closing is its own operation, never a field inside a wider edit. The
        // old route folded both into one UPDATE; this branch returns early, so
        // silently dropping the edits while answering 200 would be a partial
        // write reported as success.
        const extras = Object.keys(body).filter((k) => k !== 'completed' && k !== 'status');
        if (extras.length > 0) {
          return json(res, {
            error: 'close_with_edits',
            message: `Closing ${fid} is its own request and cannot carry other edits (${extras.join(', ')}). Send the edits first, then close.`,
          }, 400);
        }
        const refusal = closureRefusal(fid);
        if (refusal) return json(res, refusal, refusal.error === 'not_found' ? 404 : 403);
        const result = lib.completeAction(db, { fid });
        if (result.error) {
          return json(res, result, result.error === 'not_found' ? 404 : 409);
        }
        return json(res, { ok: true, completed: 1, status: 'done' });
      }

      // `completed` is deliberately NOT in the allowlist: setting it to 1 is a
      // closure (handled above), and clearing it is the reopen clause below.
      // The request body can no longer write that column directly at all.
      const allowed = ['text', 'status', 'flagged', 'notes', 'due', 'tags', 'project_fid'];
      // The pre-write row, read once: the project_fid guard below needs the
      // action's openness, and the reopen teaching needs to know whether this
      // request is a genuine reopen under a done parent.
      const before = db.prepare('SELECT completed, status, project_fid FROM actions WHERE fid = ? AND deleted_at IS NULL').get(fid);
      if (!before) {
        return json(res, { error: 'not_found', message: `No action with fid ${fid} (or it is deleted).` }, 404);
      }
      const isReopen = body.status !== undefined || intent === 'reopen';
      const sets = [];
      const params = [];
      for (const [key, val] of Object.entries(body)) {
        if (!allowed.includes(key)) continue;
        if (key === 'project_fid') {
          // The enter-invariant at its third writer (§16.3): an open action
          // may not ENTER a done container. This column was written raw with
          // no check at all — the one door of the three that had nothing.
          // Openness is judged as what this request leaves behind (a reopen
          // in the same body counts as open).
          const dest = db.prepare('SELECT fid, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(val);
          if (!dest) {
            return json(res, { error: 'bad_project', message: `Container ${JSON.stringify(val)} does not exist (or is deleted). Pass an existing container fid.` }, 400);
          }
          const openAfter = isReopen || before.completed === 0;
          if (openAfter && dest.status === 'done') {
            return json(res, { error: 'done_container', message: `Cannot move ${fid} into ${val}: that container is done, and an open action may not enter a done container. Reopen the container first, or choose a live one.` }, 403);
          }
        }
        sets.push(`${key} = ?`);
        params.push(val);
      }
      // Reopening — an explicit completed:0, or any move to a non-done status —
      // clears BOTH completion columns together, mirroring updateAction, AND
      // the typed resolution (§16.1): this branch is the second of the three
      // reopen-clearing sites, and it is the one the UI checkbox-uncheck and
      // status dropdown actually exercise — without it every UI reopen would
      // strand 'parent-outcome' on an open row, on the surface most looked at.
      if (isReopen) {
        sets.push('completed = 0', 'completed_at = NULL', 'resolution = NULL');
        // A bare `{completed: 0}` carries no status, and clearing the flag
        // without moving status off 'done' lands the row at completed = 0 /
        // status = 'done' — the half-completed state inverted, which
        // updateAction cannot produce because it only ever clears inside a
        // status write. Only touched when the row really is done, so an
        // ordinary edit never clobbers a 'blocked' or 'in-progress'.
        if (body.status === undefined && before.status === 'done') {
          sets.push("status = 'open'");
        }
      }
      if (sets.length === 0) return json(res, { error: 'Nothing to update' }, 400);
      params.push(fid);
      // `deleted_at IS NULL` + a changes check, mirroring updateAction: without
      // them this branch wrote onto tombstoned rows and answered `ok: true` for
      // a fid that does not exist. The closure branch above already refuses
      // both; a route where half the branches guard and half do not is worse
      // than either.
      const result = db
        .prepare(`UPDATE actions SET ${sets.join(', ')} WHERE fid = ? AND deleted_at IS NULL`)
        .run(...params);
      if (result.changes === 0) {
        return json(res, { error: 'not_found', message: `No action with fid ${fid} (or it is deleted).` }, 404);
      }
      // Child reopen under a done project gets a VOICE, never a refusal
      // (§16.4): reopening is an operator correction, but it re-mints
      // done-with-open-children in place, and the active-projects view no
      // longer shows the container now holding live work.
      if (isReopen && before.completed === 1 && body.project_fid === undefined && before.project_fid) {
        const parent = db.prepare('SELECT fid, name, kind, status FROM projects WHERE fid = ? AND deleted_at IS NULL').get(before.project_fid);
        if (parent && parent.kind === 'project' && parent.status === 'done') {
          return json(res, { ok: true, note: `Parent project ${parent.fid} (${parent.name}) is done — this reopen leaves live work under a completed project. Move this action to where the work now belongs, or reopen the project.` });
        }
      }
      return json(res, { ok: true });
    }

    // PATCH /api/project/:fid — update container fields, DELEGATED to the
    // library. This route used to compose its own UPDATE — the same
    // second-writer shape the action checkbox had before act:cbf19c3f, and
    // with the kind model it would have been a bypass around both completion
    // gates (an area completing, a project completing over open children).
    // Delegation retires the second writer: the kind rules, the soft-delete
    // guard, and the ISO completed_at stamp all come from updateProject.
    if (req.method === 'PATCH' && url.pathname.startsWith('/api/project/')) {
      const fid = decodeURIComponent(url.pathname.slice('/api/project/'.length));
      const body = await readBody(req);
      const allowed = ['name', 'status', 'kind', 'notes', 'due', 'tags'];
      const patch = { fid };
      for (const key of allowed) {
        if (body[key] !== undefined) patch[key] = body[key];
      }
      if (Object.keys(patch).length === 1) return json(res, { error: 'Nothing to update' }, 400);
      const result = lib.updateProject(db, patch);
      if (result.error) {
        const msg = typeof result.error === 'string' ? result.error : result.error.message;
        return json(res, { error: msg }, 400);
      }
      return json(res, { ok: true });
    }

    // GET /api/meta — project name and server info
    if (req.method === 'GET' && url.pathname === '/api/meta') {
      return json(res, { projectName: PROJECT_NAME });
    }

    // GET /api/triggered — deferred items with trigger conditions
    if (req.method === 'GET' && url.pathname === '/api/triggered') {
      const includeDone = url.searchParams.get('includeDone') === 'true';
      return json(res, lib.listTriggered(db, { includeDone }));
    }

    // POST /api/trigger-checked — record a trigger evaluation
    if (req.method === 'POST' && url.pathname === '/api/trigger-checked') {
      const body = await readBody(req);
      const result = lib.markTriggerChecked(db, body);
      if (result.error) return json(res, result, 400);
      return json(res, result);
    }

    // GET /api/trigger-history/:fid — recent checks for an item
    if (req.method === 'GET' && url.pathname.startsWith('/api/trigger-history/')) {
      const fid = decodeURIComponent(url.pathname.slice('/api/trigger-history/'.length));
      if (!lib.FID_PATTERN.test(fid)) {
        return json(res, { error: 'invalid_fid_format', message: `fid must match ${lib.FID_PATTERN}, got "${fid}"` }, 400);
      }
      const rows = db.prepare(`
        SELECT checked_at, result, notes
        FROM trigger_checks
        WHERE target_fid = ?
        ORDER BY checked_at DESC
        LIMIT 20
      `).all(fid);
      return json(res, { fid, checks: rows });
    }

    // GET /api/ready — what can I start right now (the derived next-actions list)
    //
    // DELEGATES to the library, exactly as /api/triggered delegates to
    // listTriggered. The startable predicate has six clauses; re-typing it in
    // this handler would put the definition in two places and guarantee drift
    // the first time it is tuned.
    if (req.method === 'GET' && url.pathname === '/api/ready') {
      const project = url.searchParams.get('project') || undefined;
      return json(res, lib.listStartable(db, { project }));
    }

    // GET /api/blocked — the complement: what is waiting, and on what
    if (req.method === 'GET' && url.pathname === '/api/blocked') {
      const project = url.searchParams.get('project') || undefined;
      return json(res, lib.listBlocked(db, { project }));
    }

    // GET /api/dependencies/:fid — both directions for one action
    if (req.method === 'GET' && url.pathname.startsWith('/api/dependencies/')) {
      const fid = decodeURIComponent(url.pathname.slice('/api/dependencies/'.length));
      // Validate the path parameter before it reaches SQL, matching
      // /api/trigger-history/:fid. The older /api/action/:fid routes predate
      // this discipline; new routes do not inherit that.
      if (!lib.FID_PATTERN.test(fid)) {
        return json(res, { error: 'invalid_fid_format', message: `fid must match ${lib.FID_PATTERN}, got "${fid}"` }, 400);
      }
      return json(res, lib.listDependencies(db, { fid }));
    }

    // GET /api/stats — dashboard summary
    if (req.method === 'GET' && url.pathname === '/api/stats') {
      const projects = db.prepare(`
        SELECT status, COUNT(*) as count FROM projects WHERE deleted_at IS NULL GROUP BY status
      `).all();
      const actions = db.prepare(`
        SELECT
          SUM(CASE WHEN completed = 0 THEN 1 ELSE 0 END) as open,
          SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) as done,
          SUM(CASE WHEN flagged = 1 AND completed = 0 THEN 1 ELSE 0 END) as flagged
        FROM actions WHERE deleted_at IS NULL
      `).get();
      return json(res, { projects, actions });
    }

    res.writeHead(404);
    res.end('Not found');
  } catch (err) {
    console.error(err);
    // SECURITY (security-0005): return a generic body — better-sqlite3/Node
    // error messages embed SQL fragments and absolute filesystem paths. The
    // detail stays in the server-side log above.
    json(res, { error: 'Internal server error' }, 500);
  }
});

server.listen(PREFERRED_PORT, '127.0.0.1', () => {
  const actualPort = server.address().port;
  console.log(`Work tracker at http://localhost:${actualPort}`);
  console.log(`Database: ${DB_PATH}`);
  console.log(`Project: ${PROJECT_NAME}`);
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.log(`Port ${PREFERRED_PORT} in use, finding a free port...`);
    server.listen(0, '127.0.0.1', () => {
      const actualPort = server.address().port;
      console.log(`Work tracker at http://localhost:${actualPort}`);
      console.log(`Database: ${DB_PATH}`);
      console.log(`Project: ${PROJECT_NAME}`);
    });
  } else {
    throw err;
  }
});

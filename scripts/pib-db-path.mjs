// Process-in-a-Box — pib.db path resolver (git-worktree aware)
//
// The SINGLE source of truth for "which pib.db does this process open?" Both
// the CLI (pib-db.mjs) and the MCP server (pib-db-mcp-server.mjs) import this
// and call it once at module load, so they resolve identically — there is no
// second spelling and no size heuristic.
//
// Resolution order:
//   1. Explicit PIB_DB_PATH env → returned verbatim. The deliberate override
//      (e.g. a test pointing at a specific db) always wins; git is skipped.
//   2. A linked git worktree (git-common-dir != git-dir) whose main checkout
//      HAS a pib.db → that main pib.db. Work-tracking is project-level: one
//      main db is the source of truth, shared across a project's worktrees.
//   3. Otherwise cwd/pib.db — the historical default, and also the correct
//      answer on the main checkout (where common-dir == git-dir).
//
// Mirrors the canonical SHELL spelling in
// templates/hooks/action-completion-gate.sh:29-35 (a hook can't import JS):
// `git rev-parse --path-format=absolute --git-common-dir`, then
// dirname(common-dir) for the main root. Keep the two in step.
//
// Deliberately a SEPARATE module from pib-db-lib.mjs: the lib is pure
// (db, params) ops and is imported by lib/db-setup.js WITHOUT opening a db
// (it reads SCHEMA_VERSION as text). Folding git-subprocess path logic into
// the lib would break that no-side-effect, no-native-module contract.

import { join, dirname } from 'node:path';
import { existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';

// The ONE git call every resolver here shares. Fail-open: this runs at module
// load of every CLI invocation and every MCP start, so ANY git failure (non-git
// cwd, git off PATH, a stuck index.lock that trips the timeout) returns null —
// never throws, never hangs the importing process.
function gitDirs(cwd) {
  try {
    const out = execFileSync(
      'git',
      ['rev-parse', '--path-format=absolute', '--git-common-dir', '--git-dir'],
      { cwd, timeout: 2000, stdio: ['ignore', 'pipe', 'ignore'], encoding: 'utf8' }
    );
    // --path-format=absolute forces BOTH refs absolute (a bare relative `.git`
    // from the main checkout would make the equality test and dirname() wrong).
    // Output order follows the flag order: common-dir, then git-dir.
    const [commonDir, gitDir] = out.split('\n').map((l) => l.trim()).filter(Boolean);
    if (commonDir && gitDir) return { commonDir, gitDir };
  } catch {
    /* not a git repo, git unavailable, or timed out */
  }
  return null;
}

export function resolvePibDbPath({ cwd = process.cwd(), env = process.env } = {}) {
  // 1. Explicit override wins, verbatim.
  if (env.PIB_DB_PATH) return env.PIB_DB_PATH;

  // 2. Linked worktree → the main checkout's pib.db (only when it exists).
  const dirs = gitDirs(cwd);
  if (dirs && dirs.commonDir !== dirs.gitDir) {
    const mainRoot = dirname(dirs.commonDir); // dirname(<main>/.git) === <main>
    const mainDb = join(mainRoot, 'pib.db');
    if (existsSync(mainDb)) return mainDb; // never redirect to a db that isn't there
  }

  // 3. Default: cwd/pib.db (also the main checkout's own db).
  return join(cwd, 'pib.db');
}

/**
 * The MAIN checkout's root — dirname(git-common-dir) — for any cwd inside a
 * git repository (a linked worktree resolves to main; the main checkout
 * resolves to itself, even from a subdirectory). null outside a repository.
 */
export function resolveMainCheckout({ cwd = process.cwd() } = {}) {
  const dirs = gitDirs(cwd);
  return dirs ? dirname(dirs.commonDir) : null;
}

/**
 * Where verification breadcrumbs live: `<MAIN checkout>/.claude/verification`,
 * falling back to cwd-relative outside a git repository. This is the SAME
 * spelling the completion gate reads (action-completion-gate.sh resolves
 * VERIFY_DIR the same way), which is the whole point of act:c17510a8 — the
 * writer (write-breadcrumb / pib_write_breadcrumb) and the reader (the gate)
 * compute one path; nobody types it.
 */
export function resolveVerificationDir({ cwd = process.cwd() } = {}) {
  return join(resolveMainCheckout({ cwd }) ?? cwd, '.claude', 'verification');
}

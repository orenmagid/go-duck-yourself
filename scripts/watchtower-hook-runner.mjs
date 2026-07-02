#!/usr/bin/env node

// Watchtower consumer hook runner (Plan 9, act:4a6e907c) — the formalized
// extension model for NON-INTERACTIVE consumer customs at the ring lifecycle
// seams.
//
// Background. The three rings already invoke consumer hooks at three seams:
//   - ring1-post-collect  (Ring 1, per-project, after state collection)
//   - ring2-slow-post     (Ring 2 slow tier, portfolio-wide)
//   - ring3-close-post     (Ring 3, per session close)
// Each ring reads `config.hooks[<seam>]` (an array of command strings) and
// runs every entry with the ring's state JSON on stdin. That crude mechanism
// is global (not per-project), has three different timeouts and three
// different result shapes, and has no discovery convention. This runner is
// the single uniform layer that replaces those concerns WITHOUT editing the
// rings: it is registered ONCE per seam (see `--register`), and the rings
// invoke it like any other hook command. The runner then resolves which
// project(s) the invocation is for and runs THAT project's hook scripts.
//
// Registration model — drop a file, it runs (the phase-file ergonomic).
// Consumers register hooks PER-PROJECT by dropping executable scripts into:
//   <project-root>/.claude/watchtower/hooks/<seam>/
// Presence in the directory IS the registration — no config editing. The
// runner discovers them in sorted order (numeric-prefix convention:
// 10-foo.sh before 20-bar.sh) and runs each with timeout isolation, the
// project root as cwd, the ring state JSON on stdin, and WATCHTOWER_* env.
//
// Project resolution per seam:
//   - ring1-post-collect / ring3-close-post: a SINGLE project, taken from the
//     ring's stdin state (`.path`/`.name` for Ring 1, `.project_path`/
//     `.project` for Ring 3).
//   - ring2-slow-post: portfolio-wide — the slow state carries no project, so
//     the runner enumerates `config.projects` and runs each project's dir.
//
// Budget. The runner runs inside the ring's per-command timeout (Ring 1 30s,
// Ring 2 60s, Ring 3 120s). Because the runner is a SINGLE command, all of a
// seam's hooks share that one envelope. The runner budgets accordingly: it
// reserves headroom to print its result envelope before the ring's hard kill,
// runs hooks sequentially against a deadline, and once the budget is spent it
// marks the remaining hooks `skipped` (reason: budget-exhausted) rather than
// letting the ring kill it mid-flight and lose the whole envelope. No silent
// truncation — a skipped hook is reported.
//
// Isolation. A hook failure (non-zero exit, timeout, spawn error) is captured
// into that hook's result and never aborts the loop or the runner. The runner
// is bulletproof: it catches everything, always prints a valid JSON envelope,
// and always exits 0 — so one bad consumer hook can never break a ring tick.
//
// Output contract. The runner prints one JSON envelope to stdout:
//   { schema_version, seam, targets:[{name,path}], hooks:[{hook,status,...}],
//     additional_checks?:[...] }
// `additional_checks` is the Ring 1 passthrough: any hook that prints
// `{ "additional_checks": [...] }` has its checks surfaced at the envelope top
// level, so Ring 1's existing `parsed.additional_checks` consumption keeps
// working unchanged.
//
// Interactive customs are OUT of scope — those are owned by the routine
// dispatch engine (watchtower-routines.mjs, act:c2a55c08). This runner never
// talks to the operator; hooks run unattended.

import {
  existsSync, readdirSync, statSync, accessSync, readFileSync,
  constants as fsConstants,
} from 'fs';
import { join, extname } from 'path';
import { spawnSync } from 'child_process';
import { getWatchtowerDir, loadConfig, atomicWrite } from './watchtower-lib.mjs';

// The three seams the rings actually call (config.json.template declares
// exactly these). ring2-slow-post is portfolio-wide; the other two are
// single-project.
export const SEAMS = ['ring1-post-collect', 'ring2-slow-post', 'ring3-close-post'];
export const PORTFOLIO_SEAMS = new Set(['ring2-slow-post']);

// Outer budget per seam — tracks each ring's HOOK_TIMEOUT_MS so the runner
// finishes within the ring's per-command timeout. If a ring changes its
// timeout, update the matching entry here (recorded in
// watchtower-contracts.md → "Consumer Hook Contract"). An env override lets
// tests shrink the budget and lets a future ring pass its own number.
export const SEAM_BUDGET_MS = {
  'ring1-post-collect': 30_000,
  'ring2-slow-post': 60_000,
  'ring3-close-post': 120_000,
};

// Reserve so the runner can serialize + print its envelope before the ring's
// hard kill, and a floor below which a remaining hook is skipped rather than
// launched with no runway.
const HEADROOM_MS = 2_000;
const MIN_HOOK_MS = 1_000;

const RUNNER_BASENAME = 'watchtower-hook-runner.mjs';

// File extensions we know how to run when the file is not marked executable.
// Anything else (data/docs: .json, .md, .txt, .yaml) is ignored by discovery.
const INTERPRETER_BY_EXT = {
  '.mjs': ['node'],
  '.js': ['node'],
  '.cjs': ['node'],
  '.py': ['python3'],
  '.sh': ['bash'],
  '.bash': ['bash'],
};

function isExecutable(filePath) {
  try {
    accessSync(filePath, fsConstants.X_OK);
    return true;
  } catch {
    return false;
  }
}

// resolveInterpreter — how to invoke a hook file. An executable file is run
// directly (its shebang decides the interpreter); otherwise the extension
// picks one. Returns null for a file we cannot run (discovery skips it).
export function resolveInterpreter(filePath) {
  if (isExecutable(filePath)) return { cmd: filePath, args: [] };
  const interp = INTERPRETER_BY_EXT[extname(filePath).toLowerCase()];
  if (interp) return { cmd: interp[0], args: [...interp.slice(1), filePath] };
  return null;
}

// hooksDir — the per-project convention directory for a seam.
export function hooksDir(projectPath, seam) {
  return join(projectPath, '.claude', 'watchtower', 'hooks', seam);
}

// discoverHooks — sorted runnable hook files in a project's seam directory.
// Skips dotfiles, `_`-prefixed files (disabled convention), and non-runnable
// files (docs/data with no executable bit and no known extension).
export function discoverHooks(projectPath, seam) {
  const dir = hooksDir(projectPath, seam);
  if (!existsSync(dir)) return [];
  let names;
  try {
    names = readdirSync(dir);
  } catch {
    return [];
  }
  const hooks = [];
  for (const name of names.sort()) {
    if (name.startsWith('.') || name.startsWith('_')) continue;
    const path = join(dir, name);
    let st;
    try {
      st = statSync(path);
    } catch {
      continue;
    }
    if (!st.isFile()) continue;
    if (!resolveInterpreter(path)) continue;
    hooks.push({ name, path });
  }
  return hooks;
}

// resolveTargets — which project(s) this seam invocation runs hooks for.
// Single-project seams read the ring's stdin state; the portfolio seam walks
// config.projects. Non-existent paths are dropped (never invented).
export function resolveTargets(seam, state, config) {
  if (PORTFOLIO_SEAMS.has(seam)) {
    const projects = (config && config.projects) || {};
    const targets = [];
    for (const name of Object.keys(projects)) {
      const entry = projects[name];
      const path = (entry && entry.path) || entry;
      if (typeof path === 'string' && existsSync(path)) targets.push({ name, path });
    }
    return targets;
  }
  // Single-project seam — derive from the ring state payload.
  const path = (state && (state.path || state.project_path)) || null;
  const name = (state && (state.name || state.project)) || (path ? null : null);
  if (typeof path === 'string' && path) return [{ name: name || path, path }];
  return [];
}

// buildHookEnv — the ambient environment a hook receives. Inherits the
// process env and adds the WATCHTOWER_* contract vars so a hook can read its
// context without re-parsing stdin.
export function buildHookEnv(state, { seam, target }) {
  return {
    ...process.env,
    WATCHTOWER_SEAM: seam,
    WATCHTOWER_PROJECT: (target && target.name) || (state && (state.name || state.project)) || '',
    WATCHTOWER_PROJECT_PATH: (target && target.path) || (state && (state.path || state.project_path)) || '',
    WATCHTOWER_SESSION_ID: (state && (state.session_id || state.sessionId)) || '',
    WATCHTOWER_HOOK_DIR: (target && seam) ? hooksDir(target.path, seam) : '',
  };
}

// runHook — execute one hook script with isolation, classify the result.
// Never throws: every failure mode becomes a typed status in the result.
export function runHook({ path, cwd, env, input, timeoutMs }) {
  const interp = resolveInterpreter(path);
  if (!interp) return { hook: path, status: 'skipped', reason: 'not-runnable' };

  let res;
  try {
    res = spawnSync(interp.cmd, interp.args, {
      cwd,
      env,
      input: input || '',
      timeout: Math.max(MIN_HOOK_MS, timeoutMs | 0),
      encoding: 'utf8',
      maxBuffer: 10 * 1024 * 1024,
    });
  } catch (e) {
    return { hook: path, status: 'failed', error: e.message };
  }

  if (res.error) {
    const timedOut = res.error.code === 'ETIMEDOUT' || res.signal === 'SIGTERM';
    return {
      hook: path,
      status: timedOut ? 'timeout' : 'failed',
      error: res.error.message,
    };
  }
  if (res.signal === 'SIGTERM') {
    return { hook: path, status: 'timeout', error: `killed after ${timeoutMs}ms` };
  }

  const stderr = (res.stderr || '').trim();
  if (res.status !== 0) {
    return {
      hook: path,
      status: 'failed',
      exit: res.status,
      ...(stderr ? { stderr: stderr.slice(0, 2000) } : {}),
    };
  }

  // Success. Surface structured output when the hook printed JSON.
  const out = (res.stdout || '').trim();
  let parsed = null;
  if (out) {
    try {
      parsed = JSON.parse(out);
    } catch {
      // non-JSON stdout — keep verbatim (truncated)
    }
  }
  const result = { hook: path, status: 'success' };
  if (parsed && typeof parsed === 'object') {
    result.output = parsed;
  } else if (out) {
    result.output = out.slice(0, 2000);
  }
  if (stderr) result.stderr = stderr.slice(0, 2000);
  return result;
}

// runSeam — discover and run all hooks for a seam, budget-managed, and build
// the envelope. `now` and `budgetMs` are injectable for tests.
export function runSeam({ seam, state, config, now, budgetMs }) {
  const targets = resolveTargets(seam, state || {}, config);
  const envelope = {
    schema_version: 1,
    seam,
    targets: targets.map((t) => ({ name: t.name, path: t.path })),
    hooks: [],
  };

  const totalBudget = Number(
    budgetMs != null ? budgetMs
      : (process.env.WATCHTOWER_HOOK_BUDGET_MS || SEAM_BUDGET_MS[seam] || 30_000),
  );
  const startedAt = typeof now === 'function' ? now() : Date.now();
  const deadline = startedAt + Math.max(MIN_HOOK_MS, totalBudget - HEADROOM_MS);
  const clock = typeof now === 'function' ? now : Date.now;

  const additionalChecks = [];
  let budgetExhausted = false;

  for (const target of targets) {
    const hooks = discoverHooks(target.path, seam);
    const env = buildHookEnv(state || {}, { seam, target });
    const input = JSON.stringify(state || {});

    for (const hook of hooks) {
      if (budgetExhausted) {
        envelope.hooks.push({
          hook: hook.path, project: target.name, status: 'skipped',
          reason: 'budget-exhausted',
        });
        continue;
      }
      const remaining = deadline - clock();
      if (remaining < MIN_HOOK_MS) {
        budgetExhausted = true;
        envelope.hooks.push({
          hook: hook.path, project: target.name, status: 'skipped',
          reason: 'budget-exhausted',
        });
        continue;
      }
      const result = runHook({
        path: hook.path, cwd: target.path, env, input, timeoutMs: remaining,
      });
      result.project = target.name;
      envelope.hooks.push(result);
      // Ring 1 passthrough: collect additional_checks from structured output.
      const checks = result.output && result.output.additional_checks;
      if (Array.isArray(checks)) additionalChecks.push(...checks);
    }
  }

  if (additionalChecks.length > 0) envelope.additional_checks = additionalChecks;
  return envelope;
}

// ---------------------------------------------------------------------------
// Registration — wire the runner into config.hooks for all three seams.
// Idempotent and append-only: an existing runner entry is left as-is, and any
// other (raw consumer) commands already in a seam array are preserved.
// ---------------------------------------------------------------------------

export function runnerCommand(seam, watchtowerDir) {
  const dir = watchtowerDir || getWatchtowerDir();
  return `node "${join(dir, 'scripts', RUNNER_BASENAME)}" --seam ${seam}`;
}

// registerRunner — pure: returns the config with the runner ensured present in
// every seam array. Returns { config, changed }.
export function registerRunner(config, watchtowerDir) {
  const next = config && typeof config === 'object' ? config : {};
  if (!next.hooks || typeof next.hooks !== 'object') next.hooks = {};
  let changed = false;
  for (const seam of SEAMS) {
    const arr = Array.isArray(next.hooks[seam]) ? next.hooks[seam] : [];
    const already = arr.some((c) => typeof c === 'string' && c.includes(RUNNER_BASENAME));
    if (!already) {
      arr.push(runnerCommand(seam, watchtowerDir));
      changed = true;
    }
    next.hooks[seam] = arr;
  }
  return { config: next, changed };
}

// ensureRegistered — load config, register the runner, atomic-write if
// changed. Used by `--register` (the /watchtower install step calls this).
export function ensureRegistered(watchtowerDir) {
  const dir = watchtowerDir || getWatchtowerDir();
  const config = loadConfig(dir);
  const { config: next, changed } = registerRunner(config, dir);
  if (changed) {
    atomicWrite(join(dir, 'config.json'), JSON.stringify(next, null, 2) + '\n');
  }
  return { changed };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = { seam: null, register: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--seam') args.seam = argv[++i];
    else if (a === '--register') args.register = true;
  }
  return args;
}

function readStdin() {
  try {
    const data = readFileSync(0, 'utf8');
    if (!data || !data.trim()) return {};
    return JSON.parse(data);
  } catch {
    return {};
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.register) {
    try {
      const { changed } = ensureRegistered();
      process.stdout.write(JSON.stringify({ registered: true, changed }) + '\n');
    } catch (e) {
      process.stderr.write(`hook-runner --register failed: ${e.message}\n`);
      process.exitCode = 1;
    }
    return;
  }

  if (!args.seam || !SEAMS.includes(args.seam)) {
    process.stderr.write(`hook-runner: --seam must be one of ${SEAMS.join(', ')}\n`);
    // Print an empty-but-valid envelope so a misconfigured ring still parses.
    process.stdout.write(JSON.stringify({ schema_version: 1, seam: args.seam || null, hooks: [], error: 'invalid-seam' }) + '\n');
    return;
  }

  const state = readStdin();
  let config = null;
  if (PORTFOLIO_SEAMS.has(args.seam)) {
    try {
      config = loadConfig();
    } catch {
      config = null; // no config → no portfolio targets; degrade, don't crash
    }
  }

  let envelope;
  try {
    envelope = runSeam({ seam: args.seam, state, config });
  } catch (e) {
    envelope = { schema_version: 1, seam: args.seam, hooks: [], error: e.message };
  }
  process.stdout.write(JSON.stringify(envelope) + '\n');
}

// Run as a script when invoked directly; stay a pure module when imported by
// tests (which exercise the exported helpers without a CLI round-trip).
const invokedDirectly = process.argv[1] && process.argv[1].endsWith(RUNNER_BASENAME);
if (invokedDirectly) main();

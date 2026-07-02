#!/usr/bin/env node

// Watchtower phase shim (Plan 9, act:4a6e907c) — the migration adapter that
// runs an EXISTING non-interactive consumer script as a ring hook, unchanged.
//
// Why it exists. The consumer scan found real per-project customs that lived
// as orient/debrief phases and now need a home at the ring seams: Flow's
// Railway DB pull and machine-drift check, article-rewriter's reMarkable sync
// and Sentry check, maginnis's timelog collection. Their non-interactive
// LOGIC was already a script (or trivially becomes one); what differs is the
// invocation convention — those scripts expect to run in the project root
// with an ambient environment, not "JSON on stdin from a ring." The shim is
// the one place that bridges that gap, so the port is mechanical:
//
//   config.hooks["ring1-post-collect"] += [
//     'node ~/.claude-cabinet/watchtower/scripts/watchtower-phase-shim.mjs \
//        --phase scripts/flow-railway-pull.sh'
//   ]
//
// The shim reads the ring state on stdin, runs the target script with the
// project root as cwd and the WATCHTOWER_* contract env exported, passes the
// state through on stdin, and enforces a timeout. Its stdout is the target's
// stdout, passed through transparently when the target prints JSON (so the
// Ring 1 `additional_checks` contract survives a migrated phase), wrapped
// otherwise. It propagates the target's exit code so failure is visible to
// whoever runs it — the hook runner (which classifies non-zero as `failed`)
// or a ring directly (the existing raw config.hooks mechanism).
//
// Two ways to wire a migrated phase:
//   1. Drop the script into <project>/.claude/watchtower/hooks/<seam>/ and let
//      watchtower-hook-runner discover it — best for NEW hook scripts written
//      to the contract.
//   2. Register the shim in config.hooks[<seam>] pointing at an EXISTING
//      script anywhere in the repo (the path need not move) — best for the
//      MIGRATION case, where the script already exists and speaks the
//      project-cwd/env convention rather than reading stdin.
//
// Interactive customs are OUT of scope (owned by routine dispatch,
// act:c2a55c08). The shim never talks to the operator.

import { existsSync, readFileSync } from 'fs';
import { isAbsolute, resolve as resolvePath } from 'path';
import {
  resolveInterpreter, buildHookEnv, SEAM_BUDGET_MS,
} from './watchtower-hook-runner.mjs';
import { spawnSync } from 'child_process';

const SHIM_BASENAME = 'watchtower-phase-shim.mjs';
const DEFAULT_TIMEOUT_MS = 60_000;

function parseArgs(argv) {
  const args = { phase: null, cwd: null, seam: null, timeout: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--phase') args.phase = argv[++i];
    else if (a === '--cwd') args.cwd = argv[++i];
    else if (a === '--seam') args.seam = argv[++i];
    else if (a === '--timeout') args.timeout = Number(argv[++i]);
  }
  return args;
}

function readStdinState() {
  try {
    const data = readFileSync(0, 'utf8');
    if (!data || !data.trim()) return {};
    return JSON.parse(data);
  } catch {
    return {};
  }
}

// resolveCwd — the project root the phase script expects to run in: explicit
// --cwd, else the project from the ring state, else the current directory.
export function resolveCwd(args, state) {
  return args.cwd || (state && (state.path || state.project_path)) || process.cwd();
}

// resolvePhasePath — absolute path to the target script (relative paths are
// resolved against the project cwd, matching how a phase referenced its own
// repo files).
export function resolvePhasePath(phase, cwd) {
  if (!phase) return null;
  return isAbsolute(phase) ? phase : resolvePath(cwd, phase);
}

// runPhase — execute the target script with phase conventions. Returns the
// raw spawnSync-derived result plus the resolved invocation, never throws.
export function runPhase({ phasePath, cwd, env, input, timeoutMs }) {
  if (!phasePath || !existsSync(phasePath)) {
    return { ok: false, status: 'failed', error: `phase script not found: ${phasePath || '(none)'}`, exit: 127 };
  }
  const interp = resolveInterpreter(phasePath);
  if (!interp) {
    return { ok: false, status: 'failed', error: `phase script is not runnable (no exec bit, unknown extension): ${phasePath}`, exit: 126 };
  }
  let res;
  try {
    res = spawnSync(interp.cmd, interp.args, {
      cwd, env, input: input || '',
      timeout: Math.max(1, timeoutMs | 0),
      encoding: 'utf8',
      maxBuffer: 10 * 1024 * 1024,
    });
  } catch (e) {
    return { ok: false, status: 'failed', error: e.message, exit: 1 };
  }
  if (res.error) {
    const timedOut = res.error.code === 'ETIMEDOUT' || res.signal === 'SIGTERM';
    return { ok: false, status: timedOut ? 'timeout' : 'failed', error: res.error.message, exit: timedOut ? 124 : 1, stderr: res.stderr };
  }
  if (res.signal === 'SIGTERM') {
    return { ok: false, status: 'timeout', error: `killed after ${timeoutMs}ms`, exit: 124, stderr: res.stderr };
  }
  return {
    ok: res.status === 0,
    status: res.status === 0 ? 'success' : 'failed',
    exit: res.status,
    stdout: res.stdout,
    stderr: res.stderr,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const state = readStdinState();
  const cwd = resolveCwd(args, state);
  const phasePath = resolvePhasePath(args.phase, cwd);
  const seam = args.seam || (state && state.seam) || '';

  const env = buildHookEnv(state, {
    seam,
    target: { name: (state && (state.name || state.project)) || '', path: cwd },
  });
  env.WATCHTOWER_PHASE = phasePath || '';
  env.WATCHTOWER_HOOK = '1';

  const timeoutMs = args.timeout
    || Number(process.env.WATCHTOWER_HOOK_BUDGET_MS)
    || SEAM_BUDGET_MS[seam]
    || DEFAULT_TIMEOUT_MS;

  const result = runPhase({
    phasePath, cwd, env, input: JSON.stringify(state || {}), timeoutMs,
  });

  if (result.ok) {
    // Pass the target's stdout through transparently when it is valid JSON
    // (preserves the additional_checks contract); otherwise emit a wrapper.
    const out = (result.stdout || '').trim();
    let isJson = false;
    if (out) {
      try { JSON.parse(out); isJson = true; } catch { /* not json */ }
    }
    if (isJson) process.stdout.write(out + '\n');
    else process.stdout.write(JSON.stringify({ phase: phasePath, status: 'success', output: out }) + '\n');
    process.exitCode = 0;
    return;
  }

  // Failure: emit a JSON error envelope on stdout, echo target stderr to our
  // stderr for the ring/runner log, and propagate a non-zero exit.
  if (result.stderr) process.stderr.write(result.stderr);
  process.stdout.write(JSON.stringify({
    phase: phasePath, status: result.status, error: result.error || 'phase failed', exit: result.exit,
  }) + '\n');
  process.exitCode = result.exit && result.exit !== 0 ? result.exit : 1;
}

const invokedDirectly = process.argv[1] && process.argv[1].endsWith(SHIM_BASENAME);
if (invokedDirectly) main();

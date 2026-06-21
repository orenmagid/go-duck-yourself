#!/usr/bin/env node

// Watchtower environment-advisories pass (act:f9ea075d).
//
// The single home for orient's stack-aware setup advisories — LSP plugins,
// the Railway MCP, hookify, a missing project briefing, registry orphans —
// with a per-project dismissal memory so nothing re-nags every session.
//
// HOME = the SessionStart context builder, NOT Ring 1: Ring 1's launchd cron
// PATH cannot reach `claude`, so the install-probe (`claude plugin list`)
// would fail there. The context builder runs in the real session env.
//
// This module owns ALL advisory I/O: signal computation, the throttled probe,
// reading and atomic-writing the dismissal state, and applying the rules in
// `advisories-state-schema.md`. The context builder only calls runAdvisoryPass
// and renders the returned list. The pass NEVER throws — any failure returns
// an empty list and persists nothing, so it can never block session start.
//
// State file: <projectPath>/.claude/cabinet/advisories-state.json. Read AND
// written at the SAME path (the schema blesses per-worktree divergence; the
// cardinal rule is never read one path and write another). atomicWrite is
// temp+rename — corruption-safe but not read-modify-write-safe; concurrent
// sessions accept lost-update semantics (a lost count++ = one extra nag,
// self-correcting).

import { readFileSync, readdirSync, existsSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';
import { execSync } from 'child_process';
import { atomicWrite } from './watchtower-lib.mjs';

const STATE_REL = join('.claude', 'cabinet', 'advisories-state.json');
const SCAN_MAX_DEPTH = 3;
const SCAN_DENYLIST = new Set([
  'node_modules', 'dist', 'build', 'vendor', 'coverage', 'target', 'out',
]);
const PROBE_TIMEOUT_MS = 3000;
const SURFACE_COUNT_LIMIT = 2; // a `suggested` advisory surfaces while count < 2

// --- date / IO helpers ---

function todayUTC() {
  // UTC ISO date, matching the schema's last_shown basis AND sqlite date('now')
  // (the overdue query). Local-time formatting would drift at the day boundary.
  return new Date().toISOString().slice(0, 10);
}

function safeReadJSON(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf8'));
  } catch {
    return null; // absent OR malformed → treated as "every advisory never-seen"
  }
}

// --- bounded stack-file scan ---

// Existence-only, depth-limited, denylisted, early-exit. A recursive *.ts walk
// over node_modules would be pathological on the session-start critical path;
// a root-only scan would silently miss the common src/**/*.ts layout (a TS
// project with no root tsconfig). This catches src/** while staying bounded.
function hasFileWithExt(dir, ext, depth = SCAN_MAX_DEPTH) {
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return false;
  }
  const subdirs = [];
  for (const e of entries) {
    if (e.isFile() && e.name.endsWith(ext)) return true;
    if (e.isDirectory() && !SCAN_DENYLIST.has(e.name) && !e.name.startsWith('.')) {
      subdirs.push(e.name);
    }
  }
  if (depth <= 0) return false;
  for (const sd of subdirs) {
    if (hasFileWithExt(join(dir, sd), ext, depth - 1)) return true;
  }
  return false;
}

function has(projectPath, rel) {
  return existsSync(join(projectPath, rel));
}

// Signal = a deterministic fingerprint of the indicators present. Tokens are
// SORTED before joining so the same stack always produces the same string —
// otherwise readdir order could flip "gemfile+rb" ↔ "rb+gemfile" and spuriously
// re-arm a declined advisory every session.
function signalFrom(tokens) {
  const present = tokens.filter(Boolean);
  return present.length ? present.slice().sort().join('+') : null;
}

// --- advisory descriptors ---
//
// kind:
//   'probe-suppressed' — signal is on-disk (stack files); the probe only
//      SUPPRESSES (confirms installed → terminal). Unknown probe → still
//      surface (the LSP advisories).
//   'probe-gated' — the surfacing predicate IS the probe ("X not installed");
//      unknown probe → freeze, do not surface, do not mutate (hookify).
//   'no-probe' — pure filesystem/config signal (railway, briefing-file,
//      registry-orphan).

function buildAdvisories(ctx) {
  const { projectPath, claudeJsonText, registry } = ctx;
  const list = [];

  const lsp = [
    { id: 'lsp:typescript', tokens: () => [has(projectPath, 'tsconfig.json') && 'tsconfig', hasFileWithExt(projectPath, '.ts') && 'ts'], needle: 'typescript-lsp', action: 'TypeScript detected but typescript-lsp not installed — catches missing imports/type errors after edits. Install: /plugin install typescript-lsp' },
    { id: 'lsp:python', tokens: () => [has(projectPath, 'pyproject.toml') && 'pyproject', has(projectPath, 'requirements.txt') && 'requirements', hasFileWithExt(projectPath, '.py') && 'py'], needle: 'pyright-lsp', action: 'Python detected but pyright-lsp not installed. Install: /plugin install pyright-lsp' },
    { id: 'lsp:rust', tokens: () => [has(projectPath, 'Cargo.toml') && 'cargo'], needle: 'rust-analyzer-lsp', action: 'Rust detected but rust-analyzer-lsp not installed. Install: /plugin install rust-analyzer-lsp' },
    { id: 'lsp:go', tokens: () => [has(projectPath, 'go.mod') && 'gomod'], needle: 'gopls-lsp', action: 'Go detected but gopls-lsp not installed. Install: /plugin install gopls-lsp' },
    { id: 'lsp:ruby', tokens: () => [has(projectPath, 'Gemfile') && 'gemfile', hasFileWithExt(projectPath, '.rb') && 'rb'], needle: 'ruby-lsp', action: 'Ruby detected but ruby-lsp not active. Install the plugin (/plugin install ruby-lsp@claude-plugins-official), the gem (gem install ruby-lsp), and set ENABLE_LSP_TOOL=1.' },
  ];
  for (const a of lsp) {
    const signal = signalFrom(a.tokens());
    if (signal) list.push({ id: a.id, kind: 'probe-suppressed', needle: a.needle, signal, action: a.action });
  }

  // Railway MCP — railway.toml present AND no railway key registered in
  // ~/.claude.json. NOTE: Ring 1 also marker-checks railway.toml (deploy
  // detection) — do NOT consolidate; this adds the registration predicate.
  if (has(projectPath, 'railway.toml')) {
    const registered = !!claudeJsonText && /railway/i.test(claudeJsonText);
    if (!registered) {
      list.push({ id: 'mcp:railway', kind: 'no-probe', needle: null, signal: 'railway-unregistered',
        action: 'railway.toml present but no Railway MCP registered — agents get a cleaner surface with it. Local: railway setup agent -y · Remote: register mcp.railway.com (OAuth).' });
    }
  }

  // hookify — enforcement-pipeline.md exists AND hookify not installed. Signal
  // is STATIC (the file, once created, stays), so a declined hookify stays
  // declined until its entry is cleared (intended sticky case).
  if (has(projectPath, join('.claude', 'rules', 'enforcement-pipeline.md'))) {
    list.push({ id: 'plugin:hookify', kind: 'probe-gated', needle: 'hookify', signal: 'enforcement-pipeline',
      action: 'This project has an enforcement pipeline but hookify is not installed — it generates hooks from natural language. Install: /plugin install hookify' });
  }

  // Briefing file presence — surface only when ABSENT.
  if (!has(projectPath, join('.claude', 'briefing', '_briefing.md'))) {
    list.push({ id: 'briefing-file', kind: 'no-probe', needle: null, signal: 'missing',
      action: 'No project briefing at .claude/briefing/_briefing.md — cabinet members bootstrap from it. Run /onboard to create one.' });
  }

  // Registry orphans — registry entries whose path no longer exists.
  if (registry && Array.isArray(registry.projects)) {
    const orphans = registry.projects
      .filter((p) => p && p.path && !existsSync(p.path))
      .map((p) => p.name || p.path)
      .sort();
    if (orphans.length > 0) {
      list.push({ id: 'registry-orphan', kind: 'no-probe', needle: null, signal: `orphans:${orphans.join('+')}`,
        action: `cc-registry lists project(s) whose path is gone: ${orphans.join(', ')} — consider removing them from ~/.claude/cc-registry.json.` });
    }
  }

  return list;
}

// --- the probe ---

// Returns the lowercased `claude plugin list` output, or null if the probe
// could not answer (claude not on PATH, nonzero exit, timeout). null is the
// tri-state's "unknown" — never conflated with "absent".
function defaultPluginProbe() {
  try {
    const out = execSync('claude plugin list', {
      encoding: 'utf8', timeout: PROBE_TIMEOUT_MS,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    return typeof out === 'string' ? out.toLowerCase() : null;
  } catch {
    return null;
  }
}

// tri-state: true | false | null
function isInstalled(probeText, needle) {
  if (probeText == null) return null;
  return probeText.includes(needle.toLowerCase());
}

// --- the rule engine (advisories-state-schema.md) ---

// Returns { surface, entry, mutated }. `entry` is the entry to store (or the
// unchanged one); `mutated` says whether state changed (drives the dirty flag
// and avoids needless writes / frozen no-ops).
function applyRule(prev, { signal, kind, installed }, today) {
  // Terminal: already installed → never surface, never change.
  if (prev && prev.status === 'installed') {
    return { surface: false, entry: prev, mutated: false };
  }

  // Probe confirms installed now → flip terminal (both probe kinds).
  if (kind !== 'no-probe' && installed === true) {
    const entry = { status: 'installed', count: prev?.count || 0, last_shown: prev?.last_shown || today, signal };
    return { surface: false, entry, mutated: true };
  }

  // Probe-gated with unknown probe → predicate unknowable → freeze (no mutation).
  if (kind === 'probe-gated' && installed === null) {
    return { surface: false, entry: prev, mutated: false };
  }

  // No entry → surface, create.
  if (!prev) {
    return { surface: true, entry: { status: 'suggested', count: 1, last_shown: today, signal }, mutated: true };
  }

  if (prev.status === 'declined') {
    if (prev.signal === signal) return { surface: false, entry: prev, mutated: false }; // silent
    // signal changed → stack evolved → re-surface once, reset.
    return { surface: true, entry: { status: 'suggested', count: 1, last_shown: today, signal }, mutated: true };
  }

  if (prev.status === 'suggested') {
    if (prev.signal !== signal) {
      // stack changed at any count → reset + surface.
      return { surface: true, entry: { status: 'suggested', count: 1, last_shown: today, signal }, mutated: true };
    }
    if ((prev.count || 0) < SURFACE_COUNT_LIMIT) {
      return { surface: true, entry: { ...prev, count: (prev.count || 0) + 1, last_shown: today, signal }, mutated: true };
    }
    return { surface: false, entry: prev, mutated: false }; // count >= 2, unchanged → quiet
  }

  return { surface: false, entry: prev, mutated: false };
}

// --- main entry ---

export function runAdvisoryPass({ projectPath, pluginProbe = defaultPluginProbe, homeDir = homedir(), now = todayUTC() } = {}) {
  try {
    if (!projectPath) return [];

    const today = now;
    const statePath = join(projectPath, STATE_REL);
    const rawState = safeReadJSON(statePath) || {};
    const meta = (rawState._meta && typeof rawState._meta === 'object') ? rawState._meta : {};
    const lastProbe = meta.last_probe ?? null;

    const claudeJsonText = (() => {
      try { return readFileSync(join(homeDir, '.claude.json'), 'utf8'); } catch { return null; }
    })();
    const registry = safeReadJSON(join(homeDir, '.claude', 'cc-registry.json'));

    const advisories = buildAdvisories({ projectPath, claudeJsonText, registry });
    if (advisories.length === 0) return [];

    // Probe at most once/day/checkout, and only if a probe-kind advisory is
    // applicable. Throttle skip leaves probeText=null → LSP still surfaces
    // from cached state, hookify stays silent (probe-gated null = freeze).
    const needProbe = advisories.some((a) => a.kind !== 'no-probe');
    const throttled = lastProbe === today;
    let probeText = null;
    let probed = false;
    if (needProbe && !throttled) {
      probeText = pluginProbe();
      probed = true;
    }

    const surfaced = [];
    let dirty = false;
    for (const a of advisories) {
      const installed = a.kind === 'no-probe' ? false : isInstalled(probeText, a.needle);
      const { surface, entry, mutated } = applyRule(rawState[a.id], { signal: a.signal, kind: a.kind, installed }, today);
      if (mutated) {
        rawState[a.id] = entry;
        dirty = true;
      }
      if (surface) surfaced.push({ id: a.id, action: a.action });
    }

    // Record the probe stamp only when a probe actually ran (non-null result).
    if (probed && probeText != null) {
      rawState._meta = { ...meta, last_probe: today };
      dirty = true;
    }

    if (dirty) {
      try { atomicWrite(statePath, rawState); } catch { /* surfacing still valid; persistence is best-effort */ }
    }

    return surfaced;
  } catch {
    return []; // never throw — must not block session start
  }
}

// CLI: print one advisory action per line (used by orient's non-watchtower
// fallback, which can shell out to the single implementation).
if (import.meta.url === `file://${process.argv[1]}`) {
  let projectPath = process.cwd();
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--project-path' && args[i + 1]) { projectPath = args[i + 1]; i++; }
  }
  for (const a of runAdvisoryPass({ projectPath })) {
    process.stdout.write(`${a.action}\n`);
  }
}

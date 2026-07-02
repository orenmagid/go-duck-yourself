#!/usr/bin/env node

// watchtower-sync.mjs — content-based 3-tier hash-diff + auto-heal for the
// watchtower script set.
//
// The watchtower runtime that launchd/cron actually executes lives at
// ~/.claude-cabinet/watchtower/. Getting a one-file fix there durably means
// keeping THREE tiers in step:
//
//   1. template — upstream source of truth (`<cc-source>/templates/`).
//      Present only in the CC source repo (or the npm package during install).
//   2. tracked  — the project's committed copies (`<project>/scripts/` +
//      `<project>/.claude/`), read by the one-time `/watchtower install` step.
//   3. runtime  — `~/.claude-cabinet/watchtower/`, what the daemon runs.
//
// Before this tool a single fix had to be hand-copied to all three; miss the
// runtime and the fix is committed but not running, miss the tracked copy and
// the next install silently reverts it. This module hash-diffs the tiers from
// their ACTUAL on-disk bytes (never a cached manifest, which can itself go
// stale) and either reports the divergence (`--check`, read-only) or heals it
// (`--heal`) by propagating the authoritative tier downward.
//
// Authority is by contract, not by mtime: template wins when present, else the
// tracked copy. The runtime is ALWAYS a heal target, never a source — and the
// template tier is NEVER written. So healing only ever flows downstream.
//
// The managed file SET is DISCOVERED by scanning the tiers (union of what's on
// disk), so this drift-detector cannot itself accumulate a stale hardcoded
// list — the one failure mode the source feedback warned about. The only fixed
// knobs are the category→subdir map and the two non-`watchtower-*` doc names.

import {
  readFileSync, writeFileSync, existsSync, readdirSync,
  mkdirSync, renameSync, chmodSync,
} from 'fs';
import { join, dirname } from 'path';
import { homedir } from 'os';
import { createHash } from 'crypto';
import { pathToFileURL } from 'url';

// ---------------------------------------------------------------------------
// Tier layout — the ONE place the per-tier subdir for each category is encoded.
// ---------------------------------------------------------------------------
//   - scripts  live in scripts/ in every tier
//   - hooks    are templates/hooks/ upstream but .claude/hooks/ when tracked
//   - cabinet  docs are templates/cabinet/ upstream but .claude/cabinet/ tracked
// In the runtime they collapse to flat scripts/ hooks/ cabinet/ subdirs.
const CATEGORY_SUBDIRS = {
  script:  { template: 'scripts', tracked: 'scripts',        runtime: 'scripts' },
  hook:    { template: 'hooks',   tracked: '.claude/hooks',  runtime: 'hooks' },
  cabinet: { template: 'cabinet', tracked: '.claude/cabinet', runtime: 'cabinet' },
};

const TIERS = ['template', 'tracked', 'runtime'];

// Which files in each category are "ours" to keep in sync.
const HOOK_FILES = ['watchtower-session-start.sh', 'watchtower-session-end.sh'];
const CABINET_FILES = ['advisories-state-schema.md', 'watchtower-contracts.md'];

function isManagedScript(name) {
  return /^watchtower-.*\.(mjs|sh)$/.test(name);
}

// Files that must be executable in the runtime/tracked tiers.
function isExecutable(category, name) {
  if (category === 'hook') return true;
  if (category === 'script' && name.endsWith('.sh')) return true;
  return false;
}

function tierDir(root, category, tier) {
  return join(root, CATEGORY_SUBDIRS[category][tier]);
}

function hashBytes(buf) {
  return createHash('sha256').update(buf).digest('hex');
}

// ---------------------------------------------------------------------------
// discoverManaged — union of managed files present across the supplied tiers.
// ---------------------------------------------------------------------------
// A file present in ANY tier participates, so a script that exists upstream but
// is missing downstream (or an orphan that lingers only in the runtime) is
// surfaced rather than silently ignored.
export function discoverManaged(roots) {
  const seen = new Map(); // `${category}/${name}` -> { category, name }
  for (const category of ['script', 'hook', 'cabinet']) {
    for (const tier of TIERS) {
      const root = roots[tier];
      if (!root) continue;
      const dir = tierDir(root, category, tier);
      if (!existsSync(dir)) continue;
      let names;
      try { names = readdirSync(dir); } catch { continue; }
      for (const name of names) {
        let ok = false;
        if (category === 'script') ok = isManagedScript(name);
        else if (category === 'hook') ok = HOOK_FILES.includes(name);
        else if (category === 'cabinet') ok = CABINET_FILES.includes(name);
        if (ok) seen.set(`${category}/${name}`, { category, name });
      }
    }
  }
  return [...seen.values()].sort((a, b) =>
    `${a.category}/${a.name}`.localeCompare(`${b.category}/${b.name}`));
}

// template wins when present, else the tracked copy. Runtime is never a source.
function authoritativeTier(roots) {
  if (roots.template) return 'template';
  if (roots.tracked) return 'tracked';
  return null;
}

function readTier(root, category, tier, name) {
  if (!root) return { tier, present: false, path: null };
  const path = join(tierDir(root, category, tier), name);
  if (!existsSync(path)) return { tier, present: false, path };
  try {
    return { tier, present: true, path, hash: hashBytes(readFileSync(path)) };
  } catch (err) {
    return { tier, present: false, path, error: err.message };
  }
}

// ---------------------------------------------------------------------------
// analyze — per-file 3-tier classification + heal plan (no writes).
// ---------------------------------------------------------------------------
export function analyze(roots) {
  const authTier = authoritativeTier(roots);
  const managed = discoverManaged(roots);
  const files = [];

  for (const { category, name } of managed) {
    const tiers = {};
    for (const tier of TIERS) {
      if (roots[tier]) tiers[tier] = readTier(roots[tier], category, tier, name);
    }

    const auth = authTier ? tiers[authTier] : null;
    const healTargets = [];
    let status;

    if (!authTier) {
      // Only the runtime is present — nothing to verify against.
      status = 'unverifiable';
    } else if (!auth || !auth.present) {
      // File lingers in a downstream tier but is gone from the source.
      status = 'orphan';
    } else {
      for (const tier of TIERS) {
        if (tier === authTier) continue;
        const t = tiers[tier];
        if (!t) continue; // tier root absent — not a target
        if (!t.present) healTargets.push({ tier, reason: 'missing' });
        else if (t.hash !== auth.hash) healTargets.push({ tier, reason: 'drift' });
      }
      if (healTargets.length === 0) status = 'in-sync';
      else if (healTargets.some((h) => h.reason === 'drift')) status = 'drift';
      else status = 'missing';
    }

    files.push({ category, name, authTier, status, tiers, healTargets });
  }

  const summary = {
    total: files.length,
    inSync: files.filter((f) => f.status === 'in-sync').length,
    drift: files.filter((f) => f.status === 'drift').length,
    missing: files.filter((f) => f.status === 'missing').length,
    orphan: files.filter((f) => f.status === 'orphan').length,
    unverifiable: files.filter((f) => f.status === 'unverifiable').length,
  };
  summary.clean = summary.drift === 0 && summary.missing === 0
    && summary.orphan === 0 && summary.unverifiable === 0;

  return { authTier, roots, files, summary };
}

// ---------------------------------------------------------------------------
// heal — propagate the authoritative tier into drifted/missing targets.
// ---------------------------------------------------------------------------
// Atomic (tmp + rename), preserves exec bits, NEVER writes the template tier.
// Orphans are reported, never auto-deleted (deletion is the riskier direction;
// a human removes a retired runtime script deliberately).
export function heal(analysis, opts = {}) {
  const dryRun = !!opts.dryRun;
  const healed = [];

  for (const f of analysis.files) {
    if (!f.healTargets.length) continue;
    const auth = f.tiers[f.authTier];
    if (!auth || !auth.present) continue; // defensive: nothing to copy
    const content = readFileSync(auth.path);

    for (const target of f.healTargets) {
      if (target.tier === 'template') continue; // never write upstream
      const root = analysis.roots[target.tier];
      if (!root) continue;
      const destPath = join(tierDir(root, f.category, target.tier), f.name);
      const record = {
        category: f.category, name: f.name, tier: target.tier,
        reason: target.reason, from: f.authTier, path: destPath,
      };
      if (!dryRun) {
        mkdirSync(dirname(destPath), { recursive: true });
        const tmp = destPath + '.tmp';
        writeFileSync(tmp, content);
        renameSync(tmp, destPath);
        if (isExecutable(f.category, f.name)) chmodSync(destPath, 0o755);
      }
      healed.push(record);
    }
  }

  return { healed, dryRun };
}

// ---------------------------------------------------------------------------
// resolveRoots — default tier roots from cwd/env, overridable per flag.
// ---------------------------------------------------------------------------
// template : <cwd>/templates only when its scripts/ exists (the CC SOURCE repo).
//            A consumer with no local templates is legitimately 2-tier.
// tracked  : an explicit dir, else <cwd> when it looks like a watchtower project
//            (scripts/watchtower-lib.mjs present).
// runtime  : an explicit dir, else $WATCHTOWER_DIR, else ~/.claude-cabinet/watchtower.
export function resolveRoots(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const env = opts.env || process.env;

  let template = opts.template;
  if (template === undefined) {
    const cand = join(cwd, 'templates');
    template = existsSync(join(cand, 'scripts')) ? cand : null;
  }

  let tracked = opts.tracked;
  if (tracked === undefined) {
    tracked = existsSync(join(cwd, 'scripts', 'watchtower-lib.mjs')) ? cwd : null;
  }

  let runtime = opts.runtime;
  if (runtime === undefined) {
    runtime = env.WATCHTOWER_DIR
      || join(homedir(), '.claude-cabinet', 'watchtower');
  }
  // A runtime root that doesn't exist on disk is treated as absent so the
  // refresh-only contract holds (nothing to diff against a missing runtime).
  if (runtime && !existsSync(runtime)) runtime = null;

  return { template: template || null, tracked: tracked || null, runtime: runtime || null };
}

// ---------------------------------------------------------------------------
// Human-readable report
// ---------------------------------------------------------------------------
function tierLabel(tier) {
  return { template: 'template', tracked: 'tracked', runtime: 'runtime' }[tier] || tier;
}

function renderReport(analysis, { heal: healResult } = {}) {
  const lines = [];
  const { roots, authTier, summary } = analysis;
  lines.push('Watchtower script sync — 3-tier hash diff');
  lines.push(`  template: ${roots.template || '(absent)'}`);
  lines.push(`  tracked:  ${roots.tracked || '(absent)'}`);
  lines.push(`  runtime:  ${roots.runtime || '(absent)'}`);
  lines.push(`  authoritative tier: ${authTier || '(none — cannot verify)'}`);
  lines.push('');

  const problems = analysis.files.filter((f) => f.status !== 'in-sync');
  if (!problems.length) {
    lines.push(`  ✓ all ${summary.total} watchtower files in sync across tiers`);
  } else {
    for (const f of problems) {
      if (f.status === 'orphan') {
        const where = TIERS.filter((t) => f.tiers[t]?.present).map(tierLabel).join(', ');
        lines.push(`  ⚠ orphan: ${f.name} — absent from ${tierLabel(f.authTier)}, lingers in ${where} (remove by hand if retired)`);
      } else if (f.status === 'unverifiable') {
        lines.push(`  ? unverifiable: ${f.name} — no source tier present`);
      } else {
        const targets = f.healTargets
          .map((h) => `${tierLabel(h.tier)} (${h.reason})`).join(', ');
        lines.push(`  ✗ ${f.status}: ${f.name} → heal ${targets} from ${tierLabel(f.authTier)}`);
      }
    }
  }

  lines.push('');
  lines.push(`  ${summary.inSync}/${summary.total} in sync · ${summary.drift} drift · ${summary.missing} missing · ${summary.orphan} orphan` +
    (summary.unverifiable ? ` · ${summary.unverifiable} unverifiable` : ''));

  if (healResult) {
    lines.push('');
    if (!healResult.healed.length) {
      lines.push('  nothing to heal');
    } else {
      lines.push(healResult.dryRun ? '  [dry-run] would heal:' : '  healed:');
      for (const h of healResult.healed) {
        lines.push(`    ${h.reason === 'missing' ? '+' : '~'} ${tierLabel(h.tier)}/${h.name} ← ${tierLabel(h.from)}`);
      }
      const trackedHealed = healResult.healed.some((h) => h.tier === 'tracked');
      if (trackedHealed && !healResult.dryRun) {
        lines.push('    (tracked-tier files changed — commit them so the fix is durable)');
      }
    }
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
function parseArgs(argv) {
  const opts = { mode: 'check', json: false, dryRun: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--check') opts.mode = 'check';
    else if (a === '--heal') opts.mode = 'heal';
    else if (a === '--json') opts.json = true;
    else if (a === '--dry-run') opts.dryRun = true;
    else if (a === '--template') opts.template = argv[++i];
    else if (a === '--tracked') opts.tracked = argv[++i];
    else if (a === '--runtime') opts.runtime = argv[++i];
    else if (a === '--help' || a === '-h') opts.help = true;
  }
  return opts;
}

const HELP = `watchtower-sync — keep the watchtower script tiers in step

Usage:
  node watchtower-sync.mjs [--check|--heal] [--json] [--dry-run] [roots]

Modes:
  --check   (default) read-only: hash-diff the tiers and report divergence
  --heal    propagate the authoritative tier into drifted/missing copies

Root overrides (else auto-detected from cwd / $WATCHTOWER_DIR):
  --template <dir>   upstream templates/ root (CC source repo only)
  --tracked  <dir>   the project root holding committed scripts/ + .claude/
  --runtime  <dir>   the live runtime root (~/.claude-cabinet/watchtower)

  --json     emit the structured analysis instead of the text report
  --dry-run  with --heal, show what would be written without writing

Exit codes: 0 = clean (or healed), 2 = drift/missing/orphan detected (--check).`;

export function main(argv) {
  const opts = parseArgs(argv);
  if (opts.help) {
    process.stdout.write(HELP + '\n');
    return 0;
  }

  const roots = resolveRoots(opts);
  const analysis = analyze(roots);

  let healResult = null;
  if (opts.mode === 'heal') {
    healResult = heal(analysis, { dryRun: opts.dryRun });
  }

  if (opts.json) {
    process.stdout.write(JSON.stringify({
      authTier: analysis.authTier,
      roots: analysis.roots,
      summary: analysis.summary,
      files: analysis.files.map((f) => ({
        category: f.category, name: f.name, status: f.status,
        authTier: f.authTier,
        healTargets: f.healTargets,
        tiers: Object.fromEntries(Object.entries(f.tiers).map(([t, v]) =>
          [t, { present: v.present, hash: v.hash || null }])),
      })),
      heal: healResult,
    }, null, 2) + '\n');
  } else {
    process.stdout.write(renderReport(analysis, { heal: healResult }) + '\n');
  }

  // After a heal, re-evaluate to report the post-heal verdict.
  if (opts.mode === 'heal' && !opts.dryRun) {
    return analyze(roots).summary.clean ? 0 : 2;
  }
  return analysis.summary.clean ? 0 : 2;
}

// Run as CLI when invoked directly (not when imported by tests).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exit(main(process.argv.slice(2)));
}

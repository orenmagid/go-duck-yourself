#!/usr/bin/env node

// Watchtower project-key migration — one-time repair for the phantom-key era.
//
// Before the canonical resolver landed, Ring 3 filed everything under
// basename(cwd) — for mux worktree sessions that's a phantom key like
// "cabinet-continue" that /inbox and the rings never look up — and the mux
// pane-close filer keyed items by desk name ("cabinet" vs the config key
// "claude-cabinet"). The damage lives in THREE stores that must move
// together, with one shared resolution, or they end up inconsistently
// half-right (worse than consistently wrong):
//
//   1. queue/items/*.json      — the `project` field (ALL statuses, not just
//                                pending: threads reference terminal items)
//   2. state/projects/<slug>/  — per-phantom-slug session dirs, MERGED into
//                                the config-slug dir (collisions suffixed)
//   3. state/threads/*.json    — sessions[].project slug values
//
// Resolution, per phantom key: (a) any live project_path resolves via the
// canonical resolver (git walks a worktree back to its main repo); (b) an
// alias map for deleted worktrees, where no tool can derive the project
// ("maginnis-*" shares no prefix with "claudeconsult-maginnis" — the
// operator has to say it); (c) neither → the items get project_unresolved
// and land in the residue report. The dry-run recomputes everything from
// the files — it never asserts prior counts.
//
// Safety: dry-run is the default; --apply backs up all three stores first;
// re-running --apply rewrites 0 items (idempotent); items already keyed to
// a config project are never touched.
//
// Usage:
//   watchtower-migrate-keys.mjs                       dry-run report
//   watchtower-migrate-keys.mjs --apply               backup, then rewrite
//   watchtower-migrate-keys.mjs --alias maginnis-=claudeconsult-maginnis
//                                                     (repeatable; prefix=key)

import {
  readFileSync, readdirSync, existsSync, mkdirSync, renameSync,
  cpSync, statSync, rmdirSync,
} from 'fs';
import { join, basename } from 'path';
import {
  getWatchtowerDir, loadConfig, slugify, atomicWrite,
  resolveProjectIdentity,
} from './watchtower-lib.mjs';

const WATCHTOWER_DIR = getWatchtowerDir();
const QUEUE_DIR = join(WATCHTOWER_DIR, 'queue', 'items');
const PROJECTS_DIR = join(WATCHTOWER_DIR, 'state', 'projects');
const THREADS_DIR = join(WATCHTOWER_DIR, 'state', 'threads');

// ---------------------------------------------------------------------------
// Alias map — operator-confirmed prefix → config key, for phantom keys whose
// worktrees no longer exist on disk (nothing mechanical can recover those).
// Extend via --alias prefix=key. Longest matching prefix wins.
// ---------------------------------------------------------------------------
const DEFAULT_ALIASES = {
  'cabinet-': 'claude-cabinet',
  'maginnis-': 'claudeconsult-maginnis',
  'flow-': 'flow',
};

function parseArgs() {
  const args = process.argv.slice(2);
  const parsed = { apply: false, aliases: { ...DEFAULT_ALIASES } };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--apply') parsed.apply = true;
    else if (args[i] === '--alias' && args[i + 1]) {
      const [prefix, key] = args[++i].split('=');
      if (prefix && key) parsed.aliases[prefix] = key;
    } else if (args[i] === '--help' || args[i] === '-h') {
      console.log('usage: watchtower-migrate-keys.mjs [--apply] [--alias prefix=configKey]...');
      process.exit(0);
    }
  }
  return parsed;
}

function readJSON(fp) {
  try {
    return JSON.parse(readFileSync(fp, 'utf8'));
  } catch {
    return null;
  }
}

function aliasTarget(phantomKey, aliases, configKeys) {
  let best = null;
  for (const [prefix, key] of Object.entries(aliases)) {
    if (phantomKey.startsWith(prefix) && configKeys.has(key)) {
      if (!best || prefix.length > best.prefix.length) best = { prefix, key };
    }
  }
  return best?.key || null;
}

function main() {
  const { apply, aliases } = parseArgs();
  const config = loadConfig();
  const configKeys = new Set(Object.keys(config.projects || {}));
  const configSlugs = new Map(
    [...configKeys].map(k => [slugify(k), k])
  );

  // --- Pass 0: scan queue items, build the phantom-key → target mapping ---
  // Strictly *.json — 430 legacy extension-less dec-* files share this dir.
  const itemFiles = existsSync(QUEUE_DIR)
    ? readdirSync(QUEUE_DIR).filter(f => f.endsWith('.json'))
    : [];
  const items = [];
  for (const f of itemFiles) {
    const item = readJSON(join(QUEUE_DIR, f));
    if (item && item.project) items.push({ file: f, item });
  }

  // mapping: phantomName -> { name, slug, path } | null (unresolvable)
  const mapping = new Map();
  const ensureMapped = (phantomKey, projectPath) => {
    if (configKeys.has(phantomKey)) return; // already a real key — untouched
    const existing = mapping.get(phantomKey);
    if (existing) return;
    // (a) live path: the resolver walks worktrees back to their main repo
    if (projectPath) {
      const id = resolveProjectIdentity(projectPath, config);
      if (id?.registered) {
        mapping.set(phantomKey, id);
        return;
      }
    }
    // (b) operator alias for dead worktrees
    const aliased = aliasTarget(phantomKey, aliases, configKeys);
    if (aliased) {
      const path = config.projects[aliased]?.path || null;
      mapping.set(phantomKey, { name: aliased, slug: slugify(aliased), path });
      return;
    }
    // (c) unresolvable — recorded so all three stores treat it identically
    mapping.set(phantomKey, null);
  };

  for (const { item } of items) ensureMapped(item.project, item.project_path);

  // Phantom slugs can also appear in stores without a matching queue item
  // (state dirs, thread sessions). Map those through the same rules.
  const threadFiles = existsSync(THREADS_DIR)
    ? readdirSync(THREADS_DIR).filter(f => f.endsWith('.json'))
    : [];
  const threads = threadFiles
    .map(f => ({ file: f, thread: readJSON(join(THREADS_DIR, f)) }))
    .filter(t => t.thread);
  for (const { thread } of threads) {
    for (const s of thread.sessions || []) {
      if (s.project && !configSlugs.has(s.project)) ensureMapped(s.project, null);
    }
  }
  const stateDirs = existsSync(PROJECTS_DIR)
    ? readdirSync(PROJECTS_DIR).filter(d => {
        try { return statSync(join(PROJECTS_DIR, d)).isDirectory(); } catch { return false; }
      })
    : [];
  for (const d of stateDirs) {
    if (!configSlugs.has(d)) ensureMapped(d, null);
  }

  // --- Report: recomputed ground truth, grouped by filed_by and phantom key ---
  const planned = { items: [], threads: [], dirs: [], residue: [] };
  const byFiler = {};
  const byKey = {};

  for (const { file, item } of items) {
    if (configKeys.has(item.project)) continue; // already correct: untouched
    const target = mapping.get(item.project);
    if (target) {
      planned.items.push({ file, item, target });
      byFiler[item.filed_by || '?'] = (byFiler[item.filed_by || '?'] || 0) + 1;
      byKey[`${item.project} → ${target.name}`] =
        (byKey[`${item.project} → ${target.name}`] || 0) + 1;
    } else if (!item.project_unresolved) {
      planned.residue.push({ file, item });
    }
  }

  for (const { file, thread } of threads) {
    const rewrites = (thread.sessions || []).filter(s =>
      s.project && !configSlugs.has(s.project) && mapping.get(s.project)
    ).length;
    const unresolvable = (thread.sessions || []).filter(s =>
      s.project && !configSlugs.has(s.project) && !mapping.get(s.project) && !s.project_unresolved
    ).length;
    if (rewrites || unresolvable) planned.threads.push({ file, thread, rewrites, unresolvable });
  }

  for (const d of stateDirs) {
    if (configSlugs.has(d)) continue;
    const target = mapping.get(d);
    if (target) planned.dirs.push({ dir: d, target });
  }

  console.log(`Watchtower project-key migration — ${apply ? 'APPLY' : 'dry-run'}`);
  console.log(`Config keys: ${[...configKeys].join(', ')}\n`);
  console.log(`Queue items scanned: ${items.length} (${itemFiles.length} .json files)`);
  console.log(`Items to re-key: ${planned.items.length}`);
  for (const [k, n] of Object.entries(byKey).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${k}: ${n}`);
  }
  console.log('By filer (cross-registry desk renames listed separately from ring3 basenames):');
  for (const [k, n] of Object.entries(byFiler)) console.log(`  ${k}: ${n}`);
  console.log(`Thread files to rewrite: ${planned.threads.length} (of ${threads.length})`);
  console.log(`State dirs to merge: ${planned.dirs.length}${planned.dirs.length ? ' — ' + planned.dirs.map(d => `${d.dir} → ${d.target.slug}`).join(', ') : ''}`);
  const unresolvableDirs = stateDirs.filter(d => !configSlugs.has(d) && !mapping.get(d));
  if (unresolvableDirs.length) {
    console.log(`State dirs unresolvable: ${unresolvableDirs.length} — left in place: ${unresolvableDirs.join(', ')}`);
  }
  if (planned.residue.length) {
    console.log(`\nRESIDUE — unresolvable, will be flagged project_unresolved (add --alias to map):`);
    for (const { item } of planned.residue) {
      console.log(`  [${item.status}] ${item.project} (${item.project_path || 'no path'}) — ${item.title?.slice(0, 60)}`);
    }
  }

  if (!apply) {
    console.log('\nDry-run only. Re-run with --apply to migrate (a backup is taken first).');
    return;
  }

  // --- Backup all three stores before any write ---
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupDir = join(WATCHTOWER_DIR, `migration-backup-${stamp}`);
  mkdirSync(backupDir, { recursive: true });
  for (const [src, name] of [[QUEUE_DIR, 'queue-items'], [PROJECTS_DIR, 'state-projects'], [THREADS_DIR, 'state-threads']]) {
    if (existsSync(src)) cpSync(src, join(backupDir, name), { recursive: true });
  }
  console.log(`\nBackup: ${backupDir}`);

  // --- Store 1: queue items ---
  let rewritten = 0;
  for (const { file, item, target } of planned.items) {
    item.project = target.name;
    if (target.path) item.project_path = target.path;
    delete item.project_unresolved;
    atomicWrite(join(QUEUE_DIR, file), item);
    rewritten++;
  }
  let flagged = 0;
  for (const { file, item } of planned.residue) {
    item.project_unresolved = true;
    atomicWrite(join(QUEUE_DIR, file), item);
    flagged++;
  }

  // --- Store 2: thread files (same mapping, slug values) ---
  let threadsRewritten = 0;
  for (const { file, thread } of planned.threads) {
    for (const s of thread.sessions || []) {
      if (!s.project || configSlugs.has(s.project)) continue;
      const target = mapping.get(s.project);
      if (target) {
        s.project = target.slug;
        delete s.project_unresolved;
      } else if (!s.project_unresolved) {
        s.project_unresolved = true;
      }
    }
    atomicWrite(join(THREADS_DIR, file), thread);
    threadsRewritten++;
  }

  // --- Store 3: state/projects dir merges (collision → keep both, suffix) ---
  let dirsMerged = 0;
  for (const { dir, target } of planned.dirs) {
    const srcDir = join(PROJECTS_DIR, dir);
    const dstDir = join(PROJECTS_DIR, target.slug);
    const walk = (rel) => {
      const abs = join(srcDir, rel);
      for (const entry of readdirSync(abs, { withFileTypes: true })) {
        const childRel = join(rel, entry.name);
        if (entry.isDirectory()) {
          mkdirSync(join(dstDir, childRel), { recursive: true });
          walk(childRel);
          try { rmdirSync(join(srcDir, childRel)); } catch { /* not empty */ }
        } else {
          let dest = join(dstDir, childRel);
          if (existsSync(dest)) {
            const dot = entry.name.lastIndexOf('.');
            const suffixed = dot > 0
              ? `${entry.name.slice(0, dot)}.from-${dir}${entry.name.slice(dot)}`
              : `${entry.name}.from-${dir}`;
            dest = join(dstDir, join(childRel, '..'), suffixed);
          }
          mkdirSync(join(dest, '..'), { recursive: true });
          renameSync(join(srcDir, childRel), dest);
        }
      }
    };
    mkdirSync(dstDir, { recursive: true });
    walk('');
    try { rmdirSync(srcDir); } catch { /* leftovers stay; report below */ }
    dirsMerged++;
  }

  console.log(`Applied: ${rewritten} items re-keyed, ${flagged} flagged unresolved, ${threadsRewritten} thread files rewritten, ${dirsMerged} state dirs merged.`);
  console.log('Verify idempotency: re-run with --apply — it should rewrite 0 items.');
}

main();

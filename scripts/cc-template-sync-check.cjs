#!/usr/bin/env node
// CC Template Sync Check — installed copies MISSING or STALE against their
// CURRENT templates (act:565b3c0c). The direction cc-drift-check cannot see.
//
// cc-drift-check compares each installed file to the hash the manifest
// recorded AT INSTALL TIME: a template that was never installed has no key
// and is invisible; a stale copy matches its equally-stale hash and reads
// clean. This check compares each installed copy to the template as it is
// NOW, enumerating exactly the paths the installer would write for the
// modules .ccrc.json says are installed, and classifying each file with the
// installer's own ownership cascade (lib/copy.js classifyFileOwnership) —
// so "project-owned" and "frozen" mean precisely what the installer means.
//
// Usage:
//   node scripts/cc-template-sync-check.cjs          # text report
//   node scripts/cc-template-sync-check.cjs --json   # machine-readable
//
// Exit codes — three distinct outcomes, none of which reads as another:
//   0  every enumerated file is in sync (or project-owned / frozen)
//   1  at least one file is missing or stale
//   2  BLIND — no .ccrc.json, or the create-claude-cabinet package (its
//      templates/ + lib/) could not be located, so nothing was compared.
//      Blind is NOT green.
//
// Package location, in order: $CC_PACKAGE_ROOT; the project itself when it
// IS the package (the source repo dogfooding); `create-claude-cabinet`
// resolvable from the project (npm link / a dev dependency). Consumers that
// install via npx and keep no copy should pass CC_PACKAGE_ROOT.
//
// Known, deliberate boundary: this mirrors the installer, so a phase file
// the installer never copies (skills outside ALWAYS_COPY_PHASES, unless
// listed as an instruction phase) is out of scope here too — an installed
// phase file trailing its template is the project's own customization
// channel, not drift.

const fs = require('fs');
const path = require('path');

const args = process.argv.slice(2);
const asJson = args.includes('--json');

function findProjectRoot() {
  let dir = process.cwd();
  while (dir !== path.dirname(dir)) {
    if (fs.existsSync(path.join(dir, '.ccrc.json'))) return dir;
    dir = path.dirname(dir);
  }
  return null;
}

function blind(reason) {
  if (asJson) {
    console.log(JSON.stringify({ blind: true, reason }, null, 2));
  } else {
    console.log(`BLIND — ${reason}`);
    console.log('This is NOT an in-sync result: nothing was compared.');
  }
  process.exit(2);
}

function isPackageRoot(dir) {
  if (!dir) return false;
  try {
    const pkg = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
    if (pkg.name !== 'create-claude-cabinet') return false;
  } catch { return false; }
  return ['lib/modules.js', 'lib/copy.js', 'lib/cli.js', 'templates'].every((p) => fs.existsSync(path.join(dir, p)));
}

function locatePackageRoot(projectRoot) {
  const fromEnv = process.env.CC_PACKAGE_ROOT;
  if (fromEnv) {
    const abs = path.resolve(fromEnv);
    return isPackageRoot(abs) ? abs : { error: `CC_PACKAGE_ROOT=${fromEnv} is not a create-claude-cabinet package root (needs package.json name, lib/, templates/)` };
  }
  if (isPackageRoot(projectRoot)) return projectRoot;
  try {
    const pkgJson = require.resolve('create-claude-cabinet/package.json', { paths: [projectRoot] });
    const dir = path.dirname(pkgJson);
    if (isPackageRoot(dir)) return dir;
  } catch { /* not resolvable — fall through */ }
  return { error: 'cannot locate the create-claude-cabinet package (templates/ + lib/) to compare against — set CC_PACKAGE_ROOT=<path to the package or source checkout>' };
}

const projectRoot = findProjectRoot();
if (!projectRoot) blind('no .ccrc.json found walking up from ' + process.cwd() + ' — not a CC project');

let metadata;
try {
  metadata = JSON.parse(fs.readFileSync(path.join(projectRoot, '.ccrc.json'), 'utf8'));
} catch (e) {
  blind(`.ccrc.json unreadable: ${e.message}`);
}
const manifest = metadata.manifest || {};

const located = locatePackageRoot(projectRoot);
if (typeof located !== 'string') blind(located.error);
const packageRoot = located;

let MODULES, classifyFileOwnership, templateDestPath, manifestPath, skipsPhases, instructionPhasesFor, expandFrozenClosure, packageVersion;
try {
  ({ MODULES } = require(path.join(packageRoot, 'lib', 'modules.js')));
  ({ classifyFileOwnership } = require(path.join(packageRoot, 'lib', 'copy.js')));
  ({ templateDestPath, manifestPath, skipsPhases, instructionPhasesFor, expandFrozenClosure } = require(path.join(packageRoot, 'lib', 'cli.js')));
  packageVersion = require(path.join(packageRoot, 'package.json')).version;
} catch (e) {
  blind(`package at ${packageRoot} could not be loaded: ${e.message}`);
}
for (const [name, fn] of Object.entries({ classifyFileOwnership, templateDestPath, manifestPath, skipsPhases, instructionPhasesFor, expandFrozenClosure })) {
  if (typeof fn !== 'function') blind(`package at ${packageRoot} (v${packageVersion}) predates this check — lib/ does not export ${name}`);
}
const templateRoot = path.join(packageRoot, 'templates');

const installedModules = Object.keys(metadata.modules || {}).filter((k) => metadata.modules[k] && MODULES[k]);

// The engagement module OVERLAYS scripts/pib-db-{lib,mcp-server,schema.sql,.mjs}
// with templates/engagement/pib-db-patches/* at install (lib/engagement-setup.js
// FILES), and the manifest keeps the BASE hash. On an engagement install the
// overlay twin is therefore the template of record for those files; the
// patches directory listing IS the mapping (basename → scripts/<basename>).
const overlayDir = path.join(templateRoot, 'engagement', 'pib-db-patches');
const engagementInstalled = installedModules.includes('engagement');
function overlayTemplateFor(tmpl) {
  if (!engagementInstalled || !tmpl.startsWith('scripts/')) return null;
  const twin = path.join(overlayDir, path.basename(tmpl));
  return fs.existsSync(twin) ? twin : null;
}
const overlaid = [];
const frozenSet = new Set(
  [...expandFrozenClosure(metadata.frozenModules || [], MODULES)].filter((k) => installedModules.includes(k))
);

const missing = [];
const stale = [];
const templateMissing = [];
let inSync = 0;
let projectOwned = 0;
let frozen = 0;
const seen = new Set();

function walk(dir, relBase, visit, skipPhases) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const rel = relBase ? relBase + '/' + entry.name : entry.name;
    if (entry.isDirectory()) {
      if (skipPhases && entry.name === 'phases') continue;
      walk(path.join(dir, entry.name), rel, visit, skipPhases);
    } else {
      visit(path.join(dir, entry.name), rel);
    }
  }
}

function classify({ key, srcPath, destPath, relPath, isPhaseFile, isInstructionPhase, isFrozenModule }) {
  if (seen.has(key)) return; // the same template may ship from two modules
  seen.add(key);
  const incoming = fs.readFileSync(srcPath, 'utf8');
  const destExists = fs.existsSync(destPath);
  const existingContent = destExists ? fs.readFileSync(destPath, 'utf8') : null;
  const decision = classifyFileOwnership({
    relPath,
    destExists,
    existingContent,
    incoming,
    isPhaseFile,
    isInstructionPhase,
    frozen: isFrozenModule,
    isCcOwned: Boolean(manifest[key]),
    skipConflicts: true, // the non-interactive cascade: manifest-driven, no prompt
  });
  switch (decision.action) {
    case 'copy-new': missing.push(key); break;
    case 'overwrite': stale.push(key); break;
    case 'skip-identical': inSync++; break;
    case 'freeze-hold':
    case 'freeze-hold-new': frozen++; break;
    default: projectOwned++; // seed / preserve-phase / skip-omit
  }
}

for (const modKey of installedModules) {
  const mod = MODULES[modKey];
  const isFrozenModule = frozenSet.has(modKey);
  for (const tmpl of mod.templates || []) {
    const srcPath = path.join(templateRoot, tmpl);
    if (!fs.existsSync(srcPath)) { templateMissing.push(tmpl); continue; }
    const destPath = templateDestPath(tmpl, projectRoot);
    if (fs.statSync(srcPath).isDirectory()) {
      const instr = instructionPhasesFor(tmpl, MODULES);
      const prefix = manifestPath(tmpl);
      walk(srcPath, '', (fileSrc, rel) => {
        classify({
          key: prefix + '/' + rel,
          srcPath: fileSrc,
          destPath: path.join(destPath, rel),
          relPath: rel,
          isPhaseFile: rel.includes('phases/'),
          isInstructionPhase: instr.has(rel),
          isFrozenModule,
        });
      }, skipsPhases(tmpl));
    } else {
      const isPhaseFile = tmpl.includes('/phases/');
      const overlay = overlayTemplateFor(tmpl);
      if (overlay) overlaid.push(manifestPath(tmpl));
      classify({
        key: manifestPath(tmpl),
        srcPath: overlay || srcPath,
        destPath,
        relPath: tmpl,
        isPhaseFile,
        isInstructionPhase: isPhaseFile && mod.templates.includes(tmpl),
        isFrozenModule,
      });
    }
  }
}

missing.sort();
stale.sort();
const red = missing.length > 0 || stale.length > 0;

if (asJson) {
  console.log(JSON.stringify({ missing, stale, inSync, projectOwned, frozen, overlaid, templateMissing, packageRoot, packageVersion, installedModules }, null, 2));
} else {
  const tally = `${inSync} in sync, ${projectOwned} project-owned, ${frozen} frozen` + (overlaid.length ? `, ${overlaid.length} compared against the engagement overlay` : '');
  if (!red) {
    console.log(`Template sync: all installed copies match their current templates (${tally}; package v${packageVersion} at ${packageRoot}).`);
  } else {
    console.log(`Template sync: ${missing.length} missing, ${stale.length} stale (${tally}; package v${packageVersion} at ${packageRoot})`);
    if (missing.length) {
      console.log(`\nMissing (${missing.length} — an installed module's template with no file on disk):`);
      for (const f of missing) console.log(`  ${f}`);
    }
    if (stale.length) {
      console.log(`\nStale (${stale.length} — installed copy differs from its CURRENT template; the manifest agrees with the copy, so cc-drift-check reads clean):`);
      for (const f of stale) console.log(`  ${f}`);
    }
    console.log('\nFix: reinstall on the MAIN checkout — npx create-claude-cabinet --yes  (source repo: node bin/create-claude-cabinet.js --yes).');
    console.log('An install run inside a linked worktree adopts into that lane\'s gitignored .ccrc.json and never reaches main (act:565b3c0c).');
  }
  if (templateMissing.length) {
    console.log(`\nNote: ${templateMissing.length} MODULES path(s) have no template on disk (the installer warns the same): ${templateMissing.join(', ')}`);
  }
}

process.exit(red ? 1 : 0);

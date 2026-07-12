#!/usr/bin/env node
// Cross-project pattern-contamination guard (process-therapist-0005, act:62608a5d).
//
// patterns-project.md files under .claude/skills/cabinet-*/ hold a project's
// OWN confirmed recurring findings — and the deliberative-audit Stage-1 prompt
// now instructs each member to read+apply them. A pattern promoted from a
// FOREIGN project (observed: a "51 Notion-migrated meetings" entry in this
// repo's process-therapist file) then poisons every future audit in the wrong
// repo. Same class as pattern-migration-destroys-project-files.
//
// The write path is prose (debrief's audit-pattern-capture phase), so this is
// the DETECT half of the guard: audit-pattern-capture now stamps each new
// entry with `**Project:** <repo>`, and this check flags any stamped entry
// whose project ≠ the current repo. Unstamped legacy entries are NOT flagged
// (they predate the stamp; flagging them all would be noise) — the enforcement
// is on new, stamped writes.
//
// Usage:
//   node scripts/patterns-scope-check.mjs [--project <name>]
// Warning, not a gate (exit 0): a legitimately mis-stamped entry is a data
// error to fix, not a reason to fail every /validate. Fail-open on any IO.

import { readdirSync, readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

// ---------------------------------------------------------------------------
// Pure core (unit-tested)
// ---------------------------------------------------------------------------

const norm = s => String(s || '').trim().toLowerCase();

/**
 * Parse `## ` / `### ` headed pattern entries and return those whose
 * `**Project:** <name>` stamp does not match repoProject. Unstamped entries
 * are skipped (legacy). Match is case-insensitive on a trimmed name.
 * @param {string} text     patterns-project.md contents
 * @param {string} repoProject  the current repo's project name
 * @returns {Array<{name:string, project:string}>}
 */
export function findForeignPatternEntries(text, repoProject) {
  const target = norm(repoProject);
  if (!text || !target) return [];
  const lines = text.split('\n');
  const foreign = [];
  let curName = null;
  let curProject = null;
  const flush = () => {
    if (curName && curProject != null && norm(curProject) !== target) {
      foreign.push({ name: curName, project: curProject });
    }
  };
  for (const line of lines) {
    const h = line.match(/^#{2,3}\s+(.*\S)\s*$/);
    if (h) {
      flush();
      curName = h[1].trim();
      curProject = null;
      continue;
    }
    const p = line.match(/^\s*\*\*Project:\*\*\s*(.+?)\s*$/);
    if (p && curName && curProject == null) curProject = p[1].trim();
  }
  flush();
  return foreign;
}

// ---------------------------------------------------------------------------
// CLI wiring (fail-open)
// ---------------------------------------------------------------------------

function resolveRepoProject(argProject) {
  if (argProject) return argProject;
  try {
    const pkg = JSON.parse(readFileSync('package.json', 'utf8'));
    if (pkg && pkg.name) return pkg.name;
  } catch { /* fall through */ }
  return process.cwd().split('/').filter(Boolean).pop() || '';
}

function main() {
  const argv = process.argv.slice(2);
  const pi = argv.indexOf('--project');
  const repoProject = resolveRepoProject(pi !== -1 ? argv[pi + 1] : null);
  const skillsRoot = '.claude/skills';
  if (!existsSync(skillsRoot)) {
    console.log('patterns-scope: no .claude/skills — skipping');
    return 0;
  }
  let dirs;
  try {
    dirs = readdirSync(skillsRoot).filter(d => d.startsWith('cabinet-'));
  } catch (e) {
    console.log(`patterns-scope: skipped (${e.message})`);
    return 0;
  }
  const offenders = [];
  for (const d of dirs) {
    const f = join(skillsRoot, d, 'patterns-project.md');
    if (!existsSync(f)) continue;
    let text;
    try { text = readFileSync(f, 'utf8'); } catch { continue; }
    for (const e of findForeignPatternEntries(text, repoProject)) {
      offenders.push({ file: f, ...e });
    }
  }
  if (offenders.length === 0) {
    console.log(`patterns-scope: no cross-project contamination (project "${repoProject}").`);
    return 0;
  }
  console.log('');
  console.log(`⚠ patterns-scope: ${offenders.length} pattern entr(y/ies) stamped for a FOREIGN project`);
  console.log(`  (this repo is "${repoProject}"). A pattern from another project poisons this`);
  console.log('  repo\'s audits — remove it, or re-file it in its own project:');
  for (const o of offenders) {
    console.log(`    • ${o.file}: "${o.name}" (stamped **Project:** ${o.project})`);
  }
  return 0; // warning, not a gate
}

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exit(main());
}

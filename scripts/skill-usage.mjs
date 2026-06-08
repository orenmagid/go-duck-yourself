#!/usr/bin/env node
// skill-usage.mjs — dead-skill reader over the skill-telemetry JSONL.
//
// The hooks module's skill-telemetry.sh / skill-tool-telemetry.sh write a
// `skill-invoke` record every time a skill runs. This reader closes the loop
// eval-protocol.md already assumes: cross-reference installed user-invocable
// skills against those records to surface NEVER-INVOKED (dead) and STALE
// skills — the anti-bloat signal roster-check and process-therapist reason
// about. Read-only: it never mutates telemetry or skills.
//
// Usage:
//   node scripts/skill-usage.mjs [--days N] [--json] [--quiet]
//        [--telemetry PATH] [--skills-dir PATH]
//
// Defaults:
//   --telemetry   ~/.claude/telemetry/telemetry.jsonl
//   --skills-dir  .claude/skills (falls back to ~/.claude/skills)
//   --days        30  (stale threshold; matches eval-protocol's monthly cadence)
//
// Output:
//   default  human-readable report
//   --json   structured object { generated, days, dead[], stale[], active[],
//            orphanTelemetry[], excluded[] }
//   --quiet  prints only when there is something to flag (dead/stale); for
//            embedding in roster-check / process-therapist passes
//
// Note on scope: only skills WITHOUT `user-invocable: false` in their
// frontmatter are candidates. Cabinet members and other non-invocable skills
// are spawned as agents, not invoked as slash/Skill calls, so they never
// appear in telemetry — counting them as "dead" would be a false positive.
// They are listed under `excluded` for transparency.

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

function parseArgs(argv) {
  const args = { days: 30, json: false, quiet: false, telemetry: null, skillsDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--json') args.json = true;
    else if (a === '--quiet') args.quiet = true;
    else if (a === '--days') args.days = parseInt(argv[++i], 10);
    else if (a === '--telemetry') args.telemetry = argv[++i];
    else if (a === '--skills-dir') args.skillsDir = argv[++i];
  }
  if (!Number.isFinite(args.days) || args.days < 0) args.days = 30;
  return args;
}

function resolveTelemetry(explicit) {
  if (explicit) return explicit;
  return path.join(os.homedir(), '.claude', 'telemetry', 'telemetry.jsonl');
}

function resolveSkillsDir(explicit) {
  if (explicit) return explicit;
  const local = path.resolve('.claude/skills');
  if (fs.existsSync(local)) return local;
  return path.join(os.homedir(), '.claude', 'skills');
}

// Read invocation records → Map(skill → {count, firstTs, lastTs}).
function readTelemetry(file) {
  const usage = new Map();
  let lines = [];
  try {
    lines = fs.readFileSync(file, 'utf8').split('\n');
  } catch {
    return usage; // missing telemetry → everything reads as never-invoked
  }
  for (const line of lines) {
    const t = line.trim();
    if (!t) continue;
    let rec;
    try { rec = JSON.parse(t); } catch { continue; }
    if (rec.event !== 'skill-invoke' || !rec.skill) continue;
    const ts = rec.ts || '';
    const cur = usage.get(rec.skill) || { count: 0, firstTs: ts, lastTs: ts };
    cur.count += 1;
    if (ts && ts < cur.firstTs) cur.firstTs = ts;
    if (ts && ts > cur.lastTs) cur.lastTs = ts;
    usage.set(rec.skill, cur);
  }
  return usage;
}

// Discover installed skills → [{name, invocable}].
function discoverSkills(dir) {
  const out = [];
  let entries = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    if (e.name.startsWith('_')) continue; // _template etc.
    const skillMd = path.join(dir, e.name, 'SKILL.md');
    if (!fs.existsSync(skillMd)) continue;
    let invocable = true;
    try {
      const head = fs.readFileSync(skillMd, 'utf8').slice(0, 1500);
      // frontmatter flag: `user-invocable: false`
      if (/^\s*user-invocable:\s*false\s*$/m.test(head)) invocable = false;
    } catch { /* default invocable */ }
    out.push({ name: e.name, invocable });
  }
  return out;
}

function daysBetween(thenIso, nowMs) {
  if (!thenIso) return Infinity;
  const t = Date.parse(thenIso);
  if (Number.isNaN(t)) return Infinity;
  return Math.floor((nowMs - t) / 86400000);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const telemetryFile = resolveTelemetry(args.telemetry);
  const skillsDir = resolveSkillsDir(args.skillsDir);
  const nowMs = Date.now();

  const usage = readTelemetry(telemetryFile);
  const skills = discoverSkills(skillsDir);

  const dead = [];      // invocable, zero records
  const stale = [];     // invocable, last invoke older than threshold
  const active = [];     // invocable, invoked within threshold
  const excluded = [];  // non-invocable (cabinet members etc.)

  const installedNames = new Set(skills.map((s) => s.name));

  for (const s of skills) {
    if (!s.invocable) { excluded.push(s.name); continue; }
    const u = usage.get(s.name);
    if (!u || u.count === 0) { dead.push({ skill: s.name }); continue; }
    const ageDays = daysBetween(u.lastTs, nowMs);
    const row = { skill: s.name, count: u.count, lastTs: u.lastTs, ageDays };
    if (ageDays > args.days) stale.push(row);
    else active.push(row);
  }

  // Telemetry skills not in THIS project's skills dir. Telemetry is global
  // (~/.claude/telemetry) but skills-dir is project-local, so this list mixes
  // other projects' skills with genuinely removed ones. Plugin-namespaced
  // skills (`plugin:skill`) live in plugins, never .claude/skills — drop them
  // as definitional non-orphans to cut noise.
  const orphanTelemetry = [];
  for (const [skill, u] of usage) {
    if (installedNames.has(skill)) continue;
    if (skill.includes(':')) continue; // plugin skill, not a project skill
    orphanTelemetry.push({ skill, count: u.count, lastTs: u.lastTs });
  }

  dead.sort((a, b) => a.skill.localeCompare(b.skill));
  stale.sort((a, b) => b.ageDays - a.ageDays);
  active.sort((a, b) => b.count - a.count);
  orphanTelemetry.sort((a, b) => b.count - a.count);

  const result = {
    generated: new Date(nowMs).toISOString(),
    telemetryFile,
    skillsDir,
    days: args.days,
    totals: { installed: skills.length, invocable: skills.length - excluded.length,
              dead: dead.length, stale: stale.length, active: active.length },
    dead, stale, active, orphanTelemetry, excluded,
  };

  if (args.json) {
    process.stdout.write(JSON.stringify(result, null, 2) + '\n');
    return;
  }

  const hasFlags = dead.length > 0 || stale.length > 0;
  if (args.quiet && !hasFlags) return; // silent when nothing to surface

  const L = [];
  L.push(`Skill usage (telemetry: ${telemetryFile})`);
  L.push(`  ${result.totals.invocable} invocable skills · stale threshold ${args.days}d · ${excluded.length} non-invocable excluded`);
  L.push('');
  if (dead.length) {
    L.push(`DEAD — never invoked (${dead.length}): removal/trigger candidates`);
    for (const d of dead) L.push(`  • ${d.skill}`);
    L.push('');
  }
  if (stale.length) {
    L.push(`STALE — not invoked in ${args.days}+ days (${stale.length}):`);
    for (const s of stale) L.push(`  • ${s.skill}  (${s.count}× total, last ${s.ageDays}d ago)`);
    L.push('');
  }
  if (orphanTelemetry.length) {
    L.push(`ORPHAN telemetry — invoked but not in this project's skills (${orphanTelemetry.length}): other projects or removed (telemetry is global)`);
    for (const o of orphanTelemetry.slice(0, 15)) L.push(`  • ${o.skill}  (${o.count}×)`);
    L.push('');
  }
  if (!args.quiet && active.length) {
    L.push(`ACTIVE — invoked within ${args.days}d (${active.length}), most-used first:`);
    for (const a of active.slice(0, 10)) L.push(`  • ${a.skill}  (${a.count}×, last ${a.ageDays}d ago)`);
  }
  if (!dead.length && !stale.length) L.push('No dead or stale invocable skills. ✓');
  process.stdout.write(L.join('\n') + '\n');
}

main();

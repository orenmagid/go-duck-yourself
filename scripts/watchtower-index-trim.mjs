#!/usr/bin/env node
// watchtower-index-trim.mjs — the one-pass pointer removal from MEMORY.md
// (Simplification Project 1, plan C2, act:6a7dffa2).
//
// WHAT IT REMOVES (decision (h), defaults 16 / 28): ONLY what the platform
// cannot understand —
//   * region-pointer lines: the backtick-glob shape  region `<prefix>*.md` → …
//     however the line is prefixed (list bullet, indent, blockquote, or bare);
//   * the MEMORY-archive.md pointer line: a list item or a whole-line italic
//     note whose markdown link targets MEMORY-archive.md.
// EVERY other line is KEEP, unconditionally — curated lesson / decision /
// session lines, the pinned block, the recency block, section headers, the
// topic-files block, italic notes. Those are left to the platform's own
// consolidation and the sessions' native writes. One pass; there is no second.
//
// UNSURE IS KEEP. A line that mentions region pointers in prose, carries a
// glob that is not the pointer shape, or links MEMORY-archive.md from a plain
// paragraph is KEEP and listed under `review` — never DROP on a loose match.
// A KEEP line whose TEXT is known stale is flagged under `stale` for a native
// memory edit by a session; this script never edits content.
//
// MAP FIRST (rule: plan-before-bulk-or-irreversible-actions).
//   --plan --cwd <project> [--json] [--out <file>]
//       reads MEMORY.md and prints EVERY line with DROP or KEEP, the before /
//       projected-after counts, the review and stale notes, and which apply
//       gates hold today. Writes NOTHING (only the --out file when asked; the
//       recall index is not even opened — sqlite would touch its sidecars).
//   --apply --cwd <project> --map <file> [--json]
//       rewrites MEMORY.md dropping exactly the map's DROP lines and
//       preserving every other byte (line endings, trailing whitespace, final
//       newline), temp + rename, AFTER writing the pre-trim copy to
//       <WATCHTOWER_DIR>/state/recall/<slug>/MEMORY-pre-trim-<date>.md —
//       OUTSIDE the memory dir (default 29) and named from the recall
//       script's single exclusion list, so the recall corpus never indexes a
//       copy of the old index. MEMORY-archive.md and every memory file on disk
//       are left untouched (decision (c)). Reversal is a MERGE of the copy
//       back, not a copy-back: the native writer may add lines after the trim.
//
// --apply REFUSES (exit 4, every unmet condition named at once) unless ALL of:
//   1. the recall index reports docs for this project (recall `status`);
//   2. <project>/.claude/skills/recall/SKILL.md exists;
//   3. the consumer's .ccrc.json manifest hash for
//      .claude/rules/memory-capture.md is the CURRENT rewritten template —
//      the trim never lands where a session could still be reading the old
//      rule;
//   4. MEMORY.md is byte-identical to what the map was made from (the native
//      writer may have added lines — re-plan);
//   5. the map covers every line, carries only DROP / KEEP, was made for this
//      project, and DROPs nothing that is not a pointer line by THIS script's
//      pattern (an operator may flip a DROP to KEEP, never the reverse);
//   6. no same-day pre-trim copy exists (never overwritten).
//
// The rule hash is a PINNED CONSTANT because this script runs from the
// watchtower runtime tier, where no template tree exists to hash; the test
// (`__tests__/index-trim.test.mjs`) pins it against
// templates/rules/memory-capture.md so a rule edit fails the suite until the
// constant follows (the ring2 releasing-vocabulary precedent).
//
// Project and memory-dir resolution are the recall script's (resolveProject /
// prepareProject) — one resolver. Honours WATCHTOWER_DIR; --home / --config /
// --watchtower-dir redirect for fixtures. Exit codes: 0 ok, 1 usage,
// 2 unregistered project or unreadable config, 4 refused.

import {
  readFileSync, writeFileSync, existsSync, statSync, mkdirSync, renameSync,
  unlinkSync, realpathSync,
} from 'fs';
import { join, dirname, resolve as resolvePath } from 'path';
import { homedir } from 'os';
import { createHash } from 'crypto';
import { pathToFileURL } from 'url';
import { loadConfig, getWatchtowerDir } from './watchtower-lib.mjs';
import {
  MEMORY_INDEX_EXCLUSIONS, isExcludedMemoryFile, resolveProject, prepareProject,
  status as recallStatus,
} from './watchtower-recall.mjs';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const MAP_SCHEMA = 1;
export const DISPOSITIONS = ['DROP', 'KEEP'];
export const MEMORY_INDEX_NAME = 'MEMORY.md';
export const ARCHIVE_INDEX_NAME = 'MEMORY-archive.md';
export const RECALL_SKILL_PATH = join('.claude', 'skills', 'recall', 'SKILL.md');
export const MEMORY_RULE_MANIFEST_KEY = '.claude/rules/memory-capture.md';
// The installer's manifest hash (lib/copy.js: sha256, first 16 hex) of the
// CURRENT templates/rules/memory-capture.md. Pinned by the test. Moved from
// 6723122f4c360e69 when the recall cue's one-sentence A2 edit landed in the
// rule (act:e46ce733, operator ruling 2026-10-09).
export const CURRENT_MEMORY_RULE_HASH = '8905c4770df11c3e';
const TEXT_TRUNCATE = 160;

// region `<glob>` — optional indent / blockquote, optional list bullet, the
// keyword, then a backtick-quoted glob with no whitespace that contains `*`
// and ends `.md`. (The retired reachability parser's shape, widened to the
// unbulleted form and narrowed to a real glob.)
export const REGION_POINTER_RE = /^[\s>]*(?:[-*+]\s+)?region\s+`([^`\s]*\*[^`\s]*\.md)`/i;
const ARCHIVE_LINK_RE = /\]\(\s*(?:\.\/)?MEMORY-archive\.md\s*\)/;
const LIST_ITEM_RE = /^[\s>]*[-*+]\s+\S/;
const WHOLE_LINE_ITALIC_RE = /^\s*(?:_[^_].*_|\*[^*\s].*\*)\s*$/;
const REGION_MENTION_RE = /region[\s-]+pointers?/i;
// A file glob anywhere in the line: `<prefix>*….md` or a bare `*.md`. The
// star must touch a filename character (or be a bare `*.md`), so markdown bold
// around a topic file (`**decisions.md**`) is not mistaken for one.
const LOOSE_GLOB_RE = /[\w-]\*[\w.*-]*\.md\b|(?<![*\w])\*\.md\b/;
const ARCHIVE_MENTION_RE = /MEMORY-archive\.md/;
const MD_LINK_RE = /\]\(\s*([^)\s]+\.md)\s*\)/g;

// KEEP lines whose TEXT is known stale, keyed by the file the line links.
// Flagged in the map for a native memory edit; never edited here.
export const KNOWN_STALE = [
  {
    target: 'preference_parallel_exec_mux_windows_over_subagents.md',
    note: 'act:518d0e33 — the pinned windows-over-subagents ruling is stale; fix it with a native memory edit (rewrite the line and the file), not with this script',
  },
];

// ---------------------------------------------------------------------------
// Pure: bytes → lines → dispositions
// ---------------------------------------------------------------------------

export function sha256Hex(buf) {
  return createHash('sha256').update(buf).digest('hex');
}

// Split a buffer into line segments, each INCLUDING its terminator, so that
// Buffer.concat(segments) is the input byte for byte. A final line without a
// newline is its own segment; a trailing newline yields no empty segment.
export function splitSegments(buf) {
  const segments = [];
  let start = 0;
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] === 0x0a) { segments.push(buf.subarray(start, i + 1)); start = i + 1; }
  }
  if (start < buf.length) segments.push(buf.subarray(start));
  return segments;
}

export function segmentText(seg) {
  return seg.toString('utf8').replace(/\r?\n$/, '');
}

// One line → { kind, disposition, review?, stale? }. `kind` is
// 'region-pointer' | 'archive-pointer' | 'other'; only the two pointer kinds
// are ever DROP.
export function classifyLine(text) {
  if (REGION_POINTER_RE.test(text)) return { kind: 'region-pointer', disposition: 'DROP' };

  const linksArchive = ARCHIVE_LINK_RE.test(text);
  const mentionsRegion = REGION_MENTION_RE.test(text);
  if (linksArchive && !mentionsRegion && (LIST_ITEM_RE.test(text) || WHOLE_LINE_ITALIC_RE.test(text))) {
    return { kind: 'archive-pointer', disposition: 'DROP' };
  }

  const out = { kind: 'other', disposition: 'KEEP' };
  const reasons = [];
  if (mentionsRegion) reasons.push('mentions region pointers in prose — its text will be stale after the trim, but it is not a pointer line');
  if (LOOSE_GLOB_RE.test(text)) reasons.push('carries a file glob but is not the region-pointer shape (region `<prefix>*.md`)');
  if (ARCHIVE_MENTION_RE.test(text)) reasons.push(`mentions ${ARCHIVE_INDEX_NAME} but is not a dedicated pointer line (a list item or a whole-line italic note linking it)`);
  if (reasons.length) out.review = reasons.join('; ');
  for (const s of KNOWN_STALE) {
    if (text.includes(`](${s.target})`)) { out.stale = s.note; break; }
  }
  return out;
}

export function isPointerLine(text) {
  return classifyLine(text).disposition === 'DROP';
}

function isDirectIndexLine(text) {
  for (const m of text.matchAll(MD_LINK_RE)) {
    const target = m[1].replace(/^\.\//, '');
    if (target !== ARCHIVE_INDEX_NAME) return true;
  }
  return false;
}

function countStats(entries) {
  const s = { lines: 0, bytes: 0, direct_index_lines: 0, pointer_lines: 0, archive_pointer_lines: 0 };
  for (const e of entries) {
    s.lines++;
    s.bytes += e.bytes;
    if (e.kind === 'region-pointer') s.pointer_lines++;
    else if (e.kind === 'archive-pointer') s.archive_pointer_lines++;
    else if (isDirectIndexLine(e.text)) s.direct_index_lines++;
  }
  return s;
}

// The disposition map of one MEMORY.md buffer. Pure.
export function planBuffer(buf) {
  const entries = splitSegments(buf).map((seg, i) => {
    const text = segmentText(seg);
    return { n: i + 1, text, bytes: seg.length, ...classifyLine(text) };
  });
  const kept = entries.filter((e) => e.disposition === 'KEEP');
  return {
    source_sha256: sha256Hex(buf),
    before: countStats(entries),
    after: countStats(kept),
    drop_count: entries.length - kept.length,
    keep_count: kept.length,
    lines: entries.map(({ n, disposition, kind, text }) => ({ n, disposition, kind, text })),
    review: entries.filter((e) => e.review).map((e) => ({ n: e.n, reason: e.review, text: e.text })),
    stale: entries.filter((e) => e.stale).map((e) => ({ n: e.n, note: e.stale, text: e.text })),
  };
}

// ---------------------------------------------------------------------------
// Paths and gates
// ---------------------------------------------------------------------------

// The pre-trim copy's name comes from the recall script's ONE exclusion list.
export function preTrimFileName(date) {
  const pattern = MEMORY_INDEX_EXCLUSIONS.find((p) => p.includes('*'));
  if (!pattern) throw new Error('MEMORY_INDEX_EXCLUSIONS carries no pre-trim pattern');
  const name = pattern.replace('*', date);
  if (!isExcludedMemoryFile(name)) throw new Error(`${name} is not on the recall exclusion list`);
  return name;
}

export function preTrimPath(slug, date, watchtowerDir = getWatchtowerDir()) {
  return join(watchtowerDir, 'state', 'recall', slug, preTrimFileName(date));
}

function skillGate(projectPath) {
  const path = join(projectPath, RECALL_SKILL_PATH);
  return { holds: existsSync(path), path };
}

function ruleGate(projectPath) {
  const ccrcPath = join(projectPath, '.ccrc.json');
  let found = null;
  let error = null;
  try {
    const ccrc = JSON.parse(readFileSync(ccrcPath, 'utf8'));
    const v = ccrc && ccrc.manifest ? ccrc.manifest[MEMORY_RULE_MANIFEST_KEY] : null;
    found = typeof v === 'string' ? v : null;
  } catch (e) {
    error = e.code === 'ENOENT' ? 'no .ccrc.json' : `unreadable .ccrc.json (${e.message})`;
  }
  return { holds: found === CURRENT_MEMORY_RULE_HASH, expected: CURRENT_MEMORY_RULE_HASH, found, ccrc_path: ccrcPath, ...(error ? { error } : {}) };
}

function resolve({ cwd, config, watchtowerDir, homeDir }) {
  const id = resolveProject({ cwd, config, homeDir });
  if (!id) return null;
  return prepareProject({ name: id.name, projectPath: id.path, config, watchtowerDir, homeDir });
}

// ---------------------------------------------------------------------------
// --plan — reads only
// ---------------------------------------------------------------------------

export function planProject({ cwd, config, watchtowerDir = getWatchtowerDir(), homeDir = homedir(), now = new Date().toISOString() }) {
  const proj = resolve({ cwd, config, watchtowerDir, homeDir });
  if (!proj) return { status: 'unregistered', cwd };
  const base = {
    schema: MAP_SCHEMA, tool: 'watchtower-index-trim', project: proj.name, slug: proj.slug,
    project_path: proj.projectPath, memory_dir: proj.memoryDir,
  };
  if (!proj.memoryDir) return { status: 'no-memory-dir', ...base };
  const indexFile = join(proj.memoryDir, MEMORY_INDEX_NAME);
  if (!existsSync(indexFile)) return { status: 'no-memory-index', ...base, memory_index: indexFile };
  const buf = readFileSync(indexFile);

  // Plan mode never opens the sqlite index (a connection writes WAL sidecars
  // and a meta row); it reports the file's presence and leaves the doc count
  // to --apply.
  let indexBytes = 0;
  const indexExists = existsSync(proj.indexPath);
  if (indexExists) { try { indexBytes = statSync(proj.indexPath).size; } catch { /* raced away */ } }

  return {
    status: 'ok', ...base, memory_index: indexFile, generated_at: now,
    ...planBuffer(buf),
    gates: {
      recall_index: {
        index_file_exists: indexExists, index_path: proj.indexPath, bytes: indexBytes,
        note: 'not opened in plan mode — --apply requires recall `status` to report docs',
      },
      recall_skill: skillGate(proj.projectPath),
      rule_hash: ruleGate(proj.projectPath),
    },
  };
}

// ---------------------------------------------------------------------------
// --apply — gated; every unmet condition named at once; nothing partial
// ---------------------------------------------------------------------------

export function validateMap(map, { slug, segments, currentSha }) {
  const refusals = [];
  if (!map || typeof map !== 'object' || !Array.isArray(map.lines)) {
    return ['the map is not an index-trim map (no `lines` array) — make one with --plan'];
  }
  if (map.slug !== slug) {
    refusals.push(`the map was made for a different project (${map.slug || 'unnamed'}), not ${slug} — make one with --plan --cwd <this project>`);
  }
  for (const l of map.lines) {
    if (!l || !DISPOSITIONS.includes(l.disposition)) {
      refusals.push(`line ${l && l.n}: disposition ${JSON.stringify(l && l.disposition)} is not DROP or KEEP (${l && l.disposition} is not a disposition this pass has — decision (h))`);
    }
  }
  if (map.source_sha256 !== currentSha) {
    refusals.push('MEMORY.md changed since the map was made (the native writer may have added lines) — re-run --plan, review the new map, and apply that one');
    return refusals; // line numbers no longer mean anything
  }
  if (map.lines.length !== segments.length || map.lines.some((l, i) => !l || l.n !== i + 1)) {
    refusals.push(`the map does not cover every line of MEMORY.md in order (${map.lines.length} map lines, ${segments.length} file lines) — re-run --plan`);
    return refusals;
  }
  map.lines.forEach((l, i) => {
    if (l.disposition === 'DROP' && !isPointerLine(segmentText(segments[i]))) {
      refusals.push(`line ${l.n} is marked DROP but is not a pointer line by this script's pattern — this pass drops only region pointers and the ${ARCHIVE_INDEX_NAME} pointer; a wrong KEEP line is fixed by a native memory edit`);
    }
  });
  return refusals;
}

export function applyMap({ cwd, config, map, watchtowerDir = getWatchtowerDir(), homeDir = homedir(), now = new Date().toISOString() }) {
  const proj = resolve({ cwd, config, watchtowerDir, homeDir });
  if (!proj) return { status: 'unregistered', cwd };
  const refuse = (refusals) => ({ status: 'refused', project: proj.name, slug: proj.slug, refusals });
  if (!proj.memoryDir) return refuse(['the project has no resolvable memory dir']);
  const indexFile = join(proj.memoryDir, MEMORY_INDEX_NAME);
  if (!existsSync(indexFile)) return refuse([`no ${MEMORY_INDEX_NAME} at ${indexFile} — nothing to trim`]);

  const buf = readFileSync(indexFile);
  const currentSha = sha256Hex(buf);
  const segments = splitSegments(buf);
  const refusals = validateMap(map, { slug: proj.slug, segments, currentSha });

  // Gate 1 — the recall index reports docs (the recall script's own status).
  let st;
  try { st = recallStatus(proj); } catch (e) { st = { status: 'error', message: e.message }; }
  if (!(st.status === 'ok' && st.docs > 0)) {
    const why = st.status === 'ok' ? 'the index holds 0 docs' : `status ${st.status}${st.message ? ` — ${st.message}` : ''}`;
    refusals.push(`no recall index with docs for ${proj.name} (${why}; ${proj.indexPath}) — the pointer tier must not be removed before recall can reach it: run watchtower-recall.mjs build --cwd <project>`);
  }
  // Gate 2 — the consumer has the /recall skill.
  const skill = skillGate(proj.projectPath);
  if (!skill.holds) refusals.push(`${skill.path} does not exist (.claude/skills/recall/SKILL.md) — upgrade this consumer first (/cc-upgrade installs the /recall skill)`);
  // Gate 3 — the consumer's installed memory-capture rule is the current one.
  const rule = ruleGate(proj.projectPath);
  if (!rule.holds) {
    refusals.push(`${rule.ccrc_path} manifest hash for ${MEMORY_RULE_MANIFEST_KEY} is ${rule.found || rule.error || 'absent'}, not the current rewritten template (${rule.expected}) — a session here could still be reading the old rule: upgrade this consumer first`);
  }
  // Gate 6 — never overwrite a same-day copy.
  const date = String(now).slice(0, 10);
  const copyPath = preTrimPath(proj.slug, date, watchtowerDir);
  if (existsSync(copyPath)) refusals.push(`a pre-trim copy already exists at ${copyPath} — it is never overwritten; move it aside by hand (or apply tomorrow) if a second trim is really intended`);

  if (refusals.length) return refuse(refusals);

  const before = planBuffer(buf).before;
  const keptSegments = segments.filter((_, i) => map.lines[i].disposition === 'KEEP');
  const out = Buffer.concat(keptSegments);
  const droppedLines = map.lines.filter((l) => l.disposition === 'DROP').map((l) => ({ n: l.n, text: segmentText(segments[l.n - 1]) }));

  // The copy first (exclusive create), read back, then temp + rename.
  mkdirSync(dirname(copyPath), { recursive: true });
  writeFileSync(copyPath, buf, { flag: 'wx' });
  if (!readFileSync(copyPath).equals(buf)) {
    throw new Error(`the pre-trim copy at ${copyPath} did not read back byte-identical — MEMORY.md was NOT changed`);
  }
  const tmp = join(proj.memoryDir, '.MEMORY.md.index-trim.tmp');
  writeFileSync(tmp, out);
  // The native writer may have landed a line between the read and now.
  if (sha256Hex(readFileSync(indexFile)) !== currentSha) {
    unlinkSync(tmp);
    unlinkSync(copyPath);
    return refuse(['MEMORY.md changed since the map was made (it was written during the apply) — nothing was changed; re-run --plan']);
  }
  renameSync(tmp, indexFile);
  if (!readFileSync(indexFile).equals(out)) {
    throw new Error(`MEMORY.md did not read back as written — the pre-trim copy is at ${copyPath}`);
  }

  return {
    status: 'applied', project: proj.name, slug: proj.slug, memory_index: indexFile,
    pre_trim_path: copyPath, applied_at: now, dropped: droppedLines.length, dropped_lines: droppedLines,
    before, after: planBuffer(out).before,
  };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const out = { mode: null, json: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--plan' || a === '--apply') {
      if (out.mode) { console.error('give exactly one of --plan or --apply'); process.exit(1); }
      out.mode = a.slice(2);
    } else if (a === '--json') out.json = true;
    else if (a === '--cwd') out.cwd = argv[++i];
    else if (a === '--map') out.map = argv[++i];
    else if (a === '--out') out.out = argv[++i];
    else if (a === '--home') out.homeDir = argv[++i];
    else if (a === '--watchtower-dir') out.watchtowerDir = argv[++i];
    else if (a === '--config') out.configPath = argv[++i];
    else { console.error(`unknown argument: ${a}`); process.exit(1); }
  }
  return out;
}

const USAGE = [
  'usage: watchtower-index-trim.mjs --plan  --cwd <project> [--json] [--out <map file>]',
  '       watchtower-index-trim.mjs --apply --cwd <project> --map <map file> [--json]',
].join('\n');

const clip = (s) => (s.length > TEXT_TRUNCATE ? `${s.slice(0, TEXT_TRUNCATE - 1)}…` : s);
const statLine = (s) => `${s.lines} lines / ${s.bytes} bytes / ${s.direct_index_lines} direct index lines / ${s.pointer_lines} region-pointer lines / ${s.archive_pointer_lines} archive-pointer lines`;

export function renderPlan(map) {
  const out = [];
  out.push(`index-trim plan — ${map.project} (${map.slug})`);
  out.push(`MEMORY.md: ${map.memory_index}`);
  out.push(`before: ${statLine(map.before)}`);
  out.push(`after:  ${statLine(map.after)}  (projected)`);
  out.push(`DROP ${map.drop_count} · KEEP ${map.keep_count}`);
  out.push('');
  const width = String(map.lines.length).length;
  for (const l of map.lines) out.push(`${String(l.n).padStart(width)}  ${l.disposition}  ${clip(l.text)}`);
  out.push('');
  if (map.review.length) {
    out.push('review — KEEP lines the pattern was unsure about (never dropped on a loose match):');
    for (const r of map.review) out.push(`  line ${r.n}: ${r.reason}`);
  } else {
    out.push('review — none');
  }
  if (map.stale.length) {
    out.push('stale — KEEP lines whose text is known wrong (a native memory edit, not this script):');
    for (const s of map.stale) out.push(`  line ${s.n}: ${s.note}`);
  }
  const g = map.gates;
  out.push('apply gates today:');
  out.push(`  recall index: ${g.recall_index.index_file_exists ? `file present (${g.recall_index.bytes} bytes)` : 'NO index file'} — docs are counted at --apply`);
  out.push(`  /recall skill: ${g.recall_skill.holds ? 'present' : `MISSING (${g.recall_skill.path})`}`);
  out.push(`  memory-capture rule: ${g.rule_hash.holds ? 'current' : `NOT current (found ${g.rule_hash.found || g.rule_hash.error || 'absent'}, current ${g.rule_hash.expected})`}`);
  return out.join('\n') + '\n';
}

const isMain = (() => {
  try { return process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
})();

if (isMain) {
  const args = parseArgs(process.argv.slice(2));
  if (!args.mode || !args.cwd || (args.mode === 'apply' && !args.map)) {
    if (args.mode === 'apply' && args.cwd && !args.map) console.error('--apply requires --map <file> (the approved map from --plan --out)');
    console.error(USAGE);
    process.exit(1);
  }
  const watchtowerDir = args.watchtowerDir || getWatchtowerDir();
  const homeDir = args.homeDir || homedir();
  let config;
  try {
    config = args.configPath ? JSON.parse(readFileSync(args.configPath, 'utf8')) : loadConfig(watchtowerDir);
  } catch (e) {
    console.error(`index-trim: ${e.message} — run /watchtower configure to register this project.`);
    process.exit(2);
  }
  const cwd = resolvePath(args.cwd);
  const unregistered = () => {
    console.error(`index-trim: ${cwd} is not a registered watchtower project — run /watchtower configure to add it (exit 2).`);
    process.exit(2);
  };

  if (args.mode === 'plan') {
    const map = planProject({ cwd, config, watchtowerDir, homeDir });
    if (map.status === 'unregistered') unregistered();
    if (map.status !== 'ok') {
      if (args.json) process.stdout.write(JSON.stringify(map, null, 2) + '\n');
      else console.log(`index-trim: ${map.project} has no ${MEMORY_INDEX_NAME} to trim (${map.status}${map.memory_index ? `: ${map.memory_index}` : ''}).`);
      process.exit(0);
    }
    if (args.out) {
      const outPath = resolvePath(args.out);
      mkdirSync(dirname(outPath), { recursive: true });
      writeFileSync(outPath, JSON.stringify(map, null, 2) + '\n');
    }
    if (args.json) process.stdout.write(JSON.stringify(map, null, 2) + '\n');
    else {
      process.stdout.write(renderPlan(map));
      if (args.out) console.log(`map written to ${resolvePath(args.out)}`);
    }
    process.exit(0);
  }

  let map;
  try { map = JSON.parse(readFileSync(resolvePath(args.map), 'utf8')); } catch (e) {
    console.error(`index-trim: cannot read the map at ${args.map} (${e.message})`);
    process.exit(1);
  }
  const res = applyMap({ cwd, config, map, watchtowerDir, homeDir });
  if (res.status === 'unregistered') unregistered();
  if (res.status === 'refused') {
    console.error(`index-trim: REFUSED for ${res.project} — nothing was changed (exit 4):`);
    for (const r of res.refusals) console.error(`  - ${r}`);
    if (args.json) process.stdout.write(JSON.stringify(res, null, 2) + '\n');
    process.exit(4);
  }
  if (args.json) process.stdout.write(JSON.stringify(res, null, 2) + '\n');
  else {
    console.log(`index-trim: applied to ${res.project} — dropped ${res.dropped} pointer line(s).`);
    console.log(`before: ${statLine(res.before)}`);
    console.log(`after:  ${statLine(res.after)}`);
    console.log(`pre-trim copy: ${res.pre_trim_path}`);
    for (const d of res.dropped_lines) console.log(`  dropped line ${d.n}: ${clip(d.text)}`);
  }
}

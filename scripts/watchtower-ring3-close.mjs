#!/usr/bin/env node

// Watchtower Ring 3 — Close mode (post-session transcript processing).
//
// CORRECTNESS INVARIANT — end-of-transcript capture only. Every extraction
// phase reads the COMPLETE transcript once, after the session has ended.
// Nothing captures incrementally mid-session. This is deliberate, not
// incidental: in a live conversation decisions get reversed ("do X" …
// "actually Y" … "drop both"), and only the end of the transcript knows
// what survived. Incremental/streaming capture would record intermediate
// and abandoned states as if they were conclusions — wrong memories, which
// are worse than missing ones because at read time they are
// indistinguishable from right ones. A refactor toward streaming capture
// must preserve final-state-only semantics (see feedback 2026-07-29).
//
// Runs OUTSIDE of Claude Code, spawned by watchtower-session-end.sh via
// nohup/disown. Processes the session transcript through invariant phases:
//
//   Preprocessing  — compress transcript for cost control
//   2a: Worktree check — urgent inbox if session was in unmerged worktree
//   2b: Session summary — per-session file + project state pointer
//   2b2: Thread capture — identify threads, write/update cursors
//   2c: Work item closure — completion candidates to inbox
//   2d: Knowledge extraction — lessons/decisions/constraints to inbox
//   2e: Quality pattern capture — recurring patterns from any session
//   2f: Methodology capture — detect new skills/conventions
//   2g: Upstream friction — CC friction detection
//   2h: (removed 2026-06-12, act:6c3a4763 — feedback delivery is Ring 1's
//       flushFeedbackOutbox in watchtower-lib.mjs; see tombstone below)
//   2m: Session advisor pass — re-homed standing advisors, transcript-fed
//   2n: Raised-but-unhandled lens — loose ends (promises/side-issues/
//       open-questions) neither done nor filed → inbox (act:4ff2cfb3)
//   2o: Skill-candidate lens — repeated manual procedure → "make a skill?"
//       inbox item (act:4ff2cfb3)
//   2p: Checklist-catch detection — a surfaced check that caught a real bug
//       → checklist-stats.json (act:4ff2cfb3)
//   2i: Session auto-naming — generate descriptive session name
//   2j: Consumer hooks — ring3-close-post hooks
//   2k: Signal Ring 2 — write fast-trigger lock
//   2l: Health — write ring3-health.json
//
// All phases are structurally unskippable. Consumer hooks extend, not replace.

import {
  readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync,
  renameSync, statSync, realpathSync, rmSync,
} from 'fs';
import { join, basename, dirname } from 'path';
import { execSync, spawnSync } from 'child_process';
import { createRequire } from 'module';
import { homedir } from 'os';
import { pathToFileURL, fileURLToPath } from 'url';
import {
  atomicWrite, loadConfig, slugify,
  log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending, loadBetterSqlite3,
  updateThreadFile, currentCursor, resolveProjectIdentity, VERIFY_UI_PATHS,
  projectThreadCursorLines, authoredClaudeDirs, claudeChurnIsDisposable,
  buildLastSessionBlock, upsertLastSessionSection, recordSuppression, recordApiUsage,
  recentSlice,
  recordSignificanceEvent,
} from './watchtower-lib.mjs';
// Namespace view of the same module record — feature-detection seam for lib
// exports that land in a different lane's merge (a static named import of a
// not-yet-merged symbol would throw at module load and take every dynamic-
// import test suite down with it; a namespace property read is just
// undefined until the export exists). Consumer: resolveSessionProject's
// slug fallback (act:29001b07 — Lane A ships resolveProjectFromTranscriptSlug).
import * as watchtowerLib from './watchtower-lib.mjs';
// Direct queue import (precedent: ring2 imports expireItem this way) —
// watchtower-lib deliberately not extended for this (lane separation).
import { listItems, supersedeItem } from './watchtower-queue.mjs';
// Shared prospective-commitment seam (act:aa554774) — /close files them
// forward as actions; Phase 2d uses the SAME definition to decline to
// duplicate what the sweep already filed.
import { detectCommitments, findFiledCommitment } from './watchtower-commitments.mjs';
import { runRoutinePass } from './watchtower-routines.mjs';

const require = createRequire(import.meta.url);

const WATCHTOWER_DIR = getWatchtowerDir();

const CLAUDE_HOME = join(homedir(), '.claude');
const CONSUMER_HOOK_TIMEOUT_MS = 120_000;
const MODEL = 'claude-sonnet-4-6';

// --- Transcript-budget constants (M2 recall fix, act:edd79e15) ---
// The single-call input budget for the knowledge-critical passes (extraction
// 2d, advisor 2m, lenses 2n/2o/2p). preprocessTranscript caps the compressed
// transcript at ~100K tokens (~400K chars); feeding the most-recent
// SINGLE_CALL_TRANSCRIPT_BUDGET chars through recentSlice covers essentially
// every real session in one sonnet call (200K-token context). The old
// `slice(0, 50000)` read the OLDEST ~12.5K tokens and dropped late-session
// lessons — the M2 bug. Sessions whose compressed transcript exceeds this
// budget are the only ones Phase B's bounded chunk-and-merge (act:edd79e15
// pt2) handles; preprocessing's pre-truncation means that is the largest
// untruncated sessions only.
const SINGLE_CALL_TRANSCRIPT_BUDGET = 300_000;
// Completion detection (Phase 2c) stays deliberately recency-biased: a missed
// completion is recoverable next session, a missed lesson is not. It routes
// through recentSlice too — the recency bias is now an explicit CHOICE, not
// an accidental front-slice.
const COMPLETION_TRANSCRIPT_BUDGET = 50_000;
// M2 Phase B (act:edd79e15 pt2): when a compressed transcript exceeds the
// single-call budget, extraction chunks it into OVERLAPPING windows so a lesson
// straddling a boundary survives in at least one chunk. Bounded fallback only —
// preprocessTranscript already caps `compressed` at ~100K tokens (~400K chars),
// so with a 300K budget this fires only for the largest untruncated sessions
// (2 windows); MAX_EXTRACTION_CHUNKS is a defensive cap (a future preprocessing
// change can't make this unbounded). When the cap WOULD truncate, the most-
// recent windows are kept and a notice is logged (no silent truncation). Note:
// a >100K-token raw session already lost its FRONT to preprocessing before
// chunking — "full transcript" is false there; the cap is honest about it.
const PHASE_B_OVERLAP = 30_000;        // chars carried between adjacent windows
const MAX_EXTRACTION_CHUNKS = 4;

// --- Dedup tuning constants (Phase 2c/2d noise reduction) ---
// How far back resolved/dismissed inbox items count as a dedup corpus.
// Older dismissals no longer suppress — re-surfacing once a quarter is
// acceptable; the friction loop this kills is week-scale re-filing.
const RESOLUTION_CORPUS_DAYS = 90;
// Meaningful-token overlap required to suppress an extraction against ANY
// corpus (M1a parity, act:f8e7bd0a). One threshold for all four passes —
// memory, thread-cursor, pending, resolved, dismissed — so no pass can drift
// into being the weakest link. The dismissed pass used to suppress on 2-token
// overlap (boundary-man's #2 over-suppression risk); parity raised it to 3.
// "Meaningful" = post-stopword (the type-word prefix every title carries does
// not count). The short-title floor in isDuplicate guarantees a candidate has
// >=3 meaningful tokens before any pass can fire.
const OVERLAP_THRESHOLD = 3;
// A completion-review item resolved/dismissed within this window suppresses
// re-filing for the same fid (a dismissed "is this done?" must not re-file
// next session while the action is still open).
const COMPLETION_REVIEW_DEDUP_DAYS = 14;

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  const parsed = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--session-id') parsed.sessionId = args[++i];
    else if (args[i] === '--transcript') parsed.transcriptPath = args[++i];
    else if (args[i] === '--cwd') parsed.cwd = args[++i];
    else if (args[i] === '--reason') parsed.reason = args[++i];
    // --reprocess: replay ONE failed session (a fresh subprocess spawned by
    // the --reprocess-failed drain). Stamps suppressions with the session's
    // original date so a backfill can't flood the recall-canary window.
    else if (args[i] === '--reprocess') parsed.reprocess = true;
    // --reprocess-failed: drain the ring3/failed/ worklist (parent mode —
    // spawns one --reprocess subprocess per marker; never calls main()).
    else if (args[i] === '--reprocess-failed') parsed.reprocessFailed = true;
  }
  return parsed;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function log(msg) { _log('ring3-close', msg); }
function logError(msg) { _logError('ring3-close', msg); }

// Lazy-load Anthropic SDK with helpful error
// ESM import() ignores NODE_PATH; use createRequire for global installs
let _anthropicClient = null;
async function getAnthropicClient() {
  if (_anthropicClient) return _anthropicClient;
  try {
    const Anthropic = require('@anthropic-ai/sdk').default
      || require('@anthropic-ai/sdk');
    _anthropicClient = new Anthropic();
    return _anthropicClient;
  } catch (e) {
    throw new Error(
      'Ring 3 requires @anthropic-ai/sdk. Install it: npm install -g @anthropic-ai/sdk\n' +
      `Original error: ${e.message}`
    );
  }
}

// SYSTEMIC API FAILURE (act:e8793574): when the Anthropic API rejects EVERY
// call for an account-level reason (billing exhausted, bad key, quota), every
// knowledge-extraction phase silently produces nothing while Ring 3 still
// reports health "success" — the exact silent failure that let thread capture
// die for a week unnoticed (credit balance ran out 2026-07-05; no thread
// updated after). classifyApiError names those account-level classes so
// writeHealth can flip to "degraded" and the operator gets a signal. A
// one-off per-call error (a single malformed transcript) is NOT systemic and
// returns null.
export function classifyApiError(err) {
  if (!err) return null;
  const status = err.status ?? err.statusCode;
  const msg = String(err.message || err).toLowerCase();
  if (/credit balance is too low|billing|payment|purchase credits/.test(msg)) return 'billing';
  if (status === 401 || status === 403 || /invalid.*api.?key|authentication|unauthorized|permission/.test(msg)) return 'auth';
  if (status === 429 || /rate limit|quota|too many requests/.test(msg)) return 'rate-limit';
  if (status === 529 || /overloaded/.test(msg)) return 'overloaded';
  return null;
}

// Module-scoped latch: set by claudeCall on the first systemic API error so
// writeHealth (called once at the end of main) can report degraded health.
// Reset per process — each Ring 3 invocation is a fresh node process.
let systemicApiFailure = null;

// Per-process API-call accounting (act:6fb2b7d1). If EVERY API-dependent
// phase threw this run — not only the account-level `classifyApiError`
// classes — the session captured nothing and must NOT be marked processed;
// these counters detect that broader outage (network / 500 / timeout) at the
// single API chokepoint (claudeCall), without touching each phase.
let apiCallsAttempted = 0;
let apiCallsFailed = 0;

// Reprocess-mode date override (act:6fb2b7d1). In --reprocess mode this is
// the failed session's ORIGINAL date; the suppression call sites pass it as
// `record.ts` so backfilled ledger entries land in their historical window
// (aged out of the recall-canary's 14-day window) instead of spiking the
// current window with now()-stamped entries. Null in live mode → suppression
// stamps now() exactly as before (byte-identical live behavior).
let reprocessTs = null;

async function claudeCall(systemPrompt, userMessage) {
  apiCallsAttempted++;
  // Once a systemic failure is latched, every subsequent phase would fail the
  // same way — stop hammering a dead API (the July outage burned ~15 failed
  // calls per session across every phase). Fail fast; still counts as a failed
  // attempt so the all-phases-failed detector stays consistent (act:6fb2b7d1).
  if (systemicApiFailure) {
    apiCallsFailed++;
    throw new Error(`skipped — systemic API failure already latched (${systemicApiFailure.type})`);
  }
  try {
    // getAnthropicClient() is INSIDE the try: a client-construction failure
    // (e.g. a missing/invalid key during an outage) must count as a failed
    // call too, or the all-api-phases-failed detector would undercount and a
    // total-outage session could slip through as "not all failed" (act:6fb2b7d1).
    const client = await getAnthropicClient();
    const response = await client.messages.create({
      model: MODEL,
      max_tokens: 4096,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }],
    });
    recordApiUsage({ ring: 'ring3', model: MODEL, usage: response.usage }, { watchtowerDir: WATCHTOWER_DIR });
    return response.content[0]?.text || '';
  } catch (e) {
    apiCallsFailed++;
    const cls = classifyApiError(e);
    if (cls && !systemicApiFailure) {
      systemicApiFailure = { type: cls, message: String(e.message || e).slice(0, 300) };
    }
    throw e; // phases keep their own fail-open try/catch; the latch is a side-channel
  }
}

// ---------------------------------------------------------------------------
// Phase 2a: Worktree check — detect unmerged worktree at session close
// ---------------------------------------------------------------------------

function worktreeCheck(cwd, project) {
  const projectPath = project?.path;
  log('Phase 2a: Worktree check');

  if (!cwd || !projectPath) return 0;

  // Detect if session was in a worktree (cwd differs from project path
  // and lives under ~/.mux/worktrees/)
  const worktreesDir = join(homedir(), '.mux', 'worktrees');
  if (!cwd.startsWith(worktreesDir)) {
    log('Phase 2a: Not a worktree session, skipping');
    return 0;
  }

  // Check for unmerged commits relative to main
  const mainRef = safeExec('git rev-parse --verify main', { cwd: projectPath });
  if (!mainRef) {
    log('Phase 2a: Cannot resolve main branch');
    return 0;
  }

  const mergeBase = safeExec(`git merge-base HEAD ${mainRef}`, { cwd });
  if (!mergeBase) return 0;

  const aheadLog = safeExec(`git log --oneline ${mergeBase}..HEAD`, { cwd });
  const aheadCount = aheadLog ? aheadLog.split('\n').filter(l => l.trim()).length : 0;

  // Exclude CC/mux session artifacts (.claude/ infra, .mcp.json) and
  // node_modules — untracked in every mux worktree; counting it
  // produced false "unmerged work" alarms for fully-merged branches.
  // Authored .claude/ subtrees (plans, methodology, rules, …) ARE real work
  // and are re-included per the canonical exclusion contract (act:e91fdfcf,
  // claudeChurnIsDisposable in watchtower-lib).
  // safeExec trims output, which can shift porcelain column offsets —
  // match artifact patterns anywhere in the line instead
  const uncommitted = safeExec('git status --porcelain', { cwd });
  const authoredDirs = authoredClaudeDirs(cwd, safeExec);
  const uncommittedCount = uncommitted
    ? uncommitted.split('\n').filter(l => {
        if (!l.trim()) return false;
        if (claudeChurnIsDisposable(l, authoredDirs)) return false;
        if (/\s\.mcp\.json$/.test(l)) return false;
        if (/\snode_modules$/.test(l) || /\snode_modules\//.test(l)) return false;
        return true;
      }).length
    : 0;

  if (aheadCount === 0 && uncommittedCount === 0) {
    log('Phase 2a: Worktree is clean, no action needed');
    return 0;
  }

  // Check for existing item to dedup
  const branch = safeExec('git rev-parse --abbrev-ref HEAD', { cwd }) || 'unknown';
  const existing = listPending({ category: 'worktree-unmerged' });
  const isDuplicate = existing.some(item =>
    item.evidence?.branch === branch &&
    item.evidence?.worktree_path === cwd
  );
  if (isDuplicate) {
    log(`Phase 2a: Duplicate item for ${branch}, skipping`);
    return 0;
  }

  const detail = [];
  if (aheadCount > 0) detail.push(`${aheadCount} unmerged commit(s)`);
  if (uncommittedCount > 0) detail.push(`${uncommittedCount} uncommitted change(s)`);

  // Attribute the item to the MAIN repo root, not the worktree — items
  // attributed to a worktree path can never be claimed by Ring 1's
  // per-project auto-resolve pass. The canonical resolver already did the
  // worktree→main resolution, so project.path IS the main root and
  // project.name the config key the readers group by.
  createItem({
    project: project.name,
    project_path: project.path,
    ...(project.unresolved ? { project_unresolved: true } : {}),
    filed_by: 'ring3-close',
    category: 'worktree-unmerged',
    urgency: 'urgent',
    title: `Worktree "${branch}" has unmerged work`,
    summary: `Session ended in worktree ${cwd} with ${detail.join(' and ')}. Merge to main or the work may be lost.`,
    context_anchor: `git log main..${branch} in ${cwd}`,
    evidence: { branch, worktree_path: cwd, ahead: aheadCount, uncommitted: uncommittedCount },
    options: [
      { key: 'merge', label: 'Merge to main now' },
      { key: 'keep', label: 'Keep branch for later' },
      { key: 'dismiss', label: 'Dismiss (already handled)' },
    ],
  });

  log(`Phase 2a: URGENT — worktree ${branch} has ${detail.join(' and ')}`);
  return 1;
}

function safeExec(cmd, opts = {}) {
  try {
    // stderr is ignored, not inherited — otherwise expected failures
    // (e.g. non-git paths) bleed "fatal:" noise into the hook log
    return execSync(cmd, {
      encoding: 'utf8', timeout: 10_000,
      stdio: ['ignore', 'pipe', 'ignore'],
      ...opts,
    }).trim();
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Preprocessing — compress transcript for cost control
// ---------------------------------------------------------------------------

function preprocessTranscript(transcriptPath) {
  const raw = readFileSync(transcriptPath, 'utf8');
  const lines = raw.split('\n').filter(l => l.trim());

  const kept = [];
  let originalTokenEstimate = 0;

  for (const line of lines) {
    let entry;
    try {
      entry = JSON.parse(line);
    } catch {
      continue; // skip non-JSON lines
    }

    originalTokenEstimate += line.length / 4; // rough token estimate

    // Claude Code transcripts nest messages: { type: "user", message: { role, content } }
    // Normalize to the inner message for processing
    const role = entry.role || entry.type;
    const msg = entry.message || entry;
    const content = msg.content;

    // Skip non-message entries (mode, permission-mode, ai-title, etc.)
    if (!content && role !== 'system') continue;

    // Skip system messages entirely
    if (role === 'system') continue;

    // Skip file-history-snapshots, attachments, metadata entries
    if (entry.type === 'file-history-snapshot') continue;
    if (entry.type === 'attachment') continue;
    if (entry.type === 'last-prompt' || entry.type === 'mode' || entry.type === 'permission-mode') continue;
    if (entry.type === 'ai-title' || entry.type === 'queue-operation') continue;

    // For assistant messages, strip thinking blocks
    if (role === 'assistant') {
      if (Array.isArray(content)) {
        const filtered = content
          .filter(block => {
            if (block.type === 'thinking') return false;
            if (block.type === 'text') return true;
            if (block.type === 'tool_use') return true;
            return false;
          })
          .map(block => {
            if (block.type === 'tool_use') {
              const inputSummary = typeof block.input === 'string'
                ? block.input.slice(0, 200)
                : JSON.stringify(block.input).slice(0, 200);
              return {
                type: 'tool_use',
                name: block.name,
                input_summary: inputSummary,
              };
            }
            return block;
          });

        if (filtered.length > 0) {
          kept.push({ role: 'assistant', content: filtered });
        }
      } else if (typeof content === 'string') {
        kept.push({ role: 'assistant', content });
      }
      continue;
    }

    // For tool_result messages, strip the body (often huge file contents)
    if (role === 'tool' || entry.type === 'tool_result') {
      kept.push({
        role: 'tool',
        tool_use_id: entry.tool_use_id || msg.tool_use_id,
        content_summary: '(tool result body stripped)',
      });
      continue;
    }

    // Keep user messages as-is, but strip attachments
    if (role === 'user') {
      if (Array.isArray(content)) {
        const textOnly = content.filter(block =>
          block.type === 'text' || typeof block === 'string'
        );
        if (textOnly.length > 0) {
          kept.push({ role: 'user', content: textOnly });
        }
      } else if (typeof content === 'string') {
        kept.push({ role: 'user', content });
      }
      continue;
    }
  }

  // Serialize compressed transcript
  let compressed = kept.map(e => JSON.stringify(e)).join('\n');
  const compressedTokenEstimate = compressed.length / 4;

  // If still > 100K tokens after compression, truncate from beginning
  const MAX_TOKENS = 100_000;
  if (compressedTokenEstimate > MAX_TOKENS) {
    const targetChars = MAX_TOKENS * 4 * 0.8; // keep most recent 80%
    if (compressed.length > targetChars) {
      compressed = compressed.slice(compressed.length - targetChars);
      // Find the first complete line
      const firstNewline = compressed.indexOf('\n');
      if (firstNewline > 0) {
        compressed = compressed.slice(firstNewline + 1);
      }
    }
  }

  const ratio = originalTokenEstimate > 0
    ? (compressedTokenEstimate / originalTokenEstimate * 100).toFixed(1)
    : 0;

  log(`Preprocessing: ${originalTokenEstimate.toFixed(0)} -> ${compressedTokenEstimate.toFixed(0)} tokens (~${ratio}%)`);

  return { compressed, originalTokenEstimate, compressedTokenEstimate };
}

// ---------------------------------------------------------------------------
// Phase 2b: Session summary
// ---------------------------------------------------------------------------

// The pointer line both Last-Session surfaces carry (act:8c076580): the
// sessions/ file is the authoritative record, and the inline project-state
// section is an explicit projection that NAMES it — a reader of project.md
// can always find the full record. The pointer rides INSIDE the one shared
// body string, so the act:ac119994 contract holds unchanged: both surfaces
// stay byte-identical, no second formatter exists at the call site, and the
// lib-owned block format (header + attribution marker) is untouched.
export function appendRecordPointer(bullets, recordRef) {
  // Nullish/non-string bullets degrade to a pointer-only body — never a
  // literal "undefined" line above the pointer.
  const b = typeof bullets === 'string' ? bullets.trim() : '';
  const pointer = `_Full record: ${recordRef}_`;
  return b ? `${b}\n\n${pointer}` : pointer;
}

async function sessionSummary(compressed, projectSlug, sessionId) {
  log('Phase 2b: Session summary');

  const systemPrompt = `You are a session summarizer. Given a Claude Code session transcript, produce exactly 3-5 bullet points summarizing what was accomplished. Be specific about file changes, decisions made, and problems solved. Output ONLY the bullet points, one per line, starting with "- ".`;

  const response = await claudeCall(systemPrompt, compressed);
  // The COMPLETE model bullet set — the single source for BOTH the per-session
  // record and the inline "## Last Session" block. Never sliced or truncated
  // for the inline section (act:ac119994): the inline block was historically a
  // lossy subset of the per-session file, so both now derive from this one
  // string via buildLastSessionBlock.
  const bullets = response.trim();

  // Write per-session file to state/projects/<slug>/sessions/<date>-<session-id>.md
  const sessionsDir = join(WATCHTOWER_DIR, 'state', 'projects', projectSlug, 'sessions');
  mkdirSync(sessionsDir, { recursive: true });

  const date = new Date().toISOString().slice(0, 10);
  const sessionFile = join(sessionsDir, `${date}-${sessionId}.md`);
  // ONE shared body feeds BOTH surfaces, now carrying the pointer that names
  // the authoritative per-session record (appendRecordPointer above). The
  // ref is relative to state/projects/ — project.md's own directory — so a
  // consumer can resolve it mechanically, not just read it.
  const body = appendRecordPointer(bullets, `${projectSlug}/sessions/${date}-${sessionId}.md`);
  const content = `# Session ${sessionId}\n\nDate: ${new Date().toISOString()}\n\n${body}\n`;
  atomicWrite(sessionFile, content);

  // Update the project-level state file's "## Last Session" with the SAME
  // complete bullet set. Written UNCONDITIONALLY — the old `existsSync` gate
  // silently dropped the inline section whenever the project-state file didn't
  // exist yet (a fresh project, or before Ring 1's first rebuild), leaving the
  // per-session file current while the inline block stayed empty/stale and
  // readers rendered a truncated summary (act:ac119994). buildLastSessionBlock
  // is the single source of the block format; upsertLastSessionSection reuses
  // the line-anchored splice preserveRing3LastSession reads with, and the
  // attribution line it emits is the ownership marker Ring 1's rebuild keys on
  // (preserveRing3LastSession carries this section forward verbatim).
  const projectStatePath = join(WATCHTOWER_DIR, 'state', 'projects', `${projectSlug}.md`);
  const existingState = existsSync(projectStatePath)
    ? readFileSync(projectStatePath, 'utf8')
    : '';
  const block = buildLastSessionBlock({ date, sessionId, bullets: body });
  atomicWrite(projectStatePath, upsertLastSessionSection(existingState, block));

  log(`Phase 2b: Summary written to ${projectSlug}/sessions/${date}-${sessionId}.md`);
  return bullets;
}

// ---------------------------------------------------------------------------
// Phase 2b2: Thread capture
// ---------------------------------------------------------------------------

// --- Thread-capture tuning (act:e8793574) ----------------------------------
// The over-eager TAIL: a focused session was sprayed into 6-9 threads with no
// distinct per-thread contribution. Earned membership is now enforced in code,
// not just asked of the model:
//   MAX_THREADS_PER_SESSION — hard ceiling on threads a single close writes.
//   MIN_CONTRIBUTION_TOKENS — a membership needs a substantive contribution;
//     a blank/one-word "advanced it" is not earned and is dropped.
//   CONTRIBUTION_DISTINCT_OVERLAP — the same contribution restated across
//     threads (spray) is not distinct; the later copy is dropped when it
//     shares this many meaningful tokens with an already-accepted one.
const MAX_THREADS_PER_SESSION = 5;
const MIN_CONTRIBUTION_TOKENS = 2;
const CONTRIBUTION_DISTINCT_OVERLAP = 3;

// Deterministic, model-independent fid scan: pib-db work-item ids the session
// referenced (`act:<8hex>`, the FID_PATTERN). Never trusts the model to name a
// fid — a hallucinated id can't enter related_fids because it isn't parsed
// from the actual transcript. Deduped, order-preserving.
const FID_TOKEN_RE = /\bact:[0-9a-f]{8}\b/g;
export function extractRelatedFids(text) {
  if (typeof text !== 'string') return [];
  const seen = new Set();
  let m;
  FID_TOKEN_RE.lastIndex = 0;
  while ((m = FID_TOKEN_RE.exec(text)) !== null) seen.add(m[0]);
  return [...seen];
}

// selectEarnedThreads — the tail-tamer. Given the model's proposed threads (in
// its own order — initiative-first per the prompt), keep only genuinely earned
// memberships: a substantive contribution (>= MIN_CONTRIBUTION_TOKENS
// meaningful tokens) that is DISTINCT from every already-accepted contribution
// (< CONTRIBUTION_DISTINCT_OVERLAP shared meaningful tokens), capped at
// MAX_THREADS_PER_SESSION. Pure and order-stable so a focused session can no
// longer be sprayed across 6-9 threads.
export function selectEarnedThreads(threads, {
  cap = MAX_THREADS_PER_SESSION,
  minTokens = MIN_CONTRIBUTION_TOKENS,
  overlap = CONTRIBUTION_DISTINCT_OVERLAP,
} = {}) {
  if (!Array.isArray(threads)) return [];
  const accepted = [];
  const acceptedTokenSets = [];
  for (const t of threads) {
    if (accepted.length >= cap) break;
    if (!t || !t.thread || !t.cursor) continue;
    const mt = meaningfulTokens(typeof t.contribution === 'string' ? t.contribution : '');
    if (mt.length < minTokens) continue; // no substantive contribution → not earned
    const sprays = acceptedTokenSets.some((prev) => {
      let shared = 0;
      for (const tok of mt) if (prev.has(tok)) shared += 1;
      return shared >= overlap;
    });
    if (sprays) continue; // restatement of an already-claimed contribution
    accepted.push(t);
    acceptedTokenSets.push(new Set(mt));
  }
  return accepted;
}

// ---------------------------------------------------------------------------
// Reach 2 — significance reach-back (act:bcb7edd4, grp:retro-remeaning)
// ---------------------------------------------------------------------------
//
// The second reach of the one retrospective-temporality motion: when a
// session CONCLUDES something, reach back from that new understanding into
// the prior cursor history of the threads THIS session touched, and PROPOSE
// where the new understanding recolors an earlier entry. This is where "the
// feedback turned out to be the seed of the architecture" gets caught — a
// SIGNIFICANCE change, not a contradiction, which is why a contradiction
// scan is blind to the whole class (the plan's § What this is).
//
// Shape constraints, all load-bearing:
// - EVENT-DRIVEN: fires inside Phase 2b2 on session close, bounded to the
//   threads the session earned — never a periodic all-pairs sweep (the shape
//   the cabinet killed: no way to select among ~1.5M pairs, and a 30-min
//   re-scan re-proposes dismissed pairs forever).
// - One extra model call per real-work session, and only when at least one
//   earned thread has prior history. It cannot ride the capture call the way
//   Reach 1 rode extraction: the reach-back corpus (which threads matter) IS
//   that call's output, and pre-loading every active thread's history would
//   cost more than the second focused call.
// - PROPOSE only, human-gated: the proposal never modifies the thread file;
//   the apply step is /inbox's confirm follow-up (appendThreadRecolor), and
//   the category is in GATED_CATEGORIES so it can never ride a batch.
// - Anchors are validated: a proposal must cite an ACTUAL prior entry
//   (session_id + date checked against the on-disk history snapshot); a
//   hallucinated anchor drops the proposal, logged, never filed.
// - DURABLE filing exclusion: relation_key = thread|prior_session|new_
//   session|verb, checked across ALL queue statuses with NO time window —
//   a dismissed judgment about a static corpus must never re-nag (not even
//   after RESOLUTION_CORPUS_DAYS), and a reprocess replay produces the same
//   key and files nothing.

const PRIOR_HISTORY_CAP = 5;      // last K prior cursor entries shown per thread
const RECOLOR_FIELD_SLICE = 300;  // per-field char bound in the prompt
const RECOLOR_VERBS = ['reframes', 'answers', 'contradicts'];

// Bounded, labeled rendering of one thread's prior trail + this session's
// new understanding. Pure; exported for tests.
export function buildReachBackPrompt(reachBackThreads) {
  const clip = (s, n = RECOLOR_FIELD_SLICE) =>
    typeof s === 'string' ? s.slice(0, n) : '';
  const blocks = reachBackThreads.map((t) => {
    const priorLines = t.prior.map((p, i) => {
      const c = p.cursor || {};
      const oq = Array.isArray(c.open_questions) ? c.open_questions.join(' | ') : '';
      return `  [${i + 1}] date=${p.date} session=${p.session_id}\n`
        + `      what: ${clip(c.what)}\n`
        + `      where_left_off: ${clip(c.where_left_off)}\n`
        + (oq ? `      open_questions: ${clip(oq, 200)}\n` : '');
    }).join('');
    const nc = t.newCursor || {};
    return `THREAD: ${t.slug} — ${clip(t.displayName, 120)}\n`
      + `THIS SESSION'S UNDERSTANDING (new):\n`
      + `  contribution: ${clip(t.contribution)}\n`
      + `  what: ${clip(nc.what)}\n`
      + `  where_left_off: ${clip(nc.where_left_off)}\n`
      + `PRIOR UNDERSTANDING TRAIL (earlier sessions, oldest first):\n${priorLines}`;
  });
  return blocks.join('\n');
}

const REACH_BACK_SYSTEM_PROMPT = `You look for RETROSPECTIVE MEANING CHANGES: places where a session's NEW understanding changes what an EARLIER record of the same work stream meant. Later events change what earlier events were — the question filed Monday is answered Tuesday; the friction noted in June turns out in September to have been the seed of the architecture.

You are given, per thread: this session's new understanding, and the prior understanding trail (earlier sessions' snapshots). For each PRIOR entry, ask: in light of what this session concluded, does that entry now MEAN something different than it did when written?

Three verbs, use exactly these:
- "reframes" — the prior entry's SIGNIFICANCE changed: what looked like X turns out to have been Y (the most important and least obvious case)
- "answers" — the prior entry raised a question or uncertainty that this session's understanding resolves
- "contradicts" — this session's understanding conflicts with what the prior entry asserts

Most sessions recolor NOTHING — normal progress does not change what earlier entries meant, it just extends them. Return [] unless a prior entry's meaning GENUINELY shifts. Do not propose "answers" for routine next-step completion; the open question must have been a real uncertainty that this session settled in a way that recolors the earlier record.

Cite the EXACT prior entry: copy its session= and date= values verbatim from the trail. Never invent entries.

Output a JSON array (ONLY the array, no other text; [] when nothing recolors):
[{"thread":"<slug exactly as given>","prior_session_id":"<session= value>","prior_date":"<date= value>","verb":"reframes|answers|contradicts","explanation":"One or two sentences: what the prior entry meant THEN, and what it means NOW in light of this session."}]`;

// Validate the model's proposals against the real on-disk history snapshot.
// Pure; exported for tests. Returns { accepted, dropped } — every drop is
// itemized so the caller can log it (a hallucinated anchor must be visible,
// never silently absorbed).
export function validateRecolorProposals(proposals, priorBySlug) {
  const accepted = [];
  const dropped = [];
  for (const p of Array.isArray(proposals) ? proposals : []) {
    if (!p || typeof p !== 'object') { dropped.push({ p, why: 'not an object' }); continue; }
    const prior = priorBySlug.get(p.thread);
    if (!prior) { dropped.push({ p, why: `unknown thread ${JSON.stringify(p.thread)}` }); continue; }
    if (!RECOLOR_VERBS.includes(p.verb)) { dropped.push({ p, why: `illegal verb ${JSON.stringify(p.verb)}` }); continue; }
    if (typeof p.explanation !== 'string' || !p.explanation.trim()) {
      dropped.push({ p, why: 'empty explanation' }); continue;
    }
    const anchor = prior.find(
      (e) => e.session_id === p.prior_session_id && e.date === p.prior_date);
    if (!anchor) { dropped.push({ p, why: `no prior entry matches session=${p.prior_session_id} date=${p.prior_date}` }); continue; }
    accepted.push({ ...p, anchor });
  }
  return { accepted, dropped };
}

// File one significance-change proposal. Durable relation_key exclusion:
// ALL statuses, no time window (see the block comment above). Returns the
// filed (or existing) item id.
export function fileSignificanceProposal({ project, threadSlug, anchor, verb, explanation, sessionId }) {
  const key = `${threadSlug}|${anchor.session_id}|${sessionId}|${verb}`;
  const existing = listItems({ category: 'significance-change' })
    .find((i) => i.evidence && i.evidence.relation_key === key);
  if (existing) return existing.id;
  const priorExcerpt = (anchor.cursor && typeof anchor.cursor.what === 'string')
    ? anchor.cursor.what.slice(0, 200) : '';
  return createItem({
    project: project.name,
    project_path: project.path,
    ...(project.unresolved ? { project_unresolved: true } : {}),
    category: 'significance-change',
    urgency: 'normal',
    title: `Recolor: this session ${verb} a ${anchor.date} entry of "${threadSlug}"`,
    summary: explanation,
    context_anchor: `session ${sessionId}`,
    evidence: {
      relation_key: key,
      thread: threadSlug,
      verb,
      prior_session_id: anchor.session_id,
      prior_date: anchor.date,
      prior_excerpt: priorExcerpt,
      new_session_id: sessionId,
      explanation,
    },
    options: [
      { key: 'confirm', label: 'Confirm — apply the recolor to the thread record' },
      { key: 'dismiss', label: 'The earlier entry stands as it was' },
    ],
    filed_by: 'ring3-close',
    thread_ids: [threadSlug],
  });
}

// The reach-back pass. Fail-open at every stage: a reach-back failure must
// never break thread capture (the cursor writes already landed). Exported
// for tests; threadCapture is the only production caller.
export async function significanceReachBack({
  reachBackThreads, project, sessionId, callFn = claudeCall,
}) {
  const withHistory = reachBackThreads.filter((t) => t.prior.length > 0);
  if (withHistory.length === 0) return { proposals_filed: 0, prior_entries_shown: 0 };

  const priorEntriesShown = withHistory.reduce((s, t) => s + t.prior.length, 0);
  let filed = 0;
  try {
    const response = await callFn(REACH_BACK_SYSTEM_PROMPT, buildReachBackPrompt(withHistory));
    const jsonMatch = String(response).match(/\[[\s\S]*\]/);
    const proposals = jsonMatch ? JSON.parse(jsonMatch[0]) : [];
    const priorBySlug = new Map(withHistory.map((t) => [t.slug, t.prior]));
    const { accepted, dropped } = validateRecolorProposals(proposals, priorBySlug);
    for (const d of dropped) {
      log(`Phase 2b2: reach-back proposal dropped — ${d.why}`);
    }
    for (const p of accepted) {
      try {
        fileSignificanceProposal({
          project, threadSlug: p.thread, anchor: p.anchor,
          verb: p.verb, explanation: p.explanation, sessionId,
        });
        filed++;
      } catch (e) {
        logError(`Phase 2b2: reach-back filing failed for ${p.thread} (${e.message})`);
      }
    }
    if (filed > 0) {
      log(`Phase 2b2: Reach 2 — ${filed} significance-change proposal(s) filed against ${withHistory.length} thread(s) with prior history`);
    }
  } catch (e) {
    logError(`Phase 2b2: significance reach-back failed (${e.message}) — thread capture is unaffected`);
  }
  return { proposals_filed: filed, prior_entries_shown: priorEntriesShown };
}

async function threadCapture(compressed, projectSlug, sessionId, summary, transcriptPath,
  { callFn = claudeCall, project = null } = {}) {
  log('Phase 2b2: Thread capture');

  const threadsDir = join(WATCHTOWER_DIR, 'state', 'threads');
  mkdirSync(threadsDir, { recursive: true });

  // Load existing active threads for context
  const existingThreads = [];
  if (existsSync(threadsDir)) {
    for (const f of readdirSync(threadsDir)) {
      if (!f.endsWith('.json')) continue;
      try {
        const thread = JSON.parse(readFileSync(join(threadsDir, f), 'utf8'));
        if (thread.status === 'active') {
          existingThreads.push({
            id: thread.thread,
            what: currentCursor(thread).what || '',
          });
        }
      } catch { /* skip malformed */ }
    }
  }

  const threadList = existingThreads.length > 0
    ? existingThreads.map(t => `- ${t.id}: ${t.what}`).join('\n')
    : '(no active threads yet)';

  const systemPrompt = `You identify which LINES OF WORK a session contributed to, so future sessions can pick up where this one left off.

A thread is a durable work stream that spans multiple sessions — not a task, not a routine operation. Threads exist at DIFFERENT ZOOM LEVELS, and a session normally touches several at once:
- Initiative level (REQUIRED when real work happened): the specific line of work — "maginnis-email-system", "watchtower-threading", "mux-dx". This is the level that matters most and the one most often missed. Always name it.
- Project-area level (optional): the broader area the initiative sits in — "maginnis", "watchtower".
- Cross-cutting level (optional): a CONCRETE line of work that spans projects — "audit-methodology", "worktree-infrastructure", "engagement-system". Still a real initiative, just one no single project owns.
These are NOT competing categories; they are zoom levels on the same work. A session building a Maginnis email feature touches "maginnis-email-system" AND "maginnis" — list every one that genuinely applies, at whatever levels apply.

Every thread, at every zoom level, is a CONCRETE LINE OF WORK. Bad thread names are task-specific ("fix-sdk-loading-bug"), routine ("running-orient"), or abstract DIMENSIONS ("code-quality", "security", "performance") — those are evaluation lenses cabinet members apply, not lines of work. If you reach for an abstract quality word, find the concrete initiative underneath it instead ("audit-methodology", not "code-quality"). If a session just ran orient/debrief/status checks without doing real work, return an empty array.

The cursor is what makes threads valuable. It captures UNDERSTANDING — what a cold future session needs to think differently — not DESCRIPTION of what happened (the session summary already does that). Ask: "If I started a new session on this work tomorrow with no memory, what would I need to know that isn't obvious from reading the code?"

Currently active threads:
${threadList}

Choosing threads:
- ALWAYS create or join the specific INITIATIVE thread for the real work. Never settle for only a broad project-area thread — that is the collapse failure mode (everything dumped into one bucket).
- Each membership must be EARNED: include a thread only if this session genuinely advanced it, and say how in "contribution". Never add a session to a thread you cannot justify — that is overlap-as-laziness, the opposite of what threads are for.
- Reuse an existing slug when the work continues the SAME line. Mint a new slug when it is a distinct initiative — but first scan the active threads above for a near-synonym and reuse that instead of creating a near-duplicate (no "watchtower-audit" beside "watchtower-audit-remediation").

Respond with a JSON array. Each element:
{
  "thread": "short-stable-slug",
  "is_new": true/false,
  "display_name": "A DURABLE STORYLINE title for the whole line of work — what this thread IS across all its sessions, not what just happened in this one. 'Watchtower ring reliability and silent-failure hardening', never 'Fixed two Ring 1 bugs today'. A moment-snapshot ('shipped X', 'fixed Y') is wrong; name the ongoing arc. Rich and specific — carry the meaning the slug cannot; never a restatement of the slug. It evolves as understanding deepens, but stays a storyline, never a changelog entry.",
  "contribution": "What THIS session contributed to THIS thread — the reason it belongs here, one line",
  "cursor": {
    "what": "The work stream as you would frame it RIGHT NOW in one line — this evolves across sessions as understanding deepens, even when the work stream is the same",
    "why": "Why this work matters — the motivation, not the mechanism",
    "where_left_off": "Current state of understanding — what's proven, what's uncertain, what surprised you",
    "open_questions": ["Unresolved design questions, discovered risks, things that need validation"],
    "next_steps": ["Strategic direction, not a task checklist — those belong in pib-db"]
  }
}

Rules:
- Return [] for sessions that only did routine operations (orient, debrief, status checks)
- ALWAYS include the specific initiative thread for real work; add project-area and cross-cutting threads when they genuinely apply
- Expect a SMALL set at different zoom levels — typically 2-4, at most 5. A focused session that advanced ONE area does NOT belong to 6+ threads; if you find yourself listing that many, you are matching on topic, not contribution. Overlap is healthy ONLY when each thread got a genuinely distinct contribution.
- Every thread needs a DISTINCT "contribution" — the specific thing THIS session did FOR THIS thread. If two threads would carry the same (or near-same) contribution sentence, you are spraying: drop the weaker membership. A copy-pasted contribution is not earned membership.
- Before minting a new slug, reuse a near-synonym from the active list instead
- The slug is just a stable filing key — short, reused. The display_name is what carries understanding to a human — a rich, specific articulation, never a restatement of the slug
- Capture what you LEARNED, not what you DID
- Output ONLY the JSON array, no markdown fences`;

  const response = await callFn(systemPrompt, compressed);

  let threads;
  try {
    threads = JSON.parse(response.trim());
    if (!Array.isArray(threads)) throw new Error('Not an array');
  } catch (e) {
    logError(`Phase 2b2: Failed to parse thread response: ${e.message}`);
    return [];
  }

  // Tame the over-eager tail in CODE, not just in the prompt: keep only earned,
  // distinct, capped memberships (act:e8793574).
  const beforeCount = threads.length;
  threads = selectEarnedThreads(threads);
  if (threads.length < beforeCount) {
    log(`Phase 2b2: pared ${beforeCount} proposed threads to ${threads.length} earned (cap ${MAX_THREADS_PER_SESSION}, distinct contributions)`);
  }

  // Deterministic relationship enrichment (act:e8793574):
  //   related_fids — every work-item fid the session referenced, attributed to
  //     each thread the session advanced (union-deduped on disk over time).
  //   lineage — the OTHER threads co-advanced in THIS session are genuine
  //     siblings of each thread; record them so a cold session can trace the
  //     related lines of work. Empty when the session earned only one thread.
  const relatedFids = extractRelatedFids(compressed);
  const acceptedSlugs = threads.map((t) => slugify(t.thread));

  // Reach 2 snapshot (act:bcb7edd4): capture each earned thread's PRIOR
  // cursor history BEFORE this session's entry is appended below — the
  // reach-back corpus is the trail as it stood when this session started,
  // and snapshotting before the append is robust even if an append fails.
  const reachBackThreads = [];
  for (const t of threads) {
    if (!t.thread || !t.cursor) continue;
    const slug = slugify(t.thread);
    let prior = [];
    const p = join(threadsDir, `${slug}.json`);
    if (existsSync(p)) {
      try {
        const existing = JSON.parse(readFileSync(p, 'utf8'));
        if (Array.isArray(existing.cursor_history)) {
          prior = existing.cursor_history.slice(-PRIOR_HISTORY_CAP)
            .filter((e) => e && e.session_id && e.date);
        }
      } catch { /* corrupt file → no prior trail; capture handles the backup */ }
    }
    reachBackThreads.push({
      slug,
      displayName: t.display_name || slug,
      contribution: t.contribution || '',
      newCursor: t.cursor,
      prior,
    });
  }

  const threadIds = [];
  const now = new Date().toISOString();
  const date = now.slice(0, 10);

  for (const t of threads) {
    if (!t.thread || !t.cursor) continue;
    const threadSlug = slugify(t.thread);
    const threadPath = join(threadsDir, `${threadSlug}.json`);
    const lineage = acceptedSlugs
      .filter((s) => s && s !== threadSlug)
      .map((s) => ({ slug: s, relation: 'co-occurrence', session_id: sessionId, date }));

    // One cursor snapshot per session that advanced this thread — appended,
    // never overwritten (see updateThreadFile / cursor_history in
    // watchtower-lib.mjs). The qa_handoff payload is a future SIBLING of this
    // history, not a field inside the cursor (qa-handoff-protocol.md).
    const cursorEntry = { date, session_id: sessionId, cursor: t.cursor };
    const sessionRecord = {
      id: sessionId,
      contribution: t.contribution || '',
      date,
      project: projectSlug,
      summary: summary.split('\n').slice(0, 3).join(' ').slice(0, 200),
      transcript: transcriptPath,
    };

    // DISK WINS OVER MODEL: updateThreadFile appends whenever the thread
    // file exists on disk — the model's `is_new` field is advisory naming
    // metadata only and must never authorize a fresh-write over an existing
    // file (one hallucinated is_new:true would irreversibly wipe the
    // append-only cursor_history). Corrupt files are backed up aside and
    // reported, never silently replaced. Per-thread try/catch so one bad
    // thread cannot abort writes for the remaining threads.
    try {
      const outcome = updateThreadFile(
        threadPath, threadSlug, t, cursorEntry, sessionRecord, now,
        { relatedFids, lineage });
      threadIds.push(threadSlug);
      if (outcome.startsWith('recovered')) {
        logError(`Phase 2b2: Thread ${threadSlug} ${outcome}`);
      } else {
        log(`Phase 2b2: Thread ${threadSlug} ${outcome}`);
      }
    } catch (e) {
      logError(`Phase 2b2: Thread ${threadSlug} write failed: ${e.message} — continuing with remaining threads`);
    }
  }

  // Reach 2 (act:bcb7edd4): reach back from this session's understanding
  // into the touched threads' prior trails. Runs AFTER the cursor writes so
  // a reach-back failure can never cost a cursor append; records the ledger
  // event (the L2 gate's denominator) whenever real work happened, whether
  // or not anything was proposed.
  if (threadIds.length > 0) {
    const filingProject = project || { name: projectSlug, path: null };
    const { proposals_filed, prior_entries_shown } = await significanceReachBack({
      reachBackThreads, project: filingProject, sessionId, callFn,
    });
    recordSignificanceEvent({
      session_id: sessionId,
      project: filingProject.name,
      threads_touched: threadIds.length,
      threads_with_history: reachBackThreads.filter((t) => t.prior.length > 0).length,
      prior_entries_shown,
      proposals_filed,
    });
  }

  return threadIds;
}

// ---------------------------------------------------------------------------
// Phase 2c: Work item closure
// ---------------------------------------------------------------------------

// `sessionStartIso` is accepted for call-site compatibility and is no longer
// read: act:9eebbac4's create-vs-complete day-window filter — the only consumer
// of the session's own date — was deleted in act:ea23b3a5 after measurement
// showed it suppressed the highest-precision cohort and exempted the lowest.
// See the calibration note on completionReviewEmitGuard.
async function workItemClosure(compressed, project, threadIds = [], sessionStartIso = null) {
  const projectPath = project.path;
  log('Phase 2c: Work item closure');

  // No resolved project directory (unresolved identity) — there is no
  // right pib.db to evaluate against; evaluating the RUNNER's would close
  // another project's actions from this transcript (the act:29001b07 bug).
  if (!projectPath) {
    log('Phase 2c: no project path (unresolved identity), skipping');
    return { closed: 0, queued: 0 };
  }

  const dbPath = join(projectPath, 'pib.db');
  if (!existsSync(dbPath)) {
    log('Phase 2c: No pib.db found, skipping');
    return { closed: 0, queued: 0 };
  }

  const Database = loadBetterSqlite3(projectPath);
  if (!Database) {
    log('Phase 2c: better-sqlite3 not available, skipping');
    return { closed: 0, queued: 0 };
  }

  let db;
  try {
    db = new Database(dbPath, { readonly: true, timeout: 5000 });
  } catch (e) {
    logError(`Phase 2c: Cannot open pib.db: ${e.message}`);
    return { closed: 0, queued: 0 };
  }

  let openActions;
  try {
    // `notes` joined in for the acceptance-criteria gate (act:86226720) — the
    // measurement showed Phase 2c had never been shown an action's definition
    // of done, which is why it could only answer "did something change".
    openActions = db.prepare(
      "SELECT fid, text, status, notes FROM actions WHERE status IN ('open', 'in-progress') AND deleted_at IS NULL"
    ).all();
  } catch (e) {
    db.close();
    logError(`Phase 2c: Cannot query open actions: ${e.message}`);
    return { closed: 0, queued: 0 };
  }

  if (openActions.length === 0) {
    db.close();
    log('Phase 2c: No open actions to evaluate');
    return { closed: 0, queued: 0 };
  }

  // Each action is presented WITH its acceptance criteria, so the model can be
  // asked whether they are met rather than only whether activity occurred.
  const actionList = openActions.map((a) => {
    const excerpt = acceptanceCriteriaExcerpt(a.notes);
    const acBlock = excerpt ? `\n  acceptance criteria:\n${excerpt.split('\n').map(l => `    ${l}`).join('\n')}` : '';
    return `- ${a.fid}: ${a.text} (${a.status})${acBlock}`;
  }).join('\n');

  const systemPrompt = `You are evaluating which work items appear completed based on a session transcript. For each action, assess confidence:
- "high" = clearly completed in the transcript (code written, committed, tests passing, etc.)
- "medium" = partially done or implied complete but not confirmed
- "low" = mentioned but unclear if completed
- "none" = not addressed in this session

An action that was merely CREATED during this session is NOT completed — creating or filing a work item is not doing it. Use "none" for actions whose only appearance is their own creation, unless the transcript also shows the work itself being done.

Also return "quote": a VERBATIM span copied from the session transcript that states this work is finished. Copy it exactly — do not paraphrase, summarize, or reconstruct it. Quote from the transcript itself, not from the list of open actions above. If no such span exists, return "" — an empty quote is a valid and useful answer, and inventing one is worse than none.

ACCEPTANCE CRITERIA. Some actions above carry an "acceptance criteria" block — the action's own statement of what finished means. The operator does not want to know whether an action saw activity; they want to know whether it is FINISHED. So for each action return "ac_status":
- "unmet" = the criteria are stated and at least one is clearly NOT satisfied — the work cannot be closed yet. This is the common and useful answer for an action that is blocked on something that has not happened (a session that has not occurred, a device that has not arrived, a review nobody has run).
- "met" = the criteria are stated and all of them appear satisfied by what the transcript shows.
- "unstated" = no acceptance criteria are given for this action, so there is nothing to judge against.
When and ONLY when ac_status is "unmet", also return "ac_unmet": the specific unsatisfied criterion, copied VERBATIM from the acceptance criteria block above. Do not paraphrase it and do not invent one — an "unmet" with no verbatim criterion is discarded and the action is treated as unjudged. Quoting the criterion is what makes the judgment checkable.

Output JSON array: [{"fid":"act:XXXXXXXX","confidence":"high|medium|low|none","evidence":"brief reason","quote":"verbatim span or empty string","ac_status":"met|unmet|unstated","ac_unmet":"verbatim unsatisfied criterion, or empty string"}]
Output ONLY the JSON array, no other text.`;

  // The SAME slice is the model's haystack and the quote checker's — captured
  // once so the two can never drift apart (a quote can only be verified against
  // the text the model was actually shown).
  const transcriptSlice = recentSlice(compressed, COMPLETION_TRANSCRIPT_BUDGET);
  const userMessage = `Open actions:\n${actionList}\n\nSession transcript:\n${transcriptSlice}`;

  let evaluations = [];
  try {
    const response = await claudeCall(systemPrompt, userMessage);
    // Extract JSON from response (handle markdown code fences)
    const jsonMatch = response.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      evaluations = JSON.parse(jsonMatch[0]);
    }
  } catch (e) {
    logError(`Phase 2c: Claude evaluation failed: ${e.message}`);
    db.close();
    return { closed: 0, queued: 0 };
  }

  let queued = 0;
  let skipped = 0;

  // Emit guards (act:ec508dbe): one prepared status re-check statement,
  // reused across evaluations, and a per-fid dedup corpus built once from
  // pending ∪ recently-terminal completion-review items. Both setups FAIL OPEN
  // (distinct logError, empty guard) — never wholesale suppression.
  let statusStmt = null;
  try {
    statusStmt = db.prepare(
      'SELECT status FROM actions WHERE fid = ? AND deleted_at IS NULL');
  } catch (e) {
    logError(`Phase 2c: could not prepare emit-time status re-check (${e.message}) — failing open`);
  }
  let existingCompletionItems = [];
  try {
    const since = new Date(
      Date.now() - COMPLETION_REVIEW_DEDUP_DAYS * 24 * 60 * 60 * 1000).toISOString();
    existingCompletionItems = [
      ...listPending({ project: project.name, category: 'completion-review' }),
      ...listItems({
        project: project.name,
        category: 'completion-review',
        // `expired` and `superseded` are load-bearing since act:ea23b3a5 gave
        // this category a 14d expiry EQUAL to COMPLETION_REVIEW_DEDUP_DAYS:
        // without them a fid leaves every corpus at exactly the moment its
        // item expires, and the next session refiles it — forever. That is
        // Detector Symmetry §2's named failure ("dedup consults only pending
        // items, so an excluded state that files at all refiles after every
        // dismissal").
        statuses: ['resolved', 'dismissed', 'expired', 'superseded'],
        since,
      }),
    ];
  } catch (e) {
    logError(`Phase 2c: could not load existing completion-review items (${e.message}) — failing open`);
  }

  // Quote instrumentation (act:ea23b3a5) — counted for EVERY evaluation,
  // including suppressed ones, because "would a gate have been safe?" is a
  // question about the items we stop filing as much as the ones we file.
  const quoteTally = { found: 0, 'not-found': 0, 'absent-or-too-short': 0, 'no-decodable-transcript': 0 };
  // Acceptance-criteria instrumentation (act:86226720) — the same discipline,
  // for a gate that IS live. Counted for every evaluation and stamped on every
  // emitted item, so the next calibration has a corpus. The model arm ships
  // unmeasured by necessity (the model was never asked this question before),
  // and these counters are how that gets fixed rather than assumed.
  const acTally = { met: 0, unmet: 0, unstated: 0, unknown: 0, 'unmet-uncited': 0 };
  const notesByFid = new Map(openActions.map((a) => [a.fid, a.notes || '']));

  for (const evalItem of evaluations) {
    if (!evalItem.fid || evalItem.confidence === 'none') continue;

    const quoteCheck = verifyCompletionQuote(evalItem.quote, transcriptSlice);
    quoteTally[quoteCheck.reason] = (quoteTally[quoteCheck.reason] || 0) + 1;

    const acVerdict = normalizeAcVerdict(evalItem);
    acTally[acVerdict.status] = (acTally[acVerdict.status] || 0) + 1;
    // An "unmet" with no verbatim criterion is counted separately and does NOT
    // suppress — visible rather than silently downgraded.
    if (acVerdict.status === 'unmet' && !acVerdict.criterion) acTally['unmet-uncited']++;
    const unmetAcBoxes = hasUnmetAcceptanceCriteria(notesByFid.get(evalItem.fid));

    const guard = completionReviewEmitGuard(evalItem.fid, {
      statusStmt,
      existingItems: existingCompletionItems,
      confidence: evalItem.confidence,
      unmetAcBoxes,
      acVerdict,
    });
    if (!guard.emit) {
      skipped++;
      // Per-suppressed-candidate log line naming WHICH arm fired — the
      // acceptance criterion this action asks for: the filter's behavior must
      // be observable, not silent.
      log(`Phase 2c: skip ${evalItem.fid} — ${guard.reason}`
        + (guard.suppressed_by ? ` [${guard.suppressed_by}]` : ''));
      continue;
    }

    const urgencyMap = { high: 'urgent', medium: 'normal', low: 'low' };
    try {
      createItem({
        project: project.name,
        project_path: projectPath,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        category: 'completion-review',
        urgency: 'normal',
        // Top-level typed field per design doc (act:6549289e); evidence.confidence
        // kept for readers of pre-promotion items
        confidence: evalItem.confidence,
        plan_fid: evalItem.fid,
        thread_ids: threadIds,
        title: `Review completion of: ${evalItem.fid}`,
        summary: `Action "${openActions.find(a => a.fid === evalItem.fid)?.text || evalItem.fid}" may be completed (${evalItem.confidence} confidence).`,
        context_anchor: `pib.db action ${evalItem.fid}`,
        evidence: {
          fid: evalItem.fid,
          confidence: evalItem.confidence,
          reason: evalItem.evidence,
          // Recorded, never acted on (act:ea23b3a5) — the corpus a future
          // round needs to judge whether a real evidence gate is buildable.
          quote: typeof evalItem.quote === 'string' ? evalItem.quote : null,
          quote_verified: quoteCheck.verified,
          quote_check: quoteCheck.reason,
          // Stamped on every EMITTED item too (act:86226720) — an item that
          // got past the gate still carries what the model said about its
          // acceptance criteria, which is the join key for the next
          // precision measurement.
          ac_status: acVerdict.status,
          ac_unmet: acVerdict.criterion,
          ac_boxes_present: hasAcceptanceCriteriaBoxes(notesByFid.get(evalItem.fid)),
          closed_by: 'ring3-close',
        },
        options: [
          { key: 'close', label: 'Yes, close it' },
          { key: 'partial', label: 'Partially done' },
          { key: 'no', label: 'Not done' },
        ],
        filed_by: 'ring3-close',
      });
      queued++;
    } catch (e) {
      logError(`Phase 2c: Failed to queue ${evalItem.fid}: ${e.message}`);
    }
  }

  db.close();
  log(`Phase 2c: ${queued} completion candidates queued for review${skipped ? ` (${skipped} skipped by emit guards)` : ''}`);
  // Positive confirmation, not silence: a run with no verified quotes must be
  // distinguishable from a run where the instrumentation never fired.
  log(`Phase 2c: quote check (instrumentation only, gates nothing) — `
    + Object.entries(quoteTally).map(([k, n]) => `${k}=${n}`).join(', '));
  // Positive confirmation for the AC gate: "no suppressions" must be
  // distinguishable from "the model never answered".
  log(`Phase 2c: acceptance-criteria verdicts — `
    + Object.entries(acTally).map(([k, n]) => `${k}=${n}`).join(', '));
  return { closed: 0, queued };
}

// ---------------------------------------------------------------------------
// Dedup helpers — check memory index and pending inbox before filing
// ---------------------------------------------------------------------------

// parseMemoryTitles — extract the dedup corpus from MEMORY.md content.
//
// The M1a fix (act:f8e7bd0a). The old `loadMemoryIndex` returned each whole
// `- ` index line lowercased, and isDuplicate ran substring containment over
// it — so the median-133-char (max 755) em-dash DESCRIPTION TAIL on every line
// was the suppression sponge: a novel lesson sharing 3 generic words with some
// entry's prose tail got killed. The fix is to match only the TITLE segment.
//
// Per `- ` line: pull every `[Title](target)` markdown link (multi-link lines
// are common — "2026-06-16 sessions: [A](a.md), [B](b.md)" — take ALL, not the
// first). Lines with NO link are scaffolding, not entries — topic-file headers
// (`- **decisions.md** (56), ...`) and region/glob pointers (`- region
// \`session_summary_*.md\` → ...`) — and are DROPPED, never prose-matched
// (fail-direction: an unparsed entry is invisible to dedup ⇒ re-proposal
// noise, the SAFE direction, never an over-match).
function parseMemoryTitles(content) {
  const titles = [];
  for (const raw of content.split('\n')) {
    const line = raw.trim();
    if (!line.startsWith('- ')) continue;
    const body = line.slice(2).trim();
    // Region/glob pointer line — a backtick-glob standing for a class of
    // files, not an entry. Explicitly excluded (it carries no link anyway).
    if (/^region\b/i.test(body)) continue;
    // Capture title AND target; only a link to a memory FILE (`.md`) counts as
    // an entry. An inline `[text](https://…)` link inside a description tail is
    // NOT an entry — accepting it would re-inject the tail-as-suppression-
    // sponge bug M1a removes (boundary-man: latent until a tail adds a link).
    const linkRe = /\[([^\]]+)\]\(([^)]*)\)/g;
    let m;
    while ((m = linkRe.exec(body)) !== null) {
      if (!/\.md$/i.test((m[2] || '').trim())) continue;
      const t = m[1].trim().toLowerCase();
      if (t) titles.push(t);
    }
    // No memory-file link ⇒ header/bare scaffolding ⇒ dropped (never prose-
    // fallback; an unparsed entry is invisible to dedup, the SAFE direction).
  }
  return titles;
}

// extractMemoryFileTitle — a memory file's real title from its own content
// (act:421a8ab2, N5 of grp:retro-remeaning). writeMemoryFile's on-disk shape
// is `# <slug-derived title>\n\n_Captured: <date>_\n\n<content>`, and when
// content itself starts with its own `# <real title>` (every ring3-close
// draft_artifact does: `# ${item.title}\n\n${item.content}`), the file ends
// up with TWO H1 headers — a generic slug-title, then the real descriptive
// one (the act:252b17e7 double-prefix). Scanning the first few lines and
// keeping the LAST `# ` header found favors the real title when both exist,
// and degrades to the sole header when a caller supplied an explicit title
// with no embedded H1 in content.
function extractMemoryFileTitle(content, maxLines = 10) {
  if (typeof content !== 'string') return null;
  let last = null;
  for (const line of content.split('\n').slice(0, maxLines)) {
    const m = line.match(/^#\s+(.+)/);
    if (m) last = m[1].trim();
  }
  return last;
}

// loadRegionPointerTitles was DELETED here (act:471941c9 part 4). It existed
// (act:421a8ab2) to make region-pointer-reachable memory files visible to
// dedup without a per-file index line. loadMemoryCorpus below reads EVERY
// memory file on disk, which is a strict superset of that — including the true
// orphan the region-pointer version deliberately skipped. Keeping both would
// have left a live-looking function that production no longer calls.

// --- Dedup against the EXISTING memory corpus (act:471941c9, part 4) ---
//
// The 2026-07-30 reflow drain found 74 "genuine" lessons that collapsed to 8
// themes: each session re-learns and re-files the same disciplines, and
// nothing stopped it. The reported cause was "dedup compares against pending
// items, not memory". The real cause, visible in reflow's own index, is
// narrower and worse: the memory corpus was built from MEMORY.md's INDEX
// LINES, and a consolidated memory's index title is SLUG-DERIVED — "Lesson
// Verification Discipline On This Platform" — while the knowledge itself lives
// in the description tail ("a green write proves nothing") and the file body.
// A re-learned draft ("verify every write by independent read") shares zero
// meaningful tokens with the slug title, so the memory pass could never fire.
//
// So the corpus is now read from the FILES, not the index — but the two halves
// of what it feeds are deliberately calibrated in OPPOSITE directions, because
// measuring the obvious version killed it:
//
//   MEASUREMENT (2026-07-30, against the live queue). Feeding every on-disk
//   memory title + description into the ordinary 3-token overlap pass
//   suppressed 72% of maginnis's 134 untriaged drafts, up from 17%. Reading the
//   matches settled it: ~2 of 14 sampled were real duplicates; the rest were
//   topically adjacent and distinct ("Email recolor migration: freeze target
//   hex values" blocked by "{{firm_name}} in stored email template bodies bakes
//   at build time"). Raising the token threshold did not help — it destroyed
//   true positives faster than false ones (30%→11% of known re-files caught
//   while maginnis only fell 72%→30%). Normalizing to an overlap RATIO did not
//   separate them either: over 76 known re-files and 141 presumed-novel drafts
//   the two distributions overlap almost entirely, and the NEGATIVES score
//   higher on median (0.38 vs 0.25) — because a consolidated theme memory is
//   deliberately not a lexical twin of any one specimen that fed it.
//
//   The conclusion is the house rule this system has already paid for twice: a
//   lexical matcher over a large corpus may PROPOSE, never silently SUPPRESS.
//
//   promptLines — `title — description` per memory file, plus index titles.
//              This is where the corpus expansion actually pays: the M1b
//              injection shows the extraction model the nearest saved memories
//              and asks whether one covers the item, and the model is the
//              semantic engine that CAN tell "verify every write by independent
//              read" is covered by "verification discipline — a green write
//              proves nothing". Before this it was shown slug-derived index
//              titles carrying almost no content tokens, which is why reflow
//              re-learned eight themes for four days.
//   entries  — the suppression corpus, high-precision only. Index titles keep
//              the calibrated OVERLAP_THRESHOLD behavior unchanged. File-
//              derived titles and descriptions additionally require NEAR
//              IDENTITY (MEMORY_FILE_MIN_RATIO) — the same 0.8-of-the-shorter
//              bar mergeChunkExtractions already uses for "the same lesson
//              worded twice", which is exactly the re-file case. Measured at
//              that bar: 8 known re-files blocked, 2 of 141 presumed-novel
//              drafts touched.
//   titles   — the bare-title view kept for the existing suites and any caller
//              that wants titles without description tails.
//
// Every entry carries a `label` — the memory FILE it came from — so a
// suppression log line names the memory that blocked the draft, rather than
// echoing a title the operator would have to go find.
const MEMORY_INDEX_FILES = new Set(['MEMORY.md', 'MEMORY-archive.md']);
// A description is one long sentence; five shared meaningful tokens is the
// count floor before the ratio gate below is even consulted.
const MEMORY_BODY_OVERLAP_THRESHOLD = 5;
// Near-identity: shared tokens as a fraction of the SHORTER side's token count.
const MEMORY_FILE_MIN_RATIO = 0.8;
const MEMORY_DESCRIPTION_MAX_CHARS = 400;

// A memory file's one-line description: the frontmatter `description:` value
// when present (the /cc-remember shape), else the first real prose line —
// skipping headings, the `_Captured:` stamp, and frontmatter fences.
export function extractMemoryDescription(content) {
  if (typeof content !== 'string') return null;
  const lines = content.split('\n');
  let inFrontmatter = false;
  for (let i = 0; i < Math.min(lines.length, 40); i++) {
    const line = lines[i];
    const trimmed = line.trim();
    if (i === 0 && trimmed === '---') { inFrontmatter = true; continue; }
    if (inFrontmatter) {
      if (trimmed === '---') { inFrontmatter = false; continue; }
      const m = trimmed.match(/^description:\s*(.+)$/i);
      if (m) return m[1].replace(/^["']|["']$/g, '').trim().slice(0, MEMORY_DESCRIPTION_MAX_CHARS);
      continue;
    }
    if (!trimmed) continue;
    if (trimmed.startsWith('#')) continue;
    if (/^_Captured:/i.test(trimmed)) continue;
    if (/^[-*>|]/.test(trimmed)) continue;
    return trimmed.slice(0, MEMORY_DESCRIPTION_MAX_CHARS);
  }
  return null;
}

// loadMemoryCorpus — { titles, entries } for one project's memory dir.
// Fails open at every step: a missing dir, an unreadable index, or one
// unreadable file degrades the corpus, never throws.
export function loadMemoryCorpus(projectPath) {
  const empty = { titles: [], entries: [], promptLines: [] };
  // Unresolved identities carry no path — no memory dir to consult.
  if (!projectPath) return empty;
  const encoded = projectPath.replace(/\//g, '-');
  const memDir = join(homedir(), '.claude', 'projects', encoded, 'memory');
  const indexPath = join(memDir, 'MEMORY.md');
  if (!existsSync(indexPath)) return empty;
  let indexText;
  try {
    indexText = readFileSync(indexPath, 'utf8');
  } catch {
    return empty;
  }
  let directTitles;
  try {
    directTitles = parseMemoryTitles(indexText);
  } catch {
    directTitles = [];
  }

  const titles = [];
  const entries = [];
  const promptLines = [];
  const seenTitles = new Set();

  // Index titles: the calibrated corpus, count-threshold only — behavior here
  // is byte-for-byte what it was before this change.
  for (const raw of directTitles) {
    if (!raw) continue;
    const t = String(raw).toLowerCase();
    if (seenTitles.has(t)) continue;
    seenTitles.add(t);
    titles.push(t);
    entries.push({ text: t, label: t });
    promptLines.push(t);
  }

  let files = [];
  try {
    files = readdirSync(memDir).filter((f) => f.endsWith('.md') && !MEMORY_INDEX_FILES.has(f));
  } catch {
    files = [];
  }
  for (const f of files) {
    let content;
    try {
      content = readFileSync(join(memDir, f), 'utf8');
    } catch {
      continue; // one unreadable file degrades the corpus, never aborts the load
    }
    const rawTitle = extractMemoryFileTitle(content);
    const title = rawTitle ? rawTitle.toLowerCase() : null;
    const description = extractMemoryDescription(content);
    if (title && !seenTitles.has(title)) {
      seenTitles.add(title);
      titles.push(title);
      // File-derived: near-identity gate. See the MEASUREMENT note above.
      entries.push({ text: title, label: f, minRatio: MEMORY_FILE_MIN_RATIO });
    }
    if (description) {
      entries.push({
        text: description.toLowerCase(),
        label: f,
        threshold: MEMORY_BODY_OVERLAP_THRESHOLD,
        minRatio: MEMORY_FILE_MIN_RATIO,
      });
    }
    // The model-facing line carries BOTH — the title names the memory, the
    // description carries the knowledge. A slug-derived title alone was the
    // reason the novelty gate could not see what the project already knew.
    const line = title && description ? `${title} — ${description.toLowerCase()}`
      : title || (description ? description.toLowerCase() : null);
    if (line) promptLines.push(line);
  }
  return { titles, entries, promptLines };
}

// loadMemoryTitles — the bare-string title view, for the M1b prompt injection
// (selectNearbyMemoryTitles) and the existing suites. The dedup pass consumes
// `loadMemoryCorpus().entries` instead, which additionally carries per-file
// descriptions and labels.
function loadMemoryTitles(projectPath) {
  return loadMemoryCorpus(projectPath).titles;
}

function tokenize(text) {
  // Coerce non-strings to no-tokens. This is the single choke point for every
  // token operation — a null/number corpus entry (a title-less inbox item is
  // storable) must contribute ZERO overlap, never throw. Before this guard a
  // single malformed pending title threw out of isDuplicate (outside the
  // per-item try) and swallowed the WHOLE extraction batch — the exact
  // invisible knowledge-drop this program exists to prevent (boundary-man).
  if (typeof text !== 'string') return [];
  return text.toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 3);
}

// STOPWORDS — tokens that donate guaranteed overlap and must NOT count toward
// suppression (M1a, applied IDENTICALLY to both sides of every overlap and
// across all passes). Two classes: (1) the CC extraction TYPE/category words —
// every title is `${type}: ${title}` (line ~995), and lens titles are prefixed
// `unhandled:`/`skill-candidate:`/`methodology:` — so the prefix is a 100%
// donor; (2) high-frequency English fillers (the tokenizer already drops
// len<=3, so only longer fillers need listing). Domain/action words are
// deliberately NOT here — they are the meaningful signal. (`tokenize` lowercases
// and strips punctuation, so `skill-candidate` arrives as `skill`+`candidate`.)
const STOPWORDS = new Set([
  // type / category prefixes (every extraction title is `${type}: ...`;
  // lens titles are `unhandled:`/`skill-candidate:`/`methodology:`)
  'lesson', 'lessons', 'decision', 'decisions', 'constraint', 'constraints',
  'preference', 'preferences', 'feedback', 'pattern', 'patterns',
  'methodology', 'candidate', 'skill', 'unhandled', 'advisor', 'finding',
  'session', 'sessions', 'summary', 'summaries', 'memory',
  // session-advisor member-name fragments (Phase 2m titles are
  // `${shortName}: ...` — system-advocate/user-advocate/anthropic-insider/
  // historian; `advocate` is shared by two members, the worst donor)
  'advocate', 'historian', 'insider', 'anthropic',
  // generic English fillers (len>3)
  'with', 'that', 'this', 'from', 'into', 'when', 'what', 'which', 'their',
  'there', 'these', 'those', 'then', 'than', 'they', 'them', 'have', 'will',
  'would', 'should', 'could', 'about', 'after', 'before', 'being', 'been',
  'were', 'your', 'yours', 'over', 'under', 'also', 'just', 'like', 'does',
  'must', 'only', 'every', 'some', 'more', 'most', 'much', 'such', 'very',
  'onto', 'upon', 'each', 'both', 'here', 'where', 'while', 'because',
  'against', 'through', 'still', 'into', 'them',
]);

function meaningfulTokens(text) {
  return tokenize(text).filter(t => !STOPWORDS.has(t));
}

// Tokenizing a corpus entry is pure, and the memory corpus is now file-derived
// (act:471941c9 part 4) — on the largest project that is ~2200 entries, each
// re-tokenized once per candidate without this. Ring 3 is a short-lived
// process, so a plain Map is the whole cache; the size guard is a defensive
// bound, not a working limit.
const MEANINGFUL_TOKEN_MEMO_MAX = 20_000;
const _meaningfulTokenMemo = new Map();
function meaningfulTokenSet(text) {
  if (typeof text !== 'string') return new Set();
  let set = _meaningfulTokenMemo.get(text);
  if (!set) {
    set = new Set(meaningfulTokens(text));
    if (_meaningfulTokenMemo.size < MEANINGFUL_TOKEN_MEMO_MAX) {
      _meaningfulTokenMemo.set(text, set);
    }
  }
  return set;
}

// A dedup corpus entry is either a bare string (every corpus but memory) or
// { text, label, threshold } — the memory corpus carries the FILE it came from
// as `label`, so a suppression names the memory that blocked the draft, and a
// per-entry `threshold` so long prose (a description line) can require more
// overlap than a short title without moving the global bar.
function corpusEntryText(e) {
  if (typeof e === 'string') return e;
  return e && typeof e.text === 'string' ? e.text : '';
}
function corpusEntryLabel(e) {
  if (typeof e === 'string') return e;
  return (e && e.label) || corpusEntryText(e);
}
function corpusEntryThreshold(e) {
  return e && typeof e.threshold === 'number' ? e.threshold : OVERLAP_THRESHOLD;
}
// An optional NEAR-IDENTITY gate on top of the count threshold: shared tokens
// as a fraction of the shorter side. Entries that carry it (the file-derived
// memory corpus) only fire on "the same thing worded twice"; entries that don't
// keep the calibrated count-only behavior.
function corpusEntryMinRatio(e) {
  return e && typeof e.minRatio === 'number' ? e.minRatio : 0;
}

// isDuplicate — meaningful-token overlap dedup across all corpora (M1a).
//
// FIVE title/prose corpora, all matched the SAME way — whole-token overlap of
// post-stopword "meaningful" tokens, threshold OVERLAP_THRESHOLD:
//   memoryTitles    — parsed MEMORY.md titles (NO description tail; the M1a fix)
//   threadCursorLines — active thread-cursor prose ("what the system already
//                       knows you're working on"); a restatement is noise. Now
//                       whole-token overlap, NOT substring — leaving it on
//                       `line.includes` would re-introduce the exact
//                       over-suppression bug M1a kills. CALIBRATION NOTE: each
//                       cursor line is long prose (~9-19 meaningful tokens), so
//                       3-token overlap is structurally looser here than over
//                       short titles. This is strictly tighter than the old
//                       substring pass, and a DELIBERATE choice the M5
//                       suppression-ledger canary is built to measure — if the
//                       ledger shows thread-cursor over-suppression, raise its
//                       threshold then, with data (don't pre-tune blind).
//   pendingTitles / resolvedTitles / dismissedTitles — inbox-item titles.
//
// SHORT-TITLE FLOOR (boundary-man #1): a candidate whose title+content cannot
// muster >=3 meaningful tokens is NEVER suppressed — terseness must not lower
// the bar so a high-value 2-word constraint dies on one shared token. The flat
// OVERLAP_THRESHOLD (no Math.min lowering) enforces it. NOTE the floor is on
// title ∪ content[:10]: production passes `item.content`, so a terse title
// with rich content can clear the floor (and its content tokens then
// participate in overlap) — by design (the ITEM, not just the title, is what
// must be novel); the floor's hard guarantee is for genuinely contentless
// terse items.
//
// The trailing options object is the DESIGNATED GROWTH POINT for new corpora.
// Returns false, or a truthy { corpus, match } — callers log one line per
// suppression AND append a structured ledger record (recordSuppression).
function isDuplicate(title, content, memoryTitles, pendingTitles,
  { resolvedTitles = [], dismissedTitles = [], threadCursorLines = [] } = {}) {
  const titleTokens = meaningfulTokens(title);
  const contentTokens = meaningfulTokens(content).slice(0, 10);
  const allTokens = [...new Set([...titleTokens, ...contentTokens])];
  // Short-title floor: never suppress an item that can't muster 3 meaningful
  // tokens. (Also the empty-token guard.)
  if (allTokens.length < OVERLAP_THRESHOLD) return false;

  const score = (other) => {
    const otherTokens = meaningfulTokenSet(other);
    if (otherTokens.size === 0) return { shared: 0, ratio: 0 };
    const shared = allTokens.filter(t => otherTokens.has(t)).length;
    return { shared, ratio: shared / Math.min(allTokens.length, otherTokens.size) };
  };

  const passes = [
    ['memory', memoryTitles],
    ['thread-cursor', threadCursorLines],
    ['pending', pendingTitles],
    ['resolved', resolvedTitles],
    ['dismissed', dismissedTitles],
  ];
  for (const [corpus, entries] of passes) {
    for (const entry of (entries || [])) {
      const { shared, ratio } = score(corpusEntryText(entry));
      if (shared < corpusEntryThreshold(entry)) continue;
      if (ratio < corpusEntryMinRatio(entry)) continue;
      return { corpus, match: corpusEntryLabel(entry) };
    }
  }
  return false;
}

// selectNearbyMemoryTitles — the M1b prefilter (act:16904ffc). Pick the
// existing memory titles most relevant to THIS session so the extraction call
// can be shown "here is what's already saved" and self-filter dupes IN ONE
// pass (zero new model calls; embeddings stay rejected). Score each title by
// how many of its meaningful tokens appear in the (recent) transcript; take the
// top `limit`. Reuses the M1a tokenizer — this is context selection, not a
// similarity oracle (the extraction model itself is the semantic engine).
function selectNearbyMemoryTitles(memoryTitles, transcript, limit = 15) {
  if (!Array.isArray(memoryTitles) || memoryTitles.length === 0) return [];
  const tx = new Set(meaningfulTokens(transcript));
  if (tx.size === 0) return [];
  const scored = [];
  for (const title of memoryTitles) {
    const score = [...new Set(meaningfulTokens(title))].filter(t => tx.has(t)).length;
    if (score > 0) scored.push({ title, score });
  }
  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, limit).map(s => s.title);
}

// Separate cap for the pending-inbox shortlist (act:5182beda, N3/Reach 1 of
// grp:retro-remeaning) — NOT a shared budget with selectNearbyMemoryTitles'
// default 15. A large pending pile would otherwise evict memory titles from
// a shared slice and degrade the M1b novelty gate; the two corpora are
// separately capped so growth in one never starves the other.
const PENDING_RELATION_CAP = 10;

// The legal relation verbs a pending-relation proposal can carry. 'none' is
// the model's explicit no-relation answer, never a filed value.
const RELATION_VALUES = ['pairs_with', 'answers', 'contradicts'];

// selectNearbyPendingItems — the Reach-1 sibling of selectNearbyMemoryTitles:
// same relevance-scoring recipe, over a DIFFERENT corpus (pending inbox
// items, not saved memory) with its OWN cap. Returns {id, title} pairs, not
// bare strings — a relation proposal must resolve back to an actual pending
// item id, which a title string alone cannot do.
function selectNearbyPendingItems(pendingItems, transcript, limit = PENDING_RELATION_CAP) {
  if (!Array.isArray(pendingItems) || pendingItems.length === 0) return [];
  const tx = new Set(meaningfulTokens(transcript));
  if (tx.size === 0) return [];
  const scored = [];
  for (const p of pendingItems) {
    if (!p || typeof p.id !== 'string' || typeof p.title !== 'string' || !p.title.trim()) continue;
    const score = [...new Set(meaningfulTokens(p.title))].filter(t => tx.has(t)).length;
    if (score > 0) scored.push({ id: p.id, title: p.title, score });
  }
  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, limit).map(({ id, title }) => ({ id, title }));
}

// relationKey — the dedup key for a pending-relation proposal: sorted ids +
// the relation verb, so the SAME relation between the SAME pair of items
// never files twice regardless of which side is extracted first.
function relationKey(idA, idB, relation) {
  return `${[idA, idB].sort().join('|')}:${relation}`;
}

// fileRelationProposal — files (or returns the existing) 'pending-relation'
// item for a proposed relation between a freshly-filed item and a still-
// pending one. Dedup is a listItems scan by evidence.relation_key — global,
// not project-scoped, ACROSS ALL STATUSES WITH NO TIME WINDOW (act:bcb7edd4
// tightening, backported from Reach 2's durable-exclusion constraint): a
// relation the operator DISMISSED must never refile when a later session's
// extraction re-proposes it — the original pending-only check left exactly
// that hole. Never throws on a dup; the caller's try/catch covers genuine
// I/O failure only.
function fileRelationProposal({ project, fromId, fromTitle, toId, toTitle, relation, sessionId, threadIds }) {
  const key = relationKey(fromId, toId, relation);
  const existing = listItems({ category: 'pending-relation' })
    .find((i) => i.evidence && i.evidence.relation_key === key);
  if (existing) return existing.id;
  return createItem({
    project: project.name,
    project_path: project.path,
    ...(project.unresolved ? { project_unresolved: true } : {}),
    category: 'pending-relation',
    urgency: 'normal',
    title: `Relation: "${fromTitle}" ${relation} "${toTitle}"`,
    summary: `New understanding reaching back to a still-pending item: this item ${relation} `
      + `"${toTitle}", which is still awaiting triage in the inbox.`,
    context_anchor: `session ${sessionId}`,
    evidence: {
      relation,
      relation_key: key,
      from_id: fromId,
      to_id: toId,
      from_title: fromTitle,
      to_title: toTitle,
      session_id: sessionId,
    },
    options: [
      { key: 'confirm', label: 'Confirm — the relation holds' },
      { key: 'dismiss', label: 'Not related' },
    ],
    filed_by: 'ring3-close',
    thread_ids: threadIds,
  });
}

// modelRescues — the M1b rescue gate (DEFAULT-KEEP). The extraction model is
// shown the nearby saved titles and asked, per proposed item, which saved title
// covers it (`covered_by`) — "none" if genuinely novel. When isDuplicate
// LEXICALLY flags an item the model AFFIRMATIVELY judged novel, the model's
// semantic read RESCUES it (file anyway). The model may ONLY rescue, NEVER
// suppress: a missing/garbage/empty covered_by is NOT an affirmative novelty
// claim, so isDuplicate's deterministic decision stands. (The historian
// landmine, act:3975348f — a silent fuzzy SUPPRESSOR was deleted once; this
// gate can only ADD recall, never remove it.)
function modelRescues(item) {
  const cb = item && item.covered_by;
  if (typeof cb !== 'string') return false;
  return /^\s*(none|null|n\/?a|nothing|no\b)/i.test(cb.trim());
}

// chunkWithOverlap — the M2 Phase B windowing (act:edd79e15 pt2). Split `text`
// into windows of `size` chars that OVERLAP by `overlap` chars (so a lesson
// straddling a boundary appears whole in at least one window). Covers the full
// text front-to-back; if more than `maxChunks` windows are needed, the
// MOST-RECENT `maxChunks` are kept (recency — a missed late lesson is the bug
// this program fights) and `dropped` counts the front windows skipped. Returns
// { windows, capped, dropped }.
function chunkWithOverlap(text, size, overlap, maxChunks) {
  if (typeof text !== 'string' || text.length === 0) return { windows: [], capped: false, dropped: 0 };
  if (text.length <= size) return { windows: [text], capped: false, dropped: 0 };
  const step = Math.max(1, size - overlap);
  const windows = [];
  for (let start = 0; start < text.length; start += step) {
    windows.push(text.slice(start, start + size));
    if (start + size >= text.length) break;
  }
  if (windows.length <= maxChunks) return { windows, capped: false, dropped: 0 };
  const dropped = windows.length - maxChunks;
  return { windows: windows.slice(dropped), capped: true, dropped };
}

// mergeChunkExtractions — flatten per-chunk extraction arrays and dedup the
// merge with a STRICTER-than-corpus matcher (act:edd79e15 pt2). Intra-session
// titles are fresh and specific, so the overlap windows produce the SAME lesson
// twice — merge those (normalized-equal title, or >=80% of the shorter title's
// meaningful tokens shared and >=3) but NEVER merge two genuinely distinct
// lessons that merely share a few words (don't reuse the loose 3-token corpus
// bar). covered_by and all fields ride through from the first copy kept.
function mergeChunkExtractions(perChunk) {
  const norm = (s) => String(s).toLowerCase().replace(/\s+/g, ' ').trim();
  const merged = [];
  for (const arr of perChunk) {
    for (const item of (arr || [])) {
      if (!item || typeof item.title !== 'string') continue;
      const itemTokens = new Set(meaningfulTokens(item.title));
      const dup = merged.some(m => {
        if (norm(m.title) === norm(item.title)) return true;
        const mTokens = meaningfulTokens(m.title);
        if (itemTokens.size === 0 || mTokens.length === 0) return false;
        const shared = mTokens.filter(t => itemTokens.has(t)).length;
        const smaller = Math.min(itemTokens.size, mTokens.length);
        return shared >= 3 && shared >= Math.ceil(0.8 * smaller);
      });
      if (!dup) merged.push(item);
    }
  }
  return merged;
}

// runExtractionCall — one model call → parsed JSON array (or [] on no-match).
// Shared by the Phase A single call and each Phase B chunk. Throws on a call or
// JSON error so the caller decides the fail-direction (Phase A returns nothing;
// Phase B isolates per chunk so one bad window doesn't lose the others).
async function runExtractionCall(callFn, systemPrompt, userMessage) {
  const response = await callFn(systemPrompt, userMessage);
  const jsonMatch = response.match(/\[[\s\S]*\]/);
  return jsonMatch ? JSON.parse(jsonMatch[0]) : [];
}

// resolutionCorpus — titles of recently resolved/dismissed/superseded inbox
// items for a project, split into the two suppression corpora isDuplicate
// consumes. Recency-capped at RESOLUTION_CORPUS_DAYS to keep the corpus
// bounded (~hundreds of titles max).
function resolutionCorpus(projectName) {
  const since = new Date(
    Date.now() - RESOLUTION_CORPUS_DAYS * 24 * 60 * 60 * 1000).toISOString();
  const closed = listItems({
    project: projectName,
    statuses: ['resolved', 'dismissed', 'superseded'],
    since,
  });
  const dismissedTitles = closed
    .filter(i => i.status === 'dismissed' || /not.?relevant/i.test(i.resolution || ''))
    .map(i => i.title)
    .filter(Boolean);
  const dismissedSet = new Set(dismissedTitles);
  const resolvedTitles = closed
    .map(i => i.title)
    .filter(t => t && !dismissedSet.has(t));
  return { resolvedTitles, dismissedTitles };
}

// threadCursorLines — prose corpus from active thread cursors. Thread cursors
// (what / where_left_off / open_questions) are exactly "what the system
// already knows the user is working on"; an extraction that restates them is
// noise. Returned lines are lowercased; isDuplicate consumes them as the
// `threadCursorLines` corpus — whole-token overlap (M1a), not substring.
//
// Delegates to projectThreadCursorLines in watchtower-lib.mjs — the SINGLE
// thread-file reader (the same one Ring 2's enrichment uses). The former local
// copy carried a slug-substring membership FALLBACK (slugify(thread).includes(
// projectSlug)) that over-matched: a short slug like "flow" pulled in any
// "…workflow…" thread's cursor prose, silently suppressing legitimate new
// extractions. The shared reader matches ONLY by exact sessions[].project
// equality (or explicit thread_ids) — that fallback is gone (act:3975348f).
function threadCursorLines(threadsDir, projectSlug) {
  return projectThreadCursorLines(threadsDir, projectSlug);
}

// buildExtractionCorpora — assemble the dedup corpora isDuplicate consumes,
// the SAME machinery Phase 2d and Phase 2m build inline. Extracted so the
// close lenses (Phase 2n/2o) don't fork a third copy of the corpus-building
// idiom. Every builder fails OPEN — a corpus hiccup degrades dedup, never
// aborts the lens (a finding re-filing once beats losing the pass). `phase`
// labels the warning lines.
//
// M1a split: `memoryTitles` (parsed MEMORY.md titles, NO description tail) and
// `threadCursorLines` (active-thread cursor prose) are now SEPARATE named
// corpora — they used to be smuggled into one `memoryLines` array and substring
// -matched together. They are both whole-token-matched by isDuplicate now.
function buildExtractionCorpora(project, { phase } = {}) {
  const tag = phase || 'lens';
  // act:471941c9 part 4: `memoryCorpus.entries` is what isDuplicate consumes
  // (file-derived, labelled, per-entry thresholds); `titles` stays the bare
  // string view the M1b prompt injection needs.
  let memoryCorpus = { titles: [], entries: [] };
  try {
    memoryCorpus = loadMemoryCorpus(project.path);
  } catch (e) {
    logError(`${tag}: memory corpus failed (${e.message}) — continuing without it`);
  }
  const memoryTitles = memoryCorpus.titles;
  let cursorLines = [];
  try {
    cursorLines = threadCursorLines(
      join(WATCHTOWER_DIR, 'state', 'threads'), project.slug);
  } catch (e) {
    logError(`${tag}: thread-cursor corpus failed (${e.message}) — continuing without it`);
  }
  let pendingTitles = [];
  try {
    pendingTitles = listPending({ project: project.name })
      .map(p => p.title).filter(t => typeof t === 'string' && t.trim());
  } catch (e) {
    logError(`${tag}: pending corpus failed (${e.message}) — continuing without it`);
  }
  let resolutionTitles = { resolvedTitles: [], dismissedTitles: [] };
  try {
    resolutionTitles = resolutionCorpus(project.name);
  } catch (e) {
    logError(`${tag}: resolution corpus failed (${e.message}) — continuing without it`);
  }
  return {
    memoryTitles,
    memoryEntries: memoryCorpus.entries,
    memoryPromptLines: memoryCorpus.promptLines,
    threadCursorLines: cursorLines,
    pendingTitles,
    resolutionTitles,
  };
}

// --- Unmet acceptance criteria (act:86226720) --------------------------------
//
// The reported failure (2026-07-30): ten completion-review items arrived, one
// per action with recent work. All ten were open/in-progress with completed=0
// and all ten were blocked on the same external event. None closeable, none
// ambiguous. The detector answers "did something change here" when the
// operator needs "is something finished here."
//
// THE MEASUREMENT CAME FIRST, AND IT REFUTED THE PROPOSED FIX. The report
// suggested suppressing while the action carries unchecked `- [ ]` AC lines,
// "that signal already sits in the notes". Measured against the live queue:
//
//   * ZERO of the reporting ten cite an action with an unchecked box. That
//     project writes its acceptance criteria as PROSE. The proposed mechanism
//     does not exist in its own motivating corpus.
//   * Portfolio-wide, only 8 of 247 completion-review items ever filed cite an
//     action with unchecked boxes at all. Of those 8: 5 noise, 3 real
//     completions — a 62% noise rate against a 73% baseline. The checkbox
//     cohort is BETTER than average, so suppressing on it is mildly
//     ANTI-predictive, the same shape act:ea23b3a5 found for confidence:high.
//   * Every other mechanical signal measured flat: model confidence
//     (high 20% / medium 32% / low 26% against a 26% baseline), an explicit
//     `blocked` status (zero actions in the whole corpus use it), and an
//     external-blocker phrase in the notes (23% vs 27%).
//
// THE ACTUAL GAP, found by reading the prompt: Phase 2c sends the model
// `fid: text (status)` and nothing else. It has never been shown an action's
// NOTES, so it has never been shown the acceptance criteria. It cannot answer
// "is this finished" because it is not told what finished means. The report's
// intent was right; its assumed encoding was wrong.
//
// So the gate reads unmet acceptance criteria HOWEVER THEY ARE EXPRESSED:
//   arm 1 (mechanical) — unchecked `- [ ]` boxes, the literal acceptance
//          criterion this action asks for, cheap and deterministic;
//   arm 2 (model)      — the AC section is now included in the prompt and the
//          model returns `ac_status` with a VERBATIM cited criterion. Only a
//          cited `unmet` suppresses; an uncited one is not an answer.
//
// WHY GATING IS SAFE HERE, WHERE IT WASN'T FOR QUOTE VERIFICATION: this file's
// own budget comment states the asymmetry — "a missed completion is
// recoverable next session, a missed lesson is not." Phase 2c re-runs every
// session against still-open actions, so an over-suppressed candidate returns.
// That is why act:ea23b3a5 shipped quote verification as instrumentation only
// and this ships as a gate.
//
// CALIBRATION FENCE: the act:ea23b3a5 confidence measurement is fenced to "the
// Phase 2c confidence rubric AS WRITTEN". The confidence text below is
// UNCHANGED, byte for byte; `ac_status` is an ADDITIONAL field. Do not reword
// the confidence lines without re-running that measurement.

// Unchecked markdown task boxes in an action's notes.
const UNMET_AC_BOX_RE = /^[ \t]*[-*][ \t]+\[[ ]\]/m;
const MET_AC_BOX_RE = /^[ \t]*[-*][ \t]+\[[xX]\]/m;

/**
 * Does this action's notes carry at least one UNCHECKED acceptance-criteria
 * box? Returns false when there are no boxes at all — "no checklist" is not
 * "unmet checklist", and treating it as such would suppress the whole corpus.
 */
export function hasUnmetAcceptanceCriteria(notes) {
  if (typeof notes !== 'string' || !notes) return false;
  return UNMET_AC_BOX_RE.test(notes);
}

/** Does this action's notes carry a checklist at all (checked or not)? */
export function hasAcceptanceCriteriaBoxes(notes) {
  if (typeof notes !== 'string' || !notes) return false;
  return UNMET_AC_BOX_RE.test(notes) || MET_AC_BOX_RE.test(notes);
}

// How much of an action's notes to show the model. The AC section is what
// matters, so prefer it; fall back to the head of the notes when no section
// header is found. Bounded because Phase 2c sends every open action.
const AC_EXCERPT_MAX_CHARS = 1200;
const AC_HEADING_RE = /^#{1,6}\s*(acceptance criteria|acceptance|success criteria|definition of done)\b/im;

/**
 * Extract the acceptance-criteria excerpt to show the model for one action.
 * Returns '' when the notes carry nothing worth sending.
 */
export function acceptanceCriteriaExcerpt(notes) {
  if (typeof notes !== 'string' || !notes.trim()) return '';
  const m = notes.match(AC_HEADING_RE);
  if (m) {
    const start = notes.indexOf(m[0]);
    const rest = notes.slice(start + m[0].length);
    // Stop at the next heading of the same-or-higher level, so one section is
    // sent rather than the whole tail of the notes.
    const next = rest.search(/\n#{1,6}\s+\S/);
    const body = next === -1 ? rest : rest.slice(0, next);
    return `${m[0]}${body}`.trim().slice(0, AC_EXCERPT_MAX_CHARS);
  }
  return notes.trim().slice(0, AC_EXCERPT_MAX_CHARS);
}

/**
 * The model's acceptance-criteria verdict, normalized. Only an explicit
 * 'unmet' carrying a non-empty verbatim citation can suppress — an uncited
 * verdict is an assertion, not an answer, and the whole point of this gate is
 * that the model is now being SHOWN the criteria it is judging.
 * @returns {{status: string, criterion: string|null, suppresses: boolean}}
 */
export function normalizeAcVerdict(evalItem) {
  const raw = evalItem && typeof evalItem.ac_status === 'string'
    ? evalItem.ac_status.trim().toLowerCase() : '';
  const status = ['met', 'unmet', 'unstated'].includes(raw) ? raw : 'unknown';
  const criterionRaw = evalItem && typeof evalItem.ac_unmet === 'string'
    ? evalItem.ac_unmet.trim() : '';
  const criterion = criterionRaw.length > 0 ? criterionRaw : null;
  return { status, criterion, suppresses: status === 'unmet' && criterion !== null };
}

// completionReviewEmitGuard — emit-time guards for Phase 2c (one call per
// evaluation, immediately before createItem):
//
//   1. Status re-check: skip unless the action is STILL open/in-progress in
//      pib.db at emit time — kills the done-between-snapshot-and-emit and the
//      deferred classes in one check. FAIL-OPEN with a distinct logError: if
//      the SELECT throws (e.g. sqlite lock contention at session close), the
//      item emits anyway — a DB hiccup degrades to today's behavior, never to
//      silent wholesale suppression. The try/catch is the guard's own so a
//      throw can't abort the remaining evaluations.
//   2. Confidence inversion (act:ea23b3a5): skip when the model rated this fid
//      'high'. The full calibration note sits on the check itself. This is the
//      ONE fail-closed arm in the guard, and it REPLACED act:9eebbac4's
//      create-vs-complete day-window filter, which encoded the opposite theory
//      and measured net-negative.
//   3. Per-fid dedup: skip when an existing completion-review item for the
//      same fid is in `existingItems` (the caller builds that set from pending
//      ∪ resolved/dismissed/expired/superseded within
//      COMPLETION_REVIEW_DEDUP_DAYS — `expired` and `superseded` are
//      load-bearing since the category's expiry became 14d, equal to the dedup
//      window: without them an item drops out of every corpus at exactly the
//      moment it expires and refiles forever).
//
// `confidence` is the model's rating for this fid. Every param is optional and
// each check self-skips, so callers/tests using the original shape are
// untouched.
//
// Returns { emit: true } or { emit: false, reason }.
function completionReviewEmitGuard(fid, {
  statusStmt, existingItems = [], confidence = null,
  unmetAcBoxes = false, acVerdict = null,
} = {}) {
  // Unmet acceptance criteria (act:86226720). Checked FIRST because it is the
  // most specific answer available to "is this finished": the action itself
  // says what finished means, and it says no. Two arms, mechanical then model
  // — see the block comment above for why the mechanical arm alone was
  // measured insufficient and why the model arm requires a citation.
  if (unmetAcBoxes) {
    return {
      emit: false,
      reason: 'unmet acceptance criteria — the action notes carry unchecked "- [ ]" boxes',
      suppressed_by: 'unmet-ac-boxes',
    };
  }
  if (acVerdict && acVerdict.suppresses) {
    return {
      emit: false,
      reason: `unmet acceptance criteria — model cites "${acVerdict.criterion}"`,
      suppressed_by: 'unmet-ac-model',
    };
  }
  if (statusStmt) {
    try {
      const row = statusStmt.get(fid);
      const status = row ? row.status : null;
      if (status !== 'open' && status !== 'in-progress') {
        return { emit: false, reason: `status now '${status ?? 'gone'}'` };
      }
    } catch (e) {
      // FAIL-OPEN: emit despite the failed re-check.
      logError(`Phase 2c: emit-time status re-check threw for ${fid} (${e.message}) — failing open, item will emit`);
    }
  }
  // CONFIDENCE IS ANTI-PREDICTIVE HERE — measured, not assumed (act:ea23b3a5,
  // 2026-07-26, over all 223 items this category had ever filed, each joined
  // against the cited action's OWN project db):
  //
  //     high        12 acted-on / 28 dismissed-noise    30% precision
  //     medium+low  48 acted-on / 32 dismissed-noise    60% precision
  //
  // The model rates 'high' when a session TALKS about an action — files it,
  // plans it, discusses it — not when it quietly finishes one. act:9eebbac4's
  // create-vs-complete filter encoded the opposite theory (it exempted 'high'
  // from the born-this-session skip), so it suppressed the BEST cohort
  // (same-day creation + medium/low: 74% precision, 20 acted-on / 7 noise)
  // while passing the worst (same-day + high: 33%). Split at that filter's
  // 2026-07-12 ship date, the category went 0-for-23 — zero acted-on, 19
  // dismissed as noise. The filter is deleted; this replaces it.
  //
  // CALIBRATION FENCE: this holds for the Phase 2c confidence rubric AS
  // WRITTEN. Rewording that rubric changes what 'high' means and invalidates
  // the measurement — re-run it before touching the prompt's confidence text.
  if (String(confidence).toLowerCase() === 'high') {
    return {
      emit: false,
      reason: "confidence 'high' is anti-predictive for completion (30% vs 60%) — see the calibration note",
    };
  }
  const existing = existingItems.find(
    i => i.plan_fid === fid || i.evidence?.fid === fid);
  if (existing) {
    return {
      emit: false,
      reason: `existing completion-review item ${existing.id} (${existing.status})`,
    };
  }
  return { emit: true };
}

// ---------------------------------------------------------------------------
// Completion-quote verification — INSTRUMENTATION ONLY (act:ea23b3a5)
// ---------------------------------------------------------------------------
//
// Does the model's claimed completion span actually appear in the transcript it
// was shown? This records an answer; it deliberately does NOT gate emission,
// for two reasons worth keeping written down.
//
// (1) The haystack is not prose. `preprocessTranscript` ends with
//     `kept.map(e => JSON.stringify(e)).join('\n')`, so a real newline inside a
//     message is the two characters `\` + `n`, and a quote character is `\"`. A
//     substring test against those bytes REJECTS ordinary multi-line prose —
//     the shape most genuine completion statements take — while reliably
//     ACCEPTING serialized tool arguments: every TodoWrite call leaves
//     `{"todos":[{"content":"…","status":"completed"` in the haystack,
//     pre-escaped and quotable verbatim. So the matcher decodes first, and
//     `decodeTranscriptText` drops tool_use `input_summary` blobs on purpose.
// (2) Even decoded, containment proves the SPAN EXISTS — never that the span
//     means the work is done. The 2026-07-26 corpus contains a dismissal whose
//     own reason quotes "act:a587b30f — filed (…)": a verbatim quote of a
//     FILING. A gate built on this would launder that into evidence.
//
// So: measure first (the discipline act:de4a7020 stalled on and this program
// finally ran). The stamped `evidence.quote_verified` plus the per-session
// counts are the data a future round needs to decide whether a real gate —
// semantic, not lexical — is worth building.

const COMPLETION_QUOTE_MIN_CHARS = 20;

function normalizeQuoteText(s) {
  return typeof s === 'string' ? s.replace(/\s+/g, ' ').trim().toLowerCase() : '';
}

// decodeTranscriptText — recover human-readable text from a preprocessTranscript
// haystack. Each line is one JSON message whose `content` is a string or an
// array of blocks; only plain strings and `text` blocks count. Tool-use
// summaries are EXCLUDED by design (see note 1 above). An unparseable line is
// skipped, never raw-matched — matching the raw line would reintroduce the
// escaped-bytes problem this function exists to remove.
export function decodeTranscriptText(jsonl) {
  if (typeof jsonl !== 'string' || !jsonl) return '';
  const out = [];
  for (const line of jsonl.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let entry;
    try { entry = JSON.parse(trimmed); } catch { continue; }
    const content = entry && entry.content;
    if (typeof content === 'string') { out.push(content); continue; }
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (typeof block === 'string') out.push(block);
      else if (block && block.type === 'text' && typeof block.text === 'string') out.push(block.text);
    }
  }
  return out.join('\n');
}

// Returns { verified: boolean, reason: 'found' | 'not-found' | 'absent-or-too-short'
// | 'no-decodable-transcript' }. Never throws; never affects `emit`.
export function verifyCompletionQuote(quote, transcriptJsonl) {
  const needle = normalizeQuoteText(quote);
  if (needle.length < COMPLETION_QUOTE_MIN_CHARS) {
    return { verified: false, reason: 'absent-or-too-short' };
  }
  const haystack = normalizeQuoteText(decodeTranscriptText(transcriptJsonl));
  if (!haystack) return { verified: false, reason: 'no-decodable-transcript' };
  return haystack.includes(needle)
    ? { verified: true, reason: 'found' }
    : { verified: false, reason: 'not-found' };
}

// ---------------------------------------------------------------------------
// Bookkeeping suppression (act:471941c9, part 1)
// ---------------------------------------------------------------------------
//
// A fifth of one project's 105-item extraction drain (2026-07-30) was
// BOOKKEEPING: "decision: X merged to main at faf335e", "decision: core
// v0.1.12 committed and merged, not yet published", "decision: skill filed as
// act:6f79ddc4". Git and pib-db already hold those facts, more reliably and
// with less drift — a memory draft of them is a stale cache with a triage
// cost. The four substance classes named by the report are the four rules
// below: a commit sha, a merge event, a version bump, a filed-as-record.
//
// DESIGN CONSTRAINTS, all learned the expensive way by the recall-fix program:
//
//  - TITLE-GATED. Every specimen in the corpus is bookkeeping *in its title*;
//    a genuine lesson that merely MENTIONS a sha in its body is not
//    bookkeeping. Matching on content would suppress real lessons about
//    merges, versions, and filing — the exact over-suppression this system
//    has already paid for once (act:3975348f).
//  - A HEX RUN NEEDS A DIGIT. `\b[0-9a-f]{7,40}\b` alone matches ordinary
//    English ("defaced", "cabbaged"). Requiring at least one digit removes
//    the whole word class without weakening sha detection.
//  - A HEX RUN NEEDS GIT CONTEXT. A bare hex token is only a commit sha when
//    the title also talks about committing/merging/branching. Otherwise it is
//    an id of some other kind and this rule has no opinion.
//  - A FID NEEDS FILING GRAMMAR. "filed AS act:…" / "deferred TO new action
//    act:…" is a record of filing. "automations skill filed and built
//    (act:1866b723) — how Lily creates her own scheduled automations" is a
//    real decision that happens to cite its action, and the operator KEPT it.
//    The preposition, and the fid sitting at the very end of the title, are
//    what separate the two.
//
// Returns null (not bookkeeping) or { rule, matched } — the caller logs the
// rule name so every suppression is attributable to a named cause.

// A hex run of sha length that contains at least one digit.
const SHA_RUN_RE = /\b(?=[0-9a-f]{7,40}\b)[a-f0-9]*[0-9][0-9a-f]*\b/i;
// Fid tokens are stripped before the sha test. An `act:29687e65` is a work-item
// citation, not a commit — and its 8-hex tail is indistinguishable from a short
// sha once the prefix is out of frame. Measured: leaving fids in made every
// title that cited an action AND used the word "merge" read as a commit record,
// including "…send-time merge is a deferred follow-up (act:29687e65)", a real
// decision the operator kept.
const ANY_FID_TOKEN_RE = /\b(?:act|dec|prj|grp):[0-9a-z-]{4,}/gi;
// Words that make a hex run a COMMIT sha rather than some other identifier.
const GIT_CONTEXT_RE = /\b(commit|commits|committed|merge|merged|merging|branch|sha|fast-forward|pushed|landed|cherry-pick|rebased?)\b/i;
// "merged to main" — the merge-event record. PAST TENSE ONLY: "merging to main
// requires a merge commit, not fast-forward" is a lesson ABOUT merging, and the
// present participle is what separates the two.
const MERGE_EVENT_RE = /\bmerged\s+(?:in)?to\s+(?:main|master|trunk)\b/i;
// "two commits on branch mux/…" — a commit-count record with no sha in it.
const COMMIT_COUNT_RE = /\b(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+commits?\s+on\s+(?:the\s+)?branch\b/i;
// A semver-shaped version anywhere in the title.
const VERSION_RE = /\bv?\d+\.\d+\.\d+\b/;
// The verbs that turn a version mention into a release-bookkeeping record.
const RELEASE_VERB_RE = /\b(bump(?:ed)?|shipped|released?|publish(?:ed)?|committed|merged|tagged)\b/i;
// "filed as act:…", "stored in act:…", "deferred to new action act:…".
const FILED_AS_RE = /\b(?:filed|stored|tracked|logged|queued|captured|recorded|deferred|moved|split)\s+(?:as|in|to|under)\s+(?:a\s+|the\s+|new\s+|another\s+)*(?:action\s+|item\s+|handoff\s+)?(?:act|dec|prj|grp):[0-9a-z-]{4,}/i;
// A trailing "(act:…)" parenthetical on a title whose verb is a filing verb —
// the other half of the filed-as shape, where the fid is the citation rather
// than the object of the preposition.
const TRAILING_FID_RE = /\((?:act|dec|prj|grp):[0-9a-z-]{4,}\)\s*$/i;
// The routing PREPOSITION is required, not just the verb: "deferred TO Phase 4
// build gate (act:…)" is a filing record; "…send-time merge is a deferred
// follow-up (act:…)" is a real decision that cites its action, and the operator
// kept it.
const FILING_VERB_RE = /\b(?:filed|deferred|tracked|queued|split|moved|superseded|stored)\s+(?:as|to|in|into|under)\b/i;
// A queue-item disposition record ("Resolve two inbox items as addressed"). The
// QUANTIFIER is load-bearing: it separates a record of dispositioning N actual
// items from a lesson about the mechanism ("Dismissed inbox items refile on
// next tick — dedup only checks pending items", which the operator kept).
const QUEUE_BOOKKEEPING_RE = /\b(?:resolve[ds]?|dismiss(?:ed)?|supersede[ds]?|flipped)\s+(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|all|the)\b[^.]{0,40}\b(?:inbox|queue)\s+items?\b/i;

export function bookkeepingRule(title) {
  if (typeof title !== 'string' || !title.trim()) return null;
  const t = title;
  const deFidded = t.replace(ANY_FID_TOKEN_RE, ' ');
  if (SHA_RUN_RE.test(deFidded) && GIT_CONTEXT_RE.test(t)) {
    return { rule: 'commit-sha', matched: (deFidded.match(SHA_RUN_RE) || [''])[0] };
  }
  if (MERGE_EVENT_RE.test(t)) {
    return { rule: 'merge-event', matched: (t.match(MERGE_EVENT_RE) || [''])[0] };
  }
  if (COMMIT_COUNT_RE.test(t)) {
    return { rule: 'merge-event', matched: (t.match(COMMIT_COUNT_RE) || [''])[0] };
  }
  if (VERSION_RE.test(t) && RELEASE_VERB_RE.test(t)) {
    return { rule: 'version-bump', matched: (t.match(VERSION_RE) || [''])[0] };
  }
  if (FILED_AS_RE.test(t)) {
    return { rule: 'filed-as-record', matched: (t.match(FILED_AS_RE) || [''])[0] };
  }
  if (TRAILING_FID_RE.test(t) && FILING_VERB_RE.test(t)) {
    return { rule: 'filed-as-record', matched: (t.match(TRAILING_FID_RE) || [''])[0].trim() };
  }
  if (QUEUE_BOOKKEEPING_RE.test(t)) {
    return { rule: 'queue-bookkeeping', matched: (t.match(QUEUE_BOOKKEEPING_RE) || [''])[0] };
  }
  return null;
}

// ---------------------------------------------------------------------------
// Authority-path routing (act:471941c9, part 2)
// ---------------------------------------------------------------------------
//
// A project can designate a single file as THE authority on a class of
// knowledge — reflow makes `docs/cowork-capabilities.md` the one place
// platform limits live, enforced by its own validator. Ring 3 was minting the
// same facts into memory in parallel, which is precisely the drift the
// authority file exists to prevent (4 of 10 extracted constraints had been
// written there the day before).
//
// Declared in watchtower config as either a per-project or a defaults-level
// map from extraction TYPE to a repo-relative path:
//
//   "authority_paths": { "constraint": "docs/cowork-capabilities.md" }
//
// The routing is a ROUTING, not a suppression: the item is still filed, still
// triaged, still carries its full draft. Only its home changes — from
// "memory" to "authority-file", with the path named in the option label so
// the operator's one click says where it goes. Nothing is written to the
// authority file automatically; a file with a validator behind it is not
// something a cron job should edit.
export function resolveAuthorityPath(config, projectName, type) {
  if (!config || typeof type !== 'string' || !type) return null;
  const perProject = config.projects
    && config.projects[projectName]
    && config.projects[projectName].authority_paths;
  const fromDefaults = config.defaults && config.defaults.authority_paths;
  for (const map of [perProject, fromDefaults]) {
    if (!map || typeof map !== 'object') continue;
    const p = map[type];
    if (typeof p === 'string' && p.trim()) return p.trim();
  }
  return null;
}

// ---------------------------------------------------------------------------
// Commitment dedup — the other side of /close's sweep (act:aa554774)
// ---------------------------------------------------------------------------
//
// /close now files prospective, dated obligations as ACTIONS while the
// operator is still at the terminal (a dated commitment cannot wait in a
// 275-item inbox). Minutes later Ring 3 processes the same transcript and
// would extract those same commitments as inbox items — the operator's
// explicit requirement when this was filed: "the watchtower must not create
// duplicates of what the sweep files."
//
// So Phase 2d loads the actions this project created on the session's date and
// declines to file a commitment-shaped extraction that one of them already
// covers. The window is the session's calendar DAY, not the session itself,
// because pib-db's `actions.created` is a date (`GLOB '????-??-??'`) with no
// time component — coarser than ideal, and adequate: the sweep files minutes
// before the ring runs. Sessions that cross midnight are covered by taking
// everything created on or after the session-start date.
//
// This is the same seam as bookkeeping suppression (act:471941c9) from the
// other side. Bookkeeping says "pib-db already owns this event"; this says
// "pib-db already owns this obligation, as of ninety seconds ago."
function loadSessionFiledActions(project, sessionStartIso) {
  const projectPath = project && project.path;
  if (!projectPath) return [];
  const dbPath = join(projectPath, 'pib.db');
  if (!existsSync(dbPath)) return [];
  const Database = loadBetterSqlite3(projectPath);
  if (!Database) return [];
  // Fail OPEN at every step: a db hiccup means Ring 3 may file a duplicate the
  // operator dismisses in a second. Failing CLOSED would mean silently
  // dropping a commitment, which is the failure this action exists to fix.
  let db;
  try {
    db = new Database(dbPath, { readonly: true, timeout: 5000 });
  } catch (e) {
    logError(`Phase 2d: cannot open pib.db for commitment dedup (${e.message}) — continuing without it`);
    return [];
  }
  try {
    const day = (sessionStartIso && /^\d{4}-\d{2}-\d{2}/.test(sessionStartIso))
      ? sessionStartIso.slice(0, 10)
      : new Date().toISOString().slice(0, 10);
    return db.prepare(
      "SELECT fid, text FROM actions WHERE created >= ? AND deleted_at IS NULL"
    ).all(day);
  } catch (e) {
    logError(`Phase 2d: cannot query same-day actions for commitment dedup (${e.message}) — continuing without it`);
    return [];
  } finally {
    try { db.close(); } catch { /* already closed or never opened cleanly */ }
  }
}

// ---------------------------------------------------------------------------
// Phase 2d: Knowledge extraction → inbox
// ---------------------------------------------------------------------------

async function decisionExtraction(compressed, project, sessionId, transcriptPath,
  threadIds = [], { callFn = claudeCall, config = null, sessionStartIso = null } = {}) {
  const projectPath = project.path;
  log('Phase 2d: Knowledge extraction');

  // Dedup corpora — built FIRST (before the extraction call) so M1b can inject
  // the nearest saved titles into the prompt AND back the result with the
  // deterministic isDuplicate pass after. The SAME builder the close lenses use
  // (one corpus-building idiom). M1a: memory is title-matched (no description
  // tail), thread cursors are their own whole-token corpus; every builder fails
  // open. Queried by the resolved project NAME (the old basename query looked
  // up a phantom project, so dedup never matched and dups re-filed).
  const { memoryTitles, memoryEntries, memoryPromptLines, threadCursorLines: cursorLines,
    pendingTitles, resolutionTitles } = buildExtractionCorpora(project, { phase: 'Phase 2d' });
  // Positive confirmation, not silence: "no suppressions" must be
  // distinguishable from "the memory corpus never loaded".
  log(`Phase 2d: memory corpus — ${memoryTitles.length} title(s), ${memoryEntries.length} dedup entr(ies), ${memoryPromptLines.length} novelty line(s)`);

  const transcript = recentSlice(compressed, SINGLE_CALL_TRANSCRIPT_BUDGET);
  // M1b prefilter: the saved titles most relevant to THIS session (scored over
  // the full compressed transcript, not just the recent slice).
  // act:471941c9 part 4: scored over `memoryPromptLines` (title — description),
  // not bare index titles. A consolidated memory's index title is slug-derived
  // and carries almost no content tokens, so the old prefilter could neither
  // FIND the relevant memory nor SHOW the model anything it could judge against.
  const nearbyTitles = selectNearbyMemoryTitles(memoryPromptLines, compressed);

  // Reach 1 (act:5182beda): the id-paired sibling of pendingTitles, so a
  // model-proposed relation can resolve back to an actual pending item.
  // SEPARATE fetch from buildExtractionCorpora's bare-string pendingTitles
  // (that shape serves the deterministic dedup pass; this one needs ids).
  let pendingItemsForRelation = [];
  try {
    pendingItemsForRelation = listPending({ project: project.name });
  } catch (e) {
    logError(`Phase 2d: pending-relation corpus failed (${e.message}) — continuing without it`);
  }
  const nearbyPending = selectNearbyPendingItems(pendingItemsForRelation, compressed);
  const pendingByTitle = new Map(nearbyPending.map((p) => [p.title, p.id]));

  const systemPrompt = `You are extracting decisions, constraints, lessons, and user preferences from a Claude Code session transcript. For each item found, classify its home:

- "memory" = a lesson, preference, or constraint worth remembering across sessions
- "claude-md" = a convention or rule that should be added to CLAUDE.md
- "pib-db-trigger" = a deferred action with a trigger condition
- "upstream-feedback" = friction with Claude Code itself
- "derivation" = the fact is DERIVABLE (see DERIVABLE below) — its home is the recomputation itself, not a written record
- "session-record" = the fact is PERISHABLE (see PERISHABLE below) — its only durable home is the record of the session that observed it, never memory

Only extract items that represent NEW knowledge — things learned or decided in this session that aren't yet captured. Skip items that are routine, obvious, or just restating existing conventions.

NEVER EXTRACT BOOKKEEPING. Git and the work tracker already hold these facts, more reliably and with less drift, so a memory of them is a stale copy with a triage cost. Do not emit an item whose substance is any of: a commit sha or what landed at it; a merge event ("X merged to main"); a version bump, release, or publish record; or a record that something was filed, deferred, or tracked as an action ("filed as act:1234abcd"). If a session ALSO produced a durable lesson while doing one of those things, extract the lesson and leave the event out of it.

DERIVABLE: a fact is derivable when it can be recomputed on demand from a live source rather than remembered — a memory of a derivable fact is a stale cache that goes wrong the moment reality moves on. Examples: "prod is at commit 35a5d15" (recompute: check the deploy log or git log on the remote), "the census run already executed" (recompute: check the run log or the action's status in pib-db), "invoice #1's boundary is 95.25h through July 12" (recompute: re-run the timelog query). When an item is derivable, set "derivable": true and "derivation" to a short instruction for HOW to recompute it (a command, a query, a file to check), and set "home" to "derivation". NEVER omit a derivable item from the output — it is always included, just routed to its derivation instead of memory.

PERISHABLE: a fact can be non-derivable and STILL not be durable — true at the moment of this session and destined to become false as work proceeds, with no live source to recompute it from. Examples: "phase 7 hasn't started yet" (false the day phase 7 starts), "usage this month is 42 runs" (a count that moves), "the fix is deployed but not yet verified" (a transition state). A perishable fact is a point-in-time snapshot, NOT a lesson or a durable decision — writing it to memory files a sentence that silently goes wrong. The durability test: would this sentence still be true a month from now if nobody touched anything? A durable lesson would; a snapshot would not. When an item is perishable, set "perishable": true and "perishes_when" to a short statement of WHAT event or time makes it false, and set "home" to "session-record". If a fact is BOTH recomputable and perishable (a deploy SHA is both), classify it DERIVABLE — the recomputation instruction is the better record. NEVER omit a perishable item from the output — it is always included, just routed to the session record instead of memory.

SUBJECT: for every item, set "subject" to the actual project, tool, or system the knowledge is ABOUT. This is usually the same as the project you're working in, but flag it when it differs — a lesson about Claude Code itself learned while doing client work, or a lesson about a third-party API or service, belongs to THAT subject, not the filing project.

UNCLASSIFIABLE: if an item is clearly worth surfacing but does not cleanly fit any type or home above, set "type" to "unclassifiable" and "unclassifiable_reason" to a one-line reason why — this makes your uncertainty visible instead of forcing a confident wrong label. Still include "title" and "content"; "home" and "derivable" are not required for an unclassifiable item.

NOVELTY: an "ALREADY SAVED TO MEMORY" list may appear at the very end of the input. Use it as novelty context: OMIT an item ONLY when a saved title clearly and substantially covers the SAME specific knowledge. DEFAULT TO INCLUDING — when in doubt whether a saved title covers it, INCLUDE the item (re-proposing a near-duplicate is cheap; losing a novel lesson is not). For every item you DO output, set "covered_by" to the exact saved title that most covers it, or "none" if no saved title covers it.

RELATION TO PENDING ITEMS: a "PENDING IN INBOX" list may also appear at the end of the input — these are drafts from earlier sessions still awaiting human triage, NOT yet believed or saved. If an item you output clearly relates to one of them, set "relates_to" to that pending item's exact title and "relation" to "pairs_with" (same topic, worth reading together), "answers" (this item answers a question the pending one raised), or "contradicts" (this item conflicts with what the pending one says). If no pending item relates, set "relates_to" to "none" and "relation" to "none". These are a SEPARATE judgment from "covered_by" — NEVER put a pending title in "covered_by" (covered_by is for MEMORY titles only; a pending item is not yet saved, so naming it there would wrongly suppress the item you're filing right now). And NEVER omit an item, or lower its urgency, on account of a pending item — relate to it, that is all; suppression on account of a pending draft is not something the system can see or undo.

For each item, assess how time-sensitive routing is. Urgency means HOW FAST THE VALUE DECAYS if not routed — it is NOT importance:
- "urgent" = the value evaporates within days if not routed (a trigger condition about to fire, a constraint someone will trip over THIS WEEK, a decision another active session needs right now). Apply the time-decay test: "if this sits in the inbox for a week, is most of its value gone?" If no, it is not urgent.
- "normal" = worth routing but the value keeps (most decisions and constraints)
- "low" = interesting but can wait indefinitely

Lessons and preferences are almost NEVER urgent, no matter how important they are — an important-but-durable item is "normal", not "urgent".

Output JSON array: [{"type":"decision|constraint|lesson|preference|unclassifiable","home":"memory|claude-md|pib-db-trigger|upstream-feedback|derivation|session-record","urgency":"urgent|normal|low","title":"short title","content":"detailed description","covered_by":"exact already-saved title that covers this, or \\"none\\"","subject":"the actual project, tool, or system this is about","derivable":false,"derivation":null,"perishable":false,"perishes_when":null,"unclassifiable_reason":null,"relates_to":"exact pending title that this relates to, or \\"none\\"","relation":"pairs_with|answers|contradicts|none"}]
Output ONLY the JSON array, no other text. If nothing found, output [].`;

  // M1b injection: show the model what's already saved so it self-filters dupes
  // in this one pass. DEFAULT-KEEP — the wording leans toward inclusion. Empty
  // nearby set ⇒ no block ⇒ the prompt degrades to blind extraction (fail-open).
  const savedBlock = nearbyTitles.length
    ? `\n\nALREADY SAVED TO MEMORY (do NOT re-propose anything one of these substantially and specifically covers; when unsure, INCLUDE the item):\n${nearbyTitles.map(t => `- ${t}`).join('\n')}`
    : '';

  // Reach 1 injection (act:5182beda): show the model the pending inbox so it
  // can propose a relation — the first instance of the retrospective-
  // temporality motion (new understanding reaching back to what's still
  // pending). A SEPARATE block from savedBlock, never merged into it — the
  // never-omit-on-a-pending-item instruction above depends on the model
  // being able to tell "saved" from "pending" apart at a glance.
  const pendingBlock = nearbyPending.length
    ? `\n\nPENDING IN INBOX (not yet saved or believed — candidates for a RELATION only; never a reason to omit or demote anything):\n${nearbyPending.map((p) => `- ${p.title}`).join('\n')}`
    : '';

  let extractions = [];
  try {
    if (compressed.length > SINGLE_CALL_TRANSCRIPT_BUDGET) {
      // M2 Phase B: the compressed transcript exceeds one call — chunk it into
      // overlapping windows so nothing in the middle is dropped, extract each,
      // merge with a strict intra-session matcher. Per-chunk isolation: a bad
      // window logs and contributes nothing; the others still file (fail-open).
      const { windows, capped, dropped } = chunkWithOverlap(
        compressed, SINGLE_CALL_TRANSCRIPT_BUDGET, PHASE_B_OVERLAP, MAX_EXTRACTION_CHUNKS);
      log(`Phase 2d: M2-B — compressed ${compressed.length} chars > budget; chunk-and-merge over ${windows.length} overlapping window(s)`);
      if (capped) {
        log(`Phase 2d: M2-B — MAX_CHUNKS cap hit; oldest ${dropped} window(s) NOT extracted (most-recent kept)`);
      }
      const perChunk = [];
      for (const window of windows) {
        try {
          perChunk.push(await runExtractionCall(callFn, systemPrompt, `${window}${savedBlock}${pendingBlock}`));
        } catch (e) {
          logError(`Phase 2d: M2-B chunk extraction failed (${e.message}) — continuing with the other windows`);
          perChunk.push([]);
        }
      }
      extractions = mergeChunkExtractions(perChunk);
    } else {
      extractions = await runExtractionCall(callFn, systemPrompt, `${transcript}${savedBlock}${pendingBlock}`);
    }
  } catch (e) {
    logError(`Phase 2d: Claude extraction failed: ${e.message}`);
    return { autoWritten: 0, queued: 0 };
  }

  if (extractions.length === 0) {
    log('Phase 2d: No knowledge extracted');
    return { autoWritten: 0, queued: 0 };
  }
  if (nearbyTitles.length) {
    log(`Phase 2d: M1b — injected ${nearbyTitles.length} nearby saved title(s) for novelty context`);
  }
  if (nearbyPending.length) {
    log(`Phase 2d: Reach 1 — injected ${nearbyPending.length} nearby pending item(s) for relation context`);
  }

  let queued = 0;
  let deduped = 0;
  let rescued = 0;
  let relationsProposed = 0;
  let bookkept = 0;
  let authorityRouted = 0;
  let commitmentDeduped = 0;

  // act:aa554774: the actions /close's commitment sweep could have filed for
  // this session. Loaded once, outside the loop.
  const sessionFiledActions = loadSessionFiledActions(project, sessionStartIso);
  if (sessionFiledActions.length) {
    log(`Phase 2d: commitment dedup — ${sessionFiledActions.length} action(s) filed on this session's date are in scope`);
  }

  for (const item of extractions) {
    const fullTitle = `${item.type}: ${item.title}`;

    // Bookkeeping suppression (act:471941c9 part 1) runs BEFORE dedup: an
    // event git or pib-db already owns is not knowledge, so there is nothing
    // for the dedup corpora to adjudicate. Logged and ledgered under its own
    // corpus name so the rule that fired is attributable and the recall canary
    // can see it. Note this also swallows a commitment stated inside a
    // bookkeeping title ("v0.1.1 merged but not yet reinstalled — reinstall
    // still owed"); that is deliberate, and the reason /close now runs a
    // prospective-commitment sweep of its own (act:aa554774) rather than
    // leaving dated obligations to the extraction path.
    const bk = bookkeepingRule(fullTitle);
    if (bk) {
      bookkept++;
      log(`Phase 2d: suppressed "${fullTitle}" — bookkeeping [${bk.rule}] matched "${bk.matched}" (git/pib-db already own this)`);
      recordSuppression({
        project: project.name, corpus: `bookkeeping:${bk.rule}`,
        suppressed_title: fullTitle, matched_against: bk.matched,
        session_id: sessionId, ts: reprocessTs,
      });
      continue;
    }

    // Commitment dedup (act:aa554774). Scoped to COMMITMENT-SHAPED items only
    // — a pib-db-trigger home, or a title/content that trips the shared
    // detector — because a general "does any action created today share words
    // with this lesson" test would suppress ordinary knowledge. When it fires,
    // the log names the action that already covers it, which is the evidence
    // the acceptance criterion asks for.
    if (sessionFiledActions.length) {
      const commitmentShaped = item.home === 'pib-db-trigger'
        || detectCommitments(`${item.title}\n${item.content || ''}`).length > 0;
      if (commitmentShaped) {
        const filed = findFiledCommitment(item.title, sessionFiledActions);
        if (filed) {
          commitmentDeduped++;
          log(`Phase 2d: suppressed "${fullTitle}" — commitment already filed as ${filed.fid} ("${filed.text}") during this session`);
          recordSuppression({
            project: project.name, corpus: 'commitment-already-filed',
            suppressed_title: fullTitle, matched_against: `${filed.fid}: ${filed.text}`,
            session_id: sessionId, ts: reprocessTs,
          });
          continue;
        }
      }
    }

    const dup = isDuplicate(
      fullTitle, item.content || '', memoryEntries, pendingTitles,
      { ...resolutionTitles, threadCursorLines: cursorLines });
    if (dup) {
      // M1b rescue gate: a lexical flag the model AFFIRMATIVELY judged novel is
      // RESCUED (filed). The model can only rescue, never independently suppress
      // — a missing/non-"none" covered_by leaves isDuplicate's decision intact.
      if (modelRescues(item)) {
        rescued++;
        log(`Phase 2d: rescued "${fullTitle}" — ${dup.corpus} lexical match "${dup.match}" but model judged novel (covered_by: ${item.covered_by})`);
      } else {
        deduped++;
        // One line per suppression — the dismissed matcher is the loosest in
        // the system; over-suppression must be visible and tunable.
        log(`Phase 2d: suppressed "${fullTitle}" — ${dup.corpus} corpus matched "${dup.match}"`);
        recordSuppression({
          project: project.name, corpus: dup.corpus,
          suppressed_title: fullTitle, matched_against: dup.match,
          session_id: sessionId, ts: reprocessTs,
        });
        continue;
      }
    }

    try {
      // Four routing shapes (act:471dd701 N2 + act:a69c21f8 follow-up):
      // unclassifiable (model uncertainty made visible, never a forced
      // home guess), derivable (routed to its recomputation, never
      // written to memory), perishable (a point-in-time snapshot routed
      // to the session record, never written to memory; derivable WINS
      // when both flags are set — the derivation is the better record),
      // and the four pre-existing homes.
      const isUnclassifiable = item.type === 'unclassifiable';
      const isDerivable = item.derivable === true;
      const isPerishable = !isDerivable && item.perishable === true;
      // Authority-path routing (act:471941c9 part 2). A project that has
      // designated one file as THE authority for this knowledge class gets the
      // item routed THERE instead of into memory — filed and triaged exactly as
      // before, only its home and its one option label change. Applies to the
      // memory-bound arm only: a derivable/perishable/unclassifiable item has
      // already been routed away from memory for a stronger reason.
      const authorityPath = (!isUnclassifiable && !isDerivable && !isPerishable
        && (item.home === 'memory' || item.home === 'claude-md'))
        ? resolveAuthorityPath(config, project.name, item.type)
        : null;
      const home = isDerivable ? 'derivation'
        : isPerishable ? 'session-record'
          : authorityPath ? 'authority-file'
            : item.home;
      const isMemory = !isUnclassifiable && !isDerivable && !isPerishable
        && !authorityPath && home === 'memory';

      let options;
      let draftArtifact = null;
      let summary = item.content;
      if (authorityPath) {
        authorityRouted++;
        log(`Phase 2d: routed "${fullTitle}" to authority file ${authorityPath} (type "${item.type}") instead of memory`);
        options = [
          { key: 'route-to-authority', label: `Add to ${authorityPath}` },
          { key: 'write', label: 'Write to memory instead' },
          { key: 'dismiss', label: 'Dismiss' },
        ];
        draftArtifact = `# ${item.title}\n\n${item.content}`;
        summary = `${item.content}\n\nAuthority for "${item.type}" in this project: ${authorityPath}`;
      } else if (isUnclassifiable) {
        options = [
          { key: 'triage', label: 'Needs human triage' },
          { key: 'dismiss', label: 'Dismiss' },
        ];
      } else if (isDerivable) {
        options = [
          { key: 'acknowledge', label: 'Acknowledge — derivable, no write needed' },
          { key: 'dismiss', label: 'Dismiss' },
        ];
        summary = item.derivation ? `${item.content}\n\nDerivation: ${item.derivation}` : item.content;
      } else if (isPerishable) {
        options = [
          { key: 'acknowledge', label: 'Acknowledge — point-in-time snapshot, not durable' },
          { key: 'dismiss', label: 'Dismiss' },
        ];
        summary = item.perishes_when ? `${item.content}\n\nPerishes: ${item.perishes_when}` : item.content;
      } else if (isMemory) {
        options = [
          { key: 'write', label: 'Write to memory' },
          { key: 'edit', label: 'Edit before writing' },
          { key: 'dismiss', label: 'Dismiss' },
        ];
        draftArtifact = `# ${item.title}\n\n${item.content}`;
      } else {
        options = [
          { key: `route-to-${home}`, label: `Write to ${home}` },
          { key: 'dismiss', label: 'Dismiss' },
        ];
      }

      const newId = createItem({
        project: project.name,
        project_path: projectPath,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        category: 'knowledge-extraction',
        urgency: item.urgency || 'normal',
        title: fullTitle,
        summary,
        context_anchor: `session ${sessionId}`,
        evidence: {
          type: item.type,
          ...(isUnclassifiable ? {} : { home }),
          session_id: sessionId,
          ...(item.subject ? { subject: item.subject } : {}),
          ...(isDerivable ? { derivable: true, derivation: item.derivation } : {}),
          ...(isPerishable ? { perishable: true, perishes_when: item.perishes_when } : {}),
          ...(isUnclassifiable ? { unclassifiable_reason: item.unclassifiable_reason } : {}),
          ...(authorityPath ? { authority_path: authorityPath } : {}),
        },
        options,
        draft_artifact: draftArtifact,
        filed_by: 'ring3-close',
        transcript_ref: { path: transcriptPath, line_range: null },
        thread_ids: threadIds,
      });
      queued++;

      // Reach 1 (act:5182beda): the model may propose that THIS fresh item
      // relates to a still-PENDING one shown in pendingBlock. A relation
      // proposal is filed as its OWN inbox item — it never changes anything
      // about the item just created above (no suppression, no home change).
      if (RELATION_VALUES.includes(item.relation)
          && typeof item.relates_to === 'string' && item.relates_to.trim()) {
        const relatedId = pendingByTitle.get(item.relates_to);
        if (relatedId && relatedId !== newId) {
          try {
            fileRelationProposal({
              project, fromId: newId, fromTitle: fullTitle,
              toId: relatedId, toTitle: item.relates_to,
              relation: item.relation, sessionId, threadIds,
            });
            relationsProposed++;
          } catch (e) {
            logError(`Phase 2d: Reach 1 relation proposal failed for "${fullTitle}" (${e.message})`);
          }
        }
      }
    } catch (e) {
      logError(`Phase 2d: Failed to queue extraction: ${e.message}`);
    }
  }

  if (bookkept > 0) log(`Phase 2d: ${bookkept} extraction(s) suppressed as bookkeeping (git/pib-db already own them)`);
  if (commitmentDeduped > 0) log(`Phase 2d: ${commitmentDeduped} commitment(s) suppressed — already filed as actions during this session`);
  if (authorityRouted > 0) log(`Phase 2d: ${authorityRouted} extraction(s) routed to a declared authority file instead of memory`);
  if (deduped > 0) log(`Phase 2d: ${deduped} extractions skipped (already in memory or inbox)`);
  if (rescued > 0) log(`Phase 2d: ${rescued} extraction(s) rescued from a lexical match (model judged novel)`);
  if (relationsProposed > 0) log(`Phase 2d: Reach 1 — ${relationsProposed} relation(s) proposed against the pending inbox`);
  log(`Phase 2d: ${queued} extractions queued for review`);
  return { autoWritten: 0, queued };
}

// ---------------------------------------------------------------------------
// Phase 2e: Audit pattern capture
// ---------------------------------------------------------------------------

// Exported so the hermetic suite can assert the exclusions survive edits
// (act:09184ad7). Ring 2's shape gate (patternHasRequiredShape in
// watchtower-ring2.mjs) enforces the Evidence/Gap half mechanically; the
// three exclusions below are prompt-only — the 2026-07-26 consolidation
// found all three classes filed as pattern promotions (no-gap positives,
// truncated-transcript disclaimers, document echoes), and without the test
// they are one refactor away from silently vanishing.
export const QUALITY_PATTERN_SYSTEM_PROMPT = `You are analyzing a Claude Code session transcript for recurring quality patterns — issues, gaps, friction, or anti-patterns that surface during any kind of work (coding, debugging, planning, auditing, reviewing). Identify patterns worth learning from: things that keep going wrong, systematic gaps, workflow friction, or quality issues that a team member should watch for in future sessions.

Only report PROBLEMATIC patterns:
- Do NOT emit a section for behavior that was correct — a "no gap" observation is not a pattern.
- Do NOT emit observations about the transcript artifact itself (truncated, compressed, or cut off mid-record); those describe the recording, not the work.
- Do NOT reproduce document content the session merely read or wrote (status tables, wave ledgers, dependency graphs, checklists, setup recipes) as a pattern — a pattern is a recurring behavioral tendency, not a document excerpt.

Output as markdown with ## headers for each pattern found. Include **Evidence:** (what you observed) and **Gap:** (what's missing or broken) for each — sections missing either block are discarded downstream. If no meaningful patterns, output "No recurring patterns detected."`;

async function qualityPatternCapture(compressed, projectPath) {
  log('Phase 2e: Quality pattern capture');

  if (!projectPath) {
    log('Phase 2e: no project path (unresolved identity), skipping');
    return;
  }

  // Read triage history if available (enriches pattern detection for audit sessions)
  const triageHistoryPath = join(projectPath, '.claude', 'audit', 'triage-history.json');
  let triageHistory = null;
  if (existsSync(triageHistoryPath)) {
    try {
      triageHistory = readFileSync(triageHistoryPath, 'utf8');
    } catch {
      // best effort
    }
  }

  const systemPrompt = QUALITY_PATTERN_SYSTEM_PROMPT;

  const userMessage = triageHistory
    ? `Session transcript:\n${recentSlice(compressed, 30000)}\n\nAudit triage history:\n${triageHistory.slice(0, 10000)}`
    : `Session transcript:\n${recentSlice(compressed, 30000)}`;

  try {
    const response = await claudeCall(systemPrompt, userMessage);

    if (response.includes('No recurring patterns detected')) {
      log('Phase 2e: No quality patterns detected');
      return;
    }

    // Write patterns to watchtower state (append-only log, read by Ring 2 for routing)
    const patternsPath = join(WATCHTOWER_DIR, 'state', 'audit-patterns.md');
    const dateStr = new Date().toISOString().slice(0, 10);
    const content = existsSync(patternsPath)
      ? readFileSync(patternsPath, 'utf8') + `\n\n---\n\n### ${dateStr}\n\n${response.trim()}\n`
      : `# Quality Patterns\n\n### ${dateStr}\n\n${response.trim()}\n`;

    atomicWrite(patternsPath, content);
    log('Phase 2e: Quality patterns written');
  } catch (e) {
    logError(`Phase 2e: Pattern capture failed: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Phase 2f: Methodology capture
// ---------------------------------------------------------------------------

async function methodologyCapture(compressed, project, threadIds = []) {
  const projectPath = project.path;
  log('Phase 2f: Methodology capture');

  // Methodology only counts with a durable artifact verified against the
  // project root — no path, nothing to verify against.
  if (!projectPath) {
    log('Phase 2f: no project path (unresolved identity), skipping');
    return;
  }

  try {
    const response = await claudeCall(
      `You are checking whether a Claude Code session established a NEW reusable methodology — a new skill, convention, workflow pattern, or rule that should be codified for future sessions. Routine work, bug fixes, and using existing patterns do NOT count. Only report genuinely new methodology that was created or established in this session.

VERIFICATION REQUIREMENT: a methodology only counts if the session produced a durable artifact for it — a file that was created or edited to encode the methodology (a SKILL.md, a rules file, a convention doc, a template, or a test/harness file whose header documents the procedure). Merely discussing or following a pattern does not count. Name the artifact path relative to the project root.

If a new methodology was established, output JSON: {"found": true, "title": "short description", "content": "what the methodology is and how to apply it", "artifact_path": "relative/path/to/the/file"}
If nothing new was established, output JSON: {"found": false}
Output ONLY the JSON object.`,
      recentSlice(compressed, 30000),
    );

    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      log('Phase 2f: Could not parse methodology response');
      return;
    }

    const result = JSON.parse(jsonMatch[0]);
    if (!result.found) {
      log('Phase 2f: No new methodology found');
      return;
    }

    // Verification gate (act:3f3f9a31, per decision_methodology_capture_claude_verification):
    // the claimed artifact must actually exist on disk. 11/11 unverified captures
    // were dismissed over 3 weeks — without an artifact this category is pure noise.
    if (!result.artifact_path || typeof result.artifact_path !== 'string') {
      log('Phase 2f: Methodology claimed but no artifact named — skipping (verification gate)');
      return;
    }
    const artifactAbs = join(projectPath, result.artifact_path);
    if (!existsSync(artifactAbs)) {
      log(`Phase 2f: Methodology artifact not found on disk (${result.artifact_path}) — skipping (verification gate)`);
      return;
    }

    createItem({
      project: project.name,
      project_path: projectPath,
      ...(project.unresolved ? { project_unresolved: true } : {}),
      category: 'methodology-capture',
      urgency: 'normal',
      title: `methodology: ${result.title}`,
      summary: result.content,
      context_anchor: `ring3-close methodology scan — artifact: ${result.artifact_path}`,
      evidence: { artifact_path: result.artifact_path },
      filed_by: 'ring3-close',
      thread_ids: threadIds,
    });
    log(`Phase 2f: Methodology captured: ${result.title} (artifact verified: ${result.artifact_path})`);
  } catch (e) {
    logError(`Phase 2f: Methodology capture failed: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Phase 2g: Upstream friction
// ---------------------------------------------------------------------------

// The closed vocabulary of actionable routes (act:ea23b3a5). A friction report
// with no named party who could act on it is not a report, it is a mood: five
// of the 2026-07-26 dismissals were observations like "Claude made iterative
// browser tweaks without writing to file" — true, and addressed to nobody.
//
// This is Phase 2f's verification gate applied to a second lens. That gate
// (act:3f3f9a31) files a methodology capture only when the claimed artifact
// verifiably exists on disk, after 11 of 11 unverified captures were dismissed
// over three weeks. Same shape here: file only when the finding names its route.
export const FRICTION_ROUTES = ['cc-feedback', 'project-tracker', 'config-change'];

export function frictionRouteIsActionable(route) {
  return !!route
    && FRICTION_ROUTES.includes(route.type)
    && typeof route.detail === 'string'
    && route.detail.trim().length > 0;
}

async function upstreamFriction(compressed, project, threadIds = [], { callFn = claudeCall } = {}) {
  const projectPath = project.path;
  log('Phase 2g: Upstream friction');

  const systemPrompt = `You are analyzing a Claude Code session transcript for friction with Claude Code itself (bugs, limitations, confusing behavior, missing features, workarounds). Only report genuine CC friction, not user errors or project-specific issues.

For each finding, decide what can actually be DONE about it, and route it:

- "route": {"type":"cc-feedback","detail":"..."} — a fix belongs in the Claude Cabinet repo. detail names what would change there.
- "route": {"type":"project-tracker","detail":"..."} — a fix belongs in THIS project. detail names the work.
- "route": {"type":"config-change","detail":"..."} — a setting, hook, or doc the operator controls. detail names it.
- "durable": true — no one can fix it (a platform constraint, an environment quirk), but the WORKAROUND is worth remembering next time. Say the workaround in "description".
- Neither — a transient incident (a network blip, a credit limit, a one-off tool error) or a general observation about how Claude behaved. These are NOT upstream friction. Return them with no route and durable:false; they will be counted and dropped.

An observation with no named party who could act on it and nothing durable to remember is not a report. Do not invent a route to make something filable.

Output JSON: [{"title":"short title","description":"what happened","severity":"high|medium|low","route":{"type":"...","detail":"..."}|null,"durable":true|false}]
If NO friction found, output exactly: []

Be conservative. False positives waste time. Output ONLY the JSON array.`;

  try {
    const response = await callFn(systemPrompt, recentSlice(compressed, 40000));
    const jsonMatch = response.match(/\[[\s\S]*\]/);
    if (!jsonMatch) {
      log('Phase 2g: No upstream friction (parse failure)');
      return;
    }

    const frictionItems = JSON.parse(jsonMatch[0]);

    if (frictionItems.length === 0) {
      log('Phase 2g: No upstream friction detected');
      return;
    }

    const tally = { routed: 0, durable: 0, dropped: 0 };

    for (const item of frictionItems) {
      const base = {
        project: project.name,
        project_path: projectPath,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        filed_by: 'ring3-close',
        thread_ids: threadIds,
      };

      if (frictionRouteIsActionable(item.route)) {
        createItem({
          ...base,
          category: 'upstream-friction',
          urgency: item.severity === 'high' ? 'urgent' : 'normal',
          title: item.title,
          summary: `${item.description}\n\nRoute: ${item.route.type} — ${item.route.detail}`,
          context_anchor: 'ring3-close friction scan',
          evidence: { severity: item.severity, route: item.route },
        });
        tally.routed++;
        continue;
      }

      // The third arm, and it comes from the data: of the five friction items
      // the operator KEPT, four exited as `captured-to-memory` — platform
      // constraints with a durable workaround ("Claude-in-Chrome cannot connect
      // while Claude Desktop is running"). Those have no upstream route and
      // never will, and dropping them would discard the useful half of this
      // lens. They are knowledge, so they go where knowledge goes.
      if (item.durable === true) {
        createItem({
          ...base,
          category: 'knowledge-extraction',
          urgency: 'low',
          title: item.title,
          summary: item.description,
          context_anchor: 'ring3-close friction scan (durable constraint, no upstream route)',
          evidence: { type: 'constraint', home: 'memory', severity: item.severity, source: 'friction-lens' },
        });
        tally.durable++;
        continue;
      }

      tally.dropped++;
    }

    // All three counts, always. Silence here would be indistinguishable from
    // the lens never running — and the drop count is the number this change
    // exists to move, so it must be observable.
    log(`Phase 2g: friction — ${tally.routed} routed, ${tally.durable} durable→knowledge, `
      + `${tally.dropped} dropped (no route, nothing durable)`);
  } catch (e) {
    logError(`Phase 2g: Friction detection failed: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Phase 2m: Session advisor pass (act:aded4fc9)
// ---------------------------------------------------------------------------
//
// The standing session advisors (historian, system-advocate, user-advocate,
// anthropic-insider) were re-homed from /orient + /debrief standing mandates
// to this transcript-fed close pass (plus /briefing's live panel on the
// start side — the two halves of the operator's keep-the-whole-roster
// ruling). Roster discovery is INDEX-DRIVEN, never hardcoded: any member
// whose standing-mandate includes `session-close` AND declares a
// `directives.session-close` runs here, so consumer projects can re-home or
// extend the roster via directives-project.yaml without touching this
// script. Findings file as `advisor-finding` inbox items; /briefing
// re-surfaces fresh ones at the top of the owning project's chunk (a Step
// 3b candidate class — never a permanent section). Cost control: reuses
// this run's pinned-sonnet claudeCall — no Claude Code spawn, no extra
// process; silence is the expected common case.

const ADVISOR_MAX_FINDINGS = 2;          // per member per session
const ADVISOR_TRANSCRIPT_SLICE = SINGLE_CALL_TRANSCRIPT_BUDGET; // M2: full recent transcript (was 40_000 front-slice)
const ADVISOR_SKILL_SLICE = 8_000;       // chars of member identity

function discoverSessionAdvisors(projectPath) {
  if (!projectPath) {
    return { advisors: [], reason: 'no project path (unresolved identity)' };
  }
  const indexPath = join(projectPath, '.claude', 'skills', '_index.json');
  if (!existsSync(indexPath)) {
    return { advisors: [], reason: 'no skills index' };
  }
  let index;
  try {
    index = JSON.parse(readFileSync(indexPath, 'utf8'));
  } catch (e) {
    return { advisors: [], reason: `unparseable skills index (${e.message})` };
  }
  // Index shape: a JSON object with a top-level `skills` array.
  const skills = Array.isArray(index?.skills) ? index.skills : [];
  const advisors = [];
  for (const entry of skills) {
    const mandate = entry.standingMandate;
    if (!Array.isArray(mandate) || !mandate.includes('session-close')) continue;
    const directive = entry.directives?.['session-close'];
    if (!directive || typeof directive !== 'string' || !directive.trim()) {
      // A mandate without a directive is a data error — visible, not fatal.
      logError(`Phase 2m: ${entry.name} has a session-close mandate but no directives.session-close — skipping (data error)`);
      continue;
    }
    advisors.push({ name: entry.name, path: entry.path, directive });
  }
  return { advisors, reason: null };
}

function parseAdvisorFindings(text) {
  const jsonMatch = String(text || '').match(/\[[\s\S]*\]/);
  if (!jsonMatch) return [];
  let parsed;
  try {
    parsed = JSON.parse(jsonMatch[0]);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  return parsed
    .filter(f => f && typeof f.title === 'string' && f.title.trim())
    .slice(0, ADVISOR_MAX_FINDINGS)
    .map(f => ({
      title: f.title.trim(),
      summary: typeof f.summary === 'string' ? f.summary : '',
      urgency: ['urgent', 'normal', 'low'].includes(f.urgency) ? f.urgency : 'normal',
    }));
}

// callFn is injectable for hermetic tests — production callers omit it and
// get this run's pinned-sonnet claudeCall.
async function advisorPass(compressed, project, sessionId, threadIds = [],
  { callFn = claudeCall } = {}) {
  log('Phase 2m: Session advisor pass');

  const { advisors, reason } = discoverSessionAdvisors(project.path);
  if (advisors.length === 0) {
    log(`Phase 2m: No session-close advisors (${reason || 'none declared'}) — skipping`);
    return { filed: 0 };
  }
  log(`Phase 2m: Roster — ${advisors.map(a => a.name).join(', ')}`);

  // Dedup corpora — same suppression machinery as Phase 2d; builders fail
  // open (an advisor finding re-filing once beats losing the whole pass).
  let pendingTitles = [];
  try {
    pendingTitles = listPending({ project: project.name })
      .map(p => p.title).filter(t => typeof t === 'string' && t.trim());
  } catch (e) {
    logError(`Phase 2m: pending corpus failed (${e.message}) — continuing without it`);
  }
  let resolutionTitles = { resolvedTitles: [], dismissedTitles: [] };
  try {
    resolutionTitles = resolutionCorpus(project.name);
  } catch (e) {
    logError(`Phase 2m: resolution corpus failed (${e.message}) — continuing without it`);
  }

  const transcriptSlice = recentSlice(compressed, ADVISOR_TRANSCRIPT_SLICE);

  const results = await Promise.all(advisors.map(async (advisor) => {
    try {
      const skillPath = join(project.path, advisor.path);
      const skillBody = existsSync(skillPath)
        ? readFileSync(skillPath, 'utf8').slice(0, ADVISOR_SKILL_SLICE)
        : '';
      const systemPrompt = `You are the cabinet member "${advisor.name}", running your AUTOMATIC SESSION-CLOSE pass over a finished Claude Code session transcript. Your identity:

${skillBody}

Your session-close directive: ${advisor.directive}

Review the transcript through that directive ONLY — do not free-range into other domains. File at most ${ADVISOR_MAX_FINDINGS} findings, and only things that genuinely meet the bar: a finding must be worth the operator's attention at their next briefing, not commentary on the session. SILENCE IS FINE and is the expected common case.

Output a JSON array: [{"title":"short title","summary":"what you observed and why it matters","urgency":"urgent|normal|low"}]
If nothing meets the bar, output exactly: []
Output ONLY the JSON array, no other text.`;

      const response = await callFn(systemPrompt, transcriptSlice);
      return { advisor, findings: parseAdvisorFindings(response) };
    } catch (e) {
      logError(`Phase 2m: ${advisor.name} pass failed: ${e.message}`);
      return { advisor, findings: [] };
    }
  }));

  let filed = 0;
  let suppressed = 0;
  for (const { advisor, findings } of results) {
    const shortName = advisor.name.replace(/^cabinet-/, '');
    for (const f of findings) {
      const fullTitle = `${shortName}: ${f.title}`;
      const dup = isDuplicate(fullTitle, f.summary, [], pendingTitles, resolutionTitles);
      if (dup) {
        suppressed++;
        log(`Phase 2m: suppressed "${fullTitle}" — ${dup.corpus} corpus matched "${dup.match}"`);
        recordSuppression({
          project: project.name, corpus: dup.corpus,
          suppressed_title: fullTitle, matched_against: dup.match,
          session_id: sessionId, ts: reprocessTs,
        });
        continue;
      }
      try {
        createItem({
          project: project.name,
          project_path: project.path,
          ...(project.unresolved ? { project_unresolved: true } : {}),
          category: 'advisor-finding',
          urgency: f.urgency,
          title: fullTitle,
          summary: f.summary,
          context_anchor: `session ${sessionId} — ${advisor.name} close pass`,
          evidence: {
            member: advisor.name,
            directive_key: 'session-close',
            session_id: sessionId,
          },
          filed_by: 'ring3-close',
          thread_ids: threadIds,
        });
        filed++;
      } catch (e) {
        logError(`Phase 2m: Failed to file finding from ${advisor.name}: ${e.message}`);
      }
    }
  }

  log(`Phase 2m: ${filed} advisor finding(s) filed${suppressed ? ` (${suppressed} suppressed as duplicates)` : ''}`);
  return { filed };
}

// ---------------------------------------------------------------------------
// Session-close extraction lenses (act:4ff2cfb3) — three additions to the
// transcript pass, decided in the 2026-06-12 ledger walkthrough. Each is the
// SAME shape as the existing lenses (Phase 2d / Phase 2m): one pinned-sonnet
// call, structured-JSON output, the shared dedup corpora, a per-lens cap, and
// one log line per suppression. callFn is injectable for hermetic tests
// (production omits it → this run's claudeCall).
//
//   2n: Raised-but-unhandled — anything the session RAISED but neither did
//       nor filed (passing promises, side-issues, hanging questions) → inbox.
//   2o: Skill-candidate — a manual procedure repeated by hand that a skill
//       would automate → inbox. The skill-discovery extension of the
//       knowledge-extraction concern (Phase 2d's sibling).
//   2p: Checklist-catch — a surfaced change-impact check that caught a real
//       bug → recorded to checklist-stats.json (the catch-recording side of
//       /debrief's checklist-feedback phase, which dies with debrief; feeds
//       the audit pruning loop).
// ---------------------------------------------------------------------------

const RAISED_UNHANDLED_MAX = 5;   // loose-end items filed per session
const SKILL_CANDIDATE_MAX = 3;    // skill candidates filed per session
const CHECKLIST_CATCH_MAX = 5;    // catches recorded per session
const LENS_TRANSCRIPT_SLICE = SINGLE_CALL_TRANSCRIPT_BUDGET; // M2: full recent transcript (was 50_000 front-slice)

// parseLensFindings — shared structured-output parser for the inbox lenses.
// Extracts the first JSON array from the model text, keeps only entries with
// a non-empty title, normalizes urgency, caps the count. `extraKeys` carries
// through lens-specific string fields (e.g. kind, repetition) onto evidence.
function parseLensFindings(text, { cap, extraKeys = [] } = {}) {
  const jsonMatch = String(text || '').match(/\[[\s\S]*\]/);
  if (!jsonMatch) return [];
  let parsed;
  try {
    parsed = JSON.parse(jsonMatch[0]);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  return parsed
    .filter(f => f && typeof f.title === 'string' && f.title.trim())
    .slice(0, cap)
    .map(f => {
      const out = {
        title: f.title.trim(),
        summary: typeof f.summary === 'string' ? f.summary : '',
        urgency: ['urgent', 'normal', 'low'].includes(f.urgency) ? f.urgency : 'normal',
      };
      for (const k of extraKeys) {
        if (typeof f[k] === 'string' && f[k].trim()) out[k] = f[k].trim();
      }
      return out;
    });
}

// ---------------------------------------------------------------------------
// Phase 2n: Raised-but-unhandled lens
// ---------------------------------------------------------------------------
//
// Operator framing (2026-06-12): "anything that came up that's unhandled,"
// NOT just other-project strays. A promise made in passing ("I'll also wire
// X"), a side-issue noticed and set down, a question asked and never answered
// — if the session ends with it neither done nor filed, it evaporates. This
// lens catches it as a low-ceremony inbox item the operator can promote to an
// action or dismiss. It runs AFTER Phase 2c/2d/2m so its pending corpus
// already contains this session's completion candidates, extractions, and
// advisor findings — a loose end one of those already captured is suppressed.
async function raisedUnhandledLens(compressed, project, sessionId, transcriptPath,
  threadIds = [], { callFn = claudeCall } = {}) {
  log('Phase 2n: Raised-but-unhandled lens');

  const systemPrompt = `You are scanning a finished Claude Code session transcript for loose ends: things that were RAISED during the session but were neither completed nor recorded anywhere before it ended. Three kinds:

- "promise" — something the assistant or operator said would be done ("I'll also...", "we should later...", "next we need to...") that never happened this session.
- "side-issue" — a problem or risk noticed in passing and set aside without being fixed or filed.
- "open-question" — a question raised that was never answered or resolved.

ONLY surface items that would be LOST if not captured — there is no action, no inbox item, no commit, and no note recording them. Do NOT surface:
- work that WAS completed (that is not a loose end),
- durable lessons/decisions/constraints (a separate lens already captures those),
- "is this action done?" candidates (a separate lens already captures those),
- routine next-steps that are obvious from the work itself.

Be conservative. SILENCE IS FINE and is the expected common case — most sessions tie off their own loose ends.

Output a JSON array, at most ${RAISED_UNHANDLED_MAX} items: [{"title":"short imperative title","summary":"what was raised and why it would be lost","kind":"promise|side-issue|open-question","urgency":"urgent|normal|low"}]
Urgency is value-decay speed, not importance: "urgent" only if the loose end loses its value within days. Output ONLY the JSON array. If nothing qualifies, output exactly: [].`;

  let findings = [];
  try {
    const response = await callFn(systemPrompt, recentSlice(compressed, LENS_TRANSCRIPT_SLICE));
    findings = parseLensFindings(response, { cap: RAISED_UNHANDLED_MAX, extraKeys: ['kind'] });
  } catch (e) {
    logError(`Phase 2n: Claude scan failed: ${e.message}`);
    return { queued: 0 };
  }
  if (findings.length === 0) {
    log('Phase 2n: No unhandled items raised');
    return { queued: 0 };
  }

  const { memoryEntries, threadCursorLines: cursorLines, pendingTitles, resolutionTitles } =
    buildExtractionCorpora(project, { phase: 'Phase 2n' });

  let queued = 0;
  let suppressed = 0;
  for (const f of findings) {
    const fullTitle = `unhandled: ${f.title}`;
    const dup = isDuplicate(fullTitle, f.summary, memoryEntries, pendingTitles,
      { ...resolutionTitles, threadCursorLines: cursorLines });
    if (dup) {
      suppressed++;
      log(`Phase 2n: suppressed "${fullTitle}" — ${dup.corpus} corpus matched "${dup.match}"`);
      recordSuppression({
        project: project.name, corpus: dup.corpus,
        suppressed_title: fullTitle, matched_against: dup.match,
        session_id: sessionId, ts: reprocessTs,
      });
      continue;
    }
    try {
      createItem({
        project: project.name,
        project_path: project.path,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        category: 'raised-unhandled',
        urgency: f.urgency,
        title: fullTitle,
        summary: f.summary,
        context_anchor: `session ${sessionId}`,
        evidence: {
          kind: f.kind || 'side-issue',
          session_id: sessionId,
        },
        options: [
          { key: 'create-action', label: 'Create an action to handle it' },
          { key: 'keep', label: 'Keep as a reminder' },
          { key: 'dismiss', label: 'Dismiss (already handled or not worth it)' },
        ],
        filed_by: 'ring3-close',
        transcript_ref: { path: transcriptPath, line_range: null },
        thread_ids: threadIds,
      });
      queued++;
    } catch (e) {
      logError(`Phase 2n: Failed to queue loose end: ${e.message}`);
    }
  }

  log(`Phase 2n: ${queued} loose end(s) queued${suppressed ? ` (${suppressed} suppressed as duplicates)` : ''}`);
  return { queued };
}

// ---------------------------------------------------------------------------
// Phase 2o: Skill-candidate lens
// ---------------------------------------------------------------------------
//
// The skill-discovery extension of the knowledge-extraction concern (Phase
// 2d's sibling): when the session shows the operator performing the SAME
// multi-step manual procedure more than once by hand, a Claude Code skill or
// slash-command would have automated it. Surface it as a "should this become
// a skill?" inbox item. Same dedup discipline as every other lens.
async function skillCandidateLens(compressed, project, sessionId, transcriptPath,
  threadIds = [], { callFn = claudeCall } = {}) {
  log('Phase 2o: Skill-candidate lens');

  const systemPrompt = `You are scanning a finished Claude Code session transcript for REPEATED MANUAL PROCEDURES that should become a reusable Claude Code skill (slash-command). Look for a multi-step sequence the operator or assistant performed BY HAND two or more times, or an ad-hoc procedure clearly done routinely, where a skill would have automated it.

A real candidate has:
- a repeated, multi-step shape (not a one-off, not a single command),
- a clear name for what the procedure accomplishes,
- evidence in THIS transcript that it recurred or is recurring manual toil.

Do NOT surface:
- procedures already covered by an existing skill (if a skill was invoked, it is not a candidate),
- one-time sequences with no sign of repetition,
- generic advice ("you could write a script") with no concrete repeated procedure observed.

Be conservative. SILENCE IS FINE and is the expected common case.

Output a JSON array, at most ${SKILL_CANDIDATE_MAX} items: [{"title":"the procedure as a skill name","summary":"what the procedure does and why a skill fits","repetition":"the evidence it recurred this session","urgency":"urgent|normal|low"}]
Skill candidates are durable — they are almost never urgent. Output ONLY the JSON array. If nothing qualifies, output exactly: [].`;

  let findings = [];
  try {
    const response = await callFn(systemPrompt, recentSlice(compressed, LENS_TRANSCRIPT_SLICE));
    findings = parseLensFindings(response, { cap: SKILL_CANDIDATE_MAX, extraKeys: ['repetition'] });
  } catch (e) {
    logError(`Phase 2o: Claude scan failed: ${e.message}`);
    return { queued: 0 };
  }
  if (findings.length === 0) {
    log('Phase 2o: No skill candidates detected');
    return { queued: 0 };
  }

  const { memoryEntries, threadCursorLines: cursorLines, pendingTitles, resolutionTitles } =
    buildExtractionCorpora(project, { phase: 'Phase 2o' });

  let queued = 0;
  let suppressed = 0;
  for (const f of findings) {
    const fullTitle = `skill-candidate: ${f.title}`;
    const dup = isDuplicate(fullTitle, f.summary, memoryEntries, pendingTitles,
      { ...resolutionTitles, threadCursorLines: cursorLines });
    if (dup) {
      suppressed++;
      log(`Phase 2o: suppressed "${fullTitle}" — ${dup.corpus} corpus matched "${dup.match}"`);
      recordSuppression({
        project: project.name, corpus: dup.corpus,
        suppressed_title: fullTitle, matched_against: dup.match,
        session_id: sessionId, ts: reprocessTs,
      });
      continue;
    }
    try {
      createItem({
        project: project.name,
        project_path: project.path,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        category: 'skill-candidate',
        urgency: f.urgency,
        title: fullTitle,
        summary: f.repetition ? `${f.summary}\n\nObserved repetition: ${f.repetition}` : f.summary,
        context_anchor: `session ${sessionId}`,
        evidence: {
          repetition: f.repetition || '',
          session_id: sessionId,
        },
        options: [
          { key: 'draft-skill', label: 'Draft a skill for it' },
          { key: 'dismiss', label: 'Dismiss (not worth a skill)' },
        ],
        filed_by: 'ring3-close',
        transcript_ref: { path: transcriptPath, line_range: null },
        thread_ids: threadIds,
      });
      queued++;
    } catch (e) {
      logError(`Phase 2o: Failed to queue skill candidate: ${e.message}`);
    }
  }

  log(`Phase 2o: ${queued} skill candidate(s) queued${suppressed ? ` (${suppressed} suppressed as duplicates)` : ''}`);
  return { queued };
}

// ---------------------------------------------------------------------------
// Phase 2p: Checklist-catch detection
// ---------------------------------------------------------------------------
//
// The catch-recording side of /debrief's checklist-feedback phase, re-homed
// to the background close pass (the interactive sharpening side cannot move
// to a ring — it needs operator approval — but the automatic catch tally can,
// and must, since debrief is being retired). When the transcript shows a
// surfaced change-impact check actually caught a real bug, append it to the
// dimension's `catches` in checklist-stats.json. Those catches are the
// evidence the audit checklist-pruning phase weighs to keep a dimension alive.
//
// Gating: silent no-op unless the project opted into the checklist
// (qa-dimensions.yaml present). Fail-open everywhere — losing a data point is
// fine; blocking the close pass over bookkeeping is not.

// extractDimensionNames — pull the top-level dimension keys out of
// qa-dimensions.yaml WITHOUT a YAML dependency (the close pass runs in a bare
// Node process; minimal-dependency footprint is intentional). The keys are
// the 2-space-indented `name:` lines under the top-level `dimensions:` map.
// Comments and deeper-nested keys (paths/severity/checks and list entries)
// are excluded. Returns [] if the file has no parseable dimensions block.
function extractDimensionNames(yamlText) {
  const lines = String(yamlText || '').split('\n');
  let inDimensions = false;
  const names = [];
  for (const raw of lines) {
    const line = raw.replace(/\r$/, '');
    const noComment = line.replace(/\s+#.*$/, '');
    if (!noComment.trim()) continue;
    // Top-level key (no leading indent).
    const topLevel = /^(\S[^:]*):\s*$/.exec(noComment);
    if (topLevel) {
      inDimensions = topLevel[1].trim() === 'dimensions';
      continue;
    }
    if (!inDimensions) continue;
    // A dimension name is a key indented exactly two spaces with nothing but
    // the colon after it (its paths/severity/checks nest four spaces deeper).
    const dim = /^ {2}([A-Za-z0-9][\w.-]*):\s*$/.exec(noComment);
    if (dim) names.push(dim[1]);
  }
  return names;
}

// recordChecklistCatches — write the catches into checklist-stats.json per the
// schema's write protocol (bootstrap-if-absent, move-aside-if-corrupt, atomic
// temp+rename). Catches are append-only evidence; we dedup only same-day
// identical entries (dimension+check+note+date) so a re-run can't double-count
// — the schema is explicit that counts are honest, not precise, so no heavier
// dedup. Returns the number of catches actually appended.
function recordChecklistCatches(projectPath, catches, { date } = {}) {
  const today = date || new Date().toISOString().slice(0, 10);
  const statsPath = join(projectPath, '.claude', 'cabinet', 'checklist-stats.json');

  let stats;
  if (existsSync(statsPath)) {
    try {
      stats = JSON.parse(readFileSync(statsPath, 'utf8'));
    } catch {
      // Unparseable — move aside (never delete), bootstrap fresh.
      try {
        renameSync(statsPath, `${statsPath}.corrupt-${today}`);
        logError(`Phase 2p: checklist-stats.json unparseable — moved aside to checklist-stats.json.corrupt-${today}`);
      } catch (e) {
        logError(`Phase 2p: checklist-stats.json unparseable and could not be moved aside (${e.message}) — skipping`);
        return 0;
      }
      stats = null;
    }
  }
  if (!stats || typeof stats !== 'object') {
    stats = { schema_version: 1, runs: 0, dimensions: {}, pruning_reviews: [] };
  }
  if (!stats.dimensions || typeof stats.dimensions !== 'object') stats.dimensions = {};

  let appended = 0;
  for (const c of catches) {
    const dim = stats.dimensions[c.dimension]
      || (stats.dimensions[c.dimension] = { fires: 0, last_fired: null, catches: [] });
    if (!Array.isArray(dim.catches)) dim.catches = [];
    const already = dim.catches.some(
      e => e && e.date === today && e.check === c.check && e.note === c.note);
    if (already) continue;
    dim.catches.push({ date: today, check: c.check, note: c.note });
    appended++;
  }

  if (appended === 0) return 0;
  try {
    atomicWrite(statsPath, stats);
  } catch (e) {
    logError(`Phase 2p: checklist-stats.json write failed (${e.message}) — catch evidence lost, continuing`);
    return 0;
  }
  return appended;
}

async function checklistCatchLens(compressed, project, sessionId, { callFn = claudeCall, date } = {}) {
  log('Phase 2p: Checklist-catch detection');

  if (!project.path) {
    log('Phase 2p: no project path (unresolved identity), skipping');
    return { recorded: 0 };
  }

  const yamlPath = join(project.path, '.claude', 'cabinet', 'qa-dimensions.yaml');
  if (!existsSync(yamlPath)) {
    log('Phase 2p: No qa-dimensions.yaml (checklist not opted in), skipping');
    return { recorded: 0 };
  }
  let dimensionNames = [];
  try {
    dimensionNames = extractDimensionNames(readFileSync(yamlPath, 'utf8'));
  } catch (e) {
    logError(`Phase 2p: could not read qa-dimensions.yaml (${e.message}) — skipping`);
    return { recorded: 0 };
  }
  if (dimensionNames.length === 0) {
    logError('Phase 2p: qa-dimensions.yaml has no parseable dimensions — skipping (fail-open)');
    return { recorded: 0 };
  }

  const systemPrompt = `You are inspecting a finished Claude Code session transcript for CHANGE-IMPACT CHECKLIST CATCHES. The project has a change-impact checklist organized into these dimensions:

${dimensionNames.map(n => `- ${n}`).join('\n')}

During the session, the checklist may have surfaced targeted checks (a "## Change-Impact Checklist" section, with [run] / [review] items grouped by dimension). A CATCH is when one of those SURFACED checks led to a real bug being found and fixed this session — the check did its job.

Report ONLY genuine catches: a surfaced check, a real problem it pointed at, and a fix. Do NOT report:
- bugs that slipped through (the check did NOT catch them),
- checks that were surfaced but found nothing,
- general good practices not tied to a surfaced checklist check.

Each catch must name one of the dimensions listed above EXACTLY. SILENCE IS FINE and is the expected common case — most sessions record no catch.

Output a JSON array, at most ${CHECKLIST_CATCH_MAX} catches: [{"dimension":"exact dimension name","check":"the surfaced check text, quoted","note":"what real problem it caught"}]
Output ONLY the JSON array. If there were no catches, output exactly: [].`;

  let raw = [];
  try {
    const response = await callFn(systemPrompt, recentSlice(compressed, LENS_TRANSCRIPT_SLICE));
    const jsonMatch = String(response || '').match(/\[[\s\S]*\]/);
    if (jsonMatch) raw = JSON.parse(jsonMatch[0]);
  } catch (e) {
    logError(`Phase 2p: Claude scan failed: ${e.message}`);
    return { recorded: 0 };
  }
  if (!Array.isArray(raw)) return { recorded: 0 };

  const known = new Set(dimensionNames);
  const catches = raw
    .filter(c => c && typeof c.dimension === 'string' && typeof c.check === 'string' && typeof c.note === 'string')
    .map(c => ({ dimension: c.dimension.trim(), check: c.check.trim(), note: c.note.trim() }))
    .filter(c => c.dimension && c.check && c.note && known.has(c.dimension))
    .slice(0, CHECKLIST_CATCH_MAX);

  if (catches.length === 0) {
    log('Phase 2p: No checklist catches detected');
    return { recorded: 0 };
  }

  const recorded = recordChecklistCatches(project.path, catches, { date });
  log(`Phase 2p: ${recorded} checklist catch(es) recorded to checklist-stats.json`);
  return { recorded };
}

// ---------------------------------------------------------------------------
// Phase 2h (feedback outbox flush) was REMOVED (act:6c3a4763). It marked
// items delivered without delivering them — a silent feedback-loss trap —
// and read a project-local outbox path the real pipeline never writes.
// Delivery is now a Ring 1 mechanical duty: flushFeedbackOutbox() in
// watchtower-lib.mjs reads the GLOBAL ~/.claude/cc-feedback-outbox.json
// and writes real files into the CC repo's feedback/ directory.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 2i: Session auto-naming
// ---------------------------------------------------------------------------

async function sessionAutoNaming(sessionId, summary, config) {
  log('Phase 2i: Session auto-naming');

  // Check if auto-naming is enabled (togglable default)
  const autoNaming = config.ring3?.session_auto_naming !== false;
  if (!autoNaming) {
    log('Phase 2i: Auto-naming disabled');
    return;
  }

  // Only rename UUID-style session names
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!UUID_RE.test(sessionId)) {
    log('Phase 2i: Session already has a custom name, skipping');
    return;
  }

  if (!summary || summary.trim().length === 0) {
    log('Phase 2i: No summary available for naming');
    return;
  }

  const systemPrompt = `Generate a 3-6 word descriptive name for a coding session based on its summary. Use lowercase-kebab-case. Output ONLY the name, nothing else. Examples: "add-auth-middleware", "fix-deploy-pipeline", "refactor-db-queries"`;

  try {
    const response = await claudeCall(systemPrompt, summary);
    const name = response.trim().replace(/[^a-z0-9-]/g, '').slice(0, 50);

    if (name.length < 3) {
      log('Phase 2i: Generated name too short, skipping');
      return;
    }

    // Write the name suggestion to a known location for next session to pick up
    const namingPath = join(WATCHTOWER_DIR, 'state', 'session-names.json');
    let names = {};
    if (existsSync(namingPath)) {
      try {
        names = JSON.parse(readFileSync(namingPath, 'utf8'));
      } catch {
        names = {};
      }
    }
    names[sessionId] = {
      suggested_name: name,
      generated_at: new Date().toISOString(),
    };

    atomicWrite(namingPath, JSON.stringify(names, null, 2) + '\n');
    log(`Phase 2i: Suggested name for ${sessionId}: ${name}`);
  } catch (e) {
    logError(`Phase 2i: Auto-naming failed: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Phase 2j: Consumer hooks
// ---------------------------------------------------------------------------

function runConsumerHooks(config, sessionData) {
  log('Phase 2j: Consumer hooks');

  const hooks = config.hooks?.['ring3-close-post'] || [];
  if (hooks.length === 0) {
    log('Phase 2j: No consumer hooks configured');
    return;
  }

  for (const hookCmd of hooks) {
    try {
      // Run in subshell with timeout, failures logged not fatal
      execSync(hookCmd, {
        encoding: 'utf8',
        timeout: CONSUMER_HOOK_TIMEOUT_MS,
        input: JSON.stringify(sessionData),
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      log(`Phase 2j: Hook succeeded: ${hookCmd}`);
    } catch (e) {
      logError(`Phase 2j: Hook failed (non-fatal): ${hookCmd} — ${e.message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Phase 2k: Signal Ring 2
// ---------------------------------------------------------------------------

function signalRing2() {
  log('Phase 2k: Signal Ring 2');

  const triggerPath = join(WATCHTOWER_DIR, 'lock', 'ring2-fast-trigger');
  mkdirSync(join(WATCHTOWER_DIR, 'lock'), { recursive: true });
  atomicWrite(triggerPath, new Date().toISOString());

  log('Phase 2k: Ring 2 fast-trigger written');
}

// ---------------------------------------------------------------------------
// Phase 2l: Health
// ---------------------------------------------------------------------------

// buildHealth — the pure ring3-health.json shape (exported for tests). Health
// is "success" ONLY when no account-level API failure occurred; a systemic
// failure (billing/auth/quota) flips status to "degraded" and records the
// class so silence can never again read as health. A single per-call error is
// not systemic and does not degrade the run.
export function buildHealth(sessionId, stats, project, apiFailure = null) {
  const health = {
    schema_version: 1,
    last_run: new Date().toISOString(),
    session_id: sessionId,
    items_filed: stats.itemsFiled || 0,
    actions_closed: stats.actionsClosed || 0,
    status: 'success',
  };
  if (apiFailure) {
    health.status = 'degraded';
    health.api_error = { type: apiFailure.type, message: apiFailure.message };
  }
  // Fail loud, never silently: an unresolvable project identity is the
  // anomaly that used to hide behind the basename fallback.
  if (project?.unresolved) {
    health.warnings = [
      `project identity unresolved: filed under "${project.name}" (${project.path || 'no path'}) with project_unresolved`,
    ];
  }
  return health;
}

function writeHealth(sessionId, stats, project, apiFailure = null) {
  log('Phase 2l: Health');
  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring3-health.json');
  const health = buildHealth(sessionId, stats, project, apiFailure);
  atomicWrite(healthPath, JSON.stringify(health, null, 2) + '\n');
  log(`Phase 2l: ring3-health.json written (status: ${health.status})`);
}

// ---------------------------------------------------------------------------
// Processed marker (Step 4) — prevent re-processing
// ---------------------------------------------------------------------------

function isProcessed(sessionId) {
  const markerPath = join(WATCHTOWER_DIR, 'ring3', 'processed', `${sessionId}.json`);
  return existsSync(markerPath);
}

function markProcessed(sessionId, stats) {
  const markerPath = join(WATCHTOWER_DIR, 'ring3', 'processed', `${sessionId}.json`);
  const marker = {
    schema_version: 1,
    session_id: sessionId,
    processed_at: new Date().toISOString(),
    stats,
  };
  atomicWrite(markerPath, JSON.stringify(marker, null, 2) + '\n');
}

// --- Failed-session recovery markers (act:6fb2b7d1) -------------------------
// When Ring 3 runs but captures NOTHING because the API was down, the session
// is left UNMARKED (so it stays reprocessable) and a durable marker is written
// here — the `--reprocess-failed` worklist. Written ONLY when Ring 3 actually
// ran and failed, so it never targets an open/running session.

function failedMarkerPath(sessionId) {
  return join(WATCHTOWER_DIR, 'ring3', 'failed', `${sessionId}.json`);
}

function writeFailedMarker(sessionId, { transcriptPath, cwd, failureType } = {}) {
  const p = failedMarkerPath(sessionId);
  let prior = null;
  try { if (existsSync(p)) prior = JSON.parse(readFileSync(p, 'utf8')); } catch { /* corrupt → treat as first */ }
  const now = new Date().toISOString();
  const marker = {
    schema_version: 1,
    session_id: sessionId,
    // Preserve the original transcript/cwd across re-attempts (a reprocess may
    // pass different values; the FIRST capture is the source of truth).
    transcript_path: prior?.transcript_path || transcriptPath || null,
    cwd: prior?.cwd || cwd || null,
    failure_type: failureType || 'unknown',
    first_failed_at: prior?.first_failed_at || now,
    last_failed_at: now,
    attempts: (prior?.attempts || 0) + 1,
  };
  atomicWrite(p, JSON.stringify(marker, null, 2) + '\n');
  return marker;
}

function clearFailedMarker(sessionId) {
  const p = failedMarkerPath(sessionId);
  try { if (existsSync(p)) rmSync(p); } catch { /* best-effort; a stale marker self-heals on the next drain */ }
}

// ---------------------------------------------------------------------------
// Session attribution — which project does this TRANSCRIPT belong to?
// ---------------------------------------------------------------------------

// Claude Code stores each session's transcript under
// ~/.claude/projects/<slug>/<session>.jsonl, where <slug> is the session's
// own cwd encoded by collapsing every non-alphanumeric character to '-'
// (current era; an older CC era preserved dots — 9 such dirs live on this
// portfolio's disk beside 271 current-era ones). That slug is the ONLY
// attribution evidence that survives a reprocess from a different cwd:
// the 2026-07-12 drain found 302 of 510 reprocessed extractions filed under
// the RUNNER's cwd project because resolveProject(args.cwd || process.cwd())
// trusted whatever cwd the invocation happened to carry (act:29001b07).
//
// normalizeSlugKey is the ONE comparison key for both directions: applied to
// an absolute cwd it reproduces the current-era encoding; applied to a
// recorded slug it is idempotent for current-era slugs and maps older-era
// variants ('.mux' preserved) onto the same key. The encoding is a
// third-party (Claude Code) convention, version-varying and lossy ('-', '.',
// '/' all collapse to '-'); any mismatch fails SAFE — cwd distrusted → slug
// fallback → project_unresolved — never a confident misattribution. The
// decode direction (slug → registered project, needed once the worktree dir
// is cleaned up) lives in watchtower-lib as resolveProjectFromTranscriptSlug
// (Lane A of grp:wt-noise-immunity); encoder and decoder are two halves of
// one convention — consolidation into the lib is the registry follow-up.
export function normalizeSlugKey(s) {
  if (typeof s !== 'string' || !s) return null;
  return s.replace(/[^a-zA-Z0-9]/g, '-');
}

// The transcript's project-dir name, when the transcript actually lives under
// a CC project dir (those always encode an absolute path, so they start with
// '-'). A transcript staged anywhere else yields null — callers then trust
// cwd, which is today's behavior for non-standard invocations.
export function transcriptSlugFromPath(transcriptPath) {
  if (typeof transcriptPath !== 'string' || !transcriptPath) return null;
  const dir = basename(dirname(transcriptPath));
  return dir.startsWith('-') ? dir : null;
}

// resolveSessionProject — the ONE resolution call for both entry points
// (live close and manual reprocess both enter through main()).
//
// Trust order:
//   1. cwd, only when it AGREES with the transcript slug (or no slug exists)
//      → resolveProjectIdentity, the canonical path resolver.
//   2. the transcript slug via the lib's slug resolver — covers a reprocess
//      run from the wrong cwd AND a live close whose worktree was already
//      cleaned up. Feature-detected (see the namespace import) until Lane A's
//      lib half merges; absent ⇒ null ⇒ fall through.
//   3. unresolved — file under the SESSION's evidence (the transcript dir
//      name when cwd was distrusted, else basename(cwd)); never the runner's
//      cwd project. path is null in the slug case: downstream phases that
//      need a project directory skip loudly, and no state-file key is minted
//      from a raw encoded-path name.
//
// Returns { project, cwdTrusted, slug }. cwdTrusted gates the phases that
// read live git state at cwd (Phase 2a worktreeCheck) — a distrusted cwd is
// the RUNNER's environment, not the session's.
export function resolveSessionProject({ cwd, transcriptPath }, config, deps = {}) {
  const resolveIdentity = deps.resolveIdentity || resolveProjectIdentity;
  const resolveSlug = deps.resolveSlug !== undefined
    ? deps.resolveSlug
    : (watchtowerLib.resolveProjectFromTranscriptSlug || null);

  const slug = transcriptSlugFromPath(transcriptPath);
  const cwdTrusted = !!cwd && (!slug || normalizeSlugKey(cwd) === normalizeSlugKey(slug));

  if (cwdTrusted) {
    const identity = resolveIdentity(cwd, config);
    if (identity) return { project: identity, cwdTrusted, slug };
  }

  if (slug && typeof resolveSlug === 'function') {
    try {
      const fromSlug = resolveSlug(slug, config);
      if (fromSlug) return { project: fromSlug, cwdTrusted, slug };
    } catch (e) {
      logError(`slug resolution failed for ${slug}: ${e.message} — falling through to unresolved`);
    }
  }

  if (slug && !cwdTrusted) {
    // The session's own evidence is the transcript dir name. Never cwd here:
    // a distrusted cwd is exactly the misattribution vector this fixes.
    return {
      project: {
        name: slug, path: null, slug: slugify(slug),
        registered: false, unresolved: true,
      },
      cwdTrusted, slug,
    };
  }

  const base = cwd || process.cwd();
  const dirName = basename(base);
  return {
    project: {
      name: dirName, path: cwd ? base : null, slug: slugify(dirName),
      registered: false, unresolved: true,
    },
    cwdTrusted, slug,
  };
}

// ---------------------------------------------------------------------------
// Phase 2q: verify-coverage lens — warns when the session shipped UI changes
// with no matching .feature edit (drift: the product changed, the walkthrough
// scenarios didn't). Diff-based (no API call); ports debrief's verify-coverage
// phase to session close so coverage pressure survives debrief's retirement.
// /verify update stays the executor; the lens is only the trigger. (act:1be47d42)
// ---------------------------------------------------------------------------

// Pure: from the session's changed paths, is it UI-touching but scenario-silent?
export function detectUncoveredUi(changedPaths, uiPaths = VERIFY_UI_PATHS) {
  const list = Array.isArray(changedPaths) ? changedPaths : [];
  const touchedUi = list.filter(
    (p) => typeof p === 'string' && uiPaths.some((u) => p.includes(u)));
  const touchedFeature = list.some(
    (p) => typeof p === 'string' && p.endsWith('.feature'));
  return { uncovered: touchedUi.length > 0 && !touchedFeature, uiPaths: touchedUi };
}

// Best-effort: first ISO timestamp in the transcript = session start. Returns
// null if the transcript has no parseable timestamp (the lens then no-ops).
function sessionStartFromTranscript(transcriptPath) {
  try {
    const raw = readFileSync(transcriptPath, 'utf8');
    for (const line of raw.split('\n')) {
      const t = line.trim();
      if (!t) continue;
      let o;
      try { o = JSON.parse(t); } catch { continue; }
      const ts = o.timestamp || o.time || o.ts;
      if (typeof ts === 'string' && ts) return ts;
    }
  } catch { /* unreadable transcript — no window */ }
  return null;
}

// --- Standing-debt aggregation (act:e888dd63) -------------------------------
// A coverage warning is a STANDING condition, not an event: the same debt
// recurs every session while the scenarios lag the UI. Filing one item per
// session buried the signal — the 2026-07-12 drain held 49 identical items
// for ONE month-old debt. The detector now keeps ONE pending item per
// (project, debt-class) and APPENDS evidence to it: session-id union (a
// reprocess replay cannot inflate counts), per-path counts, first_seen =
// oldest evidence (Lane C's backlog-rot ages the item by this, per the
// program's amended ACs), filed_at bumped on each append — so runExpiry's
// 30-day clock measures the LAST recurrence: an actively-recurring debt
// never silently expires, a debt that stopped recurring ages out normally.
// Resolution semantics unchanged: any disposition (resolve, dismiss, expire)
// closes the accumulated view, and post-disposition recurrence files FRESH
// with fresh counters — a new signal, not a resurrected pile.
export const COVERAGE_DEBT_CLASS = 'verify-coverage-drift';
// Urgency escalates with the debt's session count so a 90-session debt never
// sorts like a 5-session one (named thresholds per the amended AC).
export const COVERAGE_DEBT_NORMAL_SESSIONS = 5;
export const COVERAGE_DEBT_URGENT_SESSIONS = 15;

export function coverageDebtUrgency(sessions) {
  if (sessions >= COVERAGE_DEBT_URGENT_SESSIONS) return 'urgent';
  if (sessions >= COVERAGE_DEBT_NORMAL_SESSIONS) return 'normal';
  return 'low';
}

// Normalize any coverage-warning evidence — a single-session observation, a
// legacy per-session item, or an aggregated blob — into the foldable form.
export function coverageContribution(e, fallbackDate = null) {
  const ids = Array.isArray(e?.session_ids)
    ? e.session_ids.filter(Boolean)
    : (e?.session_id ? [e.session_id] : []);
  let counts = {};
  if (e?.path_counts && typeof e.path_counts === 'object') {
    counts = { ...e.path_counts };
  } else if (Array.isArray(e?.ui_paths)) {
    for (const p of e.ui_paths) if (typeof p === 'string' && p) counts[p] = 1;
  }
  const seen = e?.first_seen || e?.session_start || fallbackDate || null;
  const last = e?.last_seen || e?.session_start || fallbackDate || null;
  return { session_ids: ids, path_counts: counts, first_seen: seen, last_seen: last };
}

export function foldCoverageEvidence(base, add) {
  const a = base || { session_ids: [], path_counts: {}, first_seen: null, last_seen: null };
  const ids = [...new Set([...(a.session_ids || []), ...(add.session_ids || [])])];
  const counts = { ...(a.path_counts || {}) };
  for (const [p, n] of Object.entries(add.path_counts || {})) {
    counts[p] = (counts[p] || 0) + (Number(n) || 0);
  }
  const firsts = [a.first_seen, add.first_seen].filter(Boolean).sort();
  const lasts = [a.last_seen, add.last_seen].filter(Boolean).sort();
  return {
    session_ids: ids,
    path_counts: counts,
    first_seen: firsts[0] || null,
    last_seen: lasts[lasts.length - 1] || null,
  };
}

export function coverageDebtTitle(evidence) {
  const n = evidence.sessions || (evidence.session_ids || []).length || 1;
  if (n <= 1) return 'UI shipped this session without a scenario update';
  const since = (evidence.first_seen || '').slice(0, 10);
  return `UI drift debt: ${n} sessions without scenario updates${since ? ` since ${since}` : ''}`;
}

export function coverageDebtSummary(evidence, { topN = 3 } = {}) {
  const counts = evidence.path_counts || {};
  const ranked = Object.entries(counts).sort((x, y) => y[1] - x[1] || x[0].localeCompare(y[0]));
  const n = evidence.sessions || (evidence.session_ids || []).length || 1;
  if (n <= 1) {
    const paths = ranked.map(([p]) => p);
    const shown = paths.slice(0, topN).join(', ')
      + (paths.length > topN ? `, +${paths.length - topN} more` : '');
    return `This session changed UI (${shown}) but touched no .feature file — `
      + `drift risk: the product changed, the walkthrough scenarios didn't. Run `
      + `/verify update to propose the matching scenario edits, or accept the drift `
      + `if the change isn't user-visible.`;
  }
  const since = (evidence.first_seen || '').slice(0, 10);
  const top = ranked.slice(0, topN).map(([p, c]) => `${p} (${c})`).join(', ')
    + (ranked.length > topN ? `, +${ranked.length - topN} more paths` : '');
  return `Standing scenario-coverage debt: ${n} sessions${since ? ` since ${since}` : ''} `
    + `shipped UI changes with no .feature edit. Most-hit: ${top}. Run /verify update `
    + `to propose the matching scenario edits, or accept the drift if it isn't `
    + `user-visible. Resolving this item closes the accumulated view; if the debt `
    + `persists, the next session files fresh.`;
}

// Direct item-file write for the append. The queue CRUD library exports no
// update/append verb (and is another lane's file in this program), so the
// standing-debt append writes the item file in place — the same direct-write
// precedent Ring 2's enrichment uses. ONE helper owns the mechanics and the
// byte format matches the queue's own writer (JSON, 2-space, trailing
// newline); the primitive's proper home is a watchtower-queue verb at next
// touch — the detector-registry follow-up (act:4d11fd53) documents the
// aggregation pattern including this seam.
function queueItemPath(id) {
  return join(WATCHTOWER_DIR, 'queue', 'items', `${id}.json`);
}
function defaultReloadQueueItem(id) {
  try {
    const item = JSON.parse(readFileSync(queueItemPath(id), 'utf8'));
    // Mirror the queue reader's schema boundary: an unknown schema_version is
    // never appended-to blind — the caller takes the fresh-filing path.
    return item?.schema_version === 1 ? item : null;
  } catch {
    return null; // missing OR corrupt — caller files fresh, never throws
  }
}
function defaultSaveQueueItem(item) {
  atomicWrite(queueItemPath(item.id), JSON.stringify(item, null, 2) + '\n');
}

// Deps injectable for hermetic tests; production callers omit them. Runs against
// the RESOLVED project path (a worktree resolves to its main repo), so "shipped"
// means landed on the integration branch — un-merged worktree work isn't warned
// on until it merges, which is the correct moment.
export function verifyCoverageLens(project, sessionStartIso, sessionId, deps = {}) {
  const runGit = deps.runGit || ((cmd) => safeExec(cmd, { cwd: project.path }));
  const file = deps.file || createItem;
  const listPendingItems = deps.listPendingItems || listPending;
  const hasFeatures = deps.hasFeatures
    || ((p) => existsSync(join(p, 'e2e', 'features')));

  if (!project?.path || !hasFeatures(project.path)) return { filed: 0 };
  if (!sessionStartIso) return { filed: 0 };

  const out = runGit(
    `git log --since=${JSON.stringify(sessionStartIso)} --name-only --pretty=format: --no-renames`) || '';
  const changed = [...new Set(out.split('\n').map((s) => s.trim()).filter(Boolean))];
  const { uncovered, uiPaths } = detectUncoveredUi(changed);
  if (!uncovered) return { filed: 0 };

  const reloadItem = deps.reloadItem || defaultReloadQueueItem;
  const saveItem = deps.saveItem || defaultSaveQueueItem;
  const supersede = deps.supersede || supersedeItem;

  const listDispositioned = deps.listDispositioned
    || (() => listItems({
      project: project.name, category: 'coverage-warning',
      statuses: ['resolved', 'dismissed', 'superseded', 'expired'],
    }));

  const pendingCoverage = listPendingItems(
    { project: project.name, category: 'coverage-warning' });

  // Replay guard: a reprocess of an already-counted session must not double
  // a debt's counters — legacy per-session shape (session_id) and the
  // aggregated shape (session_ids union) both count. The DISPOSITIONED
  // corpus counts too: marker-cleared bulk reprocess is a lived workflow
  // here (52 sessions in the Jul-2026 backfill), and a replayed session
  // whose contribution the operator already resolved/dismissed must be a
  // no-op — resurrection is not recurrence. A genuinely NEW session after a
  // disposition still files fresh (its id is in no corpus).
  const carriesSession = (i) =>
    i.evidence?.session_id === sessionId
    || (Array.isArray(i.evidence?.session_ids)
        && i.evidence.session_ids.includes(sessionId));
  let alreadyCounted = pendingCoverage.some(carriesSession);
  if (!alreadyCounted) {
    try {
      alreadyCounted = listDispositioned().some(carriesSession);
    } catch (e) {
      logError(`coverage-debt dispositioned-corpus check failed (${e.message}) — continuing on the pending corpus alone`);
    }
  }
  if (alreadyCounted) return { filed: 0, appended: 0 };

  // ONE standing item per (project, debt-class). Oldest wins as the append
  // target (deterministic under the multiple-pending race); every other
  // pending coverage item — legacy per-session filings and race residue —
  // is absorbed: its evidence folds in and the item is superseded, so
  // pre-aggregation piles collapse on the first post-upgrade session.
  const standing = pendingCoverage
    .filter((i) => i.evidence?.debt_class === COVERAGE_DEBT_CLASS)
    .sort((a, b) => String(a.filed_at).localeCompare(String(b.filed_at)));
  const target = standing[0] || null;
  const absorbed = pendingCoverage.filter((i) => i !== target);

  const nowIso = new Date().toISOString();
  const sessionContrib = coverageContribution(
    { session_id: sessionId, ui_paths: uiPaths, session_start: sessionStartIso }, nowIso);
  let absorbedAgg = null;
  for (const it of absorbed) {
    absorbedAgg = foldCoverageEvidence(absorbedAgg, coverageContribution(it.evidence, it.filed_at));
  }

  const buildEvidence = (agg) => ({
    debt_class: COVERAGE_DEBT_CLASS,
    // Latest-session fields keep the legacy single-session reader shape.
    session_id: sessionId,
    session_start: sessionStartIso,
    ...agg,
    sessions: agg.session_ids.length,
    ui_paths: Object.keys(agg.path_counts).sort(),
  });

  let appended = 0;
  if (target) {
    // Write-time re-check: the item can resolve between the pending read and
    // this write. A non-pending (or unreadable) target takes the
    // fresh-after-disposition path — fresh counters, per the contract above.
    const fresh = reloadItem(target.id);
    if (fresh && fresh.status === 'pending') {
      const agg = foldCoverageEvidence(
        foldCoverageEvidence(coverageContribution(fresh.evidence, fresh.filed_at), absorbedAgg || {}),
        sessionContrib);
      const evidence = buildEvidence(agg);
      fresh.evidence = evidence;
      fresh.title = coverageDebtTitle(evidence);
      fresh.summary = coverageDebtSummary(evidence);
      fresh.urgency = coverageDebtUrgency(evidence.sessions);
      fresh.filed_at = nowIso; // expiry clock = last recurrence (header comment)
      fresh.context_anchor = `standing debt ${COVERAGE_DEBT_CLASS} — git log --since session start (${sessionId})`;
      try {
        saveItem(fresh);
        appended = 1;
      } catch (e) {
        logError(`coverage-debt append to ${target.id} failed (${e.message}) — filing fresh instead`);
      }
    } else {
      log(`coverage-debt item ${target.id} no longer pending at write time — filing fresh`);
    }
  }

  let filed = 0;
  let newId = null;
  if (!appended) {
    const agg = foldCoverageEvidence(absorbedAgg || { session_ids: [], path_counts: {} }, sessionContrib);
    const evidence = buildEvidence(agg);
    const created = file({
      project: project.name,
      project_path: project.path,
      ...(project.unresolved ? { project_unresolved: true } : {}),
      filed_by: 'ring3-close',
      category: 'coverage-warning',
      urgency: coverageDebtUrgency(evidence.sessions),
      title: coverageDebtTitle(evidence),
      summary: coverageDebtSummary(evidence),
      context_anchor: `standing debt ${COVERAGE_DEBT_CLASS} — git log --since session start (${sessionId})`,
      evidence,
      options: [
        { value: 'update', label: 'Update scenarios', description: '/verify update' },
        { value: 'accept-drift', label: 'Accept drift', description: 'Not user-visible' },
        { value: 'dismiss', label: 'Dismiss', description: 'Not worth capturing' },
      ],
    });
    // createItem returns the new item's id STRING; test doubles may return
    // an item object — accept both, never lose the successor reference.
    newId = typeof created === 'string' ? created : (created?.id || null);
    filed = 1;
  }

  // Absorbed items exit through the sanctioned verb, naming their successor.
  const successor = appended ? target.id : (newId || 'the standing coverage-debt item');
  for (const it of absorbed) {
    try {
      supersede(it.id, { reason: `absorbed into standing coverage-debt item ${successor} (${COVERAGE_DEBT_CLASS})` });
    } catch (e) {
      logError(`could not supersede absorbed coverage item ${it.id} (${e.message}) — its evidence is folded regardless`);
    }
  }

  return { filed, appended };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const startTime = Date.now();
  const args = parseArgs();

  if (!args.sessionId) {
    console.error('Usage: watchtower-ring3-close.mjs --session-id <id> --transcript <path> --cwd <path> [--reason <reason>]');
    process.exit(2);
  }

  log(`Starting Ring 3 close for session ${args.sessionId}`);

  // Check processed marker — prevent re-processing
  if (isProcessed(args.sessionId)) {
    log(`Session ${args.sessionId} already processed, exiting`);
    process.exit(0);
  }

  // Verify transcript exists
  if (!args.transcriptPath || !existsSync(args.transcriptPath)) {
    logError(`Transcript not found: ${args.transcriptPath}`);
    process.exit(1);
  }

  // Load config
  let config;
  try {
    config = loadConfig();
  } catch (e) {
    logError(`Config error: ${e.message}`);
    process.exit(1);
  }

  // Resolve project — ONE resolution call shared by the live close and the
  // manual-reprocess path (both enter through main()); the transcript's own
  // project-dir slug is the attribution evidence, cwd only corroborates.
  const runnerCwd = args.cwd || process.cwd();
  const { project, cwdTrusted, slug: transcriptSlug } = resolveSessionProject(
    { cwd: runnerCwd, transcriptPath: args.transcriptPath }, config);
  log(`Project: ${project.name} (${project.path || 'no path — unresolved identity'})`);
  if (!transcriptSlug) {
    log('No transcript project-dir slug — trusting cwd (transcript outside ~/.claude/projects)');
  } else if (!cwdTrusted) {
    log(`cwd ${runnerCwd} is not this transcript's session cwd (slug ${transcriptSlug}) — reprocess-style invocation, live-cwd phases skipped`);
  }
  if (project.unresolved) {
    logError(`Project identity UNRESOLVED (cwd ${runnerCwd}, transcript ${args.transcriptPath || 'none'}) — filing under "${project.name}" with project_unresolved`);
  }
  // Slug-derived unresolved identity: inbox filing proceeds (items carry
  // project_unresolved and land in /inbox's unresolved group), but no
  // state-file or thread key is minted from a raw encoded-path name — those
  // writers are skipped below.
  const slugUnresolved = !!project.unresolved && !project.path;

  // --- Phase 2a: Worktree check (pre-transcript, pure git, zero cost) ---
  let worktreeItemsFiled = 0;
  if (cwdTrusted) {
    try {
      worktreeItemsFiled = worktreeCheck(runnerCwd, project);
    } catch (e) {
      logError(`Phase 2a failed: ${e.message}`);
    }
  } else {
    log('Phase 2a: skipped — cwd is the runner\'s environment, not this session\'s');
  }

  // Reprocess mode: stamp backfilled suppressions with the session's ORIGINAL
  // date so they land in their historical recall-canary window, not now()'s
  // (act:6fb2b7d1). Set before any suppressing phase runs.
  if (args.reprocess) {
    reprocessTs = sessionStartFromTranscript(args.transcriptPath) || null;
    log(`Reprocess mode: suppression ledger stamped ${reprocessTs || '(no session date — using now())'}`);
  }

  // --- Preprocessing ---
  const { compressed, originalTokenEstimate, compressedTokenEstimate } = preprocessTranscript(args.transcriptPath);

  if (!compressed || compressed.trim().length === 0) {
    log('Empty transcript after preprocessing, exiting');
    markProcessed(args.sessionId, { empty: true, worktreeItemsFiled });
    clearFailedMarker(args.sessionId); // an empty session captured all there was — no API needed
    writeHealth(args.sessionId, { itemsFiled: worktreeItemsFiled, actionsClosed: 0 }, project);
    process.exit(0);
  }

  // Track stats across phases
  const stats = {
    actionsClosed: 0,
    actionsQueued: 0,
    memoryWritten: 0,
    extractionsQueued: 0,
    itemsFiled: worktreeItemsFiled,
  };

  // --- Invariant phases: structurally unskippable ---

  // Phase 2b: Session summary
  let summary = '';
  if (slugUnresolved) {
    log('Phase 2b: skipped — unresolved identity; no state-file key minted from a transcript-slug name');
  } else {
    try {
      summary = await sessionSummary(compressed, project.slug, args.sessionId);
    } catch (e) {
      logError(`Phase 2b failed: ${e.message}`);
    }
  }

  // Phase 2b2: Thread capture
  let threadIds = [];
  if (slugUnresolved) {
    log('Phase 2b2: skipped — unresolved identity; no thread key minted from a transcript-slug name');
  } else {
    try {
      threadIds = await threadCapture(compressed, project.slug, args.sessionId, summary, args.transcriptPath, { project });
      stats.threadsUpdated = threadIds.length;
    } catch (e) {
      logError(`Phase 2b2 failed: ${e.message}`);
    }
  }

  // Session start (transcript-derived) — consumed by Phase 2c's
  // create-vs-complete filter and the Phase 2q verify-coverage lens.
  const sessionStartIso = sessionStartFromTranscript(args.transcriptPath);

  // Phase 2c: Work item closure
  try {
    const result = await workItemClosure(compressed, project, threadIds, sessionStartIso);
    stats.actionsClosed = result.closed;
    stats.actionsQueued += result.queued;
    stats.itemsFiled += result.queued;
  } catch (e) {
    logError(`Phase 2c failed: ${e.message}`);
  }

  // Phase 2d: Decision/lesson extraction
  try {
    const result = await decisionExtraction(compressed, project, args.sessionId, args.transcriptPath, threadIds, { config, sessionStartIso });
    stats.memoryWritten = result.autoWritten;
    stats.extractionsQueued = result.queued;
    stats.itemsFiled += result.queued;
  } catch (e) {
    logError(`Phase 2d failed: ${e.message}`);
  }

  // Phase 2e: Audit pattern capture
  try {
    await qualityPatternCapture(compressed, project.path);
  } catch (e) {
    logError(`Phase 2e failed: ${e.message}`);
  }

  // Phase 2f: Methodology capture (feature-flagged)
  if (config.defaults?.methodology_capture !== false) {
    try {
      await methodologyCapture(compressed, project, threadIds);
    } catch (e) {
      logError(`Phase 2f failed: ${e.message}`);
    }
  }

  // Phase 2g: Upstream friction (feature-flagged)
  if (config.defaults?.upstream_friction_detection !== false) {
    try {
      await upstreamFriction(compressed, project, threadIds);
    } catch (e) {
      logError(`Phase 2g failed: ${e.message}`);
    }
  }

  // Phase 2h removed — feedback delivery is a Ring 1 mechanical duty
  // (flushFeedbackOutbox in watchtower-lib.mjs; act:6c3a4763).

  // Phase 2m: Session advisor pass (feature-flagged) — the re-homed standing
  // advisors' close-side seat (act:aded4fc9). Runs after the extraction
  // phases so threadIds tag the findings; reuses this run's pinned-sonnet
  // claudeCall for cost control.
  if (config.defaults?.session_advisors !== false) {
    try {
      const result = await advisorPass(compressed, project, args.sessionId, threadIds);
      stats.advisorFindings = result.filed;
      stats.itemsFiled += result.filed;
    } catch (e) {
      logError(`Phase 2m failed: ${e.message}`);
    }
  }

  // Phase 2n: Raised-but-unhandled lens (feature-flagged). Runs after the
  // filing phases (2c/2d/2m) so its pending corpus already holds this
  // session's completion candidates, extractions, and advisor findings — a
  // loose end one of those already captured is suppressed.
  if (config.defaults?.raised_unhandled_lens !== false) {
    try {
      const result = await raisedUnhandledLens(
        compressed, project, args.sessionId, args.transcriptPath, threadIds);
      stats.itemsFiled += result.queued;
    } catch (e) {
      logError(`Phase 2n failed: ${e.message}`);
    }
  }

  // Phase 2o: Skill-candidate lens (feature-flagged).
  if (config.defaults?.skill_candidate_lens !== false) {
    try {
      const result = await skillCandidateLens(
        compressed, project, args.sessionId, args.transcriptPath, threadIds);
      stats.itemsFiled += result.queued;
    } catch (e) {
      logError(`Phase 2o failed: ${e.message}`);
    }
  }

  // Phase 2p: Checklist-catch detection (feature-flagged). Records to the
  // project-local checklist-stats.json — no inbox item — and silently no-ops
  // unless the project opted into qa-dimensions.yaml.
  if (config.defaults?.checklist_catch_lens !== false) {
    try {
      await checklistCatchLens(compressed, project, args.sessionId);
    } catch (e) {
      logError(`Phase 2p failed: ${e.message}`);
    }
  }

  // Phase 2q: verify-coverage lens (feature-flagged; per-project verify gate).
  // Diff-based — warns when the session shipped UI with no .feature edit.
  if (config.defaults?.verify_coverage !== false) {
    try {
      const r = verifyCoverageLens(
        project, sessionStartIso, args.sessionId);
      stats.itemsFiled += r.filed;
    } catch (e) {
      logError(`Phase 2q failed: ${e.message}`);
    }
  }

  // Phase 2i: Session auto-naming (feature-flagged)
  if (config.defaults?.session_auto_naming !== false) {
    try {
      await sessionAutoNaming(args.sessionId, summary, config);
    } catch (e) {
      logError(`Phase 2i failed: ${e.message}`);
    }
  }

  // Phase 2j: Consumer hooks (after invariant phases, extend not replace)
  try {
    runConsumerHooks(config, {
      session_id: args.sessionId,
      project: project.name,
      project_path: project.path,
      summary,
      stats,
    });
  } catch (e) {
    logError(`Phase 2j failed: ${e.message}`);
  }

  // Phase 2j2: Session-close routine dispatch (feature-flagged). Declared
  // routines with a session-close trigger fire for the RESOLVED project (a
  // worktree session fires its main project's routines) and dispatch to the
  // desk's main session via the engine's single mux path. Mechanical-tick
  // triggers belong to Ring 1; only session-close events originate here.
  // Skipped for unresolved identities — a phantom project has no declared
  // routines, and firing under a basename key would split routine state.
  if (config.defaults?.routine_dispatch !== false && !project.unresolved) {
    try {
      const pass = runRoutinePass({
        config,
        event: { type: 'session-close', project: project.name },
        filedBy: 'ring3-close',
      });
      if (pass.fired.length > 0) {
        log(`Phase 2j2: fired ${pass.fired.map((f) => `${f.key} (${f.status})`).join(', ')}`);
      }
    } catch (e) {
      logError(`Phase 2j2 failed: ${e.message}`);
    }
  }

  // Phase 2k: Signal Ring 2
  try {
    signalRing2();
  } catch (e) {
    logError(`Phase 2k failed: ${e.message}`);
  }

  // A systemic API failure means EVERY Claude-dependent phase produced
  // nothing — surface it loudly so it can't hide behind a "success" run.
  if (systemicApiFailure) {
    logError(`SYSTEMIC API FAILURE (${systemicApiFailure.type}) — every knowledge-extraction phase was skipped this run (no summary, threads, extractions, or advisor findings). Ring 3 health is DEGRADED. Operator action required: ${systemicApiFailure.message}`);
    stats.apiFailure = systemicApiFailure.type;
  }

  // Phase 2l: Health
  try {
    writeHealth(args.sessionId, stats, project, systemicApiFailure);
  } catch (e) {
    logError(`Phase 2l failed: ${e.message}`);
  }

  // Step 4: Mark processed — but ONLY if capture actually happened. A systemic
  // API outage (or every API-dependent phase throwing) means this session
  // captured nothing; marking it processed would lose it permanently with no
  // re-run (nothing scans transcripts). Instead leave it UNMARKED and drop a
  // durable recovery marker — the `--reprocess-failed` worklist (act:6fb2b7d1).
  const allApiFailed = apiCallsAttempted > 0 && apiCallsFailed === apiCallsAttempted;
  const captureFailed = !!systemicApiFailure || allApiFailed;
  if (captureFailed) {
    const failureType = systemicApiFailure?.type || 'all-api-phases-failed';
    writeFailedMarker(args.sessionId, {
      transcriptPath: args.transcriptPath,
      cwd: runnerCwd,
      failureType,
    });
    logError(`Ring 3 CAPTURE FAILED for ${args.sessionId} (${failureType}, ${apiCallsFailed}/${apiCallsAttempted} API calls failed) — NOT marked processed; recovery marker written. Recover with: watchtower-ring3-close.mjs --reprocess-failed`);
    process.exitCode = 3; // distinct signal for the --reprocess-failed parent; harmless in the live SessionEnd path
  } else {
    markProcessed(args.sessionId, stats);
    clearFailedMarker(args.sessionId); // a successful (re)process clears any prior failure
  }

  const duration = Date.now() - startTime;
  log(`Ring 3 close complete in ${duration}ms. Actions closed: ${stats.actionsClosed}, items filed: ${stats.itemsFiled}, memory written: ${stats.memoryWritten}, threads updated: ${stats.threadsUpdated || 0}${systemicApiFailure ? ` — DEGRADED (${systemicApiFailure.type})` : ''}`);
}

// ---------------------------------------------------------------------------
// Reprocess drain — recover sessions the outage failed (act:6fb2b7d1)
// PARENT mode (`--reprocess-failed`). Spawns ONE fresh `--reprocess`
// subprocess per failed marker. An in-process loop is impossible: main() calls
// process.exit, and the systemicApiFailure latch is one-per-process (a shared
// process would let one session's failure poison the next). Never calls main().
// ---------------------------------------------------------------------------

const REPROCESS_BATCH = 10;        // markers per drain run (bounded)
const REPROCESS_MAX_ATTEMPTS = 5;  // stop auto-retrying after this; leave for manual review

// Count API-usage ledger records via the feature-detection namespace seam, so
// a not-yet-merged export can't break module load. Gives the write-only usage
// ledger its first reader — the drain reports its own spend (critic risk 5).
function countApiUsage() {
  try {
    const fn = watchtowerLib.readApiUsageRecords;
    if (typeof fn !== 'function') return null;
    const recs = fn({ watchtowerDir: WATCHTOWER_DIR });
    return Array.isArray(recs) ? recs.length : null;
  } catch { return null; }
}

function reprocessHealthDegraded() {
  // Don't spend on a down API: if the most recent Ring 3 health is degraded,
  // the API is currently failing and a reprocess would only burn failed calls.
  try {
    const p = join(WATCHTOWER_DIR, 'state', 'ring3-health.json');
    if (!existsSync(p)) return false;
    return JSON.parse(readFileSync(p, 'utf8'))?.status === 'degraded';
  } catch { return false; }
}

async function reprocessFailed() {
  const failedDir = join(WATCHTOWER_DIR, 'ring3', 'failed');
  if (!existsSync(failedDir)) { log('No ring3/failed/ dir — nothing to reprocess.'); return; }

  // Lightweight PID lock (a future auto-trigger could fire concurrently).
  const lockPath = join(WATCHTOWER_DIR, 'ring3', 'reprocess.lock');
  let holdingLock = false;
  try {
    if (existsSync(lockPath)) {
      const owner = parseInt(String(readFileSync(lockPath, 'utf8')).trim(), 10);
      let alive = false;
      try { if (owner) { process.kill(owner, 0); alive = true; } } catch { alive = false; }
      if (alive) { log(`Reprocess drain already running (pid ${owner}) — skipping.`); return; }
    }
    writeFileSync(lockPath, String(process.pid));
    holdingLock = true;
  } catch (e) { logError(`Reprocess lock error: ${e.message} — proceeding without lock`); }

  try {
    if (reprocessHealthDegraded()) {
      logError('Ring 3 health is DEGRADED (API currently failing) — skipping reprocess drain to avoid burning failed calls. Re-run when the API recovers.');
      return;
    }

    let markers = [];
    try { markers = readdirSync(failedDir).filter(f => f.endsWith('.json')); } catch { markers = []; }
    if (markers.length === 0) { log('No failed sessions to reprocess.'); return; }

    const selfPath = fileURLToPath(import.meta.url); // this script, however it was invoked (robust under a test runner)
    const usageBefore = countApiUsage();
    let recovered = 0, stillFailing = 0, alreadyDone = 0, atMax = 0, spawnErrors = 0;
    const batch = markers.slice(0, REPROCESS_BATCH);
    log(`Reprocessing ${batch.length} of ${markers.length} failed session(s)...`);

    for (const file of batch) {
      const sid = file.replace(/\.json$/, '');
      let marker;
      try { marker = JSON.parse(readFileSync(join(failedDir, file), 'utf8')); }
      catch { logError(`Marker ${file} unreadable — skipping.`); continue; }

      if (isProcessed(sid)) { clearFailedMarker(sid); alreadyDone++; continue; } // recovered elsewhere → self-heal
      if ((marker.attempts || 0) >= REPROCESS_MAX_ATTEMPTS) {
        logError(`Session ${sid} failed ${marker.attempts}x — leaving for manual review (not auto-retried).`);
        atMax++; continue;
      }
      if (!marker.transcript_path || !existsSync(marker.transcript_path)) {
        logError(`Session ${sid}: transcript missing (${marker.transcript_path || 'none'}) — cannot reprocess; leaving marker.`);
        stillFailing++; continue;
      }

      const childArgs = [selfPath, '--session-id', sid, '--transcript', marker.transcript_path, '--reprocess'];
      if (marker.cwd) childArgs.push('--cwd', marker.cwd);
      const res = spawnSync(process.execPath, childArgs, { stdio: 'inherit' });
      if (res.error) { logError(`Session ${sid}: spawn failed (${res.error.message})`); spawnErrors++; continue; }
      if (res.status === 0) recovered++;            // child marked processed + cleared its marker
      else if (res.status === 3) stillFailing++;    // child bumped attempts + kept the marker
      else { logError(`Session ${sid}: reprocess exited ${res.status}`); spawnErrors++; }
    }

    const usageAfter = countApiUsage();
    const callDelta = (usageBefore != null && usageAfter != null) ? (usageAfter - usageBefore) : null;
    const remaining = markers.length - batch.length;
    log(`Reprocess drain done: ${recovered} recovered, ${stillFailing} still failing, ${alreadyDone} already-processed, ${atMax} at max attempts, ${spawnErrors} spawn error(s). API calls spent this drain: ${callDelta != null ? callDelta : 'unknown'}.${remaining > 0 ? ` ${remaining} marker(s) remaining — re-run to continue.` : ''}`);
  } finally {
    if (holdingLock) { try { if (existsSync(lockPath)) rmSync(lockPath); } catch { /* best-effort */ } }
  }
}

// ---------------------------------------------------------------------------
// Entry guard — run main() only when this file is the invoked module, so
// tests can import the helpers without firing a transcript run. realpathSync
// matters: node realpath-resolves the main module for import.meta.url while
// argv[1] keeps the given path — a symlinked invocation would otherwise make
// main() silently never run.
// ---------------------------------------------------------------------------

const isMain = (() => {
  try {
    return process.argv[1]
      && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
  } catch {
    return false;
  }
})();

if (isMain) {
  const routed = parseArgs();
  const runner = routed.reprocessFailed ? reprocessFailed() : main();
  runner.catch(e => {
    logError(`Fatal${routed.reprocessFailed ? ' (reprocess)' : ''}: ${e.message}`);
    process.exit(1);
  });
}

// Exported for tests (ring3-dedup.test.mjs, advisor-pass.test.mjs). Runtime
// behavior is unchanged — the session-end hook invokes this file directly,
// which runs main() above.
export {
  tokenize,
  isDuplicate,
  parseMemoryTitles,
  selectNearbyMemoryTitles,
  loadMemoryTitles,
  // Dedup-hole fix (act:421a8ab2, N5 of grp:retro-remeaning)
  extractMemoryFileTitle,
  // Extraction-noise umbrella (act:471941c9) — bookkeepingRule,
  // resolveAuthorityPath, loadMemoryCorpus, and extractMemoryDescription are
  // exported inline at their declarations.
  // Reach 1 (act:5182beda, N3 of grp:retro-remeaning)
  selectNearbyPendingItems,
  relationKey,
  fileRelationProposal,
  modelRescues,
  chunkWithOverlap,
  mergeChunkExtractions,
  decisionExtraction,
  resolutionCorpus,
  threadCursorLines,
  buildExtractionCorpora,
  completionReviewEmitGuard,
  // Exported for the quote-instrumentation suite (act:ea23b3a5): a fixture
  // must be built by the REAL preprocessor, because its JSON-serialized output
  // — not plain prose — is what the matcher actually faces.
  preprocessTranscript,
  // Friction actionability gate (act:ea23b3a5)
  upstreamFriction,
  discoverSessionAdvisors,
  parseAdvisorFindings,
  advisorPass,
  // Session-close extraction lenses (act:4ff2cfb3)
  parseLensFindings,
  raisedUnhandledLens,
  skillCandidateLens,
  extractDimensionNames,
  recordChecklistCatches,
  checklistCatchLens,
  // Thread-capture hardening (act:e8793574). selectEarnedThreads,
  // extractRelatedFids, classifyApiError, and buildHealth are exported inline.
  threadCapture,
  // Session attribution (act:29001b07). normalizeSlugKey,
  // transcriptSlugFromPath, and resolveSessionProject are exported inline;
  // these two are exported for the null-path guard tests.
  workItemClosure,
  methodologyCapture,
  // Outage-robustness recovery (act:6fb2b7d1)
  failedMarkerPath,
  writeFailedMarker,
  clearFailedMarker,
  reprocessFailed,
};

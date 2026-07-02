#!/usr/bin/env node

// Watchtower Ring 3 — Close mode (post-session transcript processing).
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
  renameSync, statSync, realpathSync,
} from 'fs';
import { join, basename, dirname } from 'path';
import { execSync } from 'child_process';
import { createRequire } from 'module';
import { homedir } from 'os';
import { pathToFileURL } from 'url';
import {
  atomicWrite, loadConfig, slugify,
  log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending, loadBetterSqlite3,
  updateThreadFile, currentCursor, resolveProjectIdentity, VERIFY_UI_PATHS,
  projectThreadCursorLines, authoredClaudeDirs, claudeChurnIsDisposable,
  buildLastSessionBlock, upsertLastSessionSection, recordSuppression,
  recentSlice,
} from './watchtower-lib.mjs';
// Direct queue import (precedent: ring2 imports expireItem this way) —
// watchtower-lib deliberately not extended for this (lane separation).
import { listItems } from './watchtower-queue.mjs';
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

async function claudeCall(systemPrompt, userMessage) {
  const client = await getAnthropicClient();
  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 4096,
    system: systemPrompt,
    messages: [{ role: 'user', content: userMessage }],
  });
  return response.content[0]?.text || '';
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
  const content = `# Session ${sessionId}\n\nDate: ${new Date().toISOString()}\n\n${bullets}\n`;
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
  const block = buildLastSessionBlock({ date, sessionId, bullets });
  atomicWrite(projectStatePath, upsertLastSessionSection(existingState, block));

  log(`Phase 2b: Summary written to ${projectSlug}/sessions/${date}-${sessionId}.md`);
  return bullets;
}

// ---------------------------------------------------------------------------
// Phase 2b2: Thread capture
// ---------------------------------------------------------------------------

async function threadCapture(compressed, projectSlug, sessionId, summary, transcriptPath) {
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
  "display_name": "A rich one-line ARTICULATION of what this thread is really about — this is what a human reads. Make it carry the meaning the slug cannot; never just a restatement of the slug. It evolves as understanding deepens.",
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
- Expect SEVERAL threads at different zoom levels — a session touching only one thread is the exception, not the rule
- Every thread needs a justified "contribution" — earned membership, never a lazy copy
- Before minting a new slug, reuse a near-synonym from the active list instead
- The slug is just a stable filing key — short, reused. The display_name is what carries understanding to a human — a rich, specific articulation, never a restatement of the slug
- Capture what you LEARNED, not what you DID
- Output ONLY the JSON array, no markdown fences`;

  const response = await claudeCall(systemPrompt, compressed);

  let threads;
  try {
    threads = JSON.parse(response.trim());
    if (!Array.isArray(threads)) throw new Error('Not an array');
  } catch (e) {
    logError(`Phase 2b2: Failed to parse thread response: ${e.message}`);
    return [];
  }

  const threadIds = [];
  const now = new Date().toISOString();
  const date = now.slice(0, 10);

  for (const t of threads) {
    if (!t.thread || !t.cursor) continue;
    const threadSlug = slugify(t.thread);
    const threadPath = join(threadsDir, `${threadSlug}.json`);

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
      const outcome = updateThreadFile(threadPath, threadSlug, t, cursorEntry, sessionRecord, now);
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

  return threadIds;
}

// ---------------------------------------------------------------------------
// Phase 2c: Work item closure
// ---------------------------------------------------------------------------

async function workItemClosure(compressed, project, threadIds = []) {
  const projectPath = project.path;
  log('Phase 2c: Work item closure');

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
    openActions = db.prepare(
      "SELECT fid, text, status FROM actions WHERE status IN ('open', 'in-progress') AND deleted_at IS NULL"
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

  const actionList = openActions.map(a => `- ${a.fid}: ${a.text} (${a.status})`).join('\n');

  const systemPrompt = `You are evaluating which work items appear completed based on a session transcript. For each action, assess confidence:
- "high" = clearly completed in the transcript (code written, committed, tests passing, etc.)
- "medium" = partially done or implied complete but not confirmed
- "low" = mentioned but unclear if completed
- "none" = not addressed in this session

Output JSON array: [{"fid":"act:XXXXXXXX","confidence":"high|medium|low|none","evidence":"brief reason"}]
Output ONLY the JSON array, no other text.`;

  const userMessage = `Open actions:\n${actionList}\n\nSession transcript:\n${recentSlice(compressed, COMPLETION_TRANSCRIPT_BUDGET)}`;

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
  // pending ∪ recently-closed completion-review items. Both setups FAIL OPEN
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
        statuses: ['resolved', 'dismissed'],
        since,
      }),
    ];
  } catch (e) {
    logError(`Phase 2c: could not load existing completion-review items (${e.message}) — failing open`);
  }

  for (const evalItem of evaluations) {
    if (!evalItem.fid || evalItem.confidence === 'none') continue;

    const guard = completionReviewEmitGuard(evalItem.fid, {
      statusStmt,
      existingItems: existingCompletionItems,
    });
    if (!guard.emit) {
      skipped++;
      log(`Phase 2c: skip ${evalItem.fid} — ${guard.reason}`);
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

function loadMemoryTitles(projectPath) {
  const encoded = projectPath.replace(/\//g, '-');
  const memDir = join(homedir(), '.claude', 'projects', encoded, 'memory');
  const indexPath = join(memDir, 'MEMORY.md');
  if (!existsSync(indexPath)) return [];
  try {
    return parseMemoryTitles(readFileSync(indexPath, 'utf8'));
  } catch { return []; }
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

  const overlap = (other) => {
    const otherTokens = meaningfulTokens(other);
    return allTokens.filter(t => otherTokens.includes(t)).length;
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
      if (overlap(entry) >= OVERLAP_THRESHOLD) {
        return { corpus, match: entry };
      }
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
  const memoryTitles = loadMemoryTitles(project.path);
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
  return { memoryTitles, threadCursorLines: cursorLines, pendingTitles, resolutionTitles };
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
//   2. Per-fid dedup: skip when an existing completion-review item for the
//      same fid is in `existingItems` (the caller builds that set from
//      pending ∪ resolved/dismissed-within-COMPLETION_REVIEW_DEDUP_DAYS).
//
// Returns { emit: true } or { emit: false, reason }.
function completionReviewEmitGuard(fid, { statusStmt, existingItems = [] } = {}) {
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
// Phase 2d: Knowledge extraction → inbox
// ---------------------------------------------------------------------------

async function decisionExtraction(compressed, project, sessionId, transcriptPath,
  threadIds = [], { callFn = claudeCall } = {}) {
  const projectPath = project.path;
  log('Phase 2d: Knowledge extraction');

  // Dedup corpora — built FIRST (before the extraction call) so M1b can inject
  // the nearest saved titles into the prompt AND back the result with the
  // deterministic isDuplicate pass after. The SAME builder the close lenses use
  // (one corpus-building idiom). M1a: memory is title-matched (no description
  // tail), thread cursors are their own whole-token corpus; every builder fails
  // open. Queried by the resolved project NAME (the old basename query looked
  // up a phantom project, so dedup never matched and dups re-filed).
  const { memoryTitles, threadCursorLines: cursorLines, pendingTitles, resolutionTitles } =
    buildExtractionCorpora(project, { phase: 'Phase 2d' });

  const transcript = recentSlice(compressed, SINGLE_CALL_TRANSCRIPT_BUDGET);
  // M1b prefilter: the saved titles most relevant to THIS session (scored over
  // the full compressed transcript, not just the recent slice).
  const nearbyTitles = selectNearbyMemoryTitles(memoryTitles, compressed);

  const systemPrompt = `You are extracting decisions, constraints, lessons, and user preferences from a Claude Code session transcript. For each item found, classify its home:

- "memory" = a lesson, preference, or constraint worth remembering across sessions
- "claude-md" = a convention or rule that should be added to CLAUDE.md
- "pib-db-trigger" = a deferred action with a trigger condition
- "upstream-feedback" = friction with Claude Code itself

Only extract items that represent NEW durable knowledge — things learned or decided in this session that aren't yet captured. Skip items that are routine, obvious, or just restating existing conventions.

Do NOT extract transient operational state: project completion status ("X has 0 open actions"), branch merge status ("branch Y was merged"), install success confirmations ("all rings working"), or other point-in-time observations that will be stale within days. These belong in state files, not the inbox.

NOVELTY: an "ALREADY SAVED TO MEMORY" list may appear at the very end of the input. Use it as novelty context: OMIT an item ONLY when a saved title clearly and substantially covers the SAME specific knowledge. DEFAULT TO INCLUDING — when in doubt whether a saved title covers it, INCLUDE the item (re-proposing a near-duplicate is cheap; losing a novel lesson is not). For every item you DO output, set "covered_by" to the exact saved title that most covers it, or "none" if no saved title covers it.

For each item, assess how time-sensitive routing is. Urgency means HOW FAST THE VALUE DECAYS if not routed — it is NOT importance:
- "urgent" = the value evaporates within days if not routed (a trigger condition about to fire, a constraint someone will trip over THIS WEEK, a decision another active session needs right now). Apply the time-decay test: "if this sits in the inbox for a week, is most of its value gone?" If no, it is not urgent.
- "normal" = worth routing but the value keeps (most decisions and constraints)
- "low" = interesting but can wait indefinitely

Lessons and preferences are durable knowledge — their value does not decay. They are almost NEVER urgent, no matter how important they are. An important-but-durable item is "normal".

Output JSON array: [{"type":"decision|constraint|lesson|preference","home":"memory|claude-md|pib-db-trigger|upstream-feedback","urgency":"urgent|normal|low","title":"short title","content":"detailed description","covered_by":"exact already-saved title that covers this, or \\"none\\""}]
Output ONLY the JSON array, no other text. If nothing found, output [].`;

  // M1b injection: show the model what's already saved so it self-filters dupes
  // in this one pass. DEFAULT-KEEP — the wording leans toward inclusion. Empty
  // nearby set ⇒ no block ⇒ the prompt degrades to blind extraction (fail-open).
  const savedBlock = nearbyTitles.length
    ? `\n\nALREADY SAVED TO MEMORY (do NOT re-propose anything one of these substantially and specifically covers; when unsure, INCLUDE the item):\n${nearbyTitles.map(t => `- ${t}`).join('\n')}`
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
          perChunk.push(await runExtractionCall(callFn, systemPrompt, `${window}${savedBlock}`));
        } catch (e) {
          logError(`Phase 2d: M2-B chunk extraction failed (${e.message}) — continuing with the other windows`);
          perChunk.push([]);
        }
      }
      extractions = mergeChunkExtractions(perChunk);
    } else {
      extractions = await runExtractionCall(callFn, systemPrompt, `${transcript}${savedBlock}`);
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

  let queued = 0;
  let deduped = 0;
  let rescued = 0;

  for (const item of extractions) {
    const fullTitle = `${item.type}: ${item.title}`;

    const dup = isDuplicate(
      fullTitle, item.content || '', memoryTitles, pendingTitles,
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
          session_id: sessionId,
        });
        continue;
      }
    }

    try {
      const isMemory = item.home === 'memory';
      createItem({
        project: project.name,
        project_path: projectPath,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        category: 'knowledge-extraction',
        urgency: item.urgency || 'normal',
        title: fullTitle,
        summary: item.content,
        context_anchor: `session ${sessionId}`,
        evidence: {
          type: item.type,
          home: item.home,
          session_id: sessionId,
        },
        options: isMemory
          ? [
              { key: 'write', label: 'Write to memory' },
              { key: 'edit', label: 'Edit before writing' },
              { key: 'dismiss', label: 'Dismiss' },
            ]
          : [
              { key: `route-to-${item.home}`, label: `Write to ${item.home}` },
              { key: 'dismiss', label: 'Dismiss' },
            ],
        draft_artifact: isMemory ? `# ${item.title}\n\n${item.content}` : null,
        filed_by: 'ring3-close',
        transcript_ref: { path: transcriptPath, line_range: null },
        thread_ids: threadIds,
      });
      queued++;
    } catch (e) {
      logError(`Phase 2d: Failed to queue extraction: ${e.message}`);
    }
  }

  if (deduped > 0) log(`Phase 2d: ${deduped} extractions skipped (already in memory or inbox)`);
  if (rescued > 0) log(`Phase 2d: ${rescued} extraction(s) rescued from a lexical match (model judged novel)`);
  log(`Phase 2d: ${queued} extractions queued for review`);
  return { autoWritten: 0, queued };
}

// ---------------------------------------------------------------------------
// Phase 2e: Audit pattern capture
// ---------------------------------------------------------------------------

async function qualityPatternCapture(compressed, projectPath) {
  log('Phase 2e: Quality pattern capture');

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

  const systemPrompt = `You are analyzing a Claude Code session transcript for recurring quality patterns — issues, gaps, friction, or anti-patterns that surface during any kind of work (coding, debugging, planning, auditing, reviewing). Identify patterns worth learning from: things that keep going wrong, systematic gaps, workflow friction, or quality issues that a team member should watch for in future sessions. Output as markdown with ## headers for each pattern found. Include **Evidence:** (what you observed) and **Gap:** (what's missing or broken) for each. If no meaningful patterns, output "No recurring patterns detected."`;

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

  try {
    const response = await claudeCall(
      `You are checking whether a Claude Code session established a NEW reusable methodology — a new skill, convention, workflow pattern, or rule that should be codified for future sessions. Routine work, bug fixes, and using existing patterns do NOT count. Only report genuinely new methodology that was created or established in this session.

VERIFICATION REQUIREMENT: a methodology only counts if the session produced a durable artifact for it — a file that was created or edited to encode the methodology (a SKILL.md, a rules file, a convention doc, a template). Merely discussing or following a pattern does not count. Name the artifact path relative to the project root.

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

async function upstreamFriction(compressed, project, threadIds = []) {
  const projectPath = project.path;
  log('Phase 2g: Upstream friction');

  const systemPrompt = `You are analyzing a Claude Code session transcript for friction with Claude Code itself (bugs, limitations, confusing behavior, missing features, workarounds). Only report genuine CC friction, not user errors or project-specific issues.

If friction found, output JSON: [{"title":"short title","description":"what happened","severity":"high|medium|low"}]
If NO friction found, output exactly: []

Be conservative. False positives waste time. Output ONLY the JSON array.`;

  try {
    const response = await claudeCall(systemPrompt, recentSlice(compressed, 40000));
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

    for (const item of frictionItems) {
      createItem({
        project: project.name,
        project_path: projectPath,
        ...(project.unresolved ? { project_unresolved: true } : {}),
        category: 'upstream-friction',
        urgency: item.severity === 'high' ? 'urgent' : 'normal',
        title: item.title,
        summary: item.description,
        context_anchor: 'ring3-close friction scan',
        evidence: { severity: item.severity },
        filed_by: 'ring3-close',
        thread_ids: threadIds,
      });
    }

    log(`Phase 2g: ${frictionItems.length} upstream friction item(s) queued`);
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
          session_id: sessionId,
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

  const { memoryTitles, threadCursorLines: cursorLines, pendingTitles, resolutionTitles } =
    buildExtractionCorpora(project, { phase: 'Phase 2n' });

  let queued = 0;
  let suppressed = 0;
  for (const f of findings) {
    const fullTitle = `unhandled: ${f.title}`;
    const dup = isDuplicate(fullTitle, f.summary, memoryTitles, pendingTitles,
      { ...resolutionTitles, threadCursorLines: cursorLines });
    if (dup) {
      suppressed++;
      log(`Phase 2n: suppressed "${fullTitle}" — ${dup.corpus} corpus matched "${dup.match}"`);
      recordSuppression({
        project: project.name, corpus: dup.corpus,
        suppressed_title: fullTitle, matched_against: dup.match,
        session_id: sessionId,
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

  const { memoryTitles, threadCursorLines: cursorLines, pendingTitles, resolutionTitles } =
    buildExtractionCorpora(project, { phase: 'Phase 2o' });

  let queued = 0;
  let suppressed = 0;
  for (const f of findings) {
    const fullTitle = `skill-candidate: ${f.title}`;
    const dup = isDuplicate(fullTitle, f.summary, memoryTitles, pendingTitles,
      { ...resolutionTitles, threadCursorLines: cursorLines });
    if (dup) {
      suppressed++;
      log(`Phase 2o: suppressed "${fullTitle}" — ${dup.corpus} corpus matched "${dup.match}"`);
      recordSuppression({
        project: project.name, corpus: dup.corpus,
        suppressed_title: fullTitle, matched_against: dup.match,
        session_id: sessionId,
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

function writeHealth(sessionId, stats, project) {
  log('Phase 2l: Health');

  const healthPath = join(WATCHTOWER_DIR, 'state', 'ring3-health.json');
  const health = {
    schema_version: 1,
    last_run: new Date().toISOString(),
    session_id: sessionId,
    items_filed: stats.itemsFiled || 0,
    actions_closed: stats.actionsClosed || 0,
    status: 'success',
  };
  // Fail loud, never silently: an unresolvable project identity is the
  // anomaly that used to hide behind the basename fallback.
  if (project?.unresolved) {
    health.warnings = [
      `project identity unresolved: filed under "${project.name}" (${project.path}) with project_unresolved`,
    ];
  }

  atomicWrite(healthPath, JSON.stringify(health, null, 2) + '\n');
  log('Phase 2l: ring3-health.json written');
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

// ---------------------------------------------------------------------------
// Resolve project from CWD
// ---------------------------------------------------------------------------

// Thin wrapper over the canonical resolver in watchtower-lib. The old local
// implementation matched cwd.startsWith(configPath) and silently fell back to
// basename(cwd) — every mux worktree session failed the match and filed all
// its output under a phantom project the readers never looked up.
//
// Outcomes:
//   registered/benign — the resolver named a real main repo; file under it.
//   unresolved        — no repo root derivable (anomalous). We still file
//     (never drop work), under basename(cwd), but every item carries
//     project_unresolved: true and ring3 health gets a warning — fail loud,
//     never silently.
function resolveProject(cwd, config) {
  const identity = resolveProjectIdentity(cwd, config);
  if (identity) return identity;

  const dirName = basename(cwd);
  return {
    name: dirName, path: cwd, slug: slugify(dirName),
    registered: false, unresolved: true,
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

  // Dedup: one coverage-warning per session.
  const dup = listPendingItems({ project: project.name, category: 'coverage-warning' })
    .some((i) => i.evidence?.session_id === sessionId);
  if (dup) return { filed: 0 };

  const shown = uiPaths.slice(0, 3).join(', ')
    + (uiPaths.length > 3 ? `, +${uiPaths.length - 3} more` : '');
  file({
    project: project.name,
    project_path: project.path,
    filed_by: 'ring3-close',
    category: 'coverage-warning',
    urgency: 'low',
    title: 'UI shipped this session without a scenario update',
    summary: `This session changed UI (${shown}) but touched no .feature file — `
      + `drift risk: the product changed, the walkthrough scenarios didn't. Run `
      + `/verify update to propose the matching scenario edits, or accept the drift `
      + `if the change isn't user-visible.`,
    context_anchor: `git log --since session start (${sessionId})`,
    evidence: { session_id: sessionId, ui_paths: uiPaths, session_start: sessionStartIso },
    options: [
      { value: 'update', label: 'Update scenarios', description: '/verify update' },
      { value: 'accept-drift', label: 'Accept drift', description: 'Not user-visible' },
      { value: 'dismiss', label: 'Dismiss', description: 'Not worth capturing' },
    ],
  });
  return { filed: 1 };
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

  // Resolve project
  const project = resolveProject(args.cwd || process.cwd(), config);
  log(`Project: ${project.name} (${project.path})`);
  if (project.unresolved) {
    logError(`Project identity UNRESOLVED for cwd ${args.cwd || process.cwd()} — filing under "${project.name}" with project_unresolved`);
  }

  // --- Phase 2a: Worktree check (pre-transcript, pure git, zero cost) ---
  let worktreeItemsFiled = 0;
  try {
    worktreeItemsFiled = worktreeCheck(args.cwd, project);
  } catch (e) {
    logError(`Phase 2a failed: ${e.message}`);
  }

  // --- Preprocessing ---
  const { compressed, originalTokenEstimate, compressedTokenEstimate } = preprocessTranscript(args.transcriptPath);

  if (!compressed || compressed.trim().length === 0) {
    log('Empty transcript after preprocessing, exiting');
    markProcessed(args.sessionId, { empty: true, worktreeItemsFiled });
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
  try {
    summary = await sessionSummary(compressed, project.slug, args.sessionId);
  } catch (e) {
    logError(`Phase 2b failed: ${e.message}`);
  }

  // Phase 2b2: Thread capture
  let threadIds = [];
  try {
    threadIds = await threadCapture(compressed, project.slug, args.sessionId, summary, args.transcriptPath);
    stats.threadsUpdated = threadIds.length;
  } catch (e) {
    logError(`Phase 2b2 failed: ${e.message}`);
  }

  // Phase 2c: Work item closure
  try {
    const result = await workItemClosure(compressed, project, threadIds);
    stats.actionsClosed = result.closed;
    stats.actionsQueued += result.queued;
    stats.itemsFiled += result.queued;
  } catch (e) {
    logError(`Phase 2c failed: ${e.message}`);
  }

  // Phase 2d: Decision/lesson extraction
  try {
    const result = await decisionExtraction(compressed, project, args.sessionId, args.transcriptPath, threadIds);
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
        project, sessionStartFromTranscript(args.transcriptPath), args.sessionId);
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

  // Phase 2l: Health
  try {
    writeHealth(args.sessionId, stats, project);
  } catch (e) {
    logError(`Phase 2l failed: ${e.message}`);
  }

  // Step 4: Mark processed (prevent re-processing)
  markProcessed(args.sessionId, stats);

  const duration = Date.now() - startTime;
  log(`Ring 3 close complete in ${duration}ms. Actions closed: ${stats.actionsClosed}, items filed: ${stats.itemsFiled}, memory written: ${stats.memoryWritten}, threads updated: ${stats.threadsUpdated || 0}`);
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
  main().catch(e => {
    logError(`Fatal: ${e.message}`);
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
  modelRescues,
  chunkWithOverlap,
  mergeChunkExtractions,
  decisionExtraction,
  resolutionCorpus,
  threadCursorLines,
  buildExtractionCorpora,
  completionReviewEmitGuard,
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
};

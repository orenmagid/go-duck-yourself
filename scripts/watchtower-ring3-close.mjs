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
//   2h: Feedback outbox flush — deliver pending feedback items
//   2i: Session auto-naming — generate descriptive session name
//   2j: Consumer hooks — ring3-close-post hooks
//   2k: Signal Ring 2 — write fast-trigger lock
//   2l: Health — write ring3-health.json
//
// All phases are structurally unskippable. Consumer hooks extend, not replace.

import {
  readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync,
  renameSync, statSync,
} from 'fs';
import { join, basename, dirname } from 'path';
import { execSync } from 'child_process';
import { createRequire } from 'module';
import { homedir } from 'os';
import {
  atomicWrite, loadConfig, slugify,
  log as _log, logError as _logError,
  getWatchtowerDir, createItem, listPending, loadBetterSqlite3,
  migrateThreadCursor, currentCursor,
} from './watchtower-lib.mjs';

const require = createRequire(import.meta.url);

const WATCHTOWER_DIR = getWatchtowerDir();

const CLAUDE_HOME = join(homedir(), '.claude');
const CONSUMER_HOOK_TIMEOUT_MS = 120_000;
const MODEL = 'claude-sonnet-4-6';

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

function worktreeCheck(cwd, projectPath) {
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

  // Exclude CC/mux session artifacts (.claude/, .mcp.json) and
  // node_modules — untracked in every mux worktree; counting it
  // produced false "unmerged work" alarms for fully-merged branches.
  // safeExec trims output, which can shift porcelain column offsets —
  // match artifact patterns anywhere in the line instead
  const uncommitted = safeExec('git status --porcelain', { cwd });
  const uncommittedCount = uncommitted
    ? uncommitted.split('\n').filter(l => {
        if (!l.trim()) return false;
        if (/\s\.claude$/.test(l) || /\s\.claude\//.test(l)) return false;
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

  // Attribute the item to the MAIN repo root, not the worktree — for
  // worktree sessions projectPath is often the session cwd (the worktree
  // itself), and items attributed to a worktree path can never be claimed
  // by Ring 1's per-project auto-resolve pass. The worktree's
  // --git-common-dir points at the main repo's .git.
  const commonDir = safeExec(
    'git rev-parse --path-format=absolute --git-common-dir',
    { cwd }
  );
  const mainRoot = commonDir ? dirname(commonDir) : projectPath;
  const projectName = basename(mainRoot);

  createItem({
    project: projectName,
    project_path: mainRoot,
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
  const bullets = response.trim();

  // Write per-session file to state/projects/<slug>/sessions/<date>-<session-id>.md
  const sessionsDir = join(WATCHTOWER_DIR, 'state', 'projects', projectSlug, 'sessions');
  mkdirSync(sessionsDir, { recursive: true });

  const date = new Date().toISOString().slice(0, 10);
  const sessionFile = join(sessionsDir, `${date}-${sessionId}.md`);
  const content = `# Session ${sessionId}\n\nDate: ${new Date().toISOString()}\n\n${bullets}\n`;
  atomicWrite(sessionFile, content);

  // Also update the project-level state file with the latest session pointer
  const projectStatePath = join(WATCHTOWER_DIR, 'state', 'projects', `${projectSlug}.md`);
  if (existsSync(projectStatePath)) {
    let stateContent = readFileSync(projectStatePath, 'utf8');
    const sessionHeader = '## Last Session';
    const headerIdx = stateContent.indexOf(sessionHeader);
    const replacement = `${sessionHeader}\n_${date} (${sessionId})_\n${bullets}\n`;

    if (headerIdx >= 0) {
      const afterHeader = stateContent.indexOf('\n## ', headerIdx + sessionHeader.length);
      const endIdx = afterHeader > 0 ? afterHeader : stateContent.length;
      stateContent = stateContent.slice(0, headerIdx) + replacement + stateContent.slice(endIdx);
    } else {
      stateContent = stateContent.trimEnd() + `\n\n${replacement}`;
    }
    atomicWrite(projectStatePath, stateContent);
  }

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
    threadIds.push(threadSlug);
    const threadPath = join(threadsDir, `${threadSlug}.json`);

    // One cursor snapshot per session that advanced this thread — appended,
    // never overwritten (see migrateThreadCursor / cursor_history in
    // watchtower-lib.mjs). The qa_handoff payload is a future SIBLING of this
    // history, not a field inside the cursor (qa-handoff-protocol.md).
    const cursorEntry = { date, session_id: sessionId, cursor: t.cursor };

    let threadData;
    if (existsSync(threadPath) && !t.is_new) {
      threadData = JSON.parse(readFileSync(threadPath, 'utf8'));
      // Heal pre-versioning + legacy single-cursor files into cursor_history
      // (watchtower-contracts.md §Schema Versioning).
      migrateThreadCursor(threadData);
      threadData.cursor_history.push(cursorEntry);
      if (t.display_name) threadData.display_name = t.display_name;
      threadData.last_updated = now;
      threadData.sessions.push({
        id: sessionId,
        contribution: t.contribution || '',
        date,
        project: projectSlug,
        summary: summary.split('\n').slice(0, 3).join(' ').slice(0, 200),
        transcript: transcriptPath,
      });
    } else {
      threadData = {
        schema_version: 2,
        thread: threadSlug,
        display_name: t.display_name || threadSlug,
        cursor_history: [cursorEntry],
        sessions: [{
          id: sessionId,
          contribution: t.contribution || '',
          date,
          project: projectSlug,
          summary: summary.split('\n').slice(0, 3).join(' ').slice(0, 200),
          transcript: transcriptPath,
        }],
        related_fids: [],
        last_updated: now,
        status: 'active',
      };
    }

    atomicWrite(threadPath, JSON.stringify(threadData, null, 2));
    log(`Phase 2b2: Thread ${threadSlug} ${t.is_new ? 'created' : 'updated'}`);
  }

  return threadIds;
}

// ---------------------------------------------------------------------------
// Phase 2c: Work item closure
// ---------------------------------------------------------------------------

async function workItemClosure(compressed, projectPath, threadIds = []) {
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

  const userMessage = `Open actions:\n${actionList}\n\nSession transcript:\n${compressed.slice(0, 50000)}`;

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

  for (const evalItem of evaluations) {
    if (!evalItem.fid || evalItem.confidence === 'none') continue;

    const urgencyMap = { high: 'urgent', medium: 'normal', low: 'low' };
    try {
      const projectName = basename(projectPath);
      createItem({
        project: projectName,
        project_path: projectPath,
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
  log(`Phase 2c: ${queued} completion candidates queued for review`);
  return { closed: 0, queued };
}

// ---------------------------------------------------------------------------
// Dedup helpers — check memory index and pending inbox before filing
// ---------------------------------------------------------------------------

function loadMemoryIndex(projectPath) {
  const encoded = projectPath.replace(/\//g, '-');
  const memDir = join(homedir(), '.claude', 'projects', encoded, 'memory');
  const indexPath = join(memDir, 'MEMORY.md');
  if (!existsSync(indexPath)) return [];
  try {
    const content = readFileSync(indexPath, 'utf8');
    return content.split('\n')
      .filter(line => line.startsWith('- '))
      .map(line => line.toLowerCase());
  } catch { return []; }
}

function tokenize(text) {
  return text.toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 3);
}

function isDuplicate(title, content, memoryLines, pendingTitles) {
  const titleTokens = tokenize(title);
  const contentTokens = tokenize(content).slice(0, 10);
  const allTokens = [...new Set([...titleTokens, ...contentTokens])];
  if (allTokens.length === 0) return false;

  // Check against memory index lines
  for (const line of memoryLines) {
    const matchCount = allTokens.filter(t => line.includes(t)).length;
    if (matchCount >= Math.min(3, allTokens.length)) return true;
  }

  // Check against already-pending inbox items
  for (const pending of pendingTitles) {
    const pendingTokens = tokenize(pending);
    const overlap = allTokens.filter(t => pendingTokens.includes(t)).length;
    if (overlap >= Math.min(3, allTokens.length)) return true;
  }

  return false;
}

// ---------------------------------------------------------------------------
// Phase 2d: Knowledge extraction → inbox
// ---------------------------------------------------------------------------

async function decisionExtraction(compressed, projectPath, sessionId, transcriptPath, threadIds = []) {
  log('Phase 2d: Knowledge extraction');

  const systemPrompt = `You are extracting decisions, constraints, lessons, and user preferences from a Claude Code session transcript. For each item found, classify its home:

- "memory" = a lesson, preference, or constraint worth remembering across sessions
- "claude-md" = a convention or rule that should be added to CLAUDE.md
- "pib-db-trigger" = a deferred action with a trigger condition
- "upstream-feedback" = friction with Claude Code itself

Only extract items that represent NEW durable knowledge — things learned or decided in this session that aren't yet captured. Skip items that are routine, obvious, or just restating existing conventions.

Do NOT extract transient operational state: project completion status ("X has 0 open actions"), branch merge status ("branch Y was merged"), install success confirmations ("all rings working"), or other point-in-time observations that will be stale within days. These belong in state files, not the inbox.

For each item, assess how time-sensitive routing is. Urgency means HOW FAST THE VALUE DECAYS if not routed — it is NOT importance:
- "urgent" = the value evaporates within days if not routed (a trigger condition about to fire, a constraint someone will trip over THIS WEEK, a decision another active session needs right now). Apply the time-decay test: "if this sits in the inbox for a week, is most of its value gone?" If no, it is not urgent.
- "normal" = worth routing but the value keeps (most decisions and constraints)
- "low" = interesting but can wait indefinitely

Lessons and preferences are durable knowledge — their value does not decay. They are almost NEVER urgent, no matter how important they are. An important-but-durable item is "normal".

Output JSON array: [{"type":"decision|constraint|lesson|preference","home":"memory|claude-md|pib-db-trigger|upstream-feedback","urgency":"urgent|normal|low","title":"short title","content":"detailed description"}]
Output ONLY the JSON array, no other text. If nothing found, output [].`;

  let extractions = [];
  try {
    const response = await claudeCall(systemPrompt, compressed.slice(0, 50000));
    const jsonMatch = response.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      extractions = JSON.parse(jsonMatch[0]);
    }
  } catch (e) {
    logError(`Phase 2d: Claude extraction failed: ${e.message}`);
    return { autoWritten: 0, queued: 0 };
  }

  if (extractions.length === 0) {
    log('Phase 2d: No knowledge extracted');
    return { autoWritten: 0, queued: 0 };
  }

  // Load existing context for dedup
  const memoryLines = loadMemoryIndex(projectPath);
  const pending = listPending({ project: basename(projectPath) });
  const pendingTitles = pending.map(p => p.title);

  let queued = 0;
  let deduped = 0;

  for (const item of extractions) {
    const fullTitle = `${item.type}: ${item.title}`;

    if (isDuplicate(fullTitle, item.content || '', memoryLines, pendingTitles)) {
      deduped++;
      continue;
    }

    try {
      const projectName = basename(projectPath);
      const isMemory = item.home === 'memory';
      createItem({
        project: projectName,
        project_path: projectPath,
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
    ? `Session transcript:\n${compressed.slice(0, 30000)}\n\nAudit triage history:\n${triageHistory.slice(0, 10000)}`
    : `Session transcript:\n${compressed.slice(0, 30000)}`;

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

async function methodologyCapture(compressed, projectPath, threadIds = []) {
  log('Phase 2f: Methodology capture');

  try {
    const response = await claudeCall(
      `You are checking whether a Claude Code session established a NEW reusable methodology — a new skill, convention, workflow pattern, or rule that should be codified for future sessions. Routine work, bug fixes, and using existing patterns do NOT count. Only report genuinely new methodology that was created or established in this session.

VERIFICATION REQUIREMENT: a methodology only counts if the session produced a durable artifact for it — a file that was created or edited to encode the methodology (a SKILL.md, a rules file, a convention doc, a template). Merely discussing or following a pattern does not count. Name the artifact path relative to the project root.

If a new methodology was established, output JSON: {"found": true, "title": "short description", "content": "what the methodology is and how to apply it", "artifact_path": "relative/path/to/the/file"}
If nothing new was established, output JSON: {"found": false}
Output ONLY the JSON object.`,
      compressed.slice(0, 30000),
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

    const projectName = basename(projectPath);
    createItem({
      project: projectName,
      project_path: projectPath,
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

async function upstreamFriction(compressed, projectPath, threadIds = []) {
  log('Phase 2g: Upstream friction');

  const systemPrompt = `You are analyzing a Claude Code session transcript for friction with Claude Code itself (bugs, limitations, confusing behavior, missing features, workarounds). Only report genuine CC friction, not user errors or project-specific issues.

If friction found, output JSON: [{"title":"short title","description":"what happened","severity":"high|medium|low"}]
If NO friction found, output exactly: []

Be conservative. False positives waste time. Output ONLY the JSON array.`;

  try {
    const response = await claudeCall(systemPrompt, compressed.slice(0, 40000));
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
      const projectName = basename(projectPath);
      createItem({
        project: projectName,
        project_path: projectPath,
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
// Phase 2h: Feedback outbox flush
// ---------------------------------------------------------------------------

async function feedbackOutboxFlush(projectPath) {
  log('Phase 2h: Feedback outbox flush');

  const outboxPath = join(projectPath, 'cc-feedback-outbox.json');
  if (!existsSync(outboxPath)) {
    log('Phase 2h: No feedback outbox found');
    return;
  }

  let outbox;
  try {
    outbox = JSON.parse(readFileSync(outboxPath, 'utf8'));
  } catch (e) {
    logError(`Phase 2h: Cannot parse outbox: ${e.message}`);
    return;
  }

  if (!Array.isArray(outbox) || outbox.length === 0) {
    log('Phase 2h: Outbox empty');
    return;
  }

  const undelivered = outbox.filter(item => !item.delivered);
  if (undelivered.length === 0) {
    log('Phase 2h: No undelivered feedback items');
    return;
  }

  let delivered = 0;
  for (const item of undelivered) {
    // Mark as delivered (actual delivery mechanism depends on transport config)
    item.delivered = true;
    item.delivered_at = new Date().toISOString();
    item.delivered_by = 'ring3-close';
    delivered++;
  }

  // Atomic rewrite
  atomicWrite(outboxPath, JSON.stringify(outbox, null, 2) + '\n');
  log(`Phase 2h: ${delivered} feedback item(s) marked as delivered`);
}

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

function writeHealth(sessionId, stats) {
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

function resolveProject(cwd, config) {
  const projects = config.projects || {};

  for (const [name, proj] of Object.entries(projects)) {
    const projPath = typeof proj === 'string' ? proj : proj?.path;
    if (projPath && cwd.startsWith(projPath)) {
      return { name, path: projPath, slug: slugify(name) };
    }
  }

  // Fallback: use directory name
  const dirName = basename(cwd);
  return { name: dirName, path: cwd, slug: slugify(dirName) };
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

  // --- Phase 2a: Worktree check (pre-transcript, pure git, zero cost) ---
  let worktreeItemsFiled = 0;
  try {
    worktreeItemsFiled = worktreeCheck(args.cwd, project.path);
  } catch (e) {
    logError(`Phase 2a failed: ${e.message}`);
  }

  // --- Preprocessing ---
  const { compressed, originalTokenEstimate, compressedTokenEstimate } = preprocessTranscript(args.transcriptPath);

  if (!compressed || compressed.trim().length === 0) {
    log('Empty transcript after preprocessing, exiting');
    markProcessed(args.sessionId, { empty: true, worktreeItemsFiled });
    writeHealth(args.sessionId, { itemsFiled: worktreeItemsFiled, actionsClosed: 0 });
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
    const result = await workItemClosure(compressed, project.path, threadIds);
    stats.actionsClosed = result.closed;
    stats.actionsQueued += result.queued;
    stats.itemsFiled += result.queued;
  } catch (e) {
    logError(`Phase 2c failed: ${e.message}`);
  }

  // Phase 2d: Decision/lesson extraction
  try {
    const result = await decisionExtraction(compressed, project.path, args.sessionId, args.transcriptPath, threadIds);
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
      await methodologyCapture(compressed, project.path, threadIds);
    } catch (e) {
      logError(`Phase 2f failed: ${e.message}`);
    }
  }

  // Phase 2g: Upstream friction (feature-flagged)
  if (config.defaults?.upstream_friction_detection !== false) {
    try {
      await upstreamFriction(compressed, project.path, threadIds);
    } catch (e) {
      logError(`Phase 2g failed: ${e.message}`);
    }
  }

  // Phase 2h: Feedback outbox flush
  try {
    await feedbackOutboxFlush(project.path);
  } catch (e) {
    logError(`Phase 2h failed: ${e.message}`);
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

  // Phase 2k: Signal Ring 2
  try {
    signalRing2();
  } catch (e) {
    logError(`Phase 2k failed: ${e.message}`);
  }

  // Phase 2l: Health
  try {
    writeHealth(args.sessionId, stats);
  } catch (e) {
    logError(`Phase 2l failed: ${e.message}`);
  }

  // Step 4: Mark processed (prevent re-processing)
  markProcessed(args.sessionId, stats);

  const duration = Date.now() - startTime;
  log(`Ring 3 close complete in ${duration}ms. Actions closed: ${stats.actionsClosed}, items filed: ${stats.itemsFiled}, memory written: ${stats.memoryWritten}, threads updated: ${stats.threadsUpdated || 0}`);
}

main().catch(e => {
  logError(`Fatal: ${e.message}`);
  process.exit(1);
});

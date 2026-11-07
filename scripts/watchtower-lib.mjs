#!/usr/bin/env node

// Watchtower shared library.
// Canonical implementations of utilities used across all ring scripts.
// Ring scripts import from here instead of maintaining local copies.

import {
  readFileSync, writeFileSync, existsSync, appendFileSync,
  mkdirSync, renameSync, realpathSync, readdirSync,
} from 'fs';
import { join, dirname, basename, resolve as resolvePath } from 'path';
import { homedir } from 'os';
import { createRequire } from 'module';
import { execFileSync } from 'child_process';
import { pathToFileURL } from 'url';

const require = createRequire(import.meta.url);

// Re-export queue operations so ring scripts have a single import source.
export { createItem, listPending, getItem, resolveItem } from './watchtower-queue.mjs';
// Simplification Project 2, plan A9 (act:73dd31d4): the identity helpers and
// the redirect writers are DEFINED in the queue (lib imports the queue, never
// the reverse) and re-exported here so every importer keeps one name.
export {
  slugify, resolveCcSourceRepo,
  RETIRED_CATEGORIES, NARROWED_CATEGORIES, redirectHomeFor,
  carryLedgerPath, appendCarryEntry, readCarry, markCarryShown, resolveCarryEntry,
  CARRY_KINDS, CARRY_PERSISTENT_KINDS,
  feedbackOutboxPath, appendFeedbackOutboxEntry, frictionOutboxEntry, redirectLedgerPath,
  findFeedbackDuplicateCandidates,
  docDriftStatePath, readDocDrift, upsertDocDriftEntry, retractDocDriftEntries,
  appendThreadProposal,
} from './watchtower-queue.mjs';
import { slugify, resolveCcSourceRepo, appendCarryEntry } from './watchtower-queue.mjs';
// The knowledge-extraction four-axis vocabulary (act:471dd701) — defined in
// watchtower-queue.mjs (createItem's boundary check needs it with zero
// imports; this file already imports FROM queue.mjs, so the reverse would
// be a cycle), re-exported here for Ring 3 / Ring 2 / /inbox consumers.
export { KNOWLEDGE_EXTRACTION_TYPES, KNOWLEDGE_EXTRACTION_HOMES } from './watchtower-queue.mjs';
import { resolveItem as queueResolveItem, getItem as queueGetItem, listItems as queueListItems } from './watchtower-queue.mjs';

// ---------------------------------------------------------------------------
// autoReconcileItem — the ONE resolution path for machine (ring) retractions
// ---------------------------------------------------------------------------
//
// Detector-symmetry contract (watchtower-contracts.md "Detector Symmetry"):
// every auto-retraction/reconciliation a ring performs resolves through this
// helper so the stamping convention cannot drift across reconcilers —
// `resolution_type: 'auto-reconciled'` (the machine/human discriminator the
// cross-ring reader's engagement bucketing keys on: cron activity must never
// masquerade as operator engagement) plus `evidence.actor` and any
// evidence the caller supplies naming WHY (action fid / branch), so a
// semantically-wrong close stays traceable to its mechanical trigger.
//
// The queue API has no evidence-mutating verb, so the evidence merge is a
// read-modify-write against the item file after resolveItem's own atomic
// write. The path spelling below (queue/items/<id>.json under the watchtower
// dir) MIRRORS watchtower-queue.mjs's QUEUE_DIR — keep them in step; the
// hermetic parity tests (branch-diverged-reconcile.test.mjs, "autoReconcileItem
// resolves through the real queue…") round-trip through the real queue so a
// drifted spelling fails loudly. A failed evidence stamp is
// logged and tolerated: the load-bearing half (typed resolution) already
// landed atomically.

export function autoReconcileItem(id, { resolution, notes, actor = 'ring1', evidence = {} }) {
  const updated = queueResolveItem(id, {
    resolution,
    resolution_notes: notes,
    resolution_type: 'auto-reconciled',
  });
  if (!updated) return null; // not pending — someone (human) got there first
  const fp = join(getWatchtowerDir(), 'queue', 'items', `${id}.json`);
  try {
    const item = JSON.parse(readFileSync(fp, 'utf8'));
    item.evidence = { ...(item.evidence || {}), actor, ...evidence };
    atomicWrite(fp, item);
    return item;
  } catch (e) {
    logError('lib', `autoReconcileItem: resolved ${id} but could not stamp evidence.actor: ${e.message}`);
    return updated;
  }
}

// ---------------------------------------------------------------------------
// INBOX_DETECTOR_REGISTRY — the detector-symmetry ledger
// ---------------------------------------------------------------------------
//
// The Detector Symmetry contract (watchtower-contracts.md): every ring-filed
// inbox category declares its RECONCILER (the mechanical per-tick retraction
// + its filing exclusions) or an explicit EXEMPT rationale — exactly one —
// or, once its producers are deleted, RETIRED: { date, reason } (Simplification
// Project 1, plan A4). A retired entry stays so historical items remain
// readable, and the structural test asserts it has NO live filing site.
// The structural test (templates/scripts/__tests__/detector-registry.test.mjs)
// scans the `category:` literals at createItem filing sites across ring1,
// ring2, and ring3-close and fails the suite when a filed category is
// missing here (or a registry entry goes stale) — a new detector without a
// declared retraction path breaks the build, not the operator's trust
// (act:4d11fd53; four detectors shipped the same one-way-queue bug before
// the rule was encoded).
//
// "candidate future reconciler" notes inside exempt rationales are honest
// markers, not commitments — promoting one moves the entry to `reconciler`.
//
// Every entry ALSO carries `label` — the plain-English name an operator-facing
// surface uses for the category (Simplification Project 2, plan B2 / T16;
// act:faf91294). The registry is the one place every category is already
// listed, so it is the one place the name lives; the session-start block's
// "oldest here: <label>, <N> days" line reads it (fallback: the raw key).
// The snapshot's own CATEGORY_LABEL map in watchtower-snapshot.mjs is older
// and incomplete; folding it onto this field is Project 4's vocabulary
// cleanup, not this lane's.
//
// Every LIVE entry ALSO declares `reader` and `exit` (Simplification Project
// 2, plan B1 / Decision 5; act:fcb42cdd). Expiry was deleted outright (A1):
// nothing leaves the inbox by age, so every category must say who acts on it
// and how it leaves. `reader` — who acts and where they see it. `exit` —
// `{ mechanical: '<function> (<ring>): <condition>' }` (a retraction the code
// performs on an event) and/or `{ operator: '<verbs>' }`; never an age (age
// shows on the start block's oldest line, it never resolves). The structural
// test enforces both, pins the live set, and requires every mechanical
// function to exist in a ring script; B6's growth alarm reads the declared
// exits. These fields sit BESIDE reconciler | exempt — the detector-symmetry
// trichotomy is unchanged. `filed_outside_rings: true` marks a live category
// a skill files rather than a ring (qa-handoff), so the census does not
// demand a ring filing site for it.

// One reader for every live category after Project 2: the operator, on the
// three surfaces that show pending items (the start block names this
// project's oldest, with age).
const OPERATOR_READER = 'operator — /inbox, /briefing, start-block oldest line';

export const INBOX_DETECTOR_REGISTRY = {
  // ring1-filed
  'branch-diverged': {
    label: 'diverged branch',
    reader: OPERATOR_READER,
    exit: {
      mechanical: 'autoResolveBranchDivergedItems (ring1): the branch is verifiably gone, '
        + 'has no unmerged content (squash-aware), or its name became excluded',
      operator: 'merge or delete the branch, or dismiss in /inbox',
    },
    reconciler: 'autoResolveBranchDivergedItems (ring1): retracts on verified-gone '
      + '(membership in a successful branch listing), squash-aware no-unmerged-content, '
      + 'or excluded name; filing exclusion: defaults.long_lived_branches '
      + '(staging/production exact, backup/* glob), gated at the filing site so '
      + 'dismissal cannot reopen the loop (act:98649971)',
  },
  'worktree-unmerged': {
    label: 'unmerged worktree',
    reader: OPERATOR_READER,
    exit: {
      mechanical: 'autoResolveWorktreeItems (ring1): the branch is verifiably gone, or it '
        + 'has no unmerged content and no real uncommitted changes',
      operator: 'merge or remove the worktree, or dismiss in /inbox',
    },
    reconciler: 'autoResolveWorktreeItems (ring1): retracts on verified-gone branch or '
      + 'no-unmerged-content + no-real-uncommitted; fileOrphanedWorktreeItem supersedes '
      + 'on register change (unmerged ↔ uncommitted-only); filing exclusion: '
      + 'generated-state classification (GENERATED_STATE_PATTERNS, act:c008862c)',
  },
  // ring3-filed, ring1-reconciled
  'completion-review': {
    label: 'finished action to confirm',
    reader: OPERATOR_READER,
    exit: {
      mechanical: 'autoReconcileCompletionReviews (ring1): the referenced action is '
        + 'verifiably closed in its own project db',
      operator: 'complete the action, or dismiss in /inbox',
    },
    reconciler: 'autoReconcileCompletionReviews (ring1): resolves when the referenced '
      + 'action (plan_fid || evidence.fid) is verifiably closed in its OWN project db; '
      + 'fid-not-found keeps (flow vestigial-db + misattribution protection). Filing '
      + 'exclusions (act:ea23b3a5, all at ring3 Phase 2c): (a) confidence \'high\' is '
      + 'SUPPRESSED — measured anti-predictive over all 223 items ever filed (high 30% '
      + 'precision, medium+low 60%); (b) act:9eebbac4\'s create-vs-complete day-window '
      + 'filter is DELETED — it exempted \'high\' from the born-this-session skip, so it '
      + 'suppressed the best cohort (same-day + medium/low, 74%) and passed the worst, '
      + 'and the category filed 0 true positives in the 23 items after it shipped; (c) '
      + 'per-fid dedup across pending + resolved/dismissed/EXPIRED/SUPERSEDED. The last '
      + 'two statuses are load-bearing HISTORY: until Simplification Project 2 (A1, '
      + '2026-10-30) deleted expiry, this category expired at 14d — equal to '
      + 'COMPLETION_REVIEW_DEDUP_DAYS — so without them a fid left every corpus exactly '
      + 'when its item expired and refiled forever; the items already stamped expired or '
      + 'superseded still exist, so the corpus keeps reading both. A '
      + 'quoted-completion-evidence check runs as INSTRUMENTATION ONLY (evidence.'
      + 'quote_verified) and gates nothing — the transcript haystack is JSON-serialized, '
      + 'so a substring gate rejects real prose and accepts serialized tool arguments.',
  },
  // ring3-filed until Simplification Project 2 (prj:4a433dac), lane B3-residue
  // (act:73dd31d4, plan A3, 2026-10-31): the five RESIDUE categories left the
  // inbox. The question each one asked — "did this session leave a loose
  // end / a repeated manual procedure / a UI change with no scenario / a new
  // methodology / an action with no verify plan?" — is asked at /close
  // (Step 6c, one plain-text message) by the session that has the context;
  // Ring 3 keeps 2n/2o/2q only as the fallback for a session that never
  // reached /close and writes them to the SESSION RECORD, never the queue.
  // A stray filing into any of them is REDIRECTED by createItem to the
  // project's carry ledger (watchtower-queue RETIRED_CATEGORIES → 'carry').
  'coverage-warning': {
    label: 'UI change with no scenario',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A3: Phase 2q\'s filing site deleted with the '
        + 'standing-debt aggregation (act:e888dd63 — ONE accumulating item per project, '
        + 'evidence.first_seen = oldest, urgency by session count) that was its declared '
        + 'exempt semantics. The lens now appends one "UI paths changed with no .feature '
        + 'edit" line to the session record\'s ## Loose ends on the no-receipt branch; '
        + '/close Step 6c asks the session itself. See '
        + '.claude/plans/simplification-2-inbox.md.',
      moved_to: 'session record ## Loose ends (Ring 3 fallback) / the /close residue '
        + 'question; a stray filing → the project\'s carry ledger',
    },
  },
  'knowledge-extraction': {
    label: 'knowledge to review',
    retired: {
      date: '2026-09-29',
      reason: 'Project 1 (prj:a4c6052b): producers deleted (Ring 3 Phase 2d, the Phase 2g '
        + 'durable arm, Ring 2 cross-project synthesis); the ~1,180 pending + expired items '
        + 'are archived to runtime state by plan C1 and closed resolution_type \'archived\'; '
        + 'see .claude/plans/simplification-1-memory-retrieval.md. Was exempt: pure '
        + 'extraction — no world-state to re-check. The entry stays so historical items '
        + 'remain readable and the census can prove nothing files it.',
    },
  },
  'methodology-capture': {
    label: 'methodology record proposal',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A3: Phase 2f (the methodology detector, '
        + 'verification-gated since act:3f3f9a31 — 11 of 11 unverified captures were '
        + 'dismissed) is DELETED outright; one less model call per close. The methodology '
        + 'question is one line of the /close Step 6c residue check, and an accepted line '
        + 'runs the debrief methodology-capture phase\'s write path in-session. Was exempt: '
        + 'pure extraction. See .claude/plans/simplification-2-inbox.md.',
      moved_to: '/close Step 6c (write methodology record); a stray filing → the '
        + 'project\'s carry ledger',
    },
  },
  'upstream-friction': {
    label: 'friction report',
    reader: OPERATOR_READER,
    exit: { operator: 'make the config change, or dismiss in /inbox' },
    exempt: 'NARROWED to config-change friction (Project 2, prj:4a433dac, plan A5 / '
      + 'Decision 2, 2026-10-31): a setting, hook or doc only the operator controls — a '
      + 'decision with no mechanical truth condition. Ring 3 Phase 2g (routeFriction) '
      + 'files only this route. The other two routes are DELIVERED, never queued: '
      + 'cc-feedback → one /cc-feedback-shaped entry in ~/.claude/cc-feedback-outbox.json '
      + '(deduplicated against the outbox, feedback/ and feedback/resolved/; Ring 1 '
      + 'delivers it, triage-at-arrival in the CC repo is the human duty); '
      + 'project-tracker → the session record\'s ## Friction section + one friction carry '
      + 'entry, shown every start block in that project until a /close settles it (EP2). '
      + 'A stray non-config-change filing is redirected by createItem '
      + '(NARROWED_CATEGORIES, EP4).',
  },
  'advisor-finding': {
    label: 'advisor finding',
    reader: OPERATOR_READER,
    exit: { operator: 'add the proposed cabinet seat, or dismiss in /inbox' },
    exempt: 'NARROWED to cabinet-seat proposals (Project 2, prj:4a433dac, plan A4 / '
      + 'Decision 4, 2026-10-31): the ONE filer is ring2\'s cabinet-roster review '
      + '(fileRosterFinding, kind uncovered-tech, evidence.roster_kind), which proposes '
      + 'a cabinet seat and is therefore a decision — no mechanical truth condition, '
      + 'one pending per (project, roster kind). Its siblings dormant-skill and '
      + 'stale-briefing were pure measurements and moved to ambient state (act:ea23b3a5; '
      + 'state/roster-review.json → Ring 1 Standing Issues); retireRosterFindingItems '
      + 'supersedes any still pending. Every OTHER advisor finding left the inbox: ring3 '
      + 'Phase 2m and the by-hand /debrief + /orient filers write the session record\'s '
      + '## Advisor findings and one advisor carry entry (shown once in the next start '
      + 'block, read by /briefing and the weekly roundup); a stray non-seat filing is '
      + 'redirected to the carry ledger by createItem (NARROWED_CATEGORIES, EP4). '
      + 'Measured before: 49 filed per 28 days by Ring 3, 0 human-closed.',
  },
  'raised-unhandled': {
    label: 'loose end',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A3 / Decision 3: Phase 2n\'s filing site is '
        + 're-pointed at the session record (## Loose ends) and runs only when the session '
        + 'never reached /close (the receipt-driven fallback, decision default 15, with a '
        + 'D5 kill trigger). Measured yield that moved it: 96 filed per 28 days, 1 human-'
        + 'closed; a background model cannot tell a deliberate silence from a dropped '
        + 'commitment (act:9ed07d9d) — the session at /close can. Was exempt: session-'
        + 'historical fact. See .claude/plans/simplification-2-inbox.md.',
      moved_to: '/close Step 6c (file as action / drop); session record ## Loose ends on '
        + 'the Ring 3 no-receipt branch; a stray filing → the project\'s carry ledger',
    },
  },
  'skill-candidate': {
    label: 'skill candidate',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A3 / Decision 3: Phase 2o\'s filing site is '
        + 're-pointed at the session record (## Skill candidates), fallback-only like 2n. '
        + '18 filed per 28 days, 0 human-closed. Was exempt: judgment item. See '
        + '.claude/plans/simplification-2-inbox.md.',
      moved_to: '/close Step 6c (note as skill candidate); session record ## Skill '
        + 'candidates on the Ring 3 no-receipt branch; a stray filing → the project\'s '
        + 'carry ledger',
    },
  },
  // ring2-filed
  'deferred-trigger': {
    label: 'fired trigger',
    reader: OPERATOR_READER,
    exit: { operator: 'pick up the released action, or dismiss in /inbox' },
    exempt: 'one-time trigger event; dedup-vs-pending blocks refiring. Candidate future '
      + 'reconciler: the referenced deferred action closed.',
  },
  'watchtower-health': {
    label: 'watchtower health note',
    retired: {
      date: '2026-09-29',
      reason: 'Project 1 (prj:a4c6052b), plan A5 / decision default 20: memory-budget '
        + 'violations retired with the cap machinery. Its ONLY filing site was the '
        + 'memory-budget standing debt inside Ring 2 slow\'s runMemoryHygiene '
        + '(surfacePersistentViolations, act:ea23b3a5), and its reconciler was '
        + 'retractClearedMemoryBudgetItems on the same pass — both deleted with '
        + 'validate-memory.mjs, the validator they shelled. The platform enforces its '
        + 'own index cap and reminds the session; no CC machinery measures it now. '
        + 'The entry stays so historical items render and the census can prove '
        + 'nothing files it. (The stale/failing ring-window filings this category once '
        + 'covered had no live producer at retirement either.) See '
        + '.claude/plans/simplification-1-memory-retrieval.md.',
    },
  },
  // ring4 (in the census since act:ea23b3a5; ring4 was outside it before)
  'doc-drift': {
    label: 'documentation drift',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A4 / Decision 4 / decision default 18: Ring '
        + '4 has no session, so a drift claim is a PROJECT fact, not a decision to queue. '
        + 'Ring 4 upserts each finding into state/projects/<slug>/doc-drift.json (one '
        + 'entry per drift_key) and REMOVES an entry when a later pass that actually '
        + 're-checked that claim no longer finds it — the reconciler this entry used to '
        + 'name as a candidate; a claim the pass did not re-check (document not '
        + 'collected, project not selected this week) stays. Measured before: 17 filed '
        + 'per 28 days, 0 human-closed. Project 4 moves the check into /close. Was '
        + 'exempt: documentary claim with no mechanical retraction. See '
        + '.claude/plans/simplification-2-inbox.md.',
      moved_to: 'state/projects/<slug>/doc-drift.json, rendered by Ring 1 as one Standing '
        + 'Issues line (count + path) in the project state the start block injects; a '
        + 'stray filing (flow\'s ring2-slow-post/10-pulse-mechanical.mjs hook) → the same '
        + 'file via createItem\'s redirect',
    },
  },
  // routines-filed (in the census since act:ea23b3a5)
  'routine': {
    label: 'scheduled routine',
    reader: 'operator — the desk badge and mux dispatch, /inbox, /briefing, start-block '
      + 'oldest line',
    exit: {
      mechanical: 'sweepExpiredMoments (routines, on Ring 1\'s tick): a time-of-day '
        + 'firing\'s day ended; runRoutinePass supersedes a stale pending firing when the '
        + 'next due firing arrives',
      operator: 'run the routine and resolve it, or dismiss in /inbox',
    },
    exempt: 'a declared interactive routine that fired. The dispatch engine owns the '
      + 'whole lifecycle, so there is nothing for a Ring 1 reconciler to re-check: a '
      + 'pending item blocks refiring, a stale one is superseded at the next due firing, '
      + 'and since act:ea23b3a5 a time-of-day routine self-supersedes at END OF ITS '
      + 'FILING DAY on Ring 1\'s tick — its moment is over. Terminal exits clear the mux '
      + 'dispatch descriptor via DISPATCHED_CATEGORIES, so the desk badge follows.',
  },
  'stale-project': {
    label: 'stale project',
    reader: OPERATOR_READER,
    exit: { operator: 'resume, park or close the project, or dismiss in /inbox' },
    exempt: 'staleness notification. Candidate future reconciler: project activity '
      + 'resumed ⇒ retract.',
  },
  'project-completion': {
    label: 'project nearing completion',
    reader: OPERATOR_READER,
    exit: { operator: 'complete the project (pib_complete_project walk), or dismiss in /inbox' },
    exempt: 'human judgment (close a project?); never mechanically resolvable.',
  },
  'pattern-promotion': {
    label: 'recurring pattern',
    reader: OPERATOR_READER,
    exit: { operator: 'write or promote the pattern, or dismiss in /inbox' },
    exempt: 'threshold event, deduped at filing; promotion up the compliance stack is a '
      + 'human decision. Two threshold producers (Project 2, plan D1): Ring 2\'s rule '
      + 'counter files ONE promotion proposal per rule crossing its window threshold, and '
      + 'emitPatternPromotion (watchtower-queue) files one per qa-gate failure class with '
      + '≥3 sweep instances, deduped by (source_item_id, failure_class). Ring 2\'s '
      + 'novel-pattern routing is the residual filer.',
  },
  // filed by the /qa-handoff skill (Project 2, plan B1): outside the rings,
  // so the census's ring filing-site scan cannot see it — hence the flag.
  'qa-handoff': {
    label: 'QA handoff',
    reader: 'operator — the QA station (/qa-drain, the desk badge), /inbox, /briefing, '
      + 'start-block oldest line',
    exit: { operator: 'record the QA verdict (validateQaVerdict), or a typed dismissal' },
    exempt: 'filed by the /qa-handoff skill, not a ring; leaves only through the '
      + 'structural QA verdict gate (validateQaVerdict) or a typed dismissal',
    filed_outside_rings: true,
  },
  'verify-backfill': {
    label: 'missing verify plan',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A3: Ring 2\'s verifyBackfillScan (slow step 7, '
        + 'flag verify_backfill) is DELETED. "This action you touched has no Verify Plan" '
        + 'is asked by the session that touched it: watchtower-commitments.mjs --residue '
        + 'reads the touched fids from the transcript\'s own pib_* tool calls and /close '
        + 'Step 6c proposes "write verify plan". 10 filed per 28 days, 0 human-closed. Was '
        + 'exempt: backfill nudge. See .claude/plans/simplification-2-inbox.md.',
      moved_to: '/close Step 6c (write verify plan) via watchtower-commitments.mjs '
        + '--residue; a stray filing → the project\'s carry ledger',
    },
  },
  // Never ring-filed after 2026-06-10 — survived only as vocabulary
  // (watchtower-validate, item.json.schema, the snapshot label) until
  // Simplification Project 2 accounted for it here.
  'routing-decision': {
    label: 'routing decision',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A3: no producer anywhere — last filed '
        + '2026-06-10; the dead `decision_routing` config key is removed by the console in '
        + 'C4. No live home: createItem refuses it (watchtower-queue RETIRED_CATEGORIES → '
        + 'null). VALID_CATEGORIES and the snapshot label stay for history.',
    },
  },
  // ring3-filed, ring1-reconciled (act:5182beda, N3/Reach 1 of grp:retro-remeaning)
  'pending-relation': {
    label: 'proposed thread relation',
    retired: {
      date: '2026-09-29',
      reason: 'Project 1 (prj:a4c6052b), plan A1: its ONLY producer was Reach 1 '
        + '(fileRelationProposal inside Phase 2d), deleted with the extractor. '
        + 'autoReconcilePendingRelations (ring1) keeps running over the HISTORICAL items '
        + '— it retracts when either cited side leaves pending, fails toward keeping — but '
        + 'no new proposal can be filed; the filing-side dedup (evidence.relation_key, all '
        + 'statuses, unwindowed) went with the producer.',
    },
  },
  // ring3-filed, ring1-reconciled (act:bcb7edd4, Reach 2 of grp:retro-remeaning)
  'significance-change': {
    label: 'thread re-reading proposal',
    retired: {
      date: '2026-10-31',
      reason: 'Project 2 (prj:4a433dac), plan A8 / EP1: a proposal that a session\'s new '
        + 'understanding recolors an EARLIER cursor entry of a thread it touched '
        + '(reframes/answers/contradicts) is written ONTO that thread — the additive '
        + 'proposed_recolors array, through the queue-side appendThreadProposal — plus one '
        + 'recolor entry in the project\'s carry ledger, never the inbox. The durable '
        + 'relation_key exclusion (thread|prior_session|verb, the anchor and never the '
        + 'noticer) moved with it: a key already proposed, applied (recolor_log) or a '
        + 'historical queue item in any status writes nothing; another noticing session '
        + 'appends to also_noticed_by. Measured before (live queue, 2026-10-31): 45 filed in '
        + '28 days; lifetime 519 — 252 confirmed and 21 declined by the operator, all on or '
        + 'before 2026-08-27, none since; 198 expired; 48 pending; 0 retracted by the '
        + 'reconciler. autoReconcileSignificanceItems (ring1) keeps '
        + 'running over the historical items until Project 4 (it retracts when the cited '
        + 'thread file is positively gone; the on-thread proposals vanish with their '
        + 'thread). GATED_CATEGORIES keeps the category, so the walk moves the pending '
        + 'items one at a time. No confirm/dismiss verb before Project 3 (R2). See '
        + '.claude/plans/simplification-2-inbox.md.',
      moved_to: 'thread file proposed_recolors + /threads + next start block; Project 3\'s '
        + 'contradiction gate decides what returns to the inbox',
    },
  },
};

// The LIVE inbox categories — every registry entry not marked retired. The
// one derivation of "what can still be pending and should be watched": the
// inbox history samples exactly this set, and B6's alarms read it.
export function liveInboxCategories(registry = INBOX_DETECTOR_REGISTRY) {
  return Object.keys(registry).filter((k) => !registry[k].retired);
}

// ---------------------------------------------------------------------------
// Inbox history — state/inbox-history.jsonl (Simplification Project 2, plan
// B6 / EP1 / EP3; act:b58a947b)
// ---------------------------------------------------------------------------
//
// Expiry is gone (A1), so inbox growth must be a numbered, mechanical signal.
// Ring 1 appends ONE row per calendar day (UTC date), `{ date,
// pending_by_category }` — pending counts for every live category. The first
// tick of the day writes it; a later tick finds the date already present and
// appends nothing, so a row is the day's first sample. Read by Ring 1's growth
// alarm and Q2 revisit trigger, and by the §Measurement weekly growth numbers.
// A missing file is an empty history; an unparseable line is skipped (the
// alarm then sees a gap, which only ever makes it quieter).

export function inboxHistoryPath({ watchtowerDir = getWatchtowerDir() } = {}) {
  return join(watchtowerDir, 'state', 'inbox-history.jsonl');
}

export function readInboxHistory({ watchtowerDir = getWatchtowerDir() } = {}) {
  const p = inboxHistoryPath({ watchtowerDir });
  if (!existsSync(p)) return [];
  const out = [];
  for (const line of readFileSync(p, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try {
      const row = JSON.parse(line);
      if (row && typeof row.date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(row.date)
        && row.pending_by_category && typeof row.pending_by_category === 'object') {
        out.push(row);
      }
    } catch { /* skipped — a gap */ }
  }
  return out;
}

export function appendInboxHistory({ date, pending_by_category } = {}, { watchtowerDir = getWatchtowerDir() } = {}) {
  if (typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error(`appendInboxHistory: date must be YYYY-MM-DD, got ${date}`);
  }
  if (readInboxHistory({ watchtowerDir }).some((r) => r.date === date)) {
    return { appended: false, date };
  }
  const p = inboxHistoryPath({ watchtowerDir });
  const dir = dirname(p);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  appendFileSync(p, JSON.stringify({ date, pending_by_category: pending_by_category || {} }) + '\n');
  return { appended: true, date, path: p };
}

// ---------------------------------------------------------------------------
// getWatchtowerDir — resolve watchtower root directory
// ---------------------------------------------------------------------------

export function getWatchtowerDir() {
  return process.env.WATCHTOWER_DIR
    || join(homedir(), '.claude-cabinet', 'watchtower');
}

// ---------------------------------------------------------------------------
// atomicWrite — mkdir -p dirname, write to .tmp, rename
// ---------------------------------------------------------------------------

export function atomicWrite(filePath, content) {
  const dir = dirname(filePath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const tmp = filePath + '.tmp';
  const data = typeof content === 'string'
    ? content
    : JSON.stringify(content, null, 2);
  writeFileSync(tmp, data);
  renameSync(tmp, filePath);
}

// ---------------------------------------------------------------------------
// Suppression ledger — structured sidecar of every Ring 3 dedup suppression
// ---------------------------------------------------------------------------
//
// `recordSuppression` appends one structured JSON line per suppression so the
// Ring 2 slow over-suppression canary (M5) can read what was killed and what
// it matched — a STRUCTURED record, never the human `log()` prose lines
// (format-coupling = silent failure; a reworded log line would silently break
// the canary, and per-session logs rotate away). Shape is the shared contract
// documented in `watchtower-contracts.md` ("Suppression Ledger").
//
// FAIL-OPEN is load-bearing: this ledger is observability, not the filing
// decision. A failed append logs one line and continues — a dropped ledger
// record must NEVER block an extraction from filing. The record shape is
// additive: new fields ride alongside; readers default missing fields.
//
// Append-only and atomic-enough: a single short line via appendFileSync is a
// POSIX atomic append on local fs. Growth is bounded by the canary, which
// prunes the ledger to its window on each run (not on the hot path here).

export function suppressionLedgerPath(watchtowerDir) {
  return join(watchtowerDir || getWatchtowerDir(),
    'state', 'suppression-ledger.jsonl');
}

// ---------------------------------------------------------------------------
// recentSlice — the most-recent `n` chars of a (preprocessed) transcript
// ---------------------------------------------------------------------------
//
// The M2 recall fix (act:edd79e15). `preprocessTranscript` already keeps the
// most-recent ~80% of an oversized session, but every extraction lens then
// threw that away with `compressed.slice(0, n)` — the OLDEST n chars — so a
// lesson set up in the back half of a session was silently dropped. recentSlice
// keeps the TAIL instead. When `n >= text.length` the whole string is returned
// (most sessions: one full-transcript call, zero extra cost). The leading
// partial line is trimmed (same as preprocessTranscript's tail-truncation) so a
// call never starts mid-JSON-line. The SINGLE definition every lens routes
// through — no more copy-pasted front-slices.
export function recentSlice(text, n) {
  if (typeof text !== 'string' || text.length === 0) return '';
  // Fail-open (recall-favoring, invariant #9): a non-positive or non-finite
  // budget is a misconfiguration — keep the WHOLE transcript rather than
  // silently feeding the model an empty string (an empty transcript = zero
  // extraction = the exact knowledge-drop this program exists to prevent).
  if (!Number.isFinite(n) || n <= 0) return text;
  if (text.length <= n) return text;
  let tail = text.slice(text.length - n);
  const nl = tail.indexOf('\n');
  if (nl >= 0) tail = tail.slice(nl + 1);
  return tail;
}

export function recordSuppression(record = {}, { watchtowerDir, ts } = {}) {
  try {
    const path = suppressionLedgerPath(watchtowerDir);
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const line = JSON.stringify({
      ts: ts || record.ts || new Date().toISOString(),
      project: record.project ?? null,
      corpus: record.corpus ?? null,
      suppressed_title: record.suppressed_title ?? null,
      matched_against: record.matched_against ?? null,
      session_id: record.session_id ?? null,
    }) + '\n';
    appendFileSync(path, line);
  } catch (e) {
    // Fail-open: never throw out of a suppression site.
    try { logError('suppression-ledger', `append failed (${e.message}) — continuing`); }
    catch { /* logging itself must never throw the caller */ }
  }
}

// ---------------------------------------------------------------------------
// Significance ledger (act:bcb7edd4, Reach 2 of grp:retro-remeaning)
// ---------------------------------------------------------------------------
//
// One structured line per session close that ran the thread reach-back —
// the DENOMINATOR of the observed significance-change rate, the honest
// metric that gates Reach 3 (L2) per the plan (replacing the wrong
// contradiction-density gate: significance change leaves no contradiction
// to detect, so only observed proposal/keep rates can measure it). The
// NUMERATOR (proposals filed) lives on the thread files' `proposed_recolors`
// since plan A8 (act:b57df8e4), plus the historical queue items under
// category 'significance-change', which also hold the kept / dismissed
// dispositions; readSignificanceRate combines them. Named reader: the L2 pickup gate (its action notes point here) plus
// the close run's own log line — never write-only telemetry.
//
// Same fail-open discipline as recordSuppression: observability, never the
// filing decision; a dropped ledger line must not block a session close.

export function significanceLedgerPath(watchtowerDir) {
  return join(watchtowerDir || getWatchtowerDir(),
    'state', 'significance-ledger.jsonl');
}

export function recordSignificanceEvent(record = {}, { watchtowerDir, ts } = {}) {
  try {
    const path = significanceLedgerPath(watchtowerDir);
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const line = JSON.stringify({
      ts: ts || record.ts || new Date().toISOString(),
      session_id: record.session_id ?? null,
      project: record.project ?? null,
      threads_touched: record.threads_touched ?? 0,
      threads_with_history: record.threads_with_history ?? 0,
      prior_entries_shown: record.prior_entries_shown ?? 0,
      proposals_filed: record.proposals_filed ?? 0,
    }) + '\n';
    appendFileSync(path, line);
  } catch (e) {
    try { logError('significance-ledger', `append failed (${e.message}) — continuing`); }
    catch { /* logging itself must never throw the caller */ }
  }
}

/**
 * The observed significance-change rate — the L2 gate's metric. Sessions
 * come from the ledger (deduped by session_id so a reprocess replay cannot
 * inflate the denominator); proposal dispositions come from the queue by
 * category. Ratios are null when their denominator is zero — an unknown
 * rate is reported as unknown, never fabricated as 0.
 * @param {object} [opts]
 * @param {string} [opts.watchtowerDir]
 * @param {Function} [opts.listItemsFn] - injectable for tests
 * @returns {{sessions: number, sessions_with_history: number,
 *   proposals_filed_ledger: number, queue: {pending: number, kept: number,
 *   dismissed: number, expired: number, superseded: number, retracted: number},
 *   thread_proposals: number, proposals_total: number,
 *   proposals_per_session: number|null, keep_ratio: number|null}}
 *   `thread_proposals` counts every proposed_recolors entry on the thread
 *   files; `proposals_total` is the number of distinct relation_keys across
 *   the thread files and the historical queue items.
 */
export function readSignificanceRate({ watchtowerDir, listItemsFn = queueListItems } = {}) {
  const sessions = new Map(); // session_id -> latest record
  try {
    const path = significanceLedgerPath(watchtowerDir);
    if (existsSync(path)) {
      for (const line of readFileSync(path, 'utf8').split('\n')) {
        const t = line.trim();
        if (!t) continue;
        try {
          const rec = JSON.parse(t);
          if (rec && rec.session_id) sessions.set(rec.session_id, rec);
        } catch { /* skip malformed lines */ }
      }
    }
  } catch { /* fail-open: an unreadable ledger reads as zero sessions */ }

  const queue = { pending: 0, kept: 0, dismissed: 0, expired: 0, superseded: 0, retracted: 0 };
  try {
    for (const item of listItemsFn({ category: 'significance-change' })) {
      if (item.status === 'pending') queue.pending++;
      else if (item.status === 'resolved') {
        if (item.resolution_type === 'auto-reconciled') queue.retracted++;
        else queue.kept++;
      } else if (item.status === 'dismissed') queue.dismissed++;
      else if (item.status === 'expired') queue.expired++;
      else if (item.status === 'superseded') queue.superseded++;
    }
  } catch { /* fail-open: an unreadable queue reads as zero dispositions */ }

  // Since plan A8 (act:b57df8e4) a proposal lives on its thread file
  // (`proposed_recolors`), so the numerator reads the thread files too; a key
  // present on both sides (a historical item the walk moved onto its thread)
  // is one proposal, so the total is the union of relation_keys.
  const keys = new Set();
  try {
    for (const item of listItemsFn({ category: 'significance-change' })) {
      const k = item && item.evidence && item.evidence.relation_key;
      if (k) keys.add(k);
    }
  } catch { /* fail-open, as above */ }
  let threadProposals = 0;
  try {
    const threadsDir = join(watchtowerDir || getWatchtowerDir(), 'state', 'threads');
    if (existsSync(threadsDir)) {
      for (const f of readdirSync(threadsDir)) {
        if (!f.endsWith('.json')) continue;
        try {
          const t = JSON.parse(readFileSync(join(threadsDir, f), 'utf8'));
          for (const p of Array.isArray(t && t.proposed_recolors) ? t.proposed_recolors : []) {
            threadProposals++;
            if (p && p.relation_key) keys.add(p.relation_key);
          }
        } catch { /* skip an unreadable thread file */ }
      }
    }
  } catch { /* fail-open: an unreadable threads dir reads as zero proposals */ }

  const records = [...sessions.values()];
  const sessionsWithHistory = records.filter((r) => (r.threads_with_history ?? 0) > 0).length;
  const proposalsFiledLedger = records.reduce((s, r) => s + (r.proposals_filed ?? 0), 0);
  const decided = queue.kept + queue.dismissed;
  return {
    sessions: records.length,
    sessions_with_history: sessionsWithHistory,
    proposals_filed_ledger: proposalsFiledLedger,
    queue,
    thread_proposals: threadProposals,
    proposals_total: keys.size,
    proposals_per_session: sessionsWithHistory > 0
      ? proposalsFiledLedger / sessionsWithHistory : null,
    keep_ratio: decided > 0 ? queue.kept / decided : null,
  };
}

// ---------------------------------------------------------------------------
// appendThreadRecolor — the human-gated APPLY step for a confirmed
// significance-change proposal (act:bcb7edd4). When the operator confirms in
// /inbox that a session's understanding recolors an earlier thread entry,
// the reinterpretation must LAND somewhere — otherwise the confirmation is
// archival and the meaning-change evaporates (the exact disease the program
// treats). Appends to the thread file's additive `recolor_log` array (old
// readers unaffected; never touches cursor_history — the recolor ANNOTATES
// history, it never rewrites it). Called by /inbox's confirm follow-up only,
// never by any ring: the rings PROPOSE, the operator applies.
// Returns the updated thread object, or null when the thread file is
// missing/corrupt (caller surfaces it — never a silent no-op).
export function appendThreadRecolor(threadsDir, threadSlug, entry) {
  const p = join(threadsDir, `${threadSlug}.json`);
  if (!existsSync(p)) return null;
  let thread;
  try {
    thread = JSON.parse(readFileSync(p, 'utf8'));
  } catch {
    return null;
  }
  if (!Array.isArray(thread.recolor_log)) thread.recolor_log = [];
  thread.recolor_log.push({
    ts: entry.ts || new Date().toISOString(),
    prior_session_id: entry.prior_session_id ?? null,
    prior_date: entry.prior_date ?? null,
    new_session_id: entry.new_session_id ?? null,
    verb: entry.verb ?? null,
    explanation: entry.explanation ?? null,
    confirmed_via: entry.confirmed_via ?? null, // the inbox item id
  });
  atomicWrite(p, JSON.stringify(thread, null, 2));
  return thread;
}

// ---------------------------------------------------------------------------
// API usage ledger (act:df3727a9) — cost visibility for the rings' Claude calls
// ---------------------------------------------------------------------------
//
// The watchtower's Ring 2 + Ring 3 Claude calls run on a separate,
// credit-metered ANTHROPIC_API_KEY and logged ZERO usage — the blindness that
// let the 2026-07-05 credit exhaustion go unnoticed for a week. Every SDK
// response carries `usage`; we append one structured line per call so a rolling
// token/$ tally is readable. Pricing is an ESTIMATE — the Anthropic Console is
// authoritative. FAIL-OPEN: a failed append never blocks the caller.

export function apiUsageLedgerPath(watchtowerDir) {
  return join(watchtowerDir || getWatchtowerDir(), 'state', 'api-usage.jsonl');
}

// The model the rings call (ring2 exports the same literal as CLAUDE_MODEL;
// API_PRICING below is keyed on it). Shared here so a non-ring caller — the
// recall probe (plan B4) — names the same model the ledger prices.
export const WATCHTOWER_CLAUDE_MODEL = 'claude-sonnet-4-6';

// loadAnthropicClient — the SDK client the rings build, as a shared helper
// (Simplification Project 1, plan B4, act:79be8765). ESM import() ignores
// NODE_PATH, so the SDK is resolved through createRequire — the global
// install (`npm install -g @anthropic-ai/sdk`) is what the launchd/cron env
// sees. Throws with the install hint when the SDK or the key is missing; the
// rings keep their private copies of this shape until Project 4 consolidates
// them (ring2:101, ring3-close:168).
export function loadAnthropicClient({ requireKey = true } = {}) {
  let Anthropic;
  try {
    const mod = require('@anthropic-ai/sdk');
    Anthropic = mod.default || mod.Anthropic || mod;
  } catch (e) {
    throw new Error(
      'the Anthropic SDK is required (@anthropic-ai/sdk). Install it: npm install -g @anthropic-ai/sdk\n'
      + `Import error: ${e.message}`
    );
  }
  if (requireKey && !process.env.ANTHROPIC_API_KEY) {
    throw new Error('ANTHROPIC_API_KEY environment variable is not set.');
  }
  return new Anthropic();
}

// $/token estimate (Anthropic Console authoritative). Sonnet 4.6: $3/M in, $15/M out.
export const API_PRICING = {
  'claude-sonnet-4-6': { in: 3e-6, out: 15e-6, cache_write: 3.75e-6, cache_read: 0.3e-6 },
};

export function recordApiUsage(record = {}, { watchtowerDir, ts } = {}) {
  try {
    const path = apiUsageLedgerPath(watchtowerDir);
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const u = record.usage || {};
    const line = JSON.stringify({
      ts: ts || record.ts || new Date().toISOString(),
      ring: record.ring ?? null,
      model: record.model ?? null,
      input_tokens: u.input_tokens ?? 0,
      output_tokens: u.output_tokens ?? 0,
      cache_creation_input_tokens: u.cache_creation_input_tokens ?? 0,
      cache_read_input_tokens: u.cache_read_input_tokens ?? 0,
    }) + '\n';
    appendFileSync(path, line);
  } catch (e) {
    // Fail-open: never throw out of a Claude call site over a ledger append.
    try { logError('api-usage-ledger', `append failed (${e.message}) — continuing`); }
    catch { /* logging itself must never throw the caller */ }
  }
}

export function readApiUsageRecords(watchtowerDir) {
  try {
    const path = apiUsageLedgerPath(watchtowerDir);
    if (!existsSync(path)) return [];
    const out = [];
    for (const line of readFileSync(path, 'utf8').split('\n')) {
      const t = line.trim();
      if (!t) continue;
      try { out.push(JSON.parse(t)); } catch { /* skip malformed line */ }
    }
    return out;
  } catch { return []; }
}

// PURE: token totals + $ estimate for records within windowDays of nowMs.
export function summarizeApiUsage(records, { nowMs = Date.now(), windowDays = 1 } = {}) {
  const cutoff = nowMs - windowDays * 86400000;
  let calls = 0, input = 0, output = 0, cacheWrite = 0, cacheRead = 0, cost = 0;
  const byRing = {};
  for (const r of records || []) {
    const t = new Date(r.ts).getTime();
    if (!Number.isFinite(t) || t < cutoff) continue;
    const p = API_PRICING[r.model] || API_PRICING['claude-sonnet-4-6'];
    const c = (r.input_tokens || 0) * p.in + (r.output_tokens || 0) * p.out
      + (r.cache_creation_input_tokens || 0) * p.cache_write
      + (r.cache_read_input_tokens || 0) * p.cache_read;
    calls++; input += r.input_tokens || 0; output += r.output_tokens || 0;
    cacheWrite += r.cache_creation_input_tokens || 0; cacheRead += r.cache_read_input_tokens || 0;
    cost += c;
    const ring = r.ring || 'unknown';
    byRing[ring] = Math.round(((byRing[ring] || 0) + c) * 100) / 100;
  }
  return {
    window_days: windowDays, calls,
    input_tokens: input, output_tokens: output,
    cache_write_tokens: cacheWrite, cache_read_tokens: cacheRead,
    est_cost_usd: Math.round(cost * 100) / 100, by_ring: byRing,
  };
}

// ---------------------------------------------------------------------------
// Ring 2 idle gate (act:0f961349) — skip Claude passes when nothing changed
// ---------------------------------------------------------------------------
//
// Ring 2's Claude passes (deferred-trigger eval, cross-project synthesis) gated
// on "is there data" but not on "did anything change / is the operator active",
// re-evaluating the same standing triggers ~1400x/day on idle days for nothing.
// The activity signature changes ONLY on real session activity (Ring 3 close
// writes ring3-health.json; SessionStart writes a session snapshot) — NOT on
// Ring 1's per-tick git-attention rewrite. An idle FLOOR still re-runs each pass
// every few hours so date/external trigger conditions are never starved.

export const RING2_IDLE_FLOOR_MS = 6 * 60 * 60 * 1000;

export function ring2ActivityStatePath(watchtowerDir) {
  return join(watchtowerDir || getWatchtowerDir(), 'state', 'ring2-activity.json');
}

// A string signature of session activity: newest Ring 3 close + the set of
// session-start snapshots. Changes only when the operator actually works.
export function computeActivitySignature(watchtowerDir) {
  const dir = watchtowerDir || getWatchtowerDir();
  const parts = [];
  try {
    const h = join(dir, 'state', 'ring3-health.json');
    if (existsSync(h)) {
      const j = JSON.parse(readFileSync(h, 'utf8'));
      parts.push(String(j.last_run || ''), String(j.session_id || ''));
    }
  } catch { /* ignore */ }
  try {
    const snapDir = join(dir, 'state', 'session-snapshots');
    if (existsSync(snapDir)) parts.push(readdirSync(snapDir).sort().join(','));
  } catch { /* ignore */ }
  return parts.join('|');
}

// Returns { run, reason }. Runs when activity changed since the pass last ran,
// OR the idle floor elapsed, OR it never ran. Read-only — call markIdlePassRan
// after the pass actually runs.
export function shouldRunIdlePass(passName, { watchtowerDir, signature, nowMs = Date.now(), idleFloorMs = RING2_IDLE_FLOOR_MS } = {}) {
  const path = ring2ActivityStatePath(watchtowerDir);
  let state = {};
  try { if (existsSync(path)) state = JSON.parse(readFileSync(path, 'utf8')); } catch { state = {}; }
  const prev = state[passName];
  if (!prev || !prev.last_run_ms) return { run: true, reason: 'first-run' };
  if (signature !== (prev.last_signature ?? '')) return { run: true, reason: 'activity' };
  if ((nowMs - prev.last_run_ms) >= idleFloorMs) return { run: true, reason: 'idle-floor' };
  return { run: false, reason: 'idle-skip' };
}

export function markIdlePassRan(passName, { watchtowerDir, signature, nowMs = Date.now() } = {}) {
  const path = ring2ActivityStatePath(watchtowerDir);
  let state = {};
  try { if (existsSync(path)) state = JSON.parse(readFileSync(path, 'utf8')); } catch { state = {}; }
  state[passName] = { last_run_ms: nowMs, last_signature: signature };
  try { atomicWrite(path, JSON.stringify(state, null, 2)); } catch { /* fail-open */ }
}

// ---------------------------------------------------------------------------
// loadConfig — read and validate config.json with schema_version check
// ---------------------------------------------------------------------------

export function loadConfig(watchtowerDir) {
  const dir = watchtowerDir || getWatchtowerDir();
  const configPath = join(dir, 'config.json');
  if (!existsSync(configPath)) {
    throw new Error('Watchtower config.json not found. Run /watchtower install first.');
  }
  const config = JSON.parse(readFileSync(configPath, 'utf8'));
  if (config.schema_version !== 1) {
    throw new Error(`Unsupported config schema_version ${config.schema_version} (expected 1)`);
  }
  return config;
}

// ---------------------------------------------------------------------------
// slugify — lowercase, replace non-alphanum with dash, trim dashes. Defined in
// watchtower-queue.mjs since plan A9 (the queue's carry-ledger writer keys on
// it and the queue cannot import this module); re-exported at the top.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Authored-.claude re-inclusion for worktree dirty detection (act:e91fdfcf)
// ---------------------------------------------------------------------------
// The JS-side ring dirty counts (Ring 1 countRealUncommitted, Ring 3 close)
// historically BLANKET-excluded every `.claude/` porcelain line as mux infra
// churn — which silently under-reported real authored work in worktrees
// (.claude/plans, .claude/methodology, .claude/rules: authored PROJECT RECORD
// per .claude/rules/artifacts-of-thought.md). This mirrors the canonical
// shell EXCLUSION CONTRACT in templates/mux/config/worktree-dirty-check.sh
// (point 1 EXCEPTION): a top-level `.claude/` entry that holds TRACKED files
// is authored, so churn under it counts; only disposable infra is ignored.
// The shell and JS sides remain separate implementations (one bash, one JS) —
// this is the JS single source the two rings delegate to.
const AUTHORED_CLAUDE_FLOOR = ['plans', 'methodology'];

// The set of top-level `.claude/` dirs that hold tracked files in this
// worktree's index (authored project record). Derived from `git ls-files`
// at run time; the static plans/methodology floor is ALWAYS included so a
// failed/partial index read can never drop authored work (fail-DIRTY).
export function authoredClaudeDirs(cwd, execFn) {
  const dirs = new Set(AUTHORED_CLAUDE_FLOOR);
  let out;
  try {
    out = execFn('git ls-files .claude/', { cwd });
  } catch {
    return dirs;
  }
  if (!out) return dirs;
  for (const line of out.split('\n')) {
    const m = line.trim().match(/^\.claude\/([^/]+)\//);
    if (m) dirs.add(m[1]);
  }
  return dirs;
}

// Generated runtime state written by the CC/watchtower/verify harnesses —
// never authored work (act:c008862c: an untracked advisories-state.json
// identical to main's copy earned two fully-merged branches a data-loss
// "MERGE OR LOSE" warning). UNTRACKED (`??`) lines ONLY, by anchor: a
// TRACKED file of the same name is authored by definition (the
// artifacts-of-thought carve-out — existing consumers deliberately keep
// advisories-state.json tracked, and their real churn must keep counting).
// This list is the JS SSOT consumed by ring1's countRealUncommitted and
// ring3's Phase 2a dirty filter (both via claudeChurnIsDisposable below);
// the mux shell twin (templates/mux/config/worktree-dirty-check.sh) is a
// separate implementation of the same concept — the program merge tail
// diffs the two lists (grp:wt-noise-immunity step 4b).
export const GENERATED_STATE_PATTERNS = [
  /^\?\?\s+\.claude\/verification\//,
  /^\?\?\s+\.claude\/cabinet\/advisories-state\.json$/,
  /^\?\?\s+\.claude\/cabinet\/checklist-stats\.json$/,
  /^\?\?\s+e2e\/\.verify-progress\.jsonl$/,
];

export function generatedStateIsDisposable(porcelainLine) {
  return GENERATED_STATE_PATTERNS.some((re) => re.test(porcelainLine));
}

// Is this porcelain line DISPOSABLE infra/runtime churn (safe to ignore)?
// Untracked generated state (list above) is disposable even under an
// authored dir — .claude/cabinet/ is authored where tracked files live, but
// an untracked advisories-state.json there is runtime residue, not work.
// Otherwise: a `.claude/` path under an authored top-level dir is real work
// → false (count it); a non-`.claude/` line is not our concern → false;
// only `.claude/` churn outside the authored subtrees (untracked infra,
// top-level config files) is disposable → true.
export function claudeChurnIsDisposable(porcelainLine, authoredDirs) {
  if (generatedStateIsDisposable(porcelainLine)) return true;
  if (!/\s\.claude(?:\/|$)/.test(porcelainLine)) return false;
  const m = porcelainLine.match(/\.claude\/([^/\s]+)/);
  if (m && authoredDirs.has(m[1])) return false; // authored subtree → count it
  return true; // disposable .claude/ infra
}

// ---------------------------------------------------------------------------
// flushFeedbackOutbox — deliver the GLOBAL cc-feedback outbox to the CC repo
// ---------------------------------------------------------------------------
// Ring 1 mechanical duty (act:6c3a4763). Ports orient's delivery algorithm:
// read ~/.claude/cc-feedback-outbox.json, resolve the CC source repo via
// cc-registry (the entry whose package.json name is create-claude-cabinet),
// write each undelivered item's body to <cc>/feedback/{date}-{slug}.md with
// a skip-if-exists guard against feedback/ AND feedback/resolved/ — a name
// containing the slug, or a delivery-scheme name whose slug is a whole-token
// prefix/subset of the new one, means a prior session already delivered or
// resolved it — then atomically rewrite the outbox — [] on a clean pass, only the
// failed items otherwise. Fail-safe: if the destination cannot be resolved,
// the outbox is left untouched — an item is never dropped without a file
// existing for it. Ring 1 is now the SOLE feedback-delivery owner: orient's
// duplicate flush (incompatible {date}-{slug}-{seq}.md scheme) was retired
// (act:d53ff509) once this duty was verified in the live runtime.

// Resolve the CC source repo ROOT via cc-registry — the single registered
// project whose package.json name is create-claude-cabinet. The one place
// "which checkout is the CC source?" is answered, so callers (feedback
// delivery, the Ring 1 runtime-drift check) never re-derive it.
// (resolveCcSourceRepo is defined in watchtower-queue.mjs since plan A9 — the
// outbox writer's T8 dedup needs it there — and re-exported at the top.)

function resolveCcFeedbackDir(registryPath) {
  const repo = resolveCcSourceRepo(registryPath);
  return repo ? join(repo, 'feedback') : null;
}

function feedbackFileExists(feedbackDir, slug) {
  for (const dir of [feedbackDir, join(feedbackDir, 'resolved')]) {
    if (!existsSync(dir)) continue;
    for (const name of readdirSync(dir)) {
      if (name.includes(slug)) return true;
      // Reverse direction (act:ca0aee25): a re-report under a LONGER title
      // must still match its already-delivered shorter twin. Deliberately
      // narrow — only delivery-scheme names ({date}-{slug}[-{seq}].md)
      // carry slug semantics, and the existing slug must sit on whole
      // dash-token boundaries inside the new one. Silent suppression of
      // genuinely new feedback is worse than a duplicate delivery, so no
      // substring fuzz and no participation by hand-named files.
      const m = /^\d{4}-\d{2}-\d{2}-(.+?)(?:-\d+)?\.md$/.exec(name);
      if (m && `-${slug}-`.includes(`-${m[1]}-`)) return true;
    }
  }
  return false;
}

export function flushFeedbackOutbox(opts = {}) {
  const outboxPath = opts.outboxPath
    || join(homedir(), '.claude', 'cc-feedback-outbox.json');
  const registryPath = opts.registryPath
    || join(homedir(), '.claude', 'cc-registry.json');

  const result = { status: 'ok', delivered: 0, skipped: 0, kept: 0, destination: null };

  if (!existsSync(outboxPath)) {
    result.status = 'no-outbox';
    return result;
  }

  let outbox;
  try {
    outbox = JSON.parse(readFileSync(outboxPath, 'utf8'));
  } catch {
    // Malformed outbox: per orient's contract, warn and reset to [].
    atomicWrite(outboxPath, '[]\n');
    result.status = 'malformed-reset';
    return result;
  }

  if (!Array.isArray(outbox) || outbox.length === 0) {
    result.status = 'empty';
    return result;
  }

  const undelivered = outbox.filter((item) => item && item.delivered !== true);
  if (undelivered.length === 0) {
    // Only stale delivered:true markers remain — don't accumulate them.
    atomicWrite(outboxPath, '[]\n');
    result.status = 'markers-cleared';
    return result;
  }

  const feedbackDir = opts.destination || resolveCcFeedbackDir(registryPath);
  if (!feedbackDir) {
    // Cannot resolve where feedback lives. Leave the outbox untouched —
    // never mark/drop an item without a delivered file existing for it.
    result.status = 'no-destination';
    result.kept = outbox.length;
    return result;
  }
  result.destination = feedbackDir;

  const failed = [];
  for (const item of undelivered) {
    try {
      const slug = slugify(item.title || 'untitled') || 'untitled';
      if (feedbackFileExists(feedbackDir, slug)) {
        result.skipped++;
        continue;
      }
      const date = item.date || new Date().toISOString().slice(0, 10);
      if (!existsSync(feedbackDir)) mkdirSync(feedbackDir, { recursive: true });
      writeFileSync(join(feedbackDir, `${date}-${slug}.md`), item.body || item.title || '');
      result.delivered++;
    } catch {
      failed.push(item);
    }
  }

  // Clean pass resets to []; otherwise keep only the failed items.
  atomicWrite(outboxPath, JSON.stringify(failed, null, 2) + '\n');
  result.kept = failed.length;
  if (failed.length > 0) result.status = 'partial';
  return result;
}

// ---------------------------------------------------------------------------
// Thread cursor history (schema v2)
// ---------------------------------------------------------------------------
// A thread's cursor is no longer a single overwritten object; it is an
// append-only `cursor_history` array of point-in-time snapshots, one per
// session that advanced the thread. Each entry is
//   { date, session_id, cursor: { what, why, where_left_off, open_questions,
//     next_steps } }
// The "current" cursor is always the last entry. Overwriting threw away the
// journey (symptom → diagnosis → solution → abstraction); the history keeps it.
// `cursor_history` is the thread's first sibling timeline — the QA-handoff
// protocol (.claude/plans/qa-handoff-protocol.md) adds a parallel
// point-in-time event sibling rather than nesting inside the cursor.

// migrateThreadCursor — convert a legacy (schema v1) thread object that has a
// single `cursor` field into the v2 `cursor_history` shape, in place.
// Idempotent: a thread already carrying `cursor_history` is left alone (beyond
// stripping any stale `cursor` field and bumping the version). The single
// migrated entry inherits the date/session_id of the thread's most recent
// session, since the legacy cursor reflected the latest understanding.
export function migrateThreadCursor(threadData) {
  if (!threadData || typeof threadData !== 'object') return threadData;
  if (threadData.schema_version === undefined) threadData.schema_version = 1;

  if (Array.isArray(threadData.cursor_history)) {
    delete threadData.cursor; // drop any stale legacy field
    if (threadData.schema_version < 2) threadData.schema_version = 2;
    return threadData;
  }

  const legacy = threadData.cursor;
  const sessions = Array.isArray(threadData.sessions) ? threadData.sessions : [];
  const last = sessions.length ? sessions[sessions.length - 1] : null;
  threadData.cursor_history = legacy
    ? [{
        date: last?.date || (threadData.last_updated || '').slice(0, 10) || null,
        session_id: last?.id || null,
        cursor: legacy,
      }]
    : [];
  delete threadData.cursor;
  threadData.schema_version = 2;
  return threadData;
}

// currentCursor — the most recent cursor snapshot for a thread. Reads the last
// `cursor_history` entry, falling back to a legacy `cursor` field so consumers
// stay correct against any un-migrated thread file. Always returns an object.
export function currentCursor(thread) {
  const hist = thread?.cursor_history;
  if (Array.isArray(hist) && hist.length) return hist[hist.length - 1].cursor || {};
  return thread?.cursor || {};
}

// ---------------------------------------------------------------------------
// Shared thread-file reading (single source of truth)
// ---------------------------------------------------------------------------
// Reading `state/threads/*.json`, skipping corrupt files, keeping only active
// threads, and deciding which threads "belong to" a given item/project was
// independently implemented twice (Ring 2's loadActiveThreads/threadsForItem
// for enrichment context, Ring 3-close's threadCursorLines for the dedup prose
// corpus). Two copies of one concept drift — and they did: Ring 3's membership
// FALLBACK matched on slug-substring containment (slugify(thread).includes(
// projectSlug)), so a short project slug ("flow") over-matched any thread whose
// slug merely contained it ("…workflow…"), pulling FOREIGN cursor prose into a
// project's suppression corpus where it could silently suppress legitimate new
// extractions. The shared membership rule below has NO slug-substring fallback:
// a thread belongs to a project only by EXPLICIT thread_ids membership OR EXACT
// sessions[].project equality (the key Ring 3 itself writes). Both rings now
// consume these helpers.

// loadActiveThreads — read all active thread files from threadsDir. Corrupt
// JSON and dormant/non-active threads are skipped; a missing/empty dir yields
// []. Never throws.
export function loadActiveThreads(threadsDir) {
  if (!existsSync(threadsDir)) return [];
  const threads = [];
  try {
    for (const entry of readdirSync(threadsDir, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
      try {
        const thread = JSON.parse(readFileSync(join(threadsDir, entry.name), 'utf8'));
        if (thread && thread.status === 'active') threads.push(thread);
      } catch { /* skip corrupt */ }
    }
  } catch { /* skip */ }
  return threads;
}

// threadMatchesProject — THE single membership predicate. A thread belongs to a
// project iff (a) its slug is in `explicitIds` (explicit thread_ids membership)
// OR (b) any of its sessions records that exact project slug. Deliberately NO
// slug-substring fallback — that over-matched foreign threads. `projectSlug`
// must already be slugified; `explicitIds` defaults to none. Returns
// { match: bool, explicit: bool } so selectors can rank explicit matches first.
export function threadMatchesProject(thread, projectSlug, explicitIds = []) {
  const explicit = Array.isArray(explicitIds) && explicitIds.includes(thread?.thread);
  const byProject = !!projectSlug
    && Array.isArray(thread?.sessions)
    && thread.sessions.some(s => s && s.project === projectSlug);
  return { match: explicit || byProject, explicit };
}

// Threads matched to an item, capped, ranked explicit-first then by recency.
const THREAD_MATCH_CAP = 3;

// threadsForItem — match active threads to a queue item: explicit
// item.thread_ids membership OR exact sessions[].project equality (slugify(
// item.project)). Explicit matches rank first, then most recent last_updated;
// capped at THREAD_MATCH_CAP. Built on threadMatchesProject so the membership
// rule lives in one place.
export function threadsForItem(item, threads) {
  const explicitIds = Array.isArray(item?.thread_ids) ? item.thread_ids : [];
  const projectSlug = item?.project ? slugify(item.project) : '';
  const matches = [];
  for (const thread of threads) {
    const { match, explicit } = threadMatchesProject(thread, projectSlug, explicitIds);
    if (match) matches.push({ thread, explicit });
  }
  matches.sort((a, b) => {
    if (a.explicit !== b.explicit) return a.explicit ? -1 : 1;
    return new Date(b.thread.last_updated || 0) - new Date(a.thread.last_updated || 0);
  });
  return matches.slice(0, THREAD_MATCH_CAP).map(m => m.thread);
}

// projectThreadCursorLines — the dedup PROSE corpus from a project's active
// thread cursors. Thread cursors (display_name / what / where_left_off /
// open_questions) are exactly "what the system already knows the user is
// working on"; an extraction that restates them is noise. Membership is the
// shared threadMatchesProject rule (exact sessions[].project equality — NO
// slug-substring over-match). Only the CURRENT cursor counts (currentCursor),
// not history. Returned lines are lowercased, ready for the memoryLines
// containment pass in Ring 3's isDuplicate. Never throws.
export function projectThreadCursorLines(threadsDir, projectSlug) {
  if (!projectSlug) return [];
  const lines = [];
  const push = (v) => {
    if (typeof v === 'string' && v.trim()) lines.push(v.toLowerCase());
  };
  for (const thread of loadActiveThreads(threadsDir)) {
    if (!threadMatchesProject(thread, projectSlug).match) continue;
    const cursor = currentCursor(thread);
    push(thread.display_name);
    push(cursor.what);
    push(cursor.where_left_off);
    if (Array.isArray(cursor.open_questions)) cursor.open_questions.forEach(push);
  }
  return lines;
}

// updateThreadFile — disk-wins thread file update (Ring 3 thread capture).
//
// DISK WINS OVER MODEL: if the thread file exists on disk, this ALWAYS
// appends to it — the model's `is_new` claim is advisory naming metadata
// and is deliberately not consulted here. cursor_history is append-only by
// design; one hallucinated is_new:true must never wipe a thread's history,
// sessions[], or related_fids (watchtower-contracts.md §Project State
// Section Ownership / Thread File Durability).
//
// Corrupt-file handling: an existing file that fails JSON.parse is NEVER
// silently replaced. It is backed up aside as `<file>.corrupt-<ts>` and a
// fresh file is written; the returned outcome string reports it so the
// caller can log loudly.
//
// RELATIONSHIP ENRICHMENT (act:e8793574): two additive, union-deduped fields
// carry a thread's relationships so a cold session can trace it:
//   related_fids — pib-db work-item fids (`act:<8hex>`) referenced by the
//     sessions that advanced this thread (deterministically scanned from the
//     transcript by ring3-close's extractRelatedFids — never model-authored,
//     so no hallucinated fid can enter).
//   lineage      — sibling threads CO-ADVANCED in the same session. When one
//     session genuinely advances several zoom-level threads (initiative +
//     project-area + cross-cutting), those threads ARE related; each records
//     the others. This is the only relation cheap enough to assert
//     deterministically (co-occurrence in one capture is ground truth, not a
//     model guess). Entry shape: { slug, relation, session_id, date }.
// Both are optional-additive at schema_version 2 (like related_fids before
// them); a 7th arg absent → both default to [] and behavior is unchanged.
function unionFids(existing, incoming) {
  const out = Array.isArray(existing) ? existing.filter((x) => typeof x === 'string' && x) : [];
  const seen = new Set(out);
  for (const f of Array.isArray(incoming) ? incoming : []) {
    if (typeof f === 'string' && f && !seen.has(f)) { seen.add(f); out.push(f); }
  }
  return out;
}

function mergeLineage(existing, incoming) {
  const out = Array.isArray(existing) ? existing.filter((e) => e && typeof e.slug === 'string' && e.slug) : [];
  const seen = new Set(out.map((e) => e.slug));
  for (const e of Array.isArray(incoming) ? incoming : []) {
    if (e && typeof e.slug === 'string' && e.slug && !seen.has(e.slug)) { seen.add(e.slug); out.push(e); }
  }
  return out;
}

// Returns an outcome string: 'created' | 'updated' | 'recovered — …'.
export function updateThreadFile(
  threadPath, threadSlug, modelThread, cursorEntry, sessionRecord, now,
  { relatedFids = [], lineage = [] } = {}) {
  let threadData = null;
  let outcome = 'created';

  if (existsSync(threadPath)) {
    try {
      threadData = JSON.parse(readFileSync(threadPath, 'utf8'));
      outcome = 'updated';
    } catch (e) {
      const backupPath = `${threadPath}.corrupt-${Date.now()}`;
      renameSync(threadPath, backupPath);
      threadData = null;
      outcome = `recovered — corrupt JSON backed up to ${basename(backupPath)} (${e.message})`;
    }
  }

  if (threadData) {
    // Heal pre-versioning + legacy single-cursor files into cursor_history
    // (watchtower-contracts.md §Schema Versioning), then append.
    migrateThreadCursor(threadData);
    if (!Array.isArray(threadData.cursor_history)) threadData.cursor_history = [];
    threadData.cursor_history.push(cursorEntry);
    if (modelThread.display_name) threadData.display_name = modelThread.display_name;
    threadData.last_updated = now;
    if (!Array.isArray(threadData.sessions)) threadData.sessions = [];
    threadData.sessions.push(sessionRecord);
    // Additive union-dedup — never wipes what prior sessions accumulated.
    threadData.related_fids = unionFids(threadData.related_fids, relatedFids);
    threadData.lineage = mergeLineage(threadData.lineage, lineage);
  } else {
    threadData = {
      schema_version: 2,
      thread: threadSlug,
      display_name: modelThread.display_name || threadSlug,
      cursor_history: [cursorEntry],
      sessions: [sessionRecord],
      related_fids: unionFids([], relatedFids),
      lineage: mergeLineage([], lineage),
      last_updated: now,
      status: 'active',
    };
  }

  atomicWrite(threadPath, JSON.stringify(threadData, null, 2));
  return outcome;
}

// ---------------------------------------------------------------------------
// preserveRing3LastSession — project-state section ownership merge
// ---------------------------------------------------------------------------
// state/projects/<slug>.md is written by two rings. Ring 1 rebuilds the file
// from scratch every run; Ring 3 owns the "## Last Session" section once it
// has authored a rich session summary there. The Ring 3 attribution line
// (`_<date> (<session-id>)_`, written by ring3-close's sessionSummary) IS the
// ownership marker: when the existing file carries it, Ring 1's rebuild must
// carry the existing section forward verbatim instead of clobbering it with
// its own fallback ("Active: …" / last-commit line). Full ownership table:
// watchtower-contracts.md §Project State Section Ownership.
export const PROJECT_STATE_LAST_SESSION_HEADER = '## Last Session';

// Shared UI-path heuristic for the verify-coverage feature. The verify-backfill
// scan (Ring 2) and the verify-coverage lens (Ring 3) must agree on what counts
// as "UI-touching" — single source of truth so the two rings never drift.
// (act:1be47d42)
//
// Bare 'app/' is deliberately NOT in this list. It is a substring match
// (`path.includes(u)`), so on any app/-rooted backend (Rails, Django-style
// layouts) it matches every model, job, service, and mailer — measured at
// roughly 2x the intended firing rate on a live Rails+React project, whose
// three most-hit "UI" paths were a controller, a background job, and a
// service class, none with a browser surface (act:1be47d42 field report,
// 2026-08-17). A consumer needing a literal `app/`-rooted frontend
// convention (e.g. a Next.js app router) should set `defaults.verify_ui_paths`
// rather than relying on this default.
export const VERIFY_UI_PATHS = ['webapp/frontend/', 'components/', 'pages/'];

// Backend-only directory names that should never count as UI-touching even
// under a broad or consumer-supplied uiPaths entry — the safety net the
// same field report asked for independent of narrowing the positive list.
export const VERIFY_UI_EXCLUDE_PATHS = ['/jobs/', '/services/', '/mailers/', '/workers/'];

// Durable per-project override (act:1be47d42): before this, a project whose
// default list ran too hot had no way to fix it without forking the ring
// code, since `templates/` is installer-planted and any local edit is
// overwritten on the next install. `defaults.verify_ui_paths` /
// `defaults.verify_ui_paths_exclude` follow the same config.defaults.*
// override convention as `long_lived_branches` (watchtower-ring1.mjs).
export function resolveUiPaths(config) {
  const paths = Array.isArray(config?.defaults?.verify_ui_paths)
    ? config.defaults.verify_ui_paths
    : VERIFY_UI_PATHS;
  const exclude = Array.isArray(config?.defaults?.verify_ui_paths_exclude)
    ? config.defaults.verify_ui_paths_exclude
    : VERIFY_UI_EXCLUDE_PATHS;
  return { paths, exclude };
}
const RING3_LAST_SESSION_ATTRIBUTION = /^_\d{4}-\d{2}-\d{2} \(.+\)_$/;

// Line-anchored header search: the header must start a line and be the
// entire line (modulo trailing whitespace). A bare indexOf would match
// inside '### Last Session' or a mid-line mention of the header text.
function headerLineIndex(content, header) {
  const escaped = header.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const m = new RegExp(`(^|\\n)${escaped}[ \\t]*(?=\\n|$)`).exec(content);
  return m ? m.index + m[1].length : -1;
}

function extractSection(content, header) {
  const idx = headerLineIndex(content, header);
  if (idx < 0) return null;
  const next = content.indexOf('\n## ', idx + header.length);
  return content.slice(idx, next > 0 ? next : content.length);
}

// buildLastSessionBlock — THE single source of the "## Last Session" block
// format. Ring 3's sessionSummary builds the per-session file body and this
// inline block from the SAME `bullets` string, so the inline section can never
// be a lossy subset of the per-session record (act:ac119994). The
// `_<date> (<sessionId>)_` attribution line is the ownership marker
// preserveRing3LastSession keys on — emit it here so a Ring 3-authored section
// is always recognized as Ring 3's. `bullets` is the COMPLETE model bullet set;
// it is never truncated or sliced for the inline block.
export function buildLastSessionBlock({ date, sessionId, bullets }) {
  return `${PROJECT_STATE_LAST_SESSION_HEADER}\n_${date} (${sessionId})_\n${String(bullets).trim()}\n`;
}

// upsertLastSessionSection — splice a freshly-built "## Last Session" block into
// the project-state file, replacing an existing section in place or appending
// one when absent. Reuses the line-anchored headerLineIndex so a "### Last
// Session" or a mid-line mention of the header text is never matched (the same
// machinery preserveRing3LastSession reads with — one splice convention, not
// two). `block` must already end with a trailing newline (buildLastSessionBlock
// guarantees this). When the section is not the file's last, a blank line is
// inserted before the following "## " so the markdown structure stays valid.
export function upsertLastSessionSection(existingContent, block) {
  const base = existingContent || '';
  const idx = headerLineIndex(base, PROJECT_STATE_LAST_SESSION_HEADER);
  if (idx < 0) {
    if (!base.trim()) return block;
    return `${base.trimEnd()}\n\n${block}`;
  }
  const next = base.indexOf(
    '\n## ', idx + PROJECT_STATE_LAST_SESSION_HEADER.length);
  if (next > 0) {
    // Replacing a non-terminal section: keep a blank line before the next "## ".
    return base.slice(0, idx) + block.trimEnd() + '\n\n' + base.slice(next + 1);
  }
  return base.slice(0, idx) + block;
}

// ---------------------------------------------------------------------------
// Session records, residue receipts, the fallback ledger (Simplification
// Project 2, plan B3 / A3 — act:73dd31d4)
// ---------------------------------------------------------------------------

// sessionRecordPath — THE spelling of a per-session record:
// state/projects/<slug>/sessions/<date>-<sessionId>.md. Ring 3's main()
// computes it ONCE and hands it to Phase 2b (sessionSummary) and to every
// lens that appends a section, so the two cannot drift (T13).
export function sessionRecordPath(slug, date, sessionId, { watchtowerDir = getWatchtowerDir() } = {}) {
  return join(watchtowerDir, 'state', 'projects', slug, 'sessions', `${date}-${sessionId}.md`);
}

// residueRecordPath — where a session whose project slug is UNRESOLVED gets
// its residue written: state/residue/<sessionId>.record.md. Not /recall-
// indexed (stated limitation, plan §Edge Cases); logged by the caller so it
// is never a silent drop (T5).
export function residueRecordPath(sessionId, { watchtowerDir = getWatchtowerDir() } = {}) {
  return join(watchtowerDir, 'state', 'residue', `${sessionId}.record.md`);
}

// appendSessionRecordSection — re-read the record, append or EXTEND the named
// `## ` section with the given lines, atomicWrite. Never throws: returns
// { ok, written, error? } and logs. When the record does not exist (Phase 2b
// failed, T5) a stub is written first — `# Session <id>`, `Date:` and the
// "(summary unavailable — Phase 2b did not complete)" line — so the section
// is still indexed by /recall (watchtower-recall indexes sessions/*.md).
export function appendSessionRecordSection(recordPath, heading, lines, { sessionId = null, date = null, logFn = null } = {}) {
  const say = typeof logFn === 'function' ? logFn : (m) => logError('watchtower-lib', m);
  const body = (Array.isArray(lines) ? lines : [lines])
    .map((l) => String(l ?? '').replace(/\r?\n/g, ' ').trim())
    .filter(Boolean);
  if (!/^## /.test(String(heading || ''))) {
    say(`appendSessionRecordSection: heading must start with "## " (got ${JSON.stringify(heading)})`);
    return { ok: false, written: 0, error: 'bad-heading' };
  }
  if (body.length === 0) return { ok: true, written: 0 };
  try {
    let content;
    if (existsSync(recordPath)) {
      content = readFileSync(recordPath, 'utf8');
    } else {
      const sid = sessionId || basename(String(recordPath)).replace(/\.md$/, '').replace(/^\d{4}-\d{2}-\d{2}-/, '');
      content = `# Session ${sid}\n\nDate: ${date || new Date().toISOString()}\n\n(summary unavailable — Phase 2b did not complete)\n`;
    }
    const idx = headerLineIndex(content, heading);
    if (idx < 0) {
      content = `${content.trimEnd()}\n\n${heading}\n${body.join('\n')}\n`;
    } else {
      const next = content.indexOf('\n## ', idx + heading.length);
      const sectionEnd = next > 0 ? next : content.length;
      const section = content.slice(idx, sectionEnd).trimEnd();
      const rest = next > 0 ? content.slice(next) : '\n';
      content = `${content.slice(0, idx)}${section}\n${body.join('\n')}\n${rest.startsWith('\n## ') ? rest : rest.replace(/^\n+/, '\n')}`;
    }
    atomicWrite(recordPath, content);
    return { ok: true, written: body.length };
  } catch (e) {
    say(`appendSessionRecordSection: could not append ${heading} to ${recordPath}: ${e.message}`);
    return { ok: false, written: 0, error: e.message };
  }
}

// Residue receipts — state/residue/<sessionId>.json, keyed GLOBALLY by session
// id (T4: the session's resolver and Ring 3's can disagree for worktree or
// unresolved sessions; one key both sides can compute). Shape:
//   { session_id, project_path, asked_at, findings: [{ n, kind, text, proposed, carry_id? }],
//     advisor_findings: [], answered, dispositions }
// /close Step 6c writes it BEFORE it asks (answered: false), --record-answer
// stamps the reply; a receipt with findings: [] counts as answered.
export function residueReceiptPath(sessionId, { watchtowerDir = getWatchtowerDir() } = {}) {
  return join(watchtowerDir, 'state', 'residue', `${sessionId}.json`);
}

export function readResidueReceipt(sessionId, opts = {}) {
  const p = residueReceiptPath(sessionId, opts);
  try {
    if (!existsSync(p)) return null;
    const r = JSON.parse(readFileSync(p, 'utf8'));
    return r && typeof r === 'object' ? r : null;
  } catch {
    return null;
  }
}

export function writeResidueReceipt(receipt, opts = {}) {
  if (!receipt || typeof receipt.session_id !== 'string' || !receipt.session_id) {
    throw new Error('writeResidueReceipt: receipt.session_id is required');
  }
  const p = residueReceiptPath(receipt.session_id, opts);
  atomicWrite(p, JSON.stringify(receipt, null, 2) + '\n');
  return p;
}

export function receiptIsAnswered(receipt) {
  if (!receipt) return false;
  if (Array.isArray(receipt.findings) && receipt.findings.length === 0) return true;
  return receipt.answered === true;
}

// The D5 kill-trigger ledger — one line per Ring 3 close:
// { ts, session_id, project, branch: 'answered' | 'unanswered' | 'no-receipt', lines_written }.
// Fail-open: observability only.
export function appendFallbackLedger(record, { watchtowerDir = getWatchtowerDir() } = {}) {
  try {
    const dir = join(watchtowerDir, 'state', 'residue');
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    appendFileSync(join(dir, 'fallback-ledger.jsonl'), JSON.stringify({ ts: new Date().toISOString(), ...record }) + '\n');
    return true;
  } catch {
    return false;
  }
}

// Project-tracker friction from a session whose project slug is UNRESOLVED
// has no carry ledger to ride (no project key — plan §Edge Cases, decision
// default 22): one line per report here, { ts, session_id, project,
// project_path, title, summary, route }, so it is COUNTED, never dropped
// silently. Ring 1 renders the count in summary.md's ## Inbox section (plan
// B6). Returns the path written, or null on a failed write (the caller logs).
export function unresolvedFrictionPath({ watchtowerDir = getWatchtowerDir() } = {}) {
  return join(watchtowerDir, 'state', 'residue', 'unresolved-friction.jsonl');
}

export function appendUnresolvedFriction(record, { watchtowerDir = getWatchtowerDir() } = {}) {
  try {
    const p = unresolvedFrictionPath({ watchtowerDir });
    const dir = dirname(p);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    appendFileSync(p, JSON.stringify({ ts: new Date().toISOString(), ...record }) + '\n');
    return p;
  } catch {
    return null;
  }
}

// Advisor findings land in the record's ## Advisor findings AND one 'advisor'
// carry entry each (shown once in the next start block, B2). The ONE writer
// for both Ring 3's Phase 2m (advisorPass, plan A4) and the receipt branches
// (the by-hand filers in /debrief and /orient, A9's table), so the two writes
// cannot disagree. A finding is { member, directive_key?, finding, title?,
// skill? }: the record line is `- <member> (<directive_key>): <finding>`, the
// carry entry follows lane 3's render contract — `source.member` plus the
// finding text in `detail` (the start block prints `- <member>: <detail>`).
export function recordAdvisorFindings(recordPath, projectSlug, findings, { sessionId = null, logFn = null, filedBy = 'residue-receipt' } = {}) {
  const list = Array.isArray(findings) ? findings.filter((f) => f && (f.finding || f.text || f.title)) : [];
  if (list.length === 0) return 0;
  const textOf = (f) => String(f.finding || f.text || f.title);
  const lines = list.map((f) => `- ${f.member || 'advisor'}${f.directive_key ? ` (${f.directive_key})` : ''}: ${textOf(f)}`);
  appendSessionRecordSection(recordPath, '## Advisor findings', lines, { sessionId, logFn });
  let n = 0;
  for (const f of list) {
    try {
      appendCarryEntry(projectSlug, {
        kind: 'advisor', title: `${f.member || 'advisor'}: ${String(f.title || textOf(f)).slice(0, 120)}`,
        detail: textOf(f), source_session: sessionId,
        source: {
          member: f.member || null, filed_by: filedBy,
          ...(f.directive_key ? { directive_key: f.directive_key } : {}),
          ...(f.skill ? { skill: f.skill } : {}),
        },
      });
      n++;
    } catch (e) {
      (typeof logFn === 'function' ? logFn : (m) => logError('watchtower-lib', m))(`recordAdvisorFindings: carry append failed: ${e.message}`);
    }
  }
  return n;
}

export function preserveRing3LastSession(freshContent, existingContent) {
  if (!existingContent) return freshContent;
  const existing = extractSection(existingContent, PROJECT_STATE_LAST_SESSION_HEADER);
  if (!existing) return freshContent;

  // Only Ring 3-authored sections are preserved; Ring 1's own ephemeral
  // fallback content is rebuilt fresh every run.
  const firstBodyLine = existing
    .slice(PROJECT_STATE_LAST_SESSION_HEADER.length)
    .split('\n')
    .map((l) => l.trim())
    .find((l) => l.length > 0);
  if (!firstBodyLine || !RING3_LAST_SESSION_ATTRIBUTION.test(firstBodyLine)) {
    return freshContent;
  }

  const preserved = existing.trimEnd();
  const freshIdx = headerLineIndex(freshContent, PROJECT_STATE_LAST_SESSION_HEADER);
  if (freshIdx < 0) {
    return `${freshContent.trimEnd()}\n\n${preserved}\n`;
  }
  const next = freshContent.indexOf('\n## ', freshIdx + PROJECT_STATE_LAST_SESSION_HEADER.length);
  const end = next > 0 ? next : freshContent.length;
  return freshContent.slice(0, freshIdx) + preserved + '\n' + freshContent.slice(end);
}

// ---------------------------------------------------------------------------
// writeProjectStatePreservingRing3 — race-safe project-state rebuild write
// ---------------------------------------------------------------------------
// preserveRing3LastSession is a pure merge; a raw read→merge→atomicWrite
// around it has a read-then-write race: if Ring 3 writes its fresh Last
// Session between Ring 1's snapshot read and the rename, the rebuild is
// computed from a stale snapshot and silently drops the just-authored
// summary until the NEXT session close. This helper closes that window
// with a re-read check-and-retry: merge against a snapshot, re-read to
// verify nothing changed underneath us, and only then write; on change,
// re-merge against the fresh read (up to maxAttempts). If attempts are
// exhausted (pathological churn), it merges against the FRESHEST read and
// writes anyway — the rebuild is never skipped.
//
// Residual window: a write landing between the verify read and the rename
// inside atomicWrite is still theoretically possible — microseconds wide,
// down from the full merge-computation window. If it ever bites in
// practice, the structural fix is the Ring 3 sidecar-file design (option c
// in .claude/plans/watchtower-ring1-race-fix.md).
//
// opts._beforeVerifyHook is a TEST-ONLY injection seam, fired between the
// merge and the verify re-read so tests can deterministically simulate a
// concurrent Ring 3 write. Production callers must not pass it.
//
// Returns { written: true, attempts, exhausted? }.
export function writeProjectStatePreservingRing3(statePath, freshContent, opts = {}) {
  const maxAttempts = opts.maxAttempts ?? 3;
  const read = () => (existsSync(statePath) ? readFileSync(statePath, 'utf8') : null);

  let snapshot = read();
  for (let i = 0; i < maxAttempts; i++) {
    const merged = preserveRing3LastSession(freshContent, snapshot);
    opts._beforeVerifyHook?.();
    const verify = read();
    if (verify !== snapshot) {
      // Someone (Ring 3) wrote since our read — re-merge against it.
      snapshot = verify;
      continue;
    }
    atomicWrite(statePath, merged);
    return { written: true, attempts: i + 1 };
  }

  // Attempts exhausted (pathological churn): merge against the freshest
  // read and write anyway — never skip the rebuild.
  atomicWrite(statePath, preserveRing3LastSession(freshContent, read()));
  return { written: true, attempts: maxAttempts, exhausted: true };
}

// ---------------------------------------------------------------------------
// resolveProjectIdentity — THE canonical "which project is this?" answer
// ---------------------------------------------------------------------------
// Every prior keying bug (Ring 3 phantom projects, mux desk-name mismatches)
// came from call sites re-deriving identity from a path basename. This is the
// one shared resolution: realpath → worktree→main-repo via git-common-dir →
// match against the registries. Identity layering: ~/.claude/cc-registry.json
// is the canonical project list; watchtower config.projects is a projection of
// it whose KEYS are what /inbox and the rings group by, so a config-key match
// names the result. This lib placement is a way-station — the audit's Move 1
// (a mux-level registry+resolver) is the end state.
//
// Returns:
//   { name, slug, path, registered: true }  — path belongs to a watchtower-
//     configured project; `name` is the config key (the inbox grouping key)
//   { name, slug, path, registered: false } — resolved to a real main repo
//     that no registry tracks (BENIGN: an untracked project working as
//     intended; callers file under this name and must NOT warn)
//   null — could not resolve at all (missing path / no repo root derivable):
//     ANOMALOUS; callers must fail loud (file with project_unresolved + warn),
//     never silently basename.
//
// opts.registryPath overrides ~/.claude/cc-registry.json (for tests).
// `config` may be null (registry-only resolution).

function gitMainRoot(p) {
  try {
    const common = execFileSync(
      'git', ['-C', p, 'rev-parse', '--git-common-dir'],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }
    ).trim();
    // --git-common-dir is the MAIN repo's .git even from inside a worktree;
    // it may be relative (".git" from the main root itself). Match the exact
    // component, not a suffix — a BARE repo named foo.git returns itself as
    // the common dir, and endsWith would wrongly take its parent.
    const abs = resolvePath(p, common);
    return basename(abs) === '.git' ? dirname(abs) : abs;
  } catch {
    return null;
  }
}

function safeRealpath(p) {
  try {
    return realpathSync(p);
  } catch {
    return null;
  }
}

export function resolveProjectIdentity(cwdOrPath, config, opts = {}) {
  if (!cwdOrPath) return null;
  const real = safeRealpath(cwdOrPath);
  if (!real) return null;

  const configEntries = Object.entries(config?.projects || {})
    .map(([key, proj]) => ({ key, real: proj?.path && safeRealpath(proj.path) }))
    .filter(e => e.real);
  const registered = (e, path) =>
    ({ name: e.key, slug: slugify(e.key), path, registered: true });

  const mainRoot = gitMainRoot(real);
  const mainReal = mainRoot ? (safeRealpath(mainRoot) || mainRoot) : null;
  // A mux worktree's repo root lies OUTSIDE the worktree path (the worktree
  // dir is not under the main repo). That's the case the basename fallback
  // used to break on: identity belongs to the main repo, and a configured
  // ancestor of the WORKTREE path (if any) must not win over it.
  const isWorktree = !!mainReal
    && real !== mainReal && !real.startsWith(mainReal + '/');

  if (isWorktree) {
    const exact = configEntries.find(e => e.real === mainReal);
    if (exact) return registered(exact, mainReal);
  } else {
    // Inside the path itself (or no git at all): a configured project may be
    // a plain directory (no .git) or a monorepo subdirectory — the matchers
    // this resolver replaced accepted both, so the resolver must not narrow
    // that. Exact match wins; otherwise the longest configured ancestor.
    let best = null;
    for (const e of configEntries) {
      if (e.real === real) { best = e; break; }
      if (real.startsWith(e.real + '/') && (!best || e.real.length > best.real.length)) {
        best = e;
      }
    }
    if (best) return registered(best, best.real);
    if (mainReal) {
      const exact = configEntries.find(e => e.real === mainReal);
      if (exact) return registered(exact, mainReal);
    }
  }

  // No repo root derivable and no configured match — without a root there is
  // no stable identity to name. Anomalous.
  if (!mainReal) return null;

  // cc-registry.json — the canonical project list (covers projects watchtower
  // doesn't track yet).
  const registryPath = opts.registryPath
    || join(homedir(), '.claude', 'cc-registry.json');
  try {
    const reg = JSON.parse(readFileSync(registryPath, 'utf8'));
    for (const proj of reg.projects || []) {
      const projReal = proj?.path && safeRealpath(proj.path);
      if (projReal && projReal === mainReal) {
        const name = proj.name || basename(mainReal);
        return { name, slug: slugify(name), path: mainReal, registered: false };
      }
    }
  } catch {
    // no registry — fall through to the benign untracked shape
  }

  // A real repo no registry tracks: benign, name it by its main root.
  const name = basename(mainReal);
  return { name, slug: slugify(name), path: mainReal, registered: false };
}

// ---------------------------------------------------------------------------
// resolveProjectFromTranscriptSlug — transcript-slug fallback for CLEANED-UP
// mux worktrees (act:29001b07, grp:wt-noise-immunity)
// ---------------------------------------------------------------------------
// resolveProjectIdentity is filesystem-based (safeRealpath + git-common-dir),
// so a worktree that has been cleaned up — routine once auto-cleanup runs —
// leaves NO live path to resolve, and Ring 3's reprocess path fell back to
// the runner's cwd project: 302 of 510 recovered extractions were confidently
// misattributed (dec-bdadfd3c carried project claude-cabinet while its own
// transcript_ref was a northwind worktree slug).
//
// This sibling decodes the transcript DIRECTORY SLUG instead (the basename of
// ~/.claude/projects/<slug>, which encodes the session cwd with '/' and '.'
// both collapsed to '-'):
//
//   /Users/o/.mux/worktrees/northwind-accounts
//     → -Users-o--mux-worktrees-northwind-accounts
//
// Everything after the '--mux-worktrees-' marker is the worktree basename,
// which mux names `<mux-project-name>-<task>` — and the mux project name is
// NOT the registered project key (mux "northwind" → registered
// "claudeconsult-northwind"), so the decode goes through mux's own
// name→path registry (~/.config/mux/projects.json, the one place the
// worktree→project convention lives) and then hands the MAIN-CHECKOUT path
// to the canonical resolver above. Matching is LONGEST-match on a '-'
// boundary; two distinct names matching at the same length (possible because
// the encoding is lossy: 'a.b' and 'a-b' collide) is AMBIGUOUS → null.
// Every failure returns null — the caller files project_unresolved: true,
// NEVER a cwd guess (misattribution is the disease this exists to cure).
//
// ANCHORED (act:5ebf992e): the marker must sit exactly where mux's own
// worktree root puts it — immediately after the encoded home dir, i.e. the
// slug of `~/.mux/worktrees/`. The first-occurrence `indexOf` it replaced
// matched the marker ANYWHERE, so a scratch repo under `/private/tmp/...`
// inside a worktree's scratchpad (slug
// `-private-tmp-...--Users-x--mux-worktrees-cabinet-...`) decoded to the
// project — and the same fallback attributes transcripts in the retrieval
// instrument and sessions in Ring 3. A tmp path can never sit at the home
// anchor, so the anchor subsumes a separate tmp guard. `opts.homeDir`
// overrides the anchor's home (tests build fixtures under tmpdir).
//
// NAME AND SHAPE ARE A CROSS-LANE CONTRACT (act:29001b07 notes, "Cross-lane
// interface note"): ring3-close feature-detects `resolveProjectFromTranscriptSlug`
// by name on its namespace import — absent (or renamed) means every
// slug-only session silently files project_unresolved forever. The encoding
// below mirrors ring3-close's documented `encodeTranscriptSlug` rule: a
// transcript dir slug is the session cwd with every non-[a-zA-Z0-9] char
// replaced by '-'. Mux names are encoded with the SAME rule before matching,
// so a name carrying any slug-collapsed character still matches its own
// worktree slugs; a mismatch fails toward null, never a guess.
//
// opts.muxProjectsPath / opts.registryPath override the registries (tests).

const MUX_WORKTREES_SLUG_MARKER = '--mux-worktrees-';

// Claude Code's transcript-directory encoding of a path: every
// non-alphanumeric character becomes '-'. Exported so a consumer holding only
// such a slug can match it against a registered path without re-deriving the
// rule.
export function encodeSlugComponent(s) {
  return String(s).replace(/[^a-zA-Z0-9]/g, '-');
}

export function resolveProjectFromTranscriptSlug(transcriptDirSlug, config, opts = {}) {
  if (!transcriptDirSlug || typeof transcriptDirSlug !== 'string') return null;
  const anchor = encodeSlugComponent(opts.homeDir || homedir()) + MUX_WORKTREES_SLUG_MARKER;
  if (!transcriptDirSlug.startsWith(anchor)) return null; // not OUR worktree root — nothing to decode
  const rest = transcriptDirSlug.slice(anchor.length);
  if (!rest) return null;

  const muxProjectsPath = opts.muxProjectsPath
    || join(homedir(), '.config', 'mux', 'projects.json');
  let muxProjects;
  try {
    muxProjects = JSON.parse(readFileSync(muxProjectsPath, 'utf8')).projects || {};
  } catch {
    return null; // no mux registry — no convention to decode against
  }

  let best = null;
  let ambiguous = false;
  for (const [muxName, entry] of Object.entries(muxProjects)) {
    const enc = encodeSlugComponent(muxName);
    if (rest !== enc && !rest.startsWith(enc + '-')) continue;
    if (best && enc.length === best.enc.length && muxName !== best.muxName) {
      ambiguous = true;
    } else if (!best || enc.length > best.enc.length) {
      best = { muxName, enc, path: entry?.path };
      ambiguous = false;
    }
  }
  if (!best || ambiguous || !best.path) return null;

  // The mux registry maps to the MAIN checkout, which outlives the worktree —
  // this is what makes the cleaned-up-worktree case resolvable from the slug
  // alone. The canonical resolver owns everything from here (config key
  // match, cc-registry, benign-untracked, null-on-anomaly).
  return resolveProjectIdentity(best.path, config, opts);
}

// ---------------------------------------------------------------------------
// loadBetterSqlite3 — resolve better-sqlite3 from wherever it actually lives
// ---------------------------------------------------------------------------
// The ring scripts run from ~/.claude-cabinet/watchtower/scripts/ with no
// node_modules nearby. NODE_PATH resolution only sees TOP-LEVEL packages in
// each NODE_PATH entry — it does not descend into a package's own
// node_modules, so better-sqlite3 nested under the globally-installed CC
// package is invisible to a bare require(). Resolution candidates, in order:
//   1. Normal resolution from this script's location (works if a top-level
//      install exists)
//   2. The project's own node_modules — any project with a pib.db has
//      better-sqlite3 installed, since pib-db depends on it
//   3. Nested under create-claude-cabinet in each NODE_PATH entry
// Returns a VERIFIED Database constructor, or null if no candidate resolves.
//
// Verification matters: better-sqlite3 loads its native addon lazily at the
// first `new Database()`, NOT at require() — so a bare require "succeeds" on
// a copy whose addon was compiled for a different node ABI (e.g. a stray
// ~/node_modules install built under the default node, shadowing every
// healthy copy in the walk-up; act:b9414039). Constructing a throwaway
// in-memory db forces the addon load inside the per-candidate try, so a
// poisoned candidate falls through instead of winning and blowing up later
// in the caller.

export function verifyDatabaseConstructor(Database) {
  const db = new Database(':memory:');
  db.close();
  return Database;
}

export function loadBetterSqlite3(projectPath) {
  const candidates = [() => require('better-sqlite3')];
  if (projectPath) {
    candidates.push(
      () => createRequire(join(projectPath, 'package.json'))('better-sqlite3'),
      // A yarn/pnpm/bun project keeps the driver in scripts/ (act:4d571240).
      () => createRequire(join(projectPath, 'scripts', 'package.json'))('better-sqlite3')
    );
  }
  for (const entry of (process.env.NODE_PATH || '').split(':').filter(Boolean)) {
    candidates.push(
      () => createRequire(join(entry, 'create-claude-cabinet', 'package.json'))('better-sqlite3')
    );
  }
  for (const load of candidates) {
    try {
      return verifyDatabaseConstructor(load());
    } catch {
      // try the next candidate
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// (The memory-reachability mirror — parseMemoryIndex, parseMemoryRegionPointers,
// memoryGlobToRegex, isMemoryFileReachable, checkMemoryReachability — lived
// here from act:49cb1c27 until 2026-09-29. Simplification Project 1, plan A5
// (act:7c1ce89a) retired it with the validator it mirrored: the memory index
// is the platform's, holding standing facts only, and "reachable from
// MEMORY.md" is no longer a property anything enforces. Everything else is
// recalled on cue; see cabinet/memory-contract.md.)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// log / logError — structured logging with ring name and ISO timestamp
// ---------------------------------------------------------------------------

export function log(ring, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ring} ${ts}] ${msg}`);
}

export function logError(ring, msg) {
  const ts = new Date().toISOString();
  console.error(`[${ring} ${ts}] ERROR: ${msg}`);
}

// ---------------------------------------------------------------------------
// CLI — `node watchtower-lib.mjs resolve <path>` for bash callers
// ---------------------------------------------------------------------------
// Prints the resolveProjectIdentity JSON on stdout. Exit codes: 0 resolved
// (registered or benign), 2 unresolvable (callers must treat as anomalous),
// 1 usage error. Config is best-effort: a missing config.json degrades to
// registry-only resolution rather than erroring, so the CLI works on machines
// where watchtower isn't installed.

// realpath argv[1] so invocation through a symlink still matches
// import.meta.url (which node resolves to the real path).
if (process.argv[1] && import.meta.url === pathToFileURL(safeRealpath(process.argv[1]) || process.argv[1]).href) {
  const [cmd, target] = process.argv.slice(2);
  if (cmd === 'resolve' && target) {
    let config = null;
    try { config = loadConfig(); } catch { /* registry-only */ }
    const identity = resolveProjectIdentity(target, config);
    if (identity) {
      console.log(JSON.stringify(identity));
      process.exit(0);
    }
    console.error(`unresolvable: ${target}`);
    process.exit(2);
  } else if (cmd) {
    console.error('usage: watchtower-lib.mjs resolve <path>');
    process.exit(1);
  }
}

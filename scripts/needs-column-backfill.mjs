#!/usr/bin/env node
/**
 * needs-column-backfill — the one-time move of `needs:<kind>` tag values into
 * the `actions.needs` column they already answer.
 *
 * Plan: `.claude/plans/needs-column-backfill.md`. The column shipped
 * (CHECK-constrained) with readers — `qa-debt-ledger.mjs` prefers it over the
 * verdict's kind — and a writer for NEW values (`updateAction`), but the
 * one-time move of the values that already exist as tags was designed and
 * then skipped. So the payer's selection column is empty while one tracker
 * carries hundreds of open/in-progress actions whose kind sits in a
 * `needs:<kind>` tag, all reported "untyped" — permanently unselectable.
 * Same class as this repo's documented dead fields (`sort_order` at 0 on all
 * rows; `actions.area` rotted): a reader with no writer. The fix is the
 * missing write, not a re-triage.
 *
 * PROPOSE, THEN APPLY — the `proposeDependencyBackfill` shape. A dry run
 * (default) opens the database READ-ONLY and prints exactly what an apply
 * would do; `--apply` re-proposes and applies inside ONE immediate
 * transaction, so nothing can change between the look and the write.
 *
 * THE RULES (plan § "Rules the backfill must hold"):
 *   1. Never clobber. A set column is skipped even when a tag disagrees;
 *      disagreements are counted and named by fid, never resolved silently.
 *   2. Refuse ambiguity. A row carrying more than one distinct kind is
 *      REFUSED by fid — picking the first would fabricate a routing answer.
 *   3. Only the CHECK vocabulary (ACTION_NEEDS, imported — one source). A
 *      value outside it is refused, never coerced, so the write can never
 *      throw against the constraint mid-run.
 *   4. The tag stays. Provenance is permanent; the column becoming
 *      authoritative does not require deleting its source.
 *   5. One transaction, honest tally, post-commit READ-BACK. A write
 *      announced from call success is not a confirmed write.
 *   6. Idempotent. Rule 1 makes the second run a no-op.
 *   7. open/in-progress only. Closed actions are history; typing them
 *      changes nothing any reader asks.
 *
 * The write path is `updateAction` (pib-db-lib), NOT raw SQL: `needs` keeps
 * ONE writer, and an error return is CONVERTED TO A THROW inside the
 * transaction so any refusal unwinds the whole apply (the completeProject
 * pattern — all-or-nothing, never a partial run announced as a count).
 */

import { createRequire } from 'node:module';
import { ACTION_NEEDS, updateAction } from './pib-db-lib.mjs';

/**
 * Distinct kinds named by `needs:` tags, in first-appearance order.
 *
 * Tags are a comma-separated TEXT field. Only a token that STARTS with
 * `needs:` after trimming counts — a `needs:` embedded mid-token (the LIKE
 * pre-filter can match one) is not a typing claim. Values are taken verbatim:
 * no lowercasing, no trimming beyond whitespace — coercion is rule 3's job to
 * refuse, not this parser's job to hide.
 */
export function parseNeedsKinds(tags) {
  const kinds = [];
  for (const raw of String(tags || '').split(',')) {
    const token = raw.trim();
    if (!token.startsWith('needs:')) continue;
    const kind = token.slice('needs:'.length).trim();
    if (!kinds.includes(kind)) kinds.push(kind);
  }
  return kinds;
}

/**
 * Classify every tagged open/in-progress row. Writes NOTHING — callers open
 * the database read-only for a dry run, and the apply path calls this inside
 * its own transaction so the classification and the write see one snapshot.
 */
export function proposeNeedsBackfill(db) {
  const rows = db.prepare(`
    SELECT fid, tags, needs FROM actions
    WHERE deleted_at IS NULL AND status IN ('open','in-progress')
      AND tags LIKE '%needs:%'
    ORDER BY fid
  `).all();

  const proposals = [];
  const refusedAmbiguous = [];
  const refusedUnknownKind = [];
  const disagreements = [];
  const unparsed = [];
  let alreadySet = 0;

  for (const row of rows) {
    const kinds = parseNeedsKinds(row.tags);
    if (kinds.length === 0) {
      // The LIKE matched inside another token. Counted, never silent — a row
      // that looked tagged and produced nothing is a fact worth surfacing.
      unparsed.push({ fid: row.fid, tags: row.tags });
      continue;
    }
    if (row.needs) {
      // Rule 1: the column wins, unconditionally. Agreement is the quiet
      // case; anything else is a named disagreement, not a resolution.
      if (kinds.length === 1 && kinds[0] === row.needs) alreadySet++;
      else disagreements.push({ fid: row.fid, column: row.needs, tagKinds: kinds });
      continue;
    }
    if (kinds.length > 1) {
      refusedAmbiguous.push({ fid: row.fid, kinds, tags: row.tags });
      continue;
    }
    if (!ACTION_NEEDS.includes(kinds[0])) {
      refusedUnknownKind.push({ fid: row.fid, kind: kinds[0], tags: row.tags });
      continue;
    }
    proposals.push({ fid: row.fid, needs: kinds[0] });
  }

  const perKind = {};
  for (const p of proposals) perKind[p.needs] = (perKind[p.needs] || 0) + 1;

  return {
    proposals,
    perKind,
    refusedAmbiguous,
    refusedUnknownKind,
    disagreements,
    unparsed,
    alreadySet,
    scanned: rows.length,
    message: `Scanned ${rows.length} tagged open/in-progress action(s): `
      + `${proposals.length} to set, ${alreadySet} already set (agreeing), `
      + `${disagreements.length} disagreement(s), ${refusedAmbiguous.length} refused (ambiguous), `
      + `${refusedUnknownKind.length} refused (unknown kind), ${unparsed.length} unparsed.`,
  };
}

/**
 * Propose and apply in ONE immediate transaction, then read back.
 *
 * The transaction closes the propose→apply gap: sessions run live against
 * these trackers, and a column set by a peer between a separate look and
 * write would be clobbered. Proposing inside the write transaction makes
 * rule 1 hold under concurrency by construction.
 *
 * The read-back runs AFTER commit and verifies both halves of the contract:
 * the stored column equals what was proposed, and the tag is still present
 * (rule 4). Any mismatch is returned by fid — the caller decides how loud.
 */
export function applyNeedsBackfill(db, deps = {}) {
  const { updateActionFn = updateAction } = deps;

  const runInTx = db.transaction(() => {
    const proposal = proposeNeedsBackfill(db);
    let applied = 0;
    for (const p of proposal.proposals) {
      const res = updateActionFn(db, { fid: p.fid, needs: p.needs });
      if (res && res.error) {
        const msg = typeof res.error === 'string' ? res.error : res.error.message;
        throw new Error(`apply refused at ${p.fid}: ${msg} — nothing was written (all-or-nothing)`);
      }
      applied++;
    }
    return { proposal, applied };
  });
  const { proposal, applied } = runInTx.immediate();

  const check = db.prepare('SELECT needs, tags FROM actions WHERE fid = ?');
  const mismatches = [];
  let confirmed = 0;
  for (const p of proposal.proposals) {
    const row = check.get(p.fid);
    const tagStays = !!row && parseNeedsKinds(row.tags).includes(p.needs);
    if (row && row.needs === p.needs && tagStays) confirmed++;
    else mismatches.push({ fid: p.fid, expected: p.needs, stored: row ? row.needs : null, tagStays });
  }

  return { ...proposal, applied, readback: { confirmed, mismatches } };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/**
 * fileMustExist ALWAYS: a default-flags open of a missing path CREATES an
 * empty pib.db + -wal + -shm before failing (measured — qa-debt-ledger
 * carries the same guard). The dry run additionally opens read-only so
 * "writes nothing" is enforced by the connection, not just by intent.
 */
function openDb(dbPath, { writable }) {
  const require = createRequire(import.meta.url);
  const Database = require('better-sqlite3');
  return writable
    ? new Database(dbPath, { fileMustExist: true, timeout: 5000 })
    : new Database(dbPath, { readonly: true, fileMustExist: true });
}

function printReport(result, { applied }) {
  console.log(result.message);
  const kinds = Object.entries(result.perKind);
  if (kinds.length) {
    console.log('');
    for (const [kind, n] of kinds) console.log(`  ${kind.padEnd(12)} ${n}`);
  }
  for (const r of result.refusedAmbiguous) {
    console.log(`  REFUSED (ambiguous — more than one kind): ${r.fid} [${r.kinds.join(', ')}]`);
  }
  for (const r of result.refusedUnknownKind) {
    console.log(`  REFUSED (unknown kind): ${r.fid} needs:${r.kind}`);
  }
  for (const d of result.disagreements) {
    console.log(`  DISAGREEMENT (column kept): ${d.fid} column=${d.column} tags=[${d.tagKinds.join(', ')}]`);
  }
  for (const u of result.unparsed) {
    console.log(`  UNPARSED (matched but no needs: token): ${u.fid}`);
  }
  if (applied) {
    console.log(`\nApplied ${result.applied} of ${result.proposals.length} proposed; `
      + `read-back confirmed ${result.readback.confirmed}, mismatched ${result.readback.mismatches.length}.`);
    for (const m of result.readback.mismatches) {
      console.log(`  READ-BACK MISMATCH: ${m.fid} expected=${m.expected} stored=${m.stored} tagStays=${m.tagStays}`);
    }
  } else {
    console.log('\nDRY RUN — nothing was written. Re-run with --apply to set the column.');
  }
}

const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].split('/').pop());
if (isMain) {
  const args = process.argv.slice(2);
  const dbIx = args.indexOf('--db');
  const dbPath = dbIx !== -1 ? args[dbIx + 1] : null;
  const apply = args.includes('--apply');
  const json = args.includes('--json');
  if (!dbPath) {
    console.error('usage: node needs-column-backfill.mjs --db <path/to/pib.db> [--apply] [--json]');
    process.exit(2);
  }

  let db;
  try {
    db = openDb(dbPath, { writable: apply });
  } catch (err) {
    console.error(`Cannot open ${dbPath}: ${err.message.split('\n')[0]}`);
    process.exit(1);
  }

  try {
    // An unmigrated tracker has no column to fill. Refuse with the fix named
    // rather than throwing a bare no-such-column mid-classification.
    const hasNeeds = db.prepare('PRAGMA table_info(actions)').all().some((c) => c.name === 'needs');
    if (!hasNeeds) {
      console.error(`${dbPath} has no actions.needs column — run the schema migration first (pib-db.mjs init).`);
      process.exit(1);
    }

    const result = apply ? applyNeedsBackfill(db) : proposeNeedsBackfill(db);
    if (json) {
      console.log(JSON.stringify(result, null, 2));
    } else {
      printReport(result, { applied: apply });
    }
    if (apply && result.readback.mismatches.length > 0) process.exit(1);
    process.exit(0);
  } catch (err) {
    console.error(`Backfill failed: ${err.message}`);
    process.exit(1);
  } finally {
    db.close();
  }
}

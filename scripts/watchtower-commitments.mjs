#!/usr/bin/env node

// Prospective-commitment detection — the shared seam between /close's
// commitment sweep and Ring 3's dedup (act:aa554774).
//
// THE DISTINCTION THIS MODULE ENCODES. /close delegates all capture to the
// background rings, on the reasoning that the operator should be able to walk
// away in 15–30 seconds. That reasoning holds for RETROSPECTIVE capture —
// lessons, decisions, knowledge extraction. Losing a day costs nothing, so the
// inbox is the right home.
//
// It does not hold for PROSPECTIVE, TIME-BOUND commitments made during the
// session. Those have a clock. On 2026-07-30 a session produced two of them —
// "set the new device's vibration to High on arrival" and "cancel the 3-month
// trial before it auto-renews at $9.99/mo" — and both were written into the
// notes of an action that was being marked COMPLETED. Neither became an
// action. /close ran clean. The inbox they would otherwise have landed in
// opened that day at 275 pending, oldest 26 days.
//
// The test, and it is the whole rule: CAN THIS WAIT IN A QUEUE FOR A WEEK
// WITHOUT COST? Retrospective, yes — leave it to the rings. A dated
// commitment, no — it has to be filed while the operator is still there to
// confirm it.
//
// WHAT THIS MODULE IS AND IS NOT. It is a SCREEN and a shared vocabulary, not
// an oracle. /close runs inside the session and can read it semantically;
// this gives that reading a concrete, testable definition and — more
// importantly — gives Ring 3 the SAME definition, so the ring can recognize
// that a commitment it is about to extract has already been filed as an
// action minutes earlier. Two sides of one seam: /close files forward,
// Ring 3 declines to duplicate.
//
// Deliberately NOT in watchtower-lib.mjs: that module is ring-loaded and
// soak-frozen, and this is consumed by a skill and one ring phase.

import { foldTokens, overlapCoefficient } from './watchtower-queue.mjs';

// --- The cue vocabulary ---------------------------------------------------
//
// Two independent signals must BOTH be present for a line to be a candidate:
// a FUTURE-BOUND cue (this refers to something not yet true) and an
// OBLIGATION shape (somebody has to do something). Requiring both is what
// keeps ordinary past-tense narration and hypotheticals out. Each cue class
// is named so a detection can say WHY it fired.

// Something happens at, by, or before a moment — the clock half.
export const TEMPORAL_CUES = [
  ['explicit-date', /\b(19|20)\d{2}-\d{2}-\d{2}\b/i],
  ['explicit-date', /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}(st|nd|rd|th)?\b/i],
  ['deadline', /\b(by|before|no later than|ahead of|in time for)\s+(the\s+)?(\d|next|end|close|monday|tuesday|wednesday|thursday|friday|saturday|sunday|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i],
  ['deadline', /\b(due|deadline|expires?|expiry|expiration|cut ?off)\b/i],
  ['renewal', /\b(auto-?renew(s|ed|al|ing)?|renews? on|renewal date|rebills?|next billing|free trial ends)\b/i],
  ['on-event', /\b(when|once|as soon as)\s+(it|they|he|she|the\s+\w+)\s+(arrives?|lands?|ships?|returns?|comes? back|is delivered|gets? here)\b/i],
  ['on-event', /\bon\s+(arrival|delivery|receipt|landing)\b/i],
  ['relative', /\b(tomorrow|tonight|next\s+(week|month|monday|tuesday|wednesday|thursday|friday|saturday|sunday)|in\s+\d+\s+(days?|weeks?|months?))\b/i],
];

// Somebody has to DO something — the obligation half.
export const OBLIGATION_CUES = [
  ['owed', /\b(still owed|still need(s)? to|need(s)? to|must|have to|has to|should)\b/i],
  ['imperative', /\b(cancel|renew|unsubscribe|return|ship|send|pay|book|call|email|schedule|order|install|reinstall|enable|disable|set|switch|turn (on|off)|follow up|check|confirm|remind)\b/i],
  ['commitment', /\b(i'?ll|we'?ll|i will|we will|going to|plan to|promised to|committed to|agreed to)\b/i],
  ['todo', /\b(don'?t forget|remember to|make sure to|todo|to-do|action item)\b/i],
];

// Past-tense / already-handled markers. A line carrying one of these is
// reporting, not committing — "cancelled the trial before it renewed" is a
// record of a thing that HAPPENED. Without this the imperative cue list turns
// every completed-work sentence into a commitment.
export const SETTLED_MARKERS = [
  /\b(already|cancell?ed|renewed|returned|shipped|sent|paid|booked|called|emailed|scheduled|ordered|installed|enabled|disabled|switched|confirmed|done|completed|finished|no longer needed)\b/i,
];

// A commitment is a sentence, not a paragraph. Anything longer is prose that
// happens to contain a cue.
export const MAX_COMMITMENT_CHARS = 400;

/**
 * Detect prospective, time-bound obligations in a block of text.
 *
 * A line qualifies when it carries BOTH a temporal cue and an obligation cue
 * and does NOT carry a settled marker. Returns one entry per qualifying line,
 * each naming the cues that fired so a caller can explain itself.
 *
 * This is a SCREEN. It is tuned to surface candidates for a human to confirm,
 * never to file anything on its own — the operator approves every action.
 *
 * @param {string} text
 * @returns {Array<{text: string, temporal: string, obligation: string}>}
 */
export function detectCommitments(text) {
  if (typeof text !== 'string' || !text.trim()) return [];
  const out = [];
  const seen = new Set();
  // Split on sentence and line boundaries — a commitment lives in one clause.
  const lines = text
    .split(/\n+|(?<=[.!?])\s+/)
    .map((l) => l.trim())
    .filter(Boolean);
  for (const line of lines) {
    if (line.length > MAX_COMMITMENT_CHARS) continue;
    if (SETTLED_MARKERS.some((re) => re.test(line))) continue;
    const temporal = TEMPORAL_CUES.find(([, re]) => re.test(line));
    if (!temporal) continue;
    const obligation = OBLIGATION_CUES.find(([, re]) => re.test(line));
    if (!obligation) continue;
    const key = commitmentKey(line);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({ text: line, temporal: temporal[0], obligation: obligation[0] });
  }
  return out;
}

/**
 * A normalized dedup key for a commitment — sorted meaningful tokens. Two
 * wordings of the same obligation collapse to the same key only when they are
 * near-identical; for the fuzzier judgment use commitmentMatchesAction.
 * @param {string} text
 * @returns {string}
 */
export function commitmentKey(text) {
  return [...foldTokens(String(text || ''))].sort().join(' ');
}

// How much of the shorter side two texts must share to count as the SAME
// obligation. Deliberately high: the cost of a false match is a real
// commitment silently not filed, which is the exact failure this whole action
// exists to fix. The cost of a miss is one duplicate inbox item the operator
// dismisses in a second.
export const COMMITMENT_MATCH_THRESHOLD = 0.6;
// Below this many meaningful tokens, overlap is not evidence of anything.
export const COMMITMENT_MIN_TOKENS = 3;

/**
 * Does this candidate commitment describe the same obligation as an existing
 * action's text? Overlap coefficient over the shared fold tokenizer — the same
 * recipe the queue's fold pass uses, at a much higher bar.
 * @param {string} candidateText
 * @param {string} actionText
 * @returns {boolean}
 */
export function commitmentMatchesAction(candidateText, actionText) {
  const a = foldTokens(String(candidateText || ''));
  const b = foldTokens(String(actionText || ''));
  if (a.size < COMMITMENT_MIN_TOKENS || b.size < COMMITMENT_MIN_TOKENS) return false;
  return overlapCoefficient(a, b) >= COMMITMENT_MATCH_THRESHOLD;
}

/**
 * Find the already-filed action covering this commitment, if any.
 *
 * `actions` are rows of {fid, text} — the caller decides the window (Ring 3
 * passes actions CREATED during the session, which is precisely the set
 * /close's sweep could have filed).
 *
 * Returns the matching action or null. Never throws on malformed rows.
 * @param {string} candidateText
 * @param {Array<{fid: string, text: string}>} actions
 * @returns {{fid: string, text: string}|null}
 */
export function findFiledCommitment(candidateText, actions) {
  for (const a of Array.isArray(actions) ? actions : []) {
    if (!a || typeof a.text !== 'string') continue;
    if (commitmentMatchesAction(candidateText, a.text)) return a;
  }
  return null;
}

// CLI: `node watchtower-commitments.mjs --scan <file>` prints detected
// commitments as JSON. Exists so the detector can be exercised by hand against
// a real transcript without importing anything.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  const idx = process.argv.indexOf('--scan');
  if (idx === -1 || !process.argv[idx + 1]) {
    console.error('usage: watchtower-commitments.mjs --scan <file>');
    process.exit(2);
  }
  const { readFileSync } = await import('fs');
  let text;
  try {
    text = readFileSync(process.argv[idx + 1], 'utf8');
  } catch (e) {
    console.error(`cannot read ${process.argv[idx + 1]}: ${e.message}`);
    process.exit(1);
  }
  console.log(JSON.stringify(detectCommitments(text), null, 2));
}

#!/usr/bin/env node
'use strict';

// Structural validator for qa-dimensions.yaml.
// Checks: file parses, has dimensions: map, each dimension has
// paths (list, >=1), severity (high|moderate|info), checks (list, >=1).
// No npm YAML parser — hand-parses the constrained schema.

const { readFileSync, existsSync } = require('node:fs');
const { join } = require('node:path');

const candidates = [
  join(process.cwd(), '.claude', 'cabinet', 'qa-dimensions.yaml'),
];

const file = candidates.find(existsSync);
if (!file) {
  // No yaml = no error. Checklist engine is opt-in.
  process.exit(0);
}

const lines = readFileSync(file, 'utf-8').split('\n');
const errors = [];

let foundDimensions = false;
let currentDim = null;
const dimensions = {};

for (let i = 0; i < lines.length; i++) {
  const line = lines[i];
  const num = i + 1;

  if (line.match(/^dimensions:\s*$/)) {
    foundDimensions = true;
    continue;
  }

  if (!foundDimensions) continue;

  // Top-level dimension name (2-space indent, ends with colon)
  const dimMatch = line.match(/^  ([a-z][a-z0-9_-]+):\s*$/);
  if (dimMatch) {
    currentDim = dimMatch[1];
    dimensions[currentDim] = { paths: 0, severity: null, checks: 0, line: num };
    continue;
  }

  if (!currentDim) continue;

  // paths list item (6-space indent + dash)
  if (line.match(/^      - /)) {
    const parent = lines.slice(0, i).reverse().find(l => l.match(/^    (paths|checks):/));
    if (parent && parent.includes('paths:')) {
      dimensions[currentDim].paths++;
    }
    if (parent && parent.includes('checks:')) {
      dimensions[currentDim].checks++;
    }
  }

  // severity field
  const sevMatch = line.match(/^    severity:\s*(\S+)/);
  if (sevMatch) {
    const val = sevMatch[1];
    if (!['high', 'moderate', 'info'].includes(val)) {
      errors.push(`Line ${num}: severity "${val}" must be high|moderate|info`);
    }
    dimensions[currentDim].severity = val;
  }

  // check item with tag
  const tagMatch = line.match(/^      - tag:\s*(\S+)/);
  if (tagMatch) {
    const val = tagMatch[1];
    if (!['run', 'review'].includes(val)) {
      errors.push(`Line ${num}: tag "${val}" must be run|review`);
    }
  }
}

if (!foundDimensions) {
  errors.push('Missing top-level "dimensions:" key');
}

const dimNames = Object.keys(dimensions);
if (foundDimensions && dimNames.length === 0) {
  errors.push('"dimensions:" map is empty — at least one dimension required');
}

for (const [name, d] of Object.entries(dimensions)) {
  if (d.paths === 0) errors.push(`Dimension "${name}" (line ${d.line}): needs at least one path`);
  if (!d.severity) errors.push(`Dimension "${name}" (line ${d.line}): missing severity`);
  if (d.checks === 0) errors.push(`Dimension "${name}" (line ${d.line}): needs at least one check`);
}

if (errors.length > 0) {
  console.error(`qa-dimensions.yaml: ${errors.length} error(s)`);
  errors.forEach(e => console.error(`  ${e}`));
  process.exit(1);
}

console.log(`qa-dimensions.yaml: ${dimNames.length} dimensions, all valid.`);

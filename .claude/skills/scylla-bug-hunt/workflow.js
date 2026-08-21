export const meta = {
  name: 'scylla-bug-hunt',
  description: 'Per-module ScyllaDB audit: scope submodules, codegraph-backed bug research per submodule, triage/dedup, worktree+branch+reproducer per surviving candidate, impact/risk/complexity writeup',
  phases: [
    { title: 'Scope', detail: 'enumerate submodules when not given explicitly' },
    { title: 'Research', detail: 'codegraph-backed logical/performance/scale bug hunt, one agent per submodule' },
    { title: 'Triage', detail: 'dedup and rank candidates across all submodules, pick top N to reproduce' },
    { title: 'Reproduce', detail: 'worktree + branch + failing boost/cqlpy/test.py reproducer per selected candidate' },
    { title: 'Impact', detail: 'impact/risk/complexity writeup for every candidate that actually reproduced' },
  ],
}

// Confidence and severity are two different axes, both required on every candidate that
// survives to a report: confidence is about the hunt (is this real, not a misreading of the
// code); severity is about the bug (how bad if real). Severity buckets match report-template.md.
const CANDIDATE_PROPS = {
  title: { type: 'string' },
  category: { type: 'string', enum: ['logical', 'performance', 'scale'] },
  file: { type: 'string' },
  lines: { type: 'string' },
  hypothesis: { type: 'string' },
  scale_relevance: { type: 'string' },
  confidence: { type: 'string', enum: ['low', 'medium', 'high'] },
  severity: { type: 'string', enum: ['critical', 'high', 'medium', 'low'] },
  evidence: { type: 'string' },
}

const CANDIDATES_SCHEMA = {
  type: 'object',
  properties: {
    candidates: {
      type: 'array',
      items: { type: 'object', properties: CANDIDATE_PROPS, required: ['title', 'category', 'file', 'hypothesis', 'confidence', 'severity'] },
    },
  },
  required: ['candidates'],
}

const TRIAGE_SCHEMA = {
  type: 'object',
  properties: {
    selected: {
      type: 'array',
      items: { type: 'object', properties: { ...CANDIDATE_PROPS, triage_notes: { type: 'string' } }, required: ['title', 'category', 'file', 'hypothesis', 'confidence', 'severity', 'triage_notes'] },
    },
    discarded: {
      type: 'array',
      items: { type: 'object', properties: { title: { type: 'string' }, reason: { type: 'string' } }, required: ['title', 'reason'] },
    },
  },
  required: ['selected', 'discarded'],
}

const REPRO_SCHEMA = {
  type: 'object',
  properties: {
    reproduced: { type: 'boolean' },
    worktree_path: { type: 'string' },
    branch: { type: 'string' },
    reproducer_path: { type: 'string' },
    how_to_run: { type: 'string' },
    evidence: { type: 'string' },
    notes: { type: 'string' },
  },
  required: ['reproduced', 'evidence'],
  // A reproduced:true candidate isn't proven without the artifacts that let someone
  // actually run it -- don't let a candidate reach "confirmed" on evidence text alone.
  if: { properties: { reproduced: { const: true } } },
  then: { required: ['reproduced', 'evidence', 'worktree_path', 'branch', 'reproducer_path', 'how_to_run'] },
}

const IMPACT_SCHEMA = {
  type: 'object',
  properties: {
    report_markdown: { type: 'string' },
  },
  required: ['report_markdown'],
}

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    verdict: { type: 'string', enum: ['affirm', 'reject'] },
    corrected_confidence: { type: 'string', enum: ['low', 'medium', 'high'] },
    corrected_severity: { type: 'string', enum: ['critical', 'high', 'medium', 'low'] },
    rationale: { type: 'string' },
  },
  required: ['verdict', 'rationale'],
}

const CATEGORY_VALUES = ['logical', 'performance', 'scale']
const REQUIRED_SECTIONS = ['Summary', 'Evidence', 'Impact', 'Risk', 'Complexity', 'Recommendation']

// A permissive "does the line exist" regex lets "**Confidence:** invalid" or a blank line
// (swallowed by the next heading matching \S) through as confirmed. Check the values are
// actually one of the enums, and that every required section has real content before the
// next "## " heading, not just a heading with nothing under it. Also validate the candidate's
// own metadata and, for a reproduced candidate, that its reproduction artifacts are usable --
// REPRO_SCHEMA's `required`/`if`/`then` catches missing fields at the JSON level, but an empty
// or whitespace-only string still satisfies `type: 'string'`.
function validateReport(candidate, repro, reportText) {
  const problems = []
  const nonEmpty = s => typeof s === 'string' && s.trim().length > 0
  if (!nonEmpty(candidate && candidate.title)) problems.push('title')
  if (!CATEGORY_VALUES.includes(candidate && candidate.category)) problems.push(`category (got ${JSON.stringify(candidate && candidate.category)})`)
  const rating = (label, allowed) => {
    const m = reportText.match(new RegExp(`\\*\\*${label}:\\*\\*\\s*([^\\s*][^\\n]*)`))
    const value = m && m[1].trim().split(/\s|--|—/)[0].toLowerCase().replace(/[^a-z]/g, '')
    if (!value || !allowed.includes(value)) problems.push(`${label} (got ${JSON.stringify((m && m[1].trim()) || '')})`)
    return value
  }
  const confidence = rating('Confidence', ['low', 'medium', 'high'])
  const severity = rating('Severity', ['critical', 'high', 'medium', 'low'])
  for (const label of ['Module / submodule', 'Worktree', 'Branch']) {
    if (!new RegExp(`\\*\\*${label}:\\*\\*\\s*\\S`).test(reportText)) problems.push(`${label} line`)
  }
  // Split into one block per "## Heading" using each heading's own match boundaries, so an
  // empty section can't be swallowed by the next heading's content the way a lazy
  // (?=\n##\s|$) lookahead would let it.
  const re = /^##\s*([^\n]+?)\s*\n/gm
  const headings = []
  let match
  while ((match = re.exec(reportText))) headings.push({ name: match[1].trim().toLowerCase(), contentStart: re.lastIndex, headingStart: match.index })
  const blocks = new Map()
  headings.forEach((h, i) => {
    const end = i + 1 < headings.length ? headings[i + 1].headingStart : reportText.length
    blocks.set(h.name, reportText.slice(h.contentStart, end).trim())
  })
  for (const section of REQUIRED_SECTIONS) {
    if (!blocks.get(section.toLowerCase())) problems.push(`${section} section`)
  }
  if (repro && repro.reproduced) {
    for (const field of ['worktree_path', 'branch', 'reproducer_path', 'how_to_run', 'evidence']) {
      if (!nonEmpty(repro[field])) problems.push(`repro.${field}`)
    }
  }
  return { problems, confidence, severity }
}

// Rewrites the Confidence/Severity lines in place -- used when Verify corrects a rating
// the Impact stage got wrong, so the report on disk matches what was actually verified.
function applyRatingCorrections(reportText, corrections) {
  let text = reportText
  if (corrections.corrected_confidence) {
    text = text.replace(/\*\*Confidence:\*\*[^\n]*/, `**Confidence:** ${corrections.corrected_confidence} — corrected during verification: ${corrections.rationale}`)
  }
  if (corrections.corrected_severity) {
    text = text.replace(/\*\*Severity:\*\*[^\n]*/, `**Severity:** ${corrections.corrected_severity} — corrected during verification: ${corrections.rationale}`)
  }
  return text
}

if (!args || !args.module) {
  throw new Error('scylla-bug-hunt requires args.module (e.g. "compaction")')
}
const moduleName = args.module
// Number.isInteger guards against the classic `0 || 3` falsy-zero trap: an explicit
// maxReproduce: 0 must mean "reproduce nothing", not silently fall back to the default.
const maxReproduce = Number.isInteger(args.maxReproduce) && args.maxReproduce >= 0 ? args.maxReproduce : 3

// The legacy vnode repair path (repair.cc) is off-limits regardless of how submodules
// were determined -- explicit args.submodules must not be able to bypass this any more
// than a Scope-discovered list can. Case-insensitive so "Repair"/"REPAIR" are covered too.
// Scan the whole object, not just anchor_paths/name -- explicit args.submodules has no
// fixed schema, so a caller could put the anchor in any field (e.g. "path" instead of
// "anchor_paths") and slip past a check that only looked at the two known field names.
const isLegacyRepairAnchor = sm => /\brepair\.cc\b/i.test(typeof sm === 'string' ? sm : JSON.stringify(sm || ''))
function excludeLegacyRepair(mods) {
  if (!/^repair$/i.test(String(moduleName).trim())) {
    return mods
  }
  const kept = mods.filter(sm => !isLegacyRepairAnchor(sm))
  if (kept.length < mods.length) {
    log(`Repair module: excluded ${mods.length - kept.length} submodule(s) anchored in the legacy vnode repair path (repair.cc) -- only tablet/incremental repair is in scope`)
  }
  return kept
}

phase('Scope')
let submodules = args.submodules ? excludeLegacyRepair(args.submodules) : null
if (!submodules) {
  const scoped = await agent(
    `List the real submodules of the ScyllaDB "${moduleName}" module by using codegraph_explore to inspect the actual current code -- do not guess from general knowledge. Ground each submodule in real files/dirs. Aim for 3-6 submodules that are meaningfully distinct pieces of code, not a file-by-file listing. If the module is "repair", exclude the legacy vnode repair path (repair.cc) entirely -- only the tablet/incremental repair path (row_level.cc, incremental.*) is in scope.`,
    {
      label: 'scope',
      model: 'claude-haiku-4-5-20251001',
      effort: 'low',
      schema: {
        type: 'object',
        properties: {
          submodules: {
            type: 'array',
            items: { type: 'object', properties: { name: { type: 'string' }, anchor_paths: { type: 'string' }, rationale: { type: 'string' } }, required: ['name', 'anchor_paths'] },
          },
        },
        required: ['submodules'],
      },
    }
  )
  submodules = excludeLegacyRepair((scoped && scoped.submodules) || [])
  log(`Scope: ${submodules.length} submodule(s) for "${moduleName}": ${submodules.map(s => s.name).join(', ')}`)
}

phase('Research')
const researchResults = await parallel(submodules.map(sm => () =>
  agent(
    `You are auditing the ScyllaDB "${moduleName}" / "${sm.name || sm}" submodule (anchor: ${sm.anchor_paths || sm}) for real bugs. Use codegraph_explore to read the actual current code -- do not rely on general C++ knowledge alone. Hunt for three categories: ` +
    `(1) logical bugs -- races, boundary errors, incorrect merge/tombstone/retry logic; ` +
    `(2) performance issues independent of scale -- unnecessary copies, redundant work in hot loops, blocking the reactor; ` +
    `(3) scale issues -- code whose cost grows the wrong way as the number of tables, tablets, sstables per node, or CQL client connections grows large (reference points: 1000+ tables, 100,000+ tablets, 100,000+ sstables/node, 10,000+ client connections). For every scale candidate, explicitly check whether the "n" that scales is actually one of those four counts, or a small bounded constant -- only report it if it's genuinely one of the four. ` +
    `Return only candidates you'd actually bet are real; a handful of well-evidenced findings beats a long list of maybes. ` +
    `For every candidate, rate both confidence (low/medium/high: how sure you are this is real and not a misreading of the code) and severity (critical/high/medium/low: how bad it is if real -- critical means data loss/corruption/cluster-wide unavailability, high means availability/latency impact at a node or shard or one of the four scale points crossed, medium means degraded performance or a narrow correctness edge case short of data loss, low means cosmetic or only matters far outside the four scale points) -- these are two different axes, not one rating, and neither substitutes for the other.`,
    { label: `research:${sm.name || sm}`, phase: 'Research', model: 'claude-sonnet-5', effort: 'high', schema: CANDIDATES_SCHEMA }
  )
))

const allCandidates = researchResults.filter(Boolean).flatMap(r => r.candidates || [])
log(`Research: ${allCandidates.length} raw candidate(s) across ${submodules.length} submodule(s)`)

if (allCandidates.length === 0) {
  return { module: moduleName, submodules, confirmed: [], discarded: [], note: 'No candidates surfaced by research stage.' }
}

phase('Triage')
const triaged = await agent(
  `Here are ${allCandidates.length} candidate bugs found while auditing ScyllaDB's "${moduleName}" module:\n\n${JSON.stringify(allCandidates, null, 2)}\n\n` +
  `Dedup near-identical candidates, discard low-confidence entries and scale claims where the "n" isn't actually one of {tables, tablets, sstables/node, CQL connections}, and rank the rest. Select at most ${maxReproduce} to carry forward to reproduction -- pick the ones most likely to be real (confidence) AND to matter if real (severity); re-check each selected candidate's confidence/severity ratings against the evidence rather than passing them through unexamined, and correct them if the research stage over- or under-rated one. Everything not selected goes in "discarded" with a one-line reason each; do not drop any candidate without a reason.`,
  { label: 'triage', model: 'claude-opus-5', effort: 'high', schema: TRIAGE_SCHEMA }
)

// The prompt asks the triage agent to cap "selected" at maxReproduce, but nothing stops
// it from returning more -- enforce the cap here too, since Reproduce is the expensive,
// build-bound stage this limit exists to protect.
const triagedSelected = (triaged && triaged.selected) || []
const selected = triagedSelected.slice(0, maxReproduce)
const overflow = triagedSelected.slice(maxReproduce).map(c => ({ title: c.title, reason: `exceeded maxReproduce (${maxReproduce}); triage ranked it below the cutoff` }))
const triageDiscarded = [...((triaged && triaged.discarded) || []), ...overflow]
log(`Triage: selected ${selected.length} of ${allCandidates.length} for reproduction; discarded ${triageDiscarded.length}`)

if (selected.length === 0) {
  return { module: moduleName, submodules, confirmed: [], discarded: triageDiscarded, note: 'Nothing survived triage.' }
}

phase('Reproduce')
const results = await pipeline(
  selected,
  candidate => agent(
    `Reproduce this candidate ScyllaDB bug for real, or falsify it -- don't force a fake pass:\n\n${JSON.stringify(candidate, null, 2)}\n\n` +
    `Steps:\n` +
    `1) Check for another ninja/dbuild process already running on this host; if there is one, wait rather than compete with it for CPU.\n` +
    `2) Create a git worktree + branch under .worktrees/ following this repo's existing naming convention (e.g. .worktrees/bughunt-<short-slug>, branch bughunt/<short-slug>).\n` +
    `3) Read the actual current code at the candidate's location via codegraph_explore/Read before writing anything -- the candidate's file/line hints may have drifted.\n` +
    `4) Write the smallest reproducer that proves the hypothesis: a boost test, a cqlpy test, or a test.py case, whichever fits. For a scale candidate, do NOT provision the literal scale point -- isolate the data structure/algorithm and compare its cost between a small N and a larger but test-feasible N (e.g. 100 vs 5,000-20,000), asserting the growth ratio is wrong relative to what the code's contract implies.\n` +
    `5) Build and run it. For C++ builds in the worktree, use 'dbuild env SCCACHE_SERVER_PORT=<a free port> ninja ...' to avoid sccache serialization stalls. For boost, run only the relevant case with test_config.yaml args. For test.py, pass --no-gather-metrics.\n` +
    `6) If it does NOT reproduce, say so plainly with what you observed instead -- that's a valid, useful outcome, not a failure to hide.\n` +
    `Do not commit beyond what's needed for the reproducer, do not push, do not open a PR.`,
    { label: `reproduce:${candidate.title}`, phase: 'Reproduce', model: 'claude-sonnet-5', effort: 'high', schema: REPRO_SCHEMA }
  ),
  (repro, candidate) => {
    if (!repro || !repro.reproduced) {
      return { candidate, confirmed: false, reason: (repro && repro.evidence) || 'agent failed or could not reproduce', repro }
    }
    return agent(
      `Write the impact/risk/complexity report for this confirmed ScyllaDB bug. Use exactly this structure:\n\n` +
      `# <title>\n\n**Category:**\n**Confidence:** low|medium|high -- how sure the hunt is this is a real bug, not a false positive; justify in one clause\n**Severity:** critical|high|medium|low -- impact if real, per the scale in references/report-template.md\n**Module / submodule:**\n**Worktree:**\n**Branch:**\n**Reproducer:** <reproducer path> -- \`<how to run it>\`\n\n## Summary\n## Evidence\n## Impact\n## Risk\n## Complexity\n## Recommendation\n\n` +
      `Candidate: ${JSON.stringify(candidate, null, 2)}\n\nReproduction result: ${JSON.stringify(repro, null, 2)}\n\n` +
      `Fill the Reproducer line with both repro.reproducer_path and repro.how_to_run -- the path alone or the command alone isn't enough for someone else to run it. ` +
      `Use candidate.confidence and candidate.severity for the Confidence/Severity lines, but re-derive them from what actually reproduced rather than copying blindly -- a candidate rated "medium" confidence pre-reproduction that then reproduced deterministically should usually become "high," and severity should reflect what the reproducer actually showed, not just the original hypothesis. Confidence and severity are never optional and never blank. ` +
      `Be concrete: name the threshold where it starts to matter, distinguish "already happens" from "will happen once X grows," and give an honest complexity estimate for a real fix including whether the fix itself is risky (hot path, on-disk/wire format, needs a migration).`,
      { label: `impact:${candidate.title}`, phase: 'Impact', model: 'claude-opus-5', effort: 'high', schema: IMPACT_SCHEMA }
    ).then(async report => {
      // Missing/empty report text is not a confirmed bug report -- don't let it through as one.
      let reportText = report && report.report_markdown && report.report_markdown.trim()
      if (!reportText) {
        return { candidate, confirmed: false, reason: 'reproduced, but impact writeup was missing or empty', repro }
      }
      // A finding isn't ready for the user without real metadata, a valid category, valid
      // ratings, real section content, and (for a reproduced candidate) non-empty reproduction
      // artifacts -- don't let a candidate reach "confirmed" on a report or repro result that
      // dropped any of them.
      const { problems, severity } = validateReport(candidate, repro, reportText)
      if (problems.length > 0) {
        return { candidate, confirmed: false, reason: `reproduced, but report failed validation: ${problems.join(', ')}`, repro }
      }
      // The same agent that wrote the report also picked its own confidence/severity --
      // self-grading. For anything rated high/critical, get an independent second opinion
      // before it's presented to the user as a high-stakes finding.
      if (severity === 'high' || severity === 'critical') {
        const verify = await agent(
          `Independently verify this ScyllaDB bug report, without trusting its self-assigned confidence/severity. Read the actual current code at the claimed location and the reproducer via codegraph_explore/Read -- don't just re-read the prose.\n\n${reportText}\n\n` +
          `Candidate/reproduction context: ${JSON.stringify({ candidate, repro }, null, 2)}\n\n` +
          `Does the evidence actually support a "${severity}" severity, or is it overstated/understated? "reject" only if the reproducer doesn't actually demonstrate the claimed bug (a false pass, an unrelated failure, or a misread of the code) -- a real bug with a wrong severity rating is "affirm" plus a correction, not a rejection.`,
          { label: `verify:${candidate.title}`, phase: 'Impact', model: 'claude-opus-5', effort: 'high', schema: VERIFY_SCHEMA }
        )
        if (!verify || verify.verdict !== 'affirm') {
          return { candidate, confirmed: false, reason: `reproduced and written up, but failed independent verification: ${(verify && verify.rationale) || 'no verdict returned'}`, repro }
        }
        if (verify.corrected_confidence || verify.corrected_severity) {
          reportText = applyRatingCorrections(reportText, verify)
        }
      }
      return { candidate, confirmed: true, repro, report_markdown: reportText }
    })
  }
)

const confirmed = results.filter(Boolean).filter(r => r.confirmed)
const unreproduced = results.filter(Boolean).filter(r => !r.confirmed)
log(`Reproduce/Impact: ${confirmed.length} confirmed, ${unreproduced.length} did not reproduce`)

return {
  module: moduleName,
  submodules,
  confirmed,
  discarded: [
    ...triageDiscarded,
    ...unreproduced.map(u => ({ title: u.candidate && u.candidate.title, reason: u.reason })),
  ],
}

# Code Clone Review

The clone policy and commands are summarized in
[Best Practices](BEST_PRACTICES.md#quality-gates). Enforcement covers maintained
Python files under `src/` and `scripts/`; tests and documentation are outside
the ordinary scan.

## Tools And Review

The locked development environment provides two complementary detectors:
PySCN 1.33.0 with Type 1–4 analysis and jscpd 5.4.1 for normalized token and
near-miss matches. `scripts/clone_tools.py` owns their effective settings and
validates complete reports; `scripts/check_clones.py` stages the explicit
source scope and checks the findings against `.clone-acceptances.json`.
The command is local and offline. No model, provider key, or inference service
runs in CI.

Run `just clone-check` to inspect the ignored `.clone-reports/review.json`.
Clone detection also runs in blocking `just check`. Review findings by reading
the implementations, callers, and tests. Fix shared behavior when the
implementations and callers genuinely have the same contract. Keep independent
responsibilities separate when their behavior, lifecycle, or error handling
differs, and record an individual reason for each accepted finding in
`.clone-acceptances.json`.

Each acceptance records its detector, content-derived pair identity, finding
count, and review reason. New or changed findings need an individual review;
removed findings make an old acceptance stale. Do not add blanket baseline
entries or source-level detector-ignore markers. The gate checks recorded
decisions; the reviewer must assess whether a pair has the same semantic and
API contract by reading both implementations, callers, and relevant tests.

The detectors can miss clones and can report similar syntax with different
responsibilities. A finding is a review lead, not an automatic refactoring
instruction. Extended semantic or embedding audits are not part of this
project's QA policy.

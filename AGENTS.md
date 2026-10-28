# Agent Rules

This project uses hard gates. Do not weaken, skip, or locally suppress them to
make a change pass.

- `just check` is the static gate: Ruff, preview complexity/refactor checks,
  production print guard, lock sync, types, imports, workflow lint, dependency
  hygiene, supply-chain pin checks, suppression budget, clone review, compile,
  dead-code checks, and packaging smoke must pass.
- Ruff complexity and unused-argument rules are blocking. Preview complexity
  rules that are not covered by Ruff prefixes are checked explicitly, but
  Ruff complexity is only an auxiliary lint signal.
- `just crap-check` is the authoritative radon-backed CRAP threshold gate for
  every function. Coverage influences acceptance only through this CRAP score;
  there is no separate coverage floor.
- `just unit` is the targeted fast behavior lane; full verification already
  includes the complete suite through `just crap-check`.
- `just check` includes clone detection for maintained Python under `src/` and
  `scripts/`; review individual dispositions as described in
  [CODE_CLONES.md](docs/CODE_CLONES.md).
- `just runtime-smoke` must pass for Docker runtime changes.
- `just verify` is the full local gate: `check`, `crap-check` (the complete
  pytest suite, coverage input, and CRAP threshold), dependency audit, and
  `runtime-smoke` (Dockerfile and Compose validation, image build, and runtime
  checks). Do not rerun `unit` or `docker-build` separately as part of full
  verification; use them for focused checks.
- Iterate with targeted local checks. Before a local candidate install, all
  applicable local tests and linters must pass through `just verify`.
  Reuse prior results only for byte-identical relevant inputs with targeted
  checks covering any isolated change. A frozen, committed branch candidate
  may be installed for live feedback through a reviewed transactional entry
  point preserving stop, migration, rollback, and storage-integrity checks.
  Remote CI, PR, security, merge, and release gates do not block this local
  candidate install; the candidate is not a release tag.
- After live feedback, push the accepted change and pass the existing remote
  gates before protected-main merge. The GitHub release follows in the
  background. See [edge.md](edge.md) for the workflow sequence.
- Use `uv` only. Keep `uv.lock` current and use hardlink mode outside Docker.
- Bound direct `uv run pytest` commands with `timeout --signal=TERM --kill-after=5s 600s`.
  If a tool returns a running session, poll it to completion or terminate it before moving on.
- The canonical full-project mutation campaign is the sole exception: run it
  through `just mutation` with one finite 24-hour GNU timeout and a five-second
  KILL grace. Keep the 120-second test timeout, 150-second targeted-mutant
  timeout, independent 600-second coverage scan, 60-second integration
  canary, and two workers. Declaration-time mutants and any mutant whose
  actual selection is the complete suite use a separate 600-second timeout:
  a retained clean baseline collected 2,298 tests (2,296
  passed, 2 skipped) in 287.25 seconds at nice 19/idle I/O; doubling that
  measured duration for two workers leaves about 25 seconds of margin. This
  diagnostic baseline is not a native mutation result. Earlier serial g082
  benchmarks took 103.67–104.88 seconds with different diagnostic flags; the
  cause of the timing difference is unconfirmed. A native two-worker attempt
  timed out at 150.57 seconds after pytest ran 148.85 seconds. Selecting
  `tests/` instead of all 2,294 IDs saved only 1.21 seconds in that older run.
  Default resume
  requires a matching clean committed campaign receipt; use `just mutation
  fresh` only after preserving prior evidence. Any incomplete campaign remains
  incomplete and must be inspected before resuming.
- Keep stable QA and runtime practices in `docs/BEST_PRACTICES.md`; keep this
  file compact.

Fix code until the gates pass. If a gate is wrong, change the gate deliberately
and explain why in the same change.

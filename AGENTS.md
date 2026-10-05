# Agent Rules

This project uses hard gates. Do not weaken, skip, or locally suppress them to
make a change pass.

- `just check` is the static gate: Ruff, preview complexity/refactor checks,
  production print guard, lock sync, types, imports, workflow lint, dependency
  hygiene, supply-chain pin checks, compile, dead-code checks, and packaging
  smoke must pass.
- Ruff complexity and unused-argument rules are blocking. Preview complexity
  rules that are not covered by Ruff prefixes are checked explicitly, but
  Ruff complexity is only an auxiliary lint signal.
- `just crap-check` is the authoritative radon-backed CRAP threshold gate for
  every function. Coverage influences acceptance only through this CRAP score;
  there is no separate coverage floor.
- `just unit` must pass for behavior changes.
- `just runtime-smoke` must pass for Docker runtime changes.
- `just docker-build` must pass because the service runs in Docker; it includes
  Dockerfile and Compose static validation before image build.
- Use `uv` only. Keep `uv.lock` current and use hardlink mode outside Docker.
- Bound direct `uv run pytest` commands with `timeout --signal=TERM --kill-after=5s 600s`.
  If a tool returns a running session, poll it to completion or terminate it before moving on.
- The canonical full-project mutation campaign is the sole exception: run it
  through `just mutation` with one finite 24-hour GNU timeout and a five-second
  KILL grace. Keep the 120-second test timeout, 150-second targeted-mutant
  timeout, independent 600-second coverage scan, 60-second integration
  canary, and two workers. Full-suite declaration-time mutants use their
  separate 300-second timeout. Isolated serial g082 benchmarks took
  103.67–104.88 seconds; a native two-worker attempt did not complete, timing
  out at 150.57 seconds after pytest had run 148.85 seconds. Selecting `tests/`
  instead of all 2,294 IDs saved only 1.21 seconds in the serial benchmark.
  Default resume
  requires a matching clean committed campaign receipt; use `just mutation
  fresh` only after preserving prior evidence. Any incomplete campaign remains
  incomplete and must be inspected before resuming.
- Keep stable QA and runtime practices in `docs/BEST_PRACTICES.md`; keep this
  file compact.

Fix code until the gates pass. If a gate is wrong, change the gate deliberately
and explain why in the same change.

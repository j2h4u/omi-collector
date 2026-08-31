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
- Keep stable QA and runtime practices in `docs/BEST_PRACTICES.md`; keep this
  file compact.

Fix code until the gates pass. If a gate is wrong, change the gate deliberately
and explain why in the same change.

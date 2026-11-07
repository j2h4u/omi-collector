# Runtime invariants and bounded FSM contracts

This is the current technical contract for the supported collector paths. The
baseline is `18806fc`; its runtime and tests are byte-identical to candidate
`696ca55`, whose full verification receipt records 2,904 passing tests. That
receipt does not establish exhaustive state coverage. The earlier audit snapshot
`36becf4` remains separate. The obligations below describe intended behavior;
implementation status is scoped to available tests and source evidence, not a
proof of all executions.

## Assumptions and boundaries

The pendant's sequence namespace remains stable and its firmware can advance
its persisted checkpoint during a read. Only host-controlled ADVANCE is covered
by F01. Accepted audio means a contiguous prefix with durable custody, not
merely a received notification. SQLite and filesystem durability depend on
successful sync and healthy storage. A consumer ACK is truthful only if that
consumer has durably handled every packet; the collector checks identity and
digest, not the consumer's speech tail. Publication, maintenance and capture
are coordinated within the supported singleton process and device lease.
Callbacks, owned async tasks, timers, cancellation and effect completion are
ordered by their actual adapters; sequential models do not enumerate concurrent
schedules or crash cuts. Progress additionally assumes fair scheduling, stable
usable BLE, eventual successful effects and truthful ACK, sufficient or safely
reclaimable space, and a sufficiently long quiet maintenance interval. Small
subthreshold cohorts can remain waiting indefinitely.

## Obligations

Each property gives independent atomic obligations, a producer → effect →
consumer path, implementation evidence and product justification. “Supported”
means the stated paths have evidence under the assumptions above; it does not
mean all possible executions were checked.

- **F01 — durable advance (FM03/FM04/FM05/FM09).** (a) Persist the exact
  contiguous prefix before host ADVANCE. (b) Require fresh compatible INFO
  before ADVANCE. Ring receive → writer barrier and cursor reconciliation →
  session ADVANCE. Compliance: supported for covered paths; firmware's own
  checkpoint behavior remains outside this guarantee. Justification: relevant
  to preserving accepted audio. Evidence: `tests/test_batch_machine.py::test_legal_lifecycle_reaches_retired_only_after_close`.
- **F02 — custody (FM05/FM09/FM11/FM13).** (a) Preserve a durably
  accepted prefix across disconnect and cancellation. (b) Recover it after
  restart. (c) Maintenance must not silently remove unique unretired audio.
  Receive/writer → staged durable custody → recovery, quarantine and publication.
  Compliance: normal service paths supported; the permissive nonempty terminal
  deletion API is a conditional unresolved hazard, with no established normal
  producer. It is neither demonstrated service loss nor authorization to discard
  audio. Justification: relevant to the collector's core purpose. Evidence:
  `tests/test_capture_custody.py::test_ambiguous_drain_with_third_durable_source_fails_stopped`,
  `tests/test_staging_quarantine.py::test_ready_custody_conflict_cannot_be_salvaged_or_terminally_deleted`.
- **F03 — publication eligibility and atomicity (FM06/FM11).** (a) A new
  cohort requires completed drain. (b) Its aggregate duration meets the
  configured threshold. (c) Each public artifact is atomically published and
  immutable. Sealed sources → reservation/materialization and ready commit →
  downstream reader. Compliance: supported for covered cohort and recovery
  paths. Justification: relevant for complete, stable consumer input. Evidence:
  `tests/test_public_audio_v2_recovery.py::test_v2_materialization_process_loss_converges_to_reserved_artifact`.
- **F04 — physical order and time (FM09/FM10/FM11).** (a) Publish unique
  packets in physical source order without duplication. (b) Preserve gaps.
  (c) Preserve causal clock uncertainty, including completed writes. (d) Reuse
  frozen projection on recovery. Staged physical records and clock facts →
  ordered artifact and manifest → consumer timeline. Compliance: current
  producer supported; historical timestamps cannot always be reconstructed.
  Justification: relevant to truthful audio and timeline. Evidence:
  `tests/test_fsm_publication_model.py::test_generated_frontier_preserves_late_custody`,
  `tests/test_public_time_facts.py::test_completed_empty_clock_write_preserves_boundary_after_reload`,
  `tests/test_public_audio_v2_recovery.py::test_legacy_unknown_stage_and_replay_survive_current_clock_upgrade`.
- **F05 — authorized retirement (FM11/FM13).** (a) Require exact
  irrevocable ACK before retiring public audio. (b) Apply the same custody
  authorization to active replay proof. (c) Resume already committed retirement
  before considering a new ACK. Consumer checkpoint → validation and durable
  retirement marker → ready/proof removal. Compliance: covered safety and resume
  paths supported; arbitrary crash cuts are not enumerated. Justification:
  relevant to avoid loss and permit safe reclamation. Evidence:
  `tests/test_retirement_resume_order.py::test_committed_retirement_resumes_before_invalid_new_ack`,
  `tests/test_public_audio_v2_recovery.py::test_v2_retirement_resumes_after_process_loss_between_file_unlinks`.
- **F06 — compaction ownership (FM06/FM13).** (a) Compaction owns an exclusive
  safe phase. (b) It yields to capture and shutdown without losing custody.
  (c) It can complete on a slow healthy disk when a long enough quiet interval
  and resources are available. Maintenance → quiescence and SQLite VACUUM →
  capture and later maintenance. Compliance: safety and controlled yield paths
  supported; physical slow-disk completion is unmeasured. Justification: relevant
  to reclaim space without interrupting capture. Evidence:
  `tests/test_quarantine_maintenance.py::test_admitted_vacuum_ignores_window_expiry_but_yields_to_capture`,
  `tests/test_sqlite_compaction.py::test_sigkill_restart_retains_audio_and_compaction_debt`.
- **F07 — bounded work and source fairness (FM01/FM02/FM06/FM07/FM13).** (a) Unchanged
  settled input does not repeatedly read payload. (b) Retry callers honor the
  shared deadline. (c) A finite quarantine pass continues to healthy later
  sources after a source-local failure. Input revision and source iterator →
  retry/inspection → publication and maintenance. Compliance: supported for
  tested per-owner paths, not arbitrary process scheduling. Justification:
  relevant to efficient operation and progress. Evidence:
  `tests/test_quarantine_maintenance.py::test_source_authentication_io_failure_does_not_starve_next_source`,
  `tests/test_quarantine_maintenance.py::test_publication_retry_timer_observes_exact_deadline_and_saturates_backoff`.
- **F08 — scan incident chronology (FM02/FM12).** (a) Persist a truthful
  latest scan failure. (b) A stale or not-later callback cannot resolve it.
  (c) Persistence retries respect the deadline. (d) Never automatically restart
  foreign services. Scanner and callbacks → health monitor and incident sink →
  operator state. Compliance: repaired failed-start generation path and tested
  retry paths supported; effects settle only with successful storage.
  Justification: relevant to accurate diagnostics and safe autonomous operation.
  Evidence: `tests/test_ble_scan_health.py::test_failed_start_callback_during_incident_open_cannot_resolve_it`,
  `tests/test_ble_scan_health.py::test_retry_deadline_applies_to_flush_and_new_scan_failures`.
- **F09 — conditional end-to-end progress (FM01/FM03/FM06/FM09/FM11/FM13).**
  (a) A finite valid backlog can reach durable drain completion. (b) An eligible
  cohort can publish. (c) An exact truthful ACK can retire it. (d) Independently
  authorized reclamation is not circularly blocked by transient publication
  shortage. Presence/receive → durable custody → publication → ACK/maintenance.
  Compliance: controlled resource-cycle paths supported; physical disk-fill and
  shrink feasibility is unmeasured, and fairness assumptions above are required.
  Justification: relevant to unattended draining and space recovery. Evidence:
  `tests/test_maintenance_reserve_cycle.py::test_transient_publication_allows_covered_reclamation_and_compaction`,
  `tests/test_maintenance_reserve_cycle.py::test_ack_retirement_releases_reserve_for_waiting_publication`.
- **F10 — PHY and installation ownership (FM03/FM14).** (a) Recover a durable
  temporary-PHY marker before service readiness. (b) Retain the marker and fail
  startup if restoration fails. (c) Supported installation keeps a singleton
  writer. (d) Failed forward installation does not resume old writers against
  newly selected state. Override/installer → guarded recovery or forward switch
  → service owner. Compliance: supported for tested startup and reviewed
  installation paths; actual controller crash persistence is unmeasured.
  Justification: relevant to adapter recovery and custody ownership. Evidence:
  `tests/test_service_phy_recovery.py::test_service_recovers_phy_before_readiness`.

FM01–FM07 are explicit component machines; FM08 is the inferred visit/session
composition, and FM09–FM14 are distributed runtime lifecycles. FM08 provides
composition context but is not a primary F-property link. These fourteen
candidates are not a complete model of the service or all their interactions.

Aliases: the prior M01 and WC02/WC01 obligations are represented by F09;
M02 and WC03 are represented by F10.

## Source routes

Paths below are relative to `src/omi_collector/capture/`; arrows name the
principal handoff, not every intermediate call or effect.

| Property | Producer → effect → consumer |
| --- | --- |
| F01 | `application/ring_transport.py` (`RingSession`) → `adapters/attempt_writer.py` (`AttemptWriter`) durable prefix → `application/batch_reconciliation.py` (`_advance_batch`). |
| F02 | `adapters/attempt_writer.py` (`AttemptWriter`) → `adapters/internal_audio_store.py` (`InternalAudioStore`) custody → `application/quarantine_maintenance.py` (`QuarantineMaintenance`) recovery and maintenance. |
| F03 | `application/quarantine_maintenance.py` (`QuarantineMaintenance`) → `adapters/publication.py` publication coordination → `adapters/ready_bundles.py` ready artifact. |
| F04 | `domain/clock_state.py` (`ClockState`) and `adapters/public_time_facts.py` (`PublicTimeProjection`) → `adapters/ready_bundles.py` ordered artifact and frozen timeline. |
| F05 | `application/quarantine_maintenance.py` (`QuarantineMaintenance`) → `adapters/ready_bundles.py` ACK validation and retirement → `adapters/internal_audio_store.py` replay custody. |
| F06 | `application/quarantine_maintenance.py` (`_cleanup_transition`, `QuarantineMaintenance`) → `adapters/publication.py` coordination → `adapters/internal_audio_store.py` storage and reclamation. |
| F07 | `application/quarantine_maintenance.py` (`QuarantineMaintenance`) → `adapters/publication.py` retry and settlement → quarantine/source processing in `application/quarantine_maintenance.py`. |
| F08 | `application/ble_scan_health.py` (`ScanHealthMonitor`) → `adapters/ble_scan_health_store.py` (`BleScanIncidentStore`) persistence → `adapters/public_drain.py` (`PublicDrainProjection`) operator projection. |
| F09 | `application/batch_reconciliation.py` (`BatchReconciler`) → `adapters/ready_bundles.py` publication and ACK → `application/quarantine_maintenance.py` reclamation. |
| F10 | `adapters/phy_guard.py` (`ScopedPhyGuard`) handles PHY state → `application/session_lifecycle.py` (`SessionLifecycle`) owns the session; installation ownership additionally spans the deployment scripts outside this path. |

## Evidence by atomic obligation

The named node exercises the specific scenario stated here; the full baseline
receipt includes the suite, but passing a node does not establish its entire
property. A slash between letters groups only the scenario being checked.

- **F01(a):** `tests/test_attempt_writer.py::test_prefix_and_done_seal_are_worker_owned_and_ordered` checks writer prefix/seal ordering; `tests/test_batch_machine.py::test_legal_lifecycle_reaches_retired_only_after_close` checks transition ordering. Neither alone establishes actual durable writer receipt through ADVANCE. **F01(b):** fresh compatible INFO through transport is not established by these nodes; this obligation remains supported by the source trace rather than a named end-to-end test here.
- **F02(a):** `tests/test_attempt_writer.py::test_partial_tail_is_drained_before_interruption` checks interruption draining, not every cancellation interleaving. **F02(b):** `tests/test_public_audio_v2_integration.py::test_v2_accepted_cohort_without_stage_resumes_after_interruption` checks one cohort recovery path, not every crash cut. **F02(c):** `tests/test_staging_quarantine.py::test_ready_custody_conflict_cannot_be_salvaged_or_terminally_deleted` checks a conflict hold; nonempty terminal-delete authorization remains unresolved.
- **F03(a,b):** `tests/test_public_audio_v2_integration.py::test_v2_waits_for_drain_and_threshold_without_reserving_order` checks waiting on drain and cohort threshold. Previously authorized recovery may finish, and the threshold is for the cohort, not each split file. **F03(c):** `tests/test_public_audio_v2_recovery.py::test_v2_materialization_process_loss_converges_to_reserved_artifact` checks recovery to a reserved artifact, not every atomicity or immutability failure mode.
- **F04(a):** `tests/test_fsm_publication_model.py::test_generated_frontier_preserves_late_custody` checks exact replay and late unique source at the frontier, not every ordering case. **F04(b):** `tests/test_public_audio_v2_integration.py::test_v2_same_cohort_sequence_gap_splits_artifacts_and_marks_second_boundary` checks one gap boundary. **F04(c):** `tests/test_public_time_facts.py::test_completed_empty_clock_write_preserves_boundary_after_reload` checks an empty completed boundary. **F04(d):** `tests/test_public_audio_v2_recovery.py::test_legacy_unknown_stage_and_replay_survive_current_clock_upgrade` checks legacy frozen recovery; historic timestamps cannot always be reconstructed.
- **F05(a,b):** `tests/test_public_audio_v2_recovery.py::test_v2_ack_batch_preflights_both_and_retires_idempotently_after_source_gc` checks batch preflight and repeat retirement after source GC, not every ACK/consumer crash. **F05(c):** `tests/test_retirement_resume_order.py::test_committed_retirement_resumes_before_invalid_new_ack` checks committed removal before invalid new input; it does not enumerate every filesystem crash cut.
- **F06(a):** `tests/test_quarantine_maintenance.py::test_capture_priority_joins_running_retry_and_defers_new_requests` checks capture priority and joining an active retry, not all ownership schedules. **F06(b):** `tests/test_quarantine_maintenance.py::test_admitted_vacuum_ignores_window_expiry_but_yields_to_capture` checks capture yield; shutdown and all custody failure cuts are not exhaustively covered by that node. **F06(c):** the same test checks admission-window behavior in a controlled scenario; physical slow-disk completion remains unmeasured.
- **F07(a):** `tests/test_public_audio_v2_integration.py::test_v2_waiting_for_drain_does_not_read_raw_pages` checks one unchanged waiting case, not every settled input. **F07(b):** `tests/test_quarantine_maintenance.py::test_publication_retry_timer_observes_exact_deadline_and_saturates_backoff` checks the publication timer; it does not cover every caller. **F07(c):** `tests/test_quarantine_maintenance.py::test_source_authentication_io_failure_does_not_starve_next_source` checks one source-local error and later source; arbitrary scheduling is outside its scope.
- **F08(a,b):** `tests/test_ble_scan_health.py::test_failed_start_callback_during_incident_open_cannot_resolve_it` checks a callback during failed-start persistence. **F08(c):** `tests/test_ble_scan_health.py::test_retry_deadline_applies_to_flush_and_new_scan_failures` checks those two persistence paths, not all concurrent effects. **F08(d):** no foreign-service restart is a source-reviewed boundary, not measured by these nodes.
- **F09(a):** `tests/test_public_drain.py::test_completion_requires_zero_info_and_frontier_and_preserves_identity_without_token` checks drain-completion conditions, not a physical pendant transfer. **F09(b):** `tests/test_maintenance_reserve_cycle.py::test_ack_retirement_releases_reserve_for_waiting_publication` checks a controlled reserve cycle. **F09(c):** `tests/test_public_audio_v2_recovery.py::test_v2_ack_batch_preflights_both_and_retires_idempotently_after_source_gc` checks a valid batch and idempotent retirement. **F09(d):** `tests/test_maintenance_reserve_cycle.py::test_transient_publication_allows_covered_reclamation_and_compaction` checks simulated shortage; physical space fill and shrink remain unmeasured. None establishes unconditional end-to-end liveness.
- **F10(a,b):** `tests/test_service_phy_recovery.py::test_service_recovers_phy_before_readiness` checks controlled startup recovery and failure scenarios; actual controller persistence after crash remains unmeasured. **F10(c,d):** supported installation ownership and failed forward-switch behavior are source-reviewed, not established by the PHY test.

## Executable bounded contracts

The following existing tests are the checking route. Run from the project root:

```sh
timeout --signal=TERM --kill-after=5s 600s uv run --no-sync pytest -q tests/test_fsm_scan_model.py tests/test_fsm_publication_model.py tests/test_fsm_cleanup_model.py tests/test_fsm_clock_model.py --hypothesis-show-statistics
```

Configured example counts below are budgets, not measured state or transition
counts. Hypothesis resets each example, generates and shrinks sequences, and
reports sampled labels; random sampling does not establish exhaustive coverage.
Directed tests listed above exercise concrete effects. The full verification
receipt is for the baseline identified above; model statistics from other runs
must not be substituted for its measurements.

- **B — scan health.** Initial closed incident with no failure; events failure,
  stop failure, old/equal/new/foreign callback and flush; no terminal state.
  Only a current callback strictly later than the last failure resolves; stale
  generation is ignored. Maps to `ScanHealthMonitor` and incident sink. Safety
  and reachable callback classes: `tests/test_fsm_scan_model.py::test_bounded_contract_reaches_all_callback_classes`
  enumerates six classes over length four; runtime comparison:
  `tests/test_fsm_scan_model.py::test_generated_scan_failure_chronology` uses
  40 examples of 1–15 events. Flush is enabled in the abstract model; later
  callback plus successful persistence is needed for progress. Concurrent
  callback and cancellation ordering require directed async tests.
- **P — publication.** Initial AVAILABLE/Idle; capture, wake, revision change,
  completion, retry and shutdown drive work through Running, RetryWait,
  Settled, Compacting or Quiescing. CLOSED is terminal; duplicate capture and
  unknown events are rejected, stale tokens have no effect. Maps to
  `PublicationState`, `publication_transition` and publication effects. Safety,
  guards and conditional settlement: `tests/test_fsm_publication_model.py::test_generated_publication_ownership_and_fair_suffix`
  uses 60 examples of 1–20 events; `tests/test_fsm_publication_model.py::test_generated_self_revision_retry_backoff_and_eventual_settlement`
  uses 20 examples with 1–8 failures; `tests/test_fsm_publication_model.py::test_publication_terminal_and_capture_guards`
  checks rejection. The supplied successful suffix witnesses bounded progress;
  it does not prove I/O liveness or arbitrary concurrency.
- **M — maintenance.** Initial waiting; due wake enters ACK → publication →
  raw → expiry → vacuum → waiting; defer returns to waiting. Advance while
  waiting is rejected; there is no terminal state. Maps to `_CleanupState` and
  `_cleanup_transition`, with real effects owned by maintenance. Safety and
  reachability: `tests/test_fsm_cleanup_model.py::test_generated_cleanup_phase_order`
  uses 40 lists of 1–15 events; `tests/test_fsm_cleanup_model.py::test_bounded_cleanup_reachability_and_guards`
  enumerates all 3^6 abstract sequences and required transitions, then supplies
  a fair due-wake/advance suffix. This checks absence of abstract stuck states
  under that suffix, not real disk or scheduler deadlock freedom.
- **C — clock.** Initial prepared intent; mark uncertain, finish and reload
  lead to uncertain or completed state. Wrong ID and invalid completed-state
  transitions are rejected; completed state survives reload. Maps to
  `ClockState` and `PublicTimeProjection`; the independent interval oracle marks
  edges entering, inside and leaving the uncertainty boundary incomparable,
  including an empty interval. Safety and reachable completion:
  `tests/test_fsm_clock_model.py::test_generated_completed_clock_boundary_survives_reload`
  uses 40 examples with start 1–5 and length 0–3;
  `tests/test_fsm_clock_model.py::test_generated_clock_transitions_and_positive_comparability`
  uses 50 sequences of 1–15 events and a successful finish suffix. Successful
  persistence and a known end are required; concurrent writes and crashes during
  writes are outside the model.

These routes check finite safety, reachable states, rejection and supplied fair
progress witnesses. They do not prove unbounded temporal properties, all
hardware and crash behavior, or equivalence between model and implementation.

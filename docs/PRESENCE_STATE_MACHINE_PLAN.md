# Presence State Machine Plan

## Decision

Replace the presence scheduler's interacting product flags with one small,
immutable state machine. Keep Bluetooth, timers, cancellation, and scanner
teardown in the existing asynchronous driver. Add no state-machine library and
do not change the pendant protocol or download session.

The purpose is narrower than a general FSM refactor: make it structurally
impossible to lose a pending retry through inspection, release overlapping
attempts, or represent contradictory cooldown, absence, and retry modes.

## Product contract

The collector must:

- wake promptly when a returning pendant advertises;
- retain a bounded initial fallback when advertisements are unavailable;
- release exactly one collection attempt at a time;
- stop scanning successfully before releasing GATT work;
- suppress repeated attempts during the post-drain cooldown;
- retry an interrupted nearby pendant with bounded backoff;
- remain quiet after a drained pendant becomes absent, while continuing to
  scan for its return;
- tolerate soft scanner failure, cancellation, late callbacks, and repeated
  shutdown without corrupting the next decision.

## Architectural boundary

Presence scheduling is capture application policy. It decides **when** one
collection attempt may run. It does not interpret the pendant wire protocol,
perform Bluetooth operations, persist state, or know Bleak, BlueZ, DBus, and
transport exception types.

The pure model stores only an opaque application-owned connection hint. The
Bluetooth adapter creates that hint and the session provider consumes it.

DDD is used only to make the existing product language and boundary explicit;
there is no aggregate, repository, or new bounded context. SOLID is applied by
separating policy decisions from I/O. DRY is applied by having one freshness
rule, one deadline projection, and one public transition entry point.

## State algebra

Every state is a frozen dataclass. The union is closed and contains no mode
booleans.

| State | Required payload | Meaning |
| --- | --- | --- |
| `Searching` | timer epoch, ordinary fallback deadline | scan for return; initial/ordinary fallback remains available |
| `CoolingDown` | timer epoch, cooldown deadline, next recheck deadline, latest presence evidence, optional advertisement | a clean drain completed; continuous presence cannot bypass cooldown |
| `RetryWaiting` | timer epoch, retry deadline, ordinary fallback deadline, retry index, latest presence evidence, optional advertisement | an interrupted nearby attempt is waiting for bounded retry |
| `Attempting` | one tagged attempt trigger, retained waiting state | one attempt permit has been issued; no second permit is legal |
| `Closed` | none | terminal state; late events are ignored |

Presence evidence is either a successful GATT exchange or an advertisement.
Freshness is always measured from the later timestamp of those two sources. An
advertisement separately carries the opaque candidate, observation time, and
optional RSSI because a GATT proof can be fresh without providing a scanner
candidate.

The attempt trigger is itself a closed union: advertisement with a required
advertisement payload, fallback with an optional fresh advertisement, or rapid
retry with an optional fresh advertisement. This prevents contradictory wake
reason and candidate combinations. `Attempting` retains the previous waiting
state because its retry progression and latest presence evidence are needed
when the outcome arrives. Its retry index comes from a retained `RetryWaiting`
state and starts at zero for attempts released from any other state.
An advertisement trigger itself counts as fresh presence evidence when its
attempt outcome is evaluated. A fallback trigger released from `Searching`
never invents an advertisement candidate.

Mechanical driver facts remain outside product state. In particular, every
scanner start gets a driver-owned generation; its callback is discarded unless
that generation is still active. This prevents a late callback from a failed or
replaced scanner from entering the machine. Advertisement validity depends only
on this scanner generation, never on the timer epoch.

## Events, outcomes, and directives

All events carry monotonic time. Timer events also carry the waiting epoch that
created them. Timer events from an older epoch are normal no-ops; advertisement
events have already been validated by the driver's scanner generation.

Events are:

- matching advertisement;
- timer fired, including the scheduled deadline and epoch;
- attempt finished with a typed outcome;
- shutdown.

Attempt outcomes are exactly:

- clean drain;
- not connected, carrying whether durable progress occurred;
- connected interruption, carrying whether durable progress occurred;
- candidate unavailable.

Durable progress is an attribute of either retryable outcome because an earlier
batch may finish checkpointing even when the new connection fails.
`candidate unavailable` atomically discards the stale candidate and presence
evidence. There is no separate `invalidate_candidate()` mutation and no
stringly typed outcome combination.

The transition result contains the new state and exactly one driver directive:
continue observing until a deadline, stop scanning and begin one attempt, stop,
or no operation for a stale event. The transition function never reads a
clock, performs I/O, or mutates its input.

The initial state is `Searching(timer_epoch=0,
fallback_at=started_at+fallback_seconds)`. Every transition that creates or
arms a new timer increments its timer epoch; a transition that only refreshes
presence evidence does not. A timer is current only when its epoch matches the
state. Every waiting state has one pure armed-deadline projection: the fallback
for `Searching`, the cooldown or later quiet recheck for `CoolingDown`, and the
earlier live retry/fallback deadline for `RetryWaiting`.

## Normative transitions

| Current state | Event | Next state and directive |
| --- | --- | --- |
| `Searching` | current advertisement | `Attempting`; stop scan, then begin `advertisement` attempt |
| `Searching` | current timer before fallback | unchanged; continue observing |
| `Searching` | current timer at/after fallback | `Attempting`; stop scan, then begin `fallback` attempt |
| `CoolingDown` | advertisement before cooldown deadline | refresh evidence; continue observing |
| `CoolingDown` | advertisement at/after cooldown deadline | `Attempting`; stop scan, then begin `advertisement` attempt |
| `CoolingDown` | current timer before cooldown deadline | unchanged; continue observing until cooldown deadline |
| `CoolingDown` | timer at/after cooldown deadline with fresh evidence | `Attempting`; stop scan, then begin `fallback` attempt |
| `CoolingDown` | timer at/after cooldown deadline with stale evidence | remain quiet; set `recheck_at=now+absence_seconds`, increment epoch, and observe |
| `RetryWaiting` | advertisement while evidence remains fresh | refresh evidence; keep the existing retry deadline |
| `RetryWaiting` | advertisement after confirmed absence | `Attempting`; zero retry progression, stop scan, then begin `advertisement` attempt |
| `RetryWaiting` | current timer before both deadlines | unchanged; continue observing until the earlier deadline |
| `RetryWaiting` | current ordinary fallback timer before retry timer | `Attempting`; stop scan, then begin `fallback` attempt |
| `RetryWaiting` | current retry timer with fresh evidence | `Attempting`; stop scan, then begin exactly one `rapid_retry` attempt |
| `RetryWaiting` | current retry timer with stale evidence and fallback due | `Attempting`; stop scan, then begin `fallback` attempt |
| `RetryWaiting` | current retry timer with stale evidence before fallback | `Searching` with the retained fallback deadline and incremented epoch; observe |
| any waiting state | stale timer | unchanged; no operation |
| `Attempting` | timer or advertisement | unchanged; no operation |
| `Attempting` | clean drain | `CoolingDown` anchored by GATT presence proof; observe |
| `Attempting` | connected interruption | `RetryWaiting` anchored by GATT presence proof; observe |
| `Attempting` | not connected with fresh evidence | `RetryWaiting`; observe |
| `Attempting` | not connected without fresh evidence | `Searching`; observe |
| `Attempting` | candidate unavailable | `Searching` with cleared evidence and `fallback_at=now+fallback_seconds`; observe |
| any non-closed state | shutdown | `Closed`; stop |
| `Closed` | any event, including a late attempt outcome | `Closed`; no operation |

After any attempt other than clean drain, ordinary fallback is reset to
`now+fallback_seconds`. A fresh retry schedules
`retry_at=now+rapid_backoff[min(index,last)]`, then advances the stored index;
durable progress resets the index to zero before selecting that delay.
`CoolingDown` starts with both deadlines at
`now+drained_fallback_seconds`; later quiet rechecks never move the original
cooldown deadline.

At the exact absence threshold evidence is stale; one strict freshness rule is
used everywhere. When advertisement and timer become ready together, the
advertisement is handled first. Once either event moves the machine to
`Attempting`, the other event cannot release a second permit.

This arbitration may normalize the internal wake-reason label from `fallback`
to `advertisement` when both become ready during scanner startup; downstream
collection behaviour and the supplied candidate are unchanged.

When retry and fallback deadlines are equal, rapid retry wins with fresh
evidence and fallback wins with stale evidence.

An attempt outcome outside `Attempting` or `Closed` is a programmer error: the
driver closes the machine, reports the error, and raises it. Late callbacks,
stale timers, late outcomes after shutdown, and repeated shutdown are expected
races and are not errors.

## Effect ordering

`begin attempt` is a conditional directive: the driver may expose the permit to
the session lifecycle only after `observer.stop()` completes successfully.
Any failure or cancellation while completing the conditional scanner stop sends
shutdown to the machine before the error is propagated; no GATT work begins.
A normal scanner-start refusal or a timeout with proven cancellation stays soft:
the same timer decision remains live and the driver retries observation only at
the next armed deadline, never in an immediate hot loop. Uncertain cancellation
of either scanner transition closes the machine before propagation because
overlap can no longer be ruled out.

Only one `wait_for_attempt()` call may be active. The driver rejects a concurrent
caller. GATT teardown completes before the matching attempt outcome is sent to
the machine. Cancelling a wait before an attempt directive exists stops the
current scanner generation but leaves the waiting product state reusable;
cancellation while redeeming an attempt directive closes it. Shutdown is
idempotent and makes a bounded best effort to stop scanning.

Permit hand-off is fail-closed: if surrounding maintenance or session setup
raises after `wait_for_attempt()` has returned a permit but before a matching
attempt outcome can be guaranteed, the lifecycle owner closes the machine
before propagating that error. A permit cannot be silently discarded while the
machine remains in `Attempting`.

The driver continues to expose the immutable policy and a pure
`drained_cooldown_remaining_seconds` projection for operator telemetry.
Test-only mutable state inspection is removed rather than preserved.

## Library decision

Automat's deterministic transducer is the useful mental model. Existing
state-machine libraries do not remove our BlueZ effect-ordering problem, so none
becomes a dependency. Frozen dataclasses, enums, and pure functions are
sufficient.

## Delivery slices

- [x] Add the closed state/event/outcome/directive types and pure transition
  function, including the normative transition table as focused tests.
- [x] Convert the asynchronous scheduler into a thin interpreter while keeping
  its bounded scanner lifecycle.
- [x] Replace candidate invalidation plus retry with one typed outcome from the
  session lifecycle.
- [x] Verify the existing Gherkin scenarios and driver effect ordering without
  changing BLE collection behaviour.
- [x] Run the full release gates and an independent adversarial review before
  merge.

## Verification

Pure tests cover every state/event class, both sides of each deadline, stale
epochs, repeated deadline inspection, retry progression, continuous-presence
cooldown, quiet absence and return, advertisement/timer ordering, and shutdown.
The absence boundary is checked immediately before, exactly at, and immediately
after the threshold; existing boundary-sensitive tests are deliberately updated
to the single strict rule.

Driver tests cover an advertisement emitted during scan start, successful scan
stop before attempt release, stop timeout and cancellation without GATT release,
soft scanner-start failure, rejection of concurrent waiters, and absence of
scanner/GATT overlap. They also cover simultaneous advertisement/timer readiness
and a returned permit whose caller fails before using it. Real sleeps and a
property-testing dependency are not required.

The public transition entry point delegates to one small pure handler per state
so the implementation remains readable and passes the repository's blocking
complexity and CRAP gates. Policy validation remains at the existing
configuration boundary. Candidate fields use `repr=False`, and complete states
are never written to logs or metrics.

The quiet recheck is the only additional recovery timer and exists only because
`CoolingDown` can otherwise wait indefinitely after a soft scanner-start
failure; it is not a GATT heartbeat. Other states retry observation at their
ordinary armed deadline. A scanner that reports a successful start but silently
emits nothing is indistinguishable from genuine absence, so this slice
consciously preserves quiet waiting until a new advertisement or process
restart rather than adding speculative GATT probes.

## Acceptance and stopping rule

The slice is complete when product state is exactly one tagged immutable value,
all decisions are pure, repeated inspection cannot change the next transition,
stale events cannot release work, and the existing externally observable
presence behaviour passes unchanged except for the deliberate exact-threshold
consistency rule.

Stop after this presence slice. Do not extend the machine into BLE transfer,
persistence, other devices, diagram generation, a reusable FSM framework, or
hypothetical recovery cases.

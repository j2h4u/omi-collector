Feature: Opportunistic raw collection from an Omi pendant
  The collector uses every period of Bluetooth availability to preserve as much
  original audio as possible and publishes only complete local raw bundles.

  Background:
    Given the pendant may enter or leave Bluetooth range at any time
    And missing audio is worse than duplicate audio
    And GATT INFO is authoritative for currently available records

  Rule: Every visit is an opportunity to collect audio

    Scenario: A nearby pendant is detected promptly
      Given no collection session is active
      When the pendant is observed nearby
      Then one bounded collection attempt starts promptly
      And no overlapping attempt is allowed

    Scenario: Missing advertisements do not prevent collection
      Given the pendant remains nearby without a fresh advertisement
      When the periodic authoritative fallback becomes due
      Then the collector attempts to reach the pendant
      And GATT INFO determines whether records are available

    Scenario: A completed drain respects the pendant battery
      Given all currently available records were collected
      When the final GATT INFO confirms a clean drain
      Then the collector disconnects
      And it waits 900 seconds before checking again
      And advertisements cannot bypass that cooldown

    Scenario: An interrupted visit is retried
      Given a collection attempt is interrupted
      When the pendant remains nearby or later returns
      Then the collector retries with bounded backoff
      And preserves every durable record already received
      And continues collecting later available audio

  Rule: Device operations cannot silently discard available audio

    Scenario: Every read is explicitly bounded
      Given GATT INFO reports available records
      When the collector requests audio
      Then READ has an explicit positive record count
      And the collector never sends CLEAR

    Scenario: Device storage is temporarily unavailable
      Given GATT INFO reports that storage is not ready
      When the collector waits for storage
      Then it retries for a bounded period without READ or ADVANCE
      And reconnects later if storage remains unavailable

    Scenario: Explicit advance follows durable publication
      Given a complete bounded batch was received and validated
      When its original bytes are durably published as one raw bundle
      Then the collector may explicitly ADVANCE only through that batch
      And fresh GATT INFO confirms the resulting device state

    Scenario: An interruption can expose stock firmware loss
      Given stock firmware may move its persisted cursor while serving READ
      When the link fails before all requested records arrive
      Then the collector preserves the durable prefix it received
      And sends no explicit ADVANCE based on unavailable records
      And records only aggregate loss telemetry for irretrievable audio
      And continues collecting later available audio

  Rule: Restart recovery trusts only durable evidence

    Scenario: A unique valid partial survives restart
      Given one unfinished attempt has a valid durable prefix
      When a new process reconciles it with fresh GATT INFO
      Then the collector resumes from the latest position supported by both
      And never invents or silently skips available audio

    Scenario: Damaged or ambiguous unfinished evidence fails closed
      Given unfinished attempt evidence is damaged or ambiguous
      When a new process inspects it before READ
      Then all attributable bytes are preserved separately
      And no explicit ADVANCE is based on that evidence
      And collection continues only from fresh authoritative device state

    Scenario: Suspicious device state does not authorize an advance
      Given fresh cursor evidence is reset, regressed, corrupt, or beyond the batch
      When collection state is reconciled
      Then diagnostic evidence is preserved
      And no explicit ADVANCE is sent
      And later available audio remains collectible

  Rule: Publication is a local durable boundary

    Scenario: A complete batch becomes one raw bundle
      Given every record in a bounded batch was validated
      When the batch is published
      Then downstream consumers see either the complete bundle or nothing
      And the bundle contains the original record bytes without processing

    Scenario: Publication is interrupted
      Given a valid local prefix exists
      When publication cannot complete
      Then its bytes remain recoverable after restart
      And no explicit ADVANCE depends only on incomplete publication
      And a later attempt can finish publication

    Scenario: Collector scope remains narrow and private
      Given the collector processes pendant audio
      When it records state, logs, metrics, or published bundles
      Then it makes no cloud or downstream processing call
      And it never deletes a published bundle
      And raw audio and credentials never appear in logs or metrics

  Rule: Observability never competes with audio preservation

    Scenario: A collection attempt ends
      When its terminal transfer metric can be written
      Then one durable metric records volume, duration, outcome, loss, and version

    Scenario: Observability fails
      Given a log or metric cannot be written
      When audio collection can otherwise continue
      Then the observability failure does not change the collection outcome

  Rule: Optional radio workarounds are reversible

    Scenario: A temporary PHY workaround ends
      Given the collector temporarily changes controller-wide PHY selection
      When collection completes, fails, or is cancelled
      Then the exact prior controller selection is restored

Feature: Opportunistic raw collection from a pendant
  The collector wakes for nearby BLE presence, validates unread state through
  GATT INFO, and drains bounded raw batches without overlapping sessions.

  Background:
    Given scanner observations are fresh and advisory rather than cache state
    And GATT INFO is authoritative for unread packets
    And the collector allows no more than one connection attempt at a time

  Rule: Presence is opportunistic but has an authoritative fallback

    Scenario: A fresh advertisement starts a bounded attempt
      Given the pendant is absent and no GATT session is active
      When a fresh matching advertisement is observed
      Then this collector stops its scanner before provider, PHY, or GATT work
      And it starts one bounded sync attempt promptly
      And GATT INFO confirms unread state before any READ

    Scenario: A missed advertisement cannot suppress fallback
      Given the pendant remains in range but its advertisement is missed
      When no fresh advertisement is observed
      Then an authoritative fallback attempt is due within 30 seconds at startup
      And GATT INFO determines whether packets are unread

    Scenario: A clean drain has a cooldown
      Given a session drains all packets and confirms the final INFO cursor
      When the collector disconnects cleanly
      Then it waits 900 seconds before the next GATT attempt
      And continuous advertisements cannot bypass that cooldown

    Scenario: An interrupted attempt uses bounded retry
      Given a fresh advertisement released a sync attempt
      When connection or transfer is interrupted
      Then the collector records the failed attempt without advancing state
      And retries at 1, 2, 4, 8, 16, and 30 second intervals
      And it never starts an overlapping attempt

  Rule: Presence and GATT work are serialized

    Scenario: Scanner and GATT do not overlap
      Given this collector's scanner is active
      When a sync attempt is released
      Then this collector stops its scanner before GATT work
      And scanner shutdown has its own bounded transition timeout
      And address discovery, connection, and INFO retain separate timeouts

    Scenario: A phone occupying the link does not create a second session
      Given another client is occupying the pendant's BLE link
      When this collector cannot connect
      Then it records a bounded failure
      And the next attempt waits for the retry schedule or fallback
      And a second collector is rejected before READ

    Scenario: No unread packets does not hold a connection
      Given the pendant is reachable
      And authoritative GATT INFO reports no unread packets
      When the INFO check completes
      Then the collector disconnects cleanly
      And it does not hold the connection waiting for data

  Rule: One connection drains sequentially sealed batches
    Scenario: A connection drains sequentially sealed batches
      Given the pendant has unread packets
      And no other collector is consuming from it
      When the collector begins a sync session
      Then it uses one connection for sequential bounded batches
      And it subscribes to data notifications before issuing READ
      And it locally seals each batch before starting the next
      And it confirms the current cursor with fresh INFO between batches

    Scenario: Data after a batch boundary belongs to a later batch
      Given one bounded batch has been durably sealed
      When additional packets are created before the pendant leaves range
      Then the new packets are collected only by a later batch
      And the collector does not reconnect solely because of the boundary

    Scenario: A short visit can leave a recoverable remainder
      Given the pendant is in range for only a short visit
      When RF conditions interrupt a bounded batch
      Then the verified prefix remains durable
      And the next presence or fallback can collect the remaining available data

  Rule: Recovery preserves proof and never invents missing audio

    Scenario: A unique partial resumes after restart
      Given one valid partial has durable attempt and checkpoint metadata
      And a new process obtains fresh INFO
      When the partial overlaps the current device cursor
      Then it replays the overlap
      And every replayed record matches the durable prefix byte-for-byte
      And it continues from the exact next unverified record

    Scenario: Ambiguous partial evidence is quarantined
      Given partial evidence is malformed, legacy, or present in multiple attempts
      When a new process inspects staging before READ
      Then it preserves and quarantines the evidence
      And it sends no blind ADVANCE for the unverified history
      And it continues from the pendant's fresh current cursor

    Scenario: Unavailable history has no persisted identity
      Given historical records are no longer retrievable before the current cursor
      And a verified prefix is durably recorded
      When the collector reconciles with fresh INFO
      Then it records only aggregate diagnostic loss telemetry
      And it sends no ADVANCE for unavailable history
      And it creates no artifact or range identity for the missing records
      And it continues with later available audio

    Scenario: Suspicious cursor evidence does not authorize an advance
      Given fresh cursor evidence is reset, regressed, corrupt, or beyond the batch
      When the collector reconciles collection state
      Then it preserves diagnostic evidence
      And it sends no blind ADVANCE
      And it starts a later READ at the fresh current cursor when available

    Scenario: Firmware drop counters remain observations
      Given INFO reports a firmware dropped-packets counter
      When a later INFO resets or regresses that counter
      Then the latest observation is persisted separately
      And no cursor-gap identity is created
      And raw collection continues without waiting for observation persistence

  Rule: Raw publication is durable and atomic

    Scenario: A complete batch becomes one sealed bundle
      Given every record in a bounded batch has been validated
      When record bytes and checkpoint metadata are fsynced
      Then the collector seals one raw bundle
      And publishes it with an atomic rename
      And the bundle contains original record bytes without downstream processing

    Scenario: Publication failure leaves recoverable evidence
      Given a valid local prefix is sealed
      When publication cannot complete
      Then the source remains visible as salvage-pending evidence
      And no later cursor advance is based only on an incomplete publication
      And a later maintenance pass can retry the publication

    Scenario: Atomic publication survives interruption
      Given publication has renamed a sealed bundle into place
      When the process stops before terminal bookkeeping
      Then recovery verifies the published bundle
      And completes terminal bookkeeping without duplicating its bytes

    Scenario: Quarantine maintenance yields to a new presence
      Given eligible evidence is being hashed or copied for salvage
      When a fresh matching advertisement arrives
      Then maintenance yields at the next bounded I/O boundary
      And it releases the device lock before GATT work
      And unrenamed quarantine evidence remains byte-identical

  Rule: The weak-RF PHY workaround is reversible

    Scenario: A temporary LE 1M guard restores controller state
      Given the collector snapshots the controller's selected PHY set
      When a weak-RF session temporarily removes LE 2M selections
      Then the session can use LE 1M
      And normal completion restores the exact prior set
      And errors, cancellation, and recovery also restore that set

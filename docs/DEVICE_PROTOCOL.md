# Omi CV1 device protocol

This is the wire-contract reference used by the raw-only collector for the
stock Omi CV1 ring-storage interface. The public upstream reference is
[`BasedHardware/omi`](https://github.com/BasedHardware/omi/tree/6f7c57ac1545c1931c806a01605646405d398198)
at pinned revision `6f7c57ac1545c1931c806a01605646405d398198`; see its
[`models.dart`](https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/devices/models.dart),
[`ring_protocol.dart`](https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/devices/ring_protocol.dart),
and firmware
[`storage.c`](https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/omi/firmware/omi/src/lib/core/storage.c).
The implementation is in
[`ring_protocol.py`](../src/omi_collector/capture/domain/ring_protocol.py) and
[`ring_transport.py`](../src/omi_collector/capture/application/ring_transport.py).

## GATT

| Role | UUID | Operations |
| --- | --- | --- |
| Ring storage service | `30295780-4301-eabd-2904-2849adfeae43` | — |
| Control/data characteristic | `30295781-4301-eabd-2904-2849adfeae43` | write commands; notify control and data |
| Status characteristic | `30295782-4301-eabd-2904-2849adfeae43` | read and notify status |

The 16-byte status value contains four little-endian unsigned 32-bit values:
`used_bytes`, `unread_packets`, `free_bytes`, and `rtc_valid`.

## Commands and notifications

All multi-byte ring command fields are big-endian:

| Command | Wire format |
| --- | --- |
| STOP | `03` |
| INFO | `10` |
| READ | `11` + start sequence `u64`; optional count `u32` |
| ADVANCE | `12` + new read sequence `u64` |
| CLEAR | `13` |

`CLEAR` is destructive and is not exposed by the collector. Reads remain
bounded even where the device protocol permits an omitted or zero count.

Control notifications are compactly framed: ACK is `[01,status]`; INFO is
`[02, read_seq:u64, write_seq:u64, capacity:u32, dropped:u64, packet_size:u16]`;
READ_BEGIN is `[05, start_seq:u64, count:u32]`; and DONE is
`[04,status,next_seq:u64]`. DATA notifications begin with `03` and contain an
arbitrary byte fragment. Fragment boundaries are not record boundaries.

Each ring record is exactly 444 bytes: a 4-byte big-endian timestamp followed
by a 440-byte packed Opus payload. The payload contains repeated
`[size:u8][frame:size]` entries and zero padding. The collector reassembles the
byte stream before validating record framing.

## Consumption warning

Stock firmware performs a TX-confirmed, throttled auto-checkpoint while sending
`READ` data. A `READ` can therefore consume pendant data without an explicit
`ADVANCE`. Local restart guarantees end at the last fsynced checkpoint; never
treat a `READ` as nondestructive.

## Weak-RF PHY guard

For weak RF, the collector can force LE 1M by temporarily removing `LE2MTX` and
`LE2MRX` from the controller's selected PHY set. This guard is temporary and
controller-global, not per-device: it snapshots the exact prior set and
restores it on normal completion, error, cancellation, or recovery. Do not
leave a controller-wide PHY change behind after a session.

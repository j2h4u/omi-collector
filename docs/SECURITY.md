# Security

The stock Omi BLE privacy posture is a qualified risk finding, not a proven
exploit. The inspected GATT permissions are ordinary read/write permissions;
encryption-required or authentication-required permissions were not found.
Firmware enables SMP, but the collector does not request bonding by default.
Treat a nearby pendant as potentially readable by an untrusted BLE client.

No unauthorized audio-read test was performed. This document therefore does
not claim that unauthorized access has been demonstrated.

Operators should keep the pendant and the collector host in a trusted radio
environment, restrict filesystem access to raw bundles and staging state, and
avoid placing raw audio or credentials in logs. The collector does not handle
external service credentials or export data.

Please use GitHub's [private vulnerability reporting form](https://github.com/j2h4u/omi-collector/security/advisories/new)
for security reports. It keeps the report private while maintainers investigate
and coordinate a fix.

The public repository currently has secret scanning and push protection enabled,
and `dependency-review` is a required `main` branch check. GitHub secret-scanning
validity checks are unavailable on the current repository plan, so this project
does not claim that validity checks are enabled; revisit that control after a
plan upgrade.

The protocol and permission references are the pinned public
[`BasedHardware/omi`](https://github.com/BasedHardware/omi/tree/6f7c57ac1545c1931c806a01605646405d398198)
revision `6f7c57ac1545c1931c806a01605646405d398198`, especially firmware
[`storage.c`](https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/omi/firmware/omi/src/lib/core/storage.c),
[`transport.c`](https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/omi/firmware/omi/src/lib/core/transport.c),
and the BLE connector implementation. The collector's local validation and
transport live under `src/omi_collector/capture/`.

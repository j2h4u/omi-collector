# Changelog

## [0.11.15](https://github.com/j2h4u/omi-collector/compare/v0.11.14...v0.11.15) (2026-10-08)


### Fixes

* **metrics:** account for sparse ready ranges ([#213](https://github.com/j2h4u/omi-collector/issues/213)) ([dc5625b](https://github.com/j2h4u/omi-collector/commit/dc5625b1c9cd77e58718032d199b553a9b760e62))

## [0.11.14](https://github.com/j2h4u/omi-collector/compare/v0.11.13...v0.11.14) (2026-10-07)


### Tests

* improve collector mutation regression coverage ([2be4777](https://github.com/j2h4u/omi-collector/commit/2be477728ac2d2ab34e33ca92682ee9854c35ab6))

## [0.11.13](https://github.com/j2h4u/omi-collector/compare/v0.11.12...v0.11.13) (2026-10-07)


### Fixes

* **capture:** split discontinuous ready audio ([#209](https://github.com/j2h4u/omi-collector/issues/209)) ([14b78db](https://github.com/j2h4u/omi-collector/commit/14b78db4de380fa2795587e29fc7555b130f45da))

## [0.11.12](https://github.com/j2h4u/omi-collector/compare/v0.11.11...v0.11.12) (2026-10-07)


### Tests

* cover quarantine path guard edges ([#207](https://github.com/j2h4u/omi-collector/issues/207)) ([ab97949](https://github.com/j2h4u/omi-collector/commit/ab97949429de3fefb60a68b40bd9853624b4c352))

## [0.11.11](https://github.com/j2h4u/omi-collector/compare/v0.11.10...v0.11.11) (2026-10-07)


### Fixes

* **deploy:** isolate uv config and release checkout permissions ([#205](https://github.com/j2h4u/omi-collector/issues/205)) ([a48a949](https://github.com/j2h4u/omi-collector/commit/a48a949beb10152ede7c7de12a81f4b26dbf50e7))

## [0.11.10](https://github.com/j2h4u/omi-collector/compare/v0.11.9...v0.11.10) (2026-10-07)


### Fixes

* **capture:** own ready publication lifecycle ([f7290fc](https://github.com/j2h4u/omi-collector/commit/f7290fcd87f5bd1fca8d10a5dc29703e7920a162))
* **config:** remove inactive ready max wait option ([f7290fc](https://github.com/j2h4u/omi-collector/commit/f7290fcd87f5bd1fca8d10a5dc29703e7920a162))
* **mutation:** tolerate verified zombie exit races ([f7290fc](https://github.com/j2h4u/omi-collector/commit/f7290fcd87f5bd1fca8d10a5dc29703e7920a162))


### Tests

* **publication:** retain retry task before wakeup ([f7290fc](https://github.com/j2h4u/omi-collector/commit/f7290fcd87f5bd1fca8d10a5dc29703e7920a162))

## [0.11.9](https://github.com/j2h4u/omi-collector/compare/v0.11.8...v0.11.9) (2026-10-07)


### Style

* **test:** format BLE observability and presence machine tests ([2c38318](https://github.com/j2h4u/omi-collector/commit/2c383188bf4e341a6c7a3dace413073aee610261))
* **test:** format BLE observability and presence machine tests ([c2ab833](https://github.com/j2h4u/omi-collector/commit/c2ab83310683d31cb9895df1bb88def1ceaa030e))


### Tests

* assert presence machine values are immutable ([2c38318](https://github.com/j2h4u/omi-collector/commit/2c383188bf4e341a6c7a3dace413073aee610261))
* assert presence machine values are immutable ([c2ab833](https://github.com/j2h4u/omi-collector/commit/c2ab83310683d31cb9895df1bb88def1ceaa030e))
* cover BLE observation dispatch race and immutable records ([2c38318](https://github.com/j2h4u/omi-collector/commit/2c383188bf4e341a6c7a3dace413073aee610261))
* cover BLE observation dispatch race and immutable records ([c2ab833](https://github.com/j2h4u/omi-collector/commit/c2ab83310683d31cb9895df1bb88def1ceaa030e))

## [0.11.8](https://github.com/j2h4u/omi-collector/compare/v0.11.7...v0.11.8) (2026-10-07)


### Tests

* strengthen presence error mutation coverage ([18d97c0](https://github.com/j2h4u/omi-collector/commit/18d97c0be385cf5470775a3baca2bb715142168d))

## [0.11.7](https://github.com/j2h4u/omi-collector/compare/v0.11.6...v0.11.7) (2026-10-07)


### Fixes

* **test:** narrow owner receipt payload types ([7693ed7](https://github.com/j2h4u/omi-collector/commit/7693ed7361e436e398f7360982ca66316fc061c9))
* **test:** narrow owner receipt payload types ([4b41bff](https://github.com/j2h4u/omi-collector/commit/4b41bff1f8e1d6176cc79d25fbd60646e4125bd2))
* **test:** narrow owner receipt payload types ([87607dd](https://github.com/j2h4u/omi-collector/commit/87607dd466391bdce37d48de33ac35dbbeedb66a))
* **test:** narrow owner receipt payload types ([8d46ccb](https://github.com/j2h4u/omi-collector/commit/8d46ccb57aaa2fd63e2ca4712eaf96f71da81793))
* **test:** narrow owner receipt payload types ([6e48b68](https://github.com/j2h4u/omi-collector/commit/6e48b68fe8b0c1f34621b57dcd0c95f2140a0b20))
* **test:** narrow owner receipt payload types ([6b0aa6e](https://github.com/j2h4u/omi-collector/commit/6b0aa6ebf30648fc629b8e065a70facfd3ec9dcb))


### Style

* format machine immutability test signatures ([54aabc8](https://github.com/j2h4u/omi-collector/commit/54aabc83dcae47ad4babcc0264490b1e69b95fa9))
* format machine immutability test signatures ([b8a77c8](https://github.com/j2h4u/omi-collector/commit/b8a77c8c6f0777a8051669f94ba43cabc20b3aa1))
* format machine immutability test signatures ([6387f0d](https://github.com/j2h4u/omi-collector/commit/6387f0d0ca9c76ff1a9085cdc09d88da9d05a281))
* **test:** format mutation campaign helpers ([7693ed7](https://github.com/j2h4u/omi-collector/commit/7693ed7361e436e398f7360982ca66316fc061c9))
* **test:** format mutation campaign helpers ([4b41bff](https://github.com/j2h4u/omi-collector/commit/4b41bff1f8e1d6176cc79d25fbd60646e4125bd2))
* **test:** format mutation campaign helpers ([87607dd](https://github.com/j2h4u/omi-collector/commit/87607dd466391bdce37d48de33ac35dbbeedb66a))
* **test:** format mutation campaign helpers ([8d46ccb](https://github.com/j2h4u/omi-collector/commit/8d46ccb57aaa2fd63e2ca4712eaf96f71da81793))
* **test:** format mutation campaign helpers ([6e48b68](https://github.com/j2h4u/omi-collector/commit/6e48b68fe8b0c1f34621b57dcd0c95f2140a0b20))
* **test:** format mutation campaign helpers ([6b0aa6e](https://github.com/j2h4u/omi-collector/commit/6b0aa6ebf30648fc629b8e065a70facfd3ec9dcb))


### Tests

* assert machine dataclasses are frozen ([54aabc8](https://github.com/j2h4u/omi-collector/commit/54aabc83dcae47ad4babcc0264490b1e69b95fa9))
* assert machine dataclasses are frozen ([b8a77c8](https://github.com/j2h4u/omi-collector/commit/b8a77c8c6f0777a8051669f94ba43cabc20b3aa1))
* assert machine dataclasses are frozen ([6387f0d](https://github.com/j2h4u/omi-collector/commit/6387f0d0ca9c76ff1a9085cdc09d88da9d05a281))
* bound mutation scope control requests ([#194](https://github.com/j2h4u/omi-collector/issues/194)) ([90d4bf4](https://github.com/j2h4u/omi-collector/commit/90d4bf443a027b308aebacc6fd06302e64f51b50))
* **mutation:** bound owner receipt scenarios ([7693ed7](https://github.com/j2h4u/omi-collector/commit/7693ed7361e436e398f7360982ca66316fc061c9))
* **mutation:** bound owner receipt scenarios ([4b41bff](https://github.com/j2h4u/omi-collector/commit/4b41bff1f8e1d6176cc79d25fbd60646e4125bd2))
* **mutation:** bound owner receipt scenarios ([87607dd](https://github.com/j2h4u/omi-collector/commit/87607dd466391bdce37d48de33ac35dbbeedb66a))
* **mutation:** bound owner receipt scenarios ([8d46ccb](https://github.com/j2h4u/omi-collector/commit/8d46ccb57aaa2fd63e2ca4712eaf96f71da81793))
* **mutation:** bound owner receipt scenarios ([6e48b68](https://github.com/j2h4u/omi-collector/commit/6e48b68fe8b0c1f34621b57dcd0c95f2140a0b20))
* **mutation:** bound owner receipt scenarios ([6b0aa6e](https://github.com/j2h4u/omi-collector/commit/6b0aa6ebf30648fc629b8e065a70facfd3ec9dcb))
* **qa:** bound lifecycle cleanup and assert presence admission contracts ([62adaea](https://github.com/j2h4u/omi-collector/commit/62adaea11168722535c3971228afd66b17a76209))
* **qa:** reduce startup maintenance test locals ([62adaea](https://github.com/j2h4u/omi-collector/commit/62adaea11168722535c3971228afd66b17a76209))

## [0.11.6](https://github.com/j2h4u/omi-collector/compare/v0.11.5...v0.11.6) (2026-10-06)


### Tests

* **qa:** fail stalled mutation paths without waiting for timeouts ([#191](https://github.com/j2h4u/omi-collector/issues/191)) ([3190f49](https://github.com/j2h4u/omi-collector/commit/3190f491f085f6194d24d63704b224d85b62f946))

## [0.11.5](https://github.com/j2h4u/omi-collector/compare/v0.11.4...v0.11.5) (2026-10-05)


### Fixes

* **qa:** budget mutation timeouts by selected test scope ([#189](https://github.com/j2h4u/omi-collector/issues/189)) ([6b9f041](https://github.com/j2h4u/omi-collector/commit/6b9f041763ff0d052dbf2b5efd76c52b45dd37a3))
* **tests:** stabilize mutation audit baseline fixtures ([aef3c99](https://github.com/j2h4u/omi-collector/commit/aef3c99b725978111e219f3cfbcc39d42532daac))

## [0.11.4](https://github.com/j2h4u/omi-collector/compare/v0.11.3...v0.11.4) (2026-10-05)


### Fixes

* **mutation:** select resumable audit job safely ([65b1d6d](https://github.com/j2h4u/omi-collector/commit/65b1d6d67d231dab1e1f13ce8c02beecca3e7da7))

## [0.11.3](https://github.com/j2h4u/omi-collector/compare/v0.11.2...v0.11.3) (2026-10-05)


### Fixes

* **mutation:** bound full-suite and cleanup budgets ([67bc580](https://github.com/j2h4u/omi-collector/commit/67bc5804daee1e393e4d7a181fe84d95db1de795))
* **mutation:** budget full-suite workers from measured baseline ([67bc580](https://github.com/j2h4u/omi-collector/commit/67bc5804daee1e393e4d7a181fe84d95db1de795))


### Tests

* **writer:** bound native failure cleanup ([67bc580](https://github.com/j2h4u/omi-collector/commit/67bc5804daee1e393e4d7a181fe84d95db1de795))

## [0.11.2](https://github.com/j2h4u/omi-collector/compare/v0.11.1...v0.11.2) (2026-10-05)


### Fixes

* **mutation:** isolate nested owner token and report terminal state ([955f20a](https://github.com/j2h4u/omi-collector/commit/955f20a376a8a8a7edeb45d976e784ecc3d96a5e))

## [0.11.1](https://github.com/j2h4u/omi-collector/compare/v0.11.0...v0.11.1) (2026-10-05)


### Fixes

* **mutation:** ignore Gremlins coverage scratch file ([e7b3ef8](https://github.com/j2h4u/omi-collector/commit/e7b3ef8659203e9acb9b34df031d00a2dddc9562))
* **mutation:** repair public audit launch entrypoints ([e7b3ef8](https://github.com/j2h4u/omi-collector/commit/e7b3ef8659203e9acb9b34df031d00a2dddc9562))

## [0.11.0](https://github.com/j2h4u/omi-collector/compare/v0.10.2...v0.11.0) (2026-10-05)


### Features

* **mutation:** harden durable audit lifecycle ([#179](https://github.com/j2h4u/omi-collector/issues/179)) ([681d99e](https://github.com/j2h4u/omi-collector/commit/681d99e37ac6c5911a50685558f86e2ca1b99408))


### Fixes

* **qa:** audit the full project with resumable mutation results and verified outcomes ([f6a5253](https://github.com/j2h4u/omi-collector/commit/f6a525308b3cdb7492384d5bb89c38036934b03e))


### Build

* pin pytest-gremlins mutation runner ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))


### Documentation

* **qa:** document strict selector and provenance controls ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* **qa:** record finite mutation queue disposition totals ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* record CI timeout and readiness repair ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* record mutation remediation evidence ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))


### Tests

* always close BLE transport observers ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* **ble:** cover transport readiness and reader delivery ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* bound disconnect notification wakeup ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* bound opportunistic presence readiness callers ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* bound presence scanner readiness waits ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* bound stalled finalization readiness ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* **capture:** verify clock and retry boundaries ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* clarify presence readiness watchdog binding ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* close all tracked quality journals ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* close quality journals in fake sync runs ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* cover partial retired batch receipts ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* own progress pump tasks with task groups ([658c1e7](https://github.com/j2h4u/omi-collector/commit/658c1e7af0f64a1e19b79a42b7205cae32d87bd5))
* **quality:** strengthen metric and observation contracts ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* **quarantine:** preserve primary failures through cleanup ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* **queue:** reconcile finite mutation outcomes by stable identity ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* **storage:** cover durable staging and recovery invariants ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* **telemetry:** verify child cleanup and FIFO failure handling ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))
* **writer:** verify progress ownership and cancellation behavior ([194baf3](https://github.com/j2h4u/omi-collector/commit/194baf3d62d057ba1161a26f228ce96983978789))

## [0.10.2](https://github.com/j2h4u/omi-collector/compare/v0.10.1...v0.10.2) (2026-10-01)


### Fixes

* **qa:** audit lifecycle behavior with reliable mutation tests ([#171](https://github.com/j2h4u/omi-collector/issues/171)) ([c8e3372](https://github.com/j2h4u/omi-collector/commit/c8e337222c4f84906672c9514cbddb95a3e6bfc3))

## [0.10.1](https://github.com/j2h4u/omi-collector/compare/v0.10.0...v0.10.1) (2026-10-01)


### Refactoring

* **capture:** share clock ledger locking and remove CLI forwarding ([#169](https://github.com/j2h4u/omi-collector/issues/169)) ([389532c](https://github.com/j2h4u/omi-collector/commit/389532c4c28cc0e33e06ea07aedd4db3c9415489))

## [0.10.0](https://github.com/j2h4u/omi-collector/compare/v0.9.9...v0.10.0) (2026-10-01)


### Features

* **capture:** accumulate audio across drained visits before packaging ([#167](https://github.com/j2h4u/omi-collector/issues/167)) ([7319a95](https://github.com/j2h4u/omi-collector/commit/7319a956650fbb16086c03bab12d2ef9e81cd4b6))

## [0.9.9](https://github.com/j2h4u/omi-collector/compare/v0.9.8...v0.9.9) (2026-09-30)


### Tests

* **capture:** bound durable resume checks without masking retries ([#165](https://github.com/j2h4u/omi-collector/issues/165)) ([ce23e41](https://github.com/j2h4u/omi-collector/commit/ce23e418fdad2c8d68cb3906b361baa14fa7e0a3))

## [0.9.8](https://github.com/j2h4u/omi-collector/compare/v0.9.7...v0.9.8) (2026-09-30)


### Fixes

* **capture:** coordinate clock publication and foreground leases ([#163](https://github.com/j2h4u/omi-collector/issues/163)) ([dbc3f5d](https://github.com/j2h4u/omi-collector/commit/dbc3f5d77a0d78c92cd20dca928fd18f8c9fb8f4))

## [0.9.7](https://github.com/j2h4u/omi-collector/compare/v0.9.6...v0.9.7) (2026-09-30)


### Fixes

* **capture:** enforce complete lifecycle transition contracts ([#161](https://github.com/j2h4u/omi-collector/issues/161)) ([28647a5](https://github.com/j2h4u/omi-collector/commit/28647a5d26971494ac9c2be111c572c202a0a958))

## [0.9.6](https://github.com/j2h4u/omi-collector/compare/v0.9.5...v0.9.6) (2026-09-30)


### Fixes

* **status:** clear publication warning after successful no-op recovery ([#159](https://github.com/j2h4u/omi-collector/issues/159)) ([3582a12](https://github.com/j2h4u/omi-collector/commit/3582a12df86e555ad6d943dd31944ad9d8e36474))

## [0.9.5](https://github.com/j2h4u/omi-collector/compare/v0.9.4...v0.9.5) (2026-09-30)


### Fixes

* **clock:** extend presence preflight budget ([#156](https://github.com/j2h4u/omi-collector/issues/156)) ([94493b2](https://github.com/j2h4u/omi-collector/commit/94493b21d6e1b3103e4191626cf0bf6de1c950d2))

## [0.9.4](https://github.com/j2h4u/omi-collector/compare/v0.9.3...v0.9.4) (2026-09-30)


### Fixes

* **status:** preserve final info and bound battery refresh ([#154](https://github.com/j2h4u/omi-collector/issues/154)) ([628bcda](https://github.com/j2h4u/omi-collector/commit/628bcdae989ba8aa95db0bb198cfb89f3ff8432d))

## [0.9.3](https://github.com/j2h4u/omi-collector/compare/v0.9.2...v0.9.3) (2026-09-30)


### Fixes

* **status:** refresh battery after successful drain ([#152](https://github.com/j2h4u/omi-collector/issues/152)) ([5a58e0f](https://github.com/j2h4u/omi-collector/commit/5a58e0ff04c55ae54380f06cf91401690675ce9c))

## [0.9.2](https://github.com/j2h4u/omi-collector/compare/v0.9.1...v0.9.2) (2026-09-30)


### Fixes

* **clock:** recover automatic corrections and preserve audio order ([#150](https://github.com/j2h4u/omi-collector/issues/150)) ([69f70b3](https://github.com/j2h4u/omi-collector/commit/69f70b392f3fa2ae26c127965e88bd71c55a8697))

## [0.9.1](https://github.com/j2h4u/omi-collector/compare/v0.9.0...v0.9.1) (2026-09-30)


### Documentation

* **ops:** check host BLE discovery before diagnosing pendant ([#147](https://github.com/j2h4u/omi-collector/issues/147)) ([79668c9](https://github.com/j2h4u/omi-collector/commit/79668c9bc00c94d886b2c22272ba0e886a76a8f0))

## [0.9.0](https://github.com/j2h4u/omi-collector/compare/v0.8.2...v0.9.0) (2026-09-30)


### Features

* **capture:** publish audio at complete pendant visit boundaries ([0c16742](https://github.com/j2h4u/omi-collector/commit/0c1674232674dd00e61b59cc9ef530afd103f364))


### Fixes

* **capture:** recover interrupted prefixes before waiting for advertisements ([0c16742](https://github.com/j2h4u/omi-collector/commit/0c1674232674dd00e61b59cc9ef530afd103f364))
* **capture:** retain publication ownership through repeated cancellation ([0c16742](https://github.com/j2h4u/omi-collector/commit/0c1674232674dd00e61b59cc9ef530afd103f364))

## [0.8.2](https://github.com/j2h4u/omi-collector/compare/v0.8.1...v0.8.2) (2026-09-25)


### Maintenance

* **deps:** sync Ruff and GitHub Actions updates ([#139](https://github.com/j2h4u/omi-collector/issues/139)) ([db11e7c](https://github.com/j2h4u/omi-collector/commit/db11e7c7065db0d8f4276009bd31272d51ff80b3))

## [0.8.1](https://github.com/j2h4u/omi-collector/compare/v0.8.0...v0.8.1) (2026-09-25)


### Fixes

* restore Bluetooth startup and device status ([#137](https://github.com/j2h4u/omi-collector/issues/137)) ([d3510d3](https://github.com/j2h4u/omi-collector/commit/d3510d34959e8e09ffd9bebb8023cb345d0b76d1))

## [0.8.0](https://github.com/j2h4u/omi-collector/compare/v0.7.6...v0.8.0) (2026-09-24)


### Features

* batch contiguous Omi drafts into hour-scale ready bundles ([b1bdb8e](https://github.com/j2h4u/omi-collector/commit/b1bdb8e5a5219f721a87b926d2090bea0616af52))
* batch contiguous Omi drafts into hour-scale ready bundles ([3276dd0](https://github.com/j2h4u/omi-collector/commit/3276dd05484f1e19bae3232509cc3e2bf9ec94d6))

## [0.7.6](https://github.com/j2h4u/omi-collector/compare/v0.7.5...v0.7.6) (2026-09-24)


### Refactoring

* **collector:** remove unused capture results ([#133](https://github.com/j2h4u/omi-collector/issues/133)) ([715cddd](https://github.com/j2h4u/omi-collector/commit/715cddd3f04aff9664f82858556e1e84c0f6d9c6))

## [0.7.5](https://github.com/j2h4u/omi-collector/compare/v0.7.4...v0.7.5) (2026-09-23)


### Refactoring

* **collector:** remove unused compatibility capture APIs ([#131](https://github.com/j2h4u/omi-collector/issues/131)) ([301f0b5](https://github.com/j2h4u/omi-collector/commit/301f0b5c9a07959a6f8e2d7b3ed5a63089c24b56))

## [0.7.4](https://github.com/j2h4u/omi-collector/compare/v0.7.3...v0.7.4) (2026-09-23)


### Refactoring

* **collector:** remove obsolete capture paths ([#129](https://github.com/j2h4u/omi-collector/issues/129)) ([0da2550](https://github.com/j2h4u/omi-collector/commit/0da25509dae62d0e6c6b26a612de03eeed132c2e))

## [0.7.3](https://github.com/j2h4u/omi-collector/compare/v0.7.2...v0.7.3) (2026-09-23)


### Fixes

* **collector:** attribute staging lock contention in service status ([#127](https://github.com/j2h4u/omi-collector/issues/127)) ([21e46c6](https://github.com/j2h4u/omi-collector/commit/21e46c6984bececec4610bc1ba20c5d38b956e96))

## [0.7.2](https://github.com/j2h4u/omi-collector/compare/v0.7.1...v0.7.2) (2026-09-23)


### Maintenance

* remove completed two-zone migration artifacts ([#125](https://github.com/j2h4u/omi-collector/issues/125)) ([de245ce](https://github.com/j2h4u/omi-collector/commit/de245cede580e409fc2154a976fbacdddbace467))

## [0.7.1](https://github.com/j2h4u/omi-collector/compare/v0.7.0...v0.7.1) (2026-09-23)


### Fixes

* **collector:** make two-zone migration root-runnable ([#123](https://github.com/j2h4u/omi-collector/issues/123)) ([94bc6ae](https://github.com/j2h4u/omi-collector/commit/94bc6ae78d2d4bd760cf657a9d5de8459490bd92))

## [0.7.0](https://github.com/j2h4u/omi-collector/compare/v0.6.18...v0.7.0) (2026-09-23)


### Features

* **clock:** persist confirmed RTC segment evidence ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))
* **collector:** add one-time two-zone migration tool ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))
* **collector:** finalize raw drafts into ready bundles ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))
* **collector:** retire acknowledged ready bundles ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))


### Fixes

* **clock:** bound RTC timestamp uncertainty ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))
* **collector:** accept terminal ready acknowledgements ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))


### Documentation

* plan two-zone Omi pipeline migration ([0fe50ef](https://github.com/j2h4u/omi-collector/commit/0fe50ef9d62c20364a39360ea82059fb4f86cd94))

## [0.6.18](https://github.com/j2h4u/omi-collector/compare/v0.6.17...v0.6.18) (2026-09-23)


### Fixes

* **clock:** keep healthy observations ephemeral ([5b64a9d](https://github.com/j2h4u/omi-collector/commit/5b64a9d9d0fd52c97d2bae8b472c98d699d5f134))
* restore pendant clock correction ([88a5ac9](https://github.com/j2h4u/omi-collector/commit/88a5ac90b18888e0901a922790976a061e810b70))

## [0.6.17](https://github.com/j2h4u/omi-collector/compare/v0.6.16...v0.6.17) (2026-09-23)


### Tests

* bound pytest runs and close writer threads on failure ([#117](https://github.com/j2h4u/omi-collector/issues/117)) ([6929926](https://github.com/j2h4u/omi-collector/commit/69299266b0f701e7266bd90dc0be05cc4fbbc53a))

## [0.6.16](https://github.com/j2h4u/omi-collector/compare/v0.6.15...v0.6.16) (2026-09-22)


### Fixes

* preserve concurrent writer finalization ([#115](https://github.com/j2h4u/omi-collector/issues/115)) ([6d4e505](https://github.com/j2h4u/omi-collector/commit/6d4e5056c8e45116a2cb84675028d65c2a63b5f3))


### Refactoring

* type required capture ports ([#114](https://github.com/j2h4u/omi-collector/issues/114)) ([9bee6cf](https://github.com/j2h4u/omi-collector/commit/9bee6cf9133012c6e51b3dbfec5938caf61935d9))

## [0.6.15](https://github.com/j2h4u/omi-collector/compare/v0.6.14...v0.6.15) (2026-09-22)


### Fixes

* retain historical timeline repairs ([#112](https://github.com/j2h4u/omi-collector/issues/112)) ([3c90b31](https://github.com/j2h4u/omi-collector/commit/3c90b31c52474e4fadec71c609c2097a4c3aff4f))

## [0.6.14](https://github.com/j2h4u/omi-collector/compare/v0.6.13...v0.6.14) (2026-09-22)


### Fixes

* allow sealed internal Python aliases ([#110](https://github.com/j2h4u/omi-collector/issues/110)) ([d39e608](https://github.com/j2h4u/omi-collector/commit/d39e6080b4b863611dd4ad2f0a53e7780bdaad0b))

## [0.6.13](https://github.com/j2h4u/omi-collector/compare/v0.6.12...v0.6.13) (2026-09-22)


### Fixes

* verify managed Python before deployment ([#108](https://github.com/j2h4u/omi-collector/issues/108)) ([e1ce7ef](https://github.com/j2h4u/omi-collector/commit/e1ce7ef6ff0bce1539026dc2e246dba5260c09d8))

## [0.6.12](https://github.com/j2h4u/omi-collector/compare/v0.6.11...v0.6.12) (2026-09-22)


### Fixes

* anchor deployment builds to repository root ([#106](https://github.com/j2h4u/omi-collector/issues/106)) ([c3bae07](https://github.com/j2h4u/omi-collector/commit/c3bae07ad63356efeafb2a9c869debba6229b3d9))

## [0.6.11](https://github.com/j2h4u/omi-collector/compare/v0.6.10...v0.6.11) (2026-09-22)


### Fixes

* harden capture recovery and publication ([#104](https://github.com/j2h4u/omi-collector/issues/104)) ([e7f7504](https://github.com/j2h4u/omi-collector/commit/e7f7504acfa69b6ffff069809ad88e52dcbba948))

## [0.6.10](https://github.com/j2h4u/omi-collector/compare/v0.6.9...v0.6.10) (2026-09-22)


### Documentation

* add adversarial review remediation plan ([#102](https://github.com/j2h4u/omi-collector/issues/102)) ([6a09a47](https://github.com/j2h4u/omi-collector/commit/6a09a4765dedbd480aad25c71ff6a6b16bee37f2))

## [0.6.9](https://github.com/j2h4u/omi-collector/compare/v0.6.8...v0.6.9) (2026-09-22)


### Fixes

* require stable presence before automatic sync ([#100](https://github.com/j2h4u/omi-collector/issues/100)) ([0c34673](https://github.com/j2h4u/omi-collector/commit/0c346735df402baac2d00e250342381257aa9c68))

## [0.6.8](https://github.com/j2h4u/omi-collector/compare/v0.6.7...v0.6.8) (2026-09-22)


### Fixes

* report expected pendant absence as healthy ([#98](https://github.com/j2h4u/omi-collector/issues/98)) ([6e6b283](https://github.com/j2h4u/omi-collector/commit/6e6b283a7818b34fc9a44c0ca28c86fdd2d2742f))

## [0.6.7](https://github.com/j2h4u/omi-collector/compare/v0.6.6...v0.6.7) (2026-09-21)


### Fixes

* persist unattended collector operations ([#96](https://github.com/j2h4u/omi-collector/issues/96)) ([4a090df](https://github.com/j2h4u/omi-collector/commit/4a090df3f606a9de40629ee4cc5e4dde4749764c))

## [0.6.6](https://github.com/j2h4u/omi-collector/compare/v0.6.5...v0.6.6) (2026-09-21)


### Fixes

* repair existing publication ownership ([e56e61a](https://github.com/j2h4u/omi-collector/commit/e56e61a009328a5f23fea600fc2ce5abeb44d79a))


### Tests

* synchronize local contention recovery ([e56e61a](https://github.com/j2h4u/omi-collector/commit/e56e61a009328a5f23fea600fc2ce5abeb44d79a))

## [0.6.5](https://github.com/j2h4u/omi-collector/compare/v0.6.4...v0.6.5) (2026-09-21)


### Fixes

* create publication files with final mode ([6b37987](https://github.com/j2h4u/omi-collector/commit/6b37987ca6c668fda2066d9f793dcfbd7c8a3828))
* keep collector alive during local contention ([6b37987](https://github.com/j2h4u/omi-collector/commit/6b37987ca6c668fda2066d9f793dcfbd7c8a3828))
* keep new publication files private until sealed ([6b37987](https://github.com/j2h4u/omi-collector/commit/6b37987ca6c668fda2066d9f793dcfbd7c8a3828))

## [0.6.4](https://github.com/j2h4u/omi-collector/compare/v0.6.3...v0.6.4) (2026-09-20)


### Fixes

* select clock incident from full journal ([#88](https://github.com/j2h4u/omi-collector/issues/88)) ([dabff97](https://github.com/j2h4u/omi-collector/commit/dabff97fd6b5b74a56da14ea81b119b00efbc840))

## [0.6.3](https://github.com/j2h4u/omi-collector/compare/v0.6.2...v0.6.3) (2026-09-19)


### Fixes

* recover Omi timeline without reconnect ([#86](https://github.com/j2h4u/omi-collector/issues/86)) ([e1ededb](https://github.com/j2h4u/omi-collector/commit/e1ededb72f2805c15eb62f0ed0101d8fcdec77a3))

## [0.6.2](https://github.com/j2h4u/omi-collector/compare/v0.6.1...v0.6.2) (2026-09-18)


### Fixes

* resume Omi timeline publication ([#84](https://github.com/j2h4u/omi-collector/issues/84)) ([8e9c25f](https://github.com/j2h4u/omi-collector/commit/8e9c25f50e844ecad008e3ea178b73d4a76a5b79))

## [0.6.1](https://github.com/j2h4u/omi-collector/compare/v0.6.0...v0.6.1) (2026-09-18)


### Fixes

* handle unavailable BLE connection RSSI ([#82](https://github.com/j2h4u/omi-collector/issues/82)) ([af72a84](https://github.com/j2h4u/omi-collector/commit/af72a849756746add522c5a23107f8d7efcc3c47))

## [0.6.0](https://github.com/j2h4u/omi-collector/compare/v0.5.10...v0.6.0) (2026-09-18)


### Features

* report live BLE connection RSSI ([#80](https://github.com/j2h4u/omi-collector/issues/80)) ([c3704d2](https://github.com/j2h4u/omi-collector/commit/c3704d2693e89d2699c1eb20ab09ead3a7ca6edf))

## [0.5.10](https://github.com/j2h4u/omi-collector/compare/v0.5.9...v0.5.10) (2026-09-18)


### Fixes

* restore unattended collector readiness ([4441572](https://github.com/j2h4u/omi-collector/commit/4441572b2606b88f1c2d65ad3bfb7afd5994d763))


### Documentation

* add safe pendant operating routine ([#75](https://github.com/j2h4u/omi-collector/issues/75)) ([2d83fa5](https://github.com/j2h4u/omi-collector/commit/2d83fa5d77a4de68eb618a972aa77b0a36ef4f8b))
* explain Windmill pipeline boundary ([#76](https://github.com/j2h4u/omi-collector/issues/76)) ([55e9924](https://github.com/j2h4u/omi-collector/commit/55e9924aedfc8d82f032d663f99b0bb82bcc8b9f))


### Tests

* isolate systemd validation dependencies ([4441572](https://github.com/j2h4u/omi-collector/commit/4441572b2606b88f1c2d65ad3bfb7afd5994d763))

## [0.5.9](https://github.com/j2h4u/omi-collector/compare/v0.5.8...v0.5.9) (2026-09-16)


### Fixes

* deploy with shared config permissions ([#73](https://github.com/j2h4u/omi-collector/issues/73)) ([2e7a88c](https://github.com/j2h4u/omi-collector/commit/2e7a88c2a3cf55c7e2f94669859eeda1bfcba6cb))

## [0.5.8](https://github.com/j2h4u/omi-collector/compare/v0.5.7...v0.5.8) (2026-09-16)


### Fixes

* share unified config with pipeline ([39af17f](https://github.com/j2h4u/omi-collector/commit/39af17f5d2fce3d64abaee3fc635858063550bb8))


### Refactoring

* use one single-pendant configuration ([39af17f](https://github.com/j2h4u/omi-collector/commit/39af17f5d2fce3d64abaee3fc635858063550bb8))

## [0.5.7](https://github.com/j2h4u/omi-collector/compare/v0.5.6...v0.5.7) (2026-09-16)


### Documentation

* document stock pendant gotchas ([#69](https://github.com/j2h4u/omi-collector/issues/69)) ([075c483](https://github.com/j2h4u/omi-collector/commit/075c483594402b35f87402bd3738b691da27e5ae))

## [0.5.6](https://github.com/j2h4u/omi-collector/compare/v0.5.5...v0.5.6) (2026-09-15)


### Fixes

* ignore timeline generation metadata in status ([#68](https://github.com/j2h4u/omi-collector/issues/68)) ([9616c7a](https://github.com/j2h4u/omi-collector/commit/9616c7aa3c3e1251d81fcbcc27bb3b2032b61f55))
* inspect published timeline generations ([#66](https://github.com/j2h4u/omi-collector/issues/66)) ([4674133](https://github.com/j2h4u/omi-collector/commit/4674133de91c4878f639ac38f3e772f89049b759))

## [0.5.5](https://github.com/j2h4u/omi-collector/compare/v0.5.4...v0.5.5) (2026-09-15)


### Fixes

* publish normalized Omi audio timelines ([#64](https://github.com/j2h4u/omi-collector/issues/64)) ([dc29950](https://github.com/j2h4u/omi-collector/commit/dc29950c53100f14f25b543a795a2f7d9f261aed))

## [0.5.4](https://github.com/j2h4u/omi-collector/compare/v0.5.3...v0.5.4) (2026-09-15)


### Fixes

* expose clock corrections in status ([#62](https://github.com/j2h4u/omi-collector/issues/62)) ([7b5e1ef](https://github.com/j2h4u/omi-collector/commit/7b5e1ef56ebf195d3b95b41a4de554839fbbd377))

## [0.5.3](https://github.com/j2h4u/omi-collector/compare/v0.5.2...v0.5.3) (2026-09-15)


### Fixes

* persist clock correction sequence evidence ([#60](https://github.com/j2h4u/omi-collector/issues/60)) ([32c60a0](https://github.com/j2h4u/omi-collector/commit/32c60a0a4f495ffa08a3d9ade8544ac1dee53c9b))

## [0.5.2](https://github.com/j2h4u/omi-collector/compare/v0.5.1...v0.5.2) (2026-09-14)


### Fixes

* make operator status reliable and concise ([#58](https://github.com/j2h4u/omi-collector/issues/58)) ([bbe73a1](https://github.com/j2h4u/omi-collector/commit/bbe73a1f98eddc1956093214c60f06bb059df9e5))


### Maintenance

* **deps-dev:** bump the python-minor-patch group with 2 updates ([#56](https://github.com/j2h4u/omi-collector/issues/56)) ([427bd11](https://github.com/j2h4u/omi-collector/commit/427bd112669238dd58cd6a4fcaa607a0431e0810))
* **deps:** bump the github-actions group with 4 updates ([#57](https://github.com/j2h4u/omi-collector/issues/57)) ([66d35c7](https://github.com/j2h4u/omi-collector/commit/66d35c7b794607fbba3495ce7c3675c9caca27a0))

## [0.5.1](https://github.com/j2h4u/omi-collector/compare/v0.5.0...v0.5.1) (2026-09-12)


### Maintenance

* **deps-dev:** bump the python-minor-patch group across 1 directory with 3 updates ([#52](https://github.com/j2h4u/omi-collector/issues/52)) ([e3be09d](https://github.com/j2h4u/omi-collector/commit/e3be09da86a21aad4616afba40101acad134e41a))
* **deps:** bump the github-actions group with 3 updates ([#46](https://github.com/j2h4u/omi-collector/issues/46)) ([d9ba0b4](https://github.com/j2h4u/omi-collector/commit/d9ba0b45b420c2630f43aa15a80676d1dc55209f))

## [0.5.0](https://github.com/j2h4u/omi-collector/compare/v0.4.1...v0.5.0) (2026-09-12)


### Features

* unify production status reporting ([#53](https://github.com/j2h4u/omi-collector/issues/53)) ([9e3e12a](https://github.com/j2h4u/omi-collector/commit/9e3e12a738a7613f4e510e672a9ddef72da6f520))

## [0.4.1](https://github.com/j2h4u/omi-collector/compare/v0.4.0...v0.4.1) (2026-09-12)


### Maintenance

* pin local Python version ([#50](https://github.com/j2h4u/omi-collector/issues/50)) ([48f1f00](https://github.com/j2h4u/omi-collector/commit/48f1f00cf2205f06f77bb34e5c6d093022284921))

## [0.4.0](https://github.com/j2h4u/omi-collector/compare/v0.3.2...v0.4.0) (2026-09-08)


### Features

* add operator status summary ([#47](https://github.com/j2h4u/omi-collector/issues/47)) ([9780fe5](https://github.com/j2h4u/omi-collector/commit/9780fe58acf25f17f6b69a1c648d7baf203bbe3d))

## [0.3.2](https://github.com/j2h4u/omi-collector/compare/v0.3.1...v0.3.2) (2026-09-06)


### Fixes

* avoid readonly deploy variable collision ([#43](https://github.com/j2h4u/omi-collector/issues/43)) ([fbcdc48](https://github.com/j2h4u/omi-collector/commit/fbcdc48c691dbbd3560c5181cdee0129d6ae7e64))

## [0.3.1](https://github.com/j2h4u/omi-collector/compare/v0.3.0...v0.3.1) (2026-09-06)


### Maintenance

* add maintainer release deployer ([#41](https://github.com/j2h4u/omi-collector/issues/41)) ([fb898b6](https://github.com/j2h4u/omi-collector/commit/fb898b6a1d2304e08ed11964305b7369dd2835bd))

## [0.3.0](https://github.com/j2h4u/omi-collector/compare/v0.2.12...v0.3.0) (2026-09-06)


### Features

* expose total and remaining transfer bytes ([ccaa994](https://github.com/j2h4u/omi-collector/commit/ccaa994063c321ddf0284bdd996632ef4a09dc88))


### Fixes

* record RSSI before BLE sessions finish ([ccaa994](https://github.com/j2h4u/omi-collector/commit/ccaa994063c321ddf0284bdd996632ef4a09dc88))

## [0.2.12](https://github.com/j2h4u/omi-collector/compare/v0.2.11...v0.2.12) (2026-09-06)


### Fixes

* defer maintenance while device lock is busy ([#37](https://github.com/j2h4u/omi-collector/issues/37)) ([a8bc6bf](https://github.com/j2h4u/omi-collector/commit/a8bc6bf0d3167d531d3bc1def0fee27de1ef23e0))

## [0.2.11](https://github.com/j2h4u/omi-collector/compare/v0.2.10...v0.2.11) (2026-09-04)


### Fixes

* harden bundle validation and shutdown ([#35](https://github.com/j2h4u/omi-collector/issues/35)) ([e02be8e](https://github.com/j2h4u/omi-collector/commit/e02be8e10e1f05e137a76424ffd57e281c7a36e2))

## [0.2.10](https://github.com/j2h4u/omi-collector/compare/v0.2.9...v0.2.10) (2026-09-04)


### Documentation

* remove completed architecture plans ([#33](https://github.com/j2h4u/omi-collector/issues/33)) ([6707b82](https://github.com/j2h4u/omi-collector/commit/6707b828a5373e5327e34f52d351e86afaef2879))

## [0.2.9](https://github.com/j2h4u/omi-collector/compare/v0.2.8...v0.2.9) (2026-09-04)


### Fixes

* release writer lifecycle hardening ([#31](https://github.com/j2h4u/omi-collector/issues/31)) ([77110fd](https://github.com/j2h4u/omi-collector/commit/77110fd6312567924feee3763939345838168075))

## [0.2.8](https://github.com/j2h4u/omi-collector/compare/v0.2.7...v0.2.8) (2026-09-04)


### Refactoring

* make writer lifecycle explicit ([#29](https://github.com/j2h4u/omi-collector/issues/29)) ([50f8b42](https://github.com/j2h4u/omi-collector/commit/50f8b42e9575ebb92b7e43431261e3c1fafd9b32))

## [0.2.7](https://github.com/j2h4u/omi-collector/compare/v0.2.6...v0.2.7) (2026-09-04)


### Fixes

* review submitted PR dependencies ([#27](https://github.com/j2h4u/omi-collector/issues/27)) ([167f234](https://github.com/j2h4u/omi-collector/commit/167f2347a7f533eccf67d2869ffcd6ade3f3a7bf))

## [0.2.6](https://github.com/j2h4u/omi-collector/compare/v0.2.5...v0.2.6) (2026-09-04)


### Fixes

* attest automated release checks ([#26](https://github.com/j2h4u/omi-collector/issues/26)) ([3b6d85b](https://github.com/j2h4u/omi-collector/commit/3b6d85b548428e04fbb605e4a5565ce425a38515))
* make presence scheduling race-safe ([33b6755](https://github.com/j2h4u/omi-collector/commit/33b67551a6bb52baa4c4d008078a5ec1e4e9d143))


### Documentation

* design explicit presence state machine ([33b6755](https://github.com/j2h4u/omi-collector/commit/33b67551a6bb52baa4c4d008078a5ec1e4e9d143))

## [0.2.5](https://github.com/j2h4u/omi-collector/compare/v0.2.4...v0.2.5) (2026-09-04)


### Fixes

* build deployments at their final path ([#22](https://github.com/j2h4u/omi-collector/issues/22)) ([7772fe6](https://github.com/j2h4u/omi-collector/commit/7772fe630aa6184e2d101b606f9a58e8087ffcfb))

## [0.2.4](https://github.com/j2h4u/omi-collector/compare/v0.2.3...v0.2.4) (2026-09-04)


### Fixes

* harden collector for unattended operation ([#19](https://github.com/j2h4u/omi-collector/issues/19)) ([3ec4b06](https://github.com/j2h4u/omi-collector/commit/3ec4b06db0f1ec7932a4570fa0988be5ad2c141c))
* keep quality metrics private ([#21](https://github.com/j2h4u/omi-collector/issues/21)) ([829d2f3](https://github.com/j2h4u/omi-collector/commit/829d2f38124549d9a04317d72e40be7e63ee22b1))

## [0.2.3](https://github.com/j2h4u/omi-collector/compare/v0.2.2...v0.2.3) (2026-09-03)


### Documentation

* complete final collector audit ([#18](https://github.com/j2h4u/omi-collector/issues/18)) ([103c526](https://github.com/j2h4u/omi-collector/commit/103c52655d7d37f1e75168efad8f37f95bc4dad8))


### Refactoring

* simplify collector state and maintenance ([#16](https://github.com/j2h4u/omi-collector/issues/16)) ([c43c1b3](https://github.com/j2h4u/omi-collector/commit/c43c1b310d3d625c9ab9fbae3e9566dc7c9a92b9))

## [0.2.2](https://github.com/j2h4u/omi-collector/compare/v0.2.1...v0.2.2) (2026-09-03)


### Fixes

* reduce absent pendant connection attempts ([#14](https://github.com/j2h4u/omi-collector/issues/14)) ([95d593a](https://github.com/j2h4u/omi-collector/commit/95d593a1832ce48b4b8be3fe7efffa0c44e5895c))

## [0.2.1](https://github.com/j2h4u/omi-collector/compare/v0.2.0...v0.2.1) (2026-09-03)


### Fixes

* fail closed on malformed recovery evidence ([#11](https://github.com/j2h4u/omi-collector/issues/11)) ([0fdc63f](https://github.com/j2h4u/omi-collector/commit/0fdc63fda3fad9be6a857c58e2172c6fe4560b94))
* quarantine unusable recovery evidence ([#12](https://github.com/j2h4u/omi-collector/issues/12)) ([74b71bd](https://github.com/j2h4u/omi-collector/commit/74b71bd9b201dda592826c7c7c3b080135ccb5d9))


### CI

* streamline release pull requests ([#13](https://github.com/j2h4u/omi-collector/issues/13)) ([dda8750](https://github.com/j2h4u/omi-collector/commit/dda8750d22ca41ba0c4296327e753a5a00599e67))


### Documentation

* explain stock firmware audio loss ([#8](https://github.com/j2h4u/omi-collector/issues/8)) ([891cf24](https://github.com/j2h4u/omi-collector/commit/891cf24e0f8155cf002f611b710f07873b202e53))


### Refactoring

* simplify collector storage layout ([#10](https://github.com/j2h4u/omi-collector/issues/10)) ([7ee4583](https://github.com/j2h4u/omi-collector/commit/7ee4583635662e71d23ab4b572a1a1582b904857))

## [0.2.0](https://github.com/j2h4u/omi-collector/compare/v0.1.0...v0.2.0) (2026-09-02)


### Features

* record transfer quality evidence ([e5d1de3](https://github.com/j2h4u/omi-collector/commit/e5d1de30d14d9b574748ba08856637a90866f634))


### Fixes

* classify release validators as first party ([e5d1de3](https://github.com/j2h4u/omi-collector/commit/e5d1de30d14d9b574748ba08856637a90866f634))
* fetch locked dependencies during deployment ([#5](https://github.com/j2h4u/omi-collector/issues/5)) ([d93da2a](https://github.com/j2h4u/omi-collector/commit/d93da2a45b604c041b845f6ef09fa25f74e70255))
* publish only sealed capture artifacts ([#4](https://github.com/j2h4u/omi-collector/issues/4)) ([56515ca](https://github.com/j2h4u/omi-collector/commit/56515ca95ffa17cdad71201fba188aa3d7fe5284))
* verify release and deployment provenance ([e5d1de3](https://github.com/j2h4u/omi-collector/commit/e5d1de30d14d9b574748ba08856637a90866f634))


### CI

* automate releases and deployment provenance ([e5d1de3](https://github.com/j2h4u/omi-collector/commit/e5d1de30d14d9b574748ba08856637a90866f634))


### Documentation

* describe downstream VAD pipeline ([#3](https://github.com/j2h4u/omi-collector/issues/3)) ([f9aba5e](https://github.com/j2h4u/omi-collector/commit/f9aba5ef919b2c75dde926649af54430c2b53aab))

## [0.1.0](https://github.com/j2h4u/omi-collector/releases/tag/v0.1.0) (2026-08-31)

Initial public release.

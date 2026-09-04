# Changelog

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

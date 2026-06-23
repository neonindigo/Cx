# CxTest depends on CombineSchedulers for virtual-time scheduling

`CxTest` takes a dependency on Point-Free's `CombineSchedulers` package rather than implementing its own virtual-time `TestScheduler`.

Building a correct virtual-time scheduler for Combine is non-trivial. `CombineSchedulers` is battle-tested, widely adopted, and already solves this problem well. `CxTest` focuses its effort on RxTest-style assertion helpers (recording emissions, asserting sequences) layered on top. The cost is an external dependency, but the benefit is a reliable foundation without duplicating complex infrastructure.

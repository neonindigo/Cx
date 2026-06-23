# Relay types are wrapper structs, not typealiases

`PublishRelay`, `BehaviorRelay`, and `ReplayRelay` are implemented as wrapper structs that conform to `Publisher` rather than as typealiases over Combine Subject types.

A typealias (e.g. `typealias PublishRelay<T> = PassthroughSubject<T, Never>`) would give familiar names but cannot enforce the Relay contract — callers could still call `send(completion:)` and accidentally terminate the stream, which is the exact bug Relays exist to prevent. Wrapper structs hide `send(completion:)` entirely, enforcing the contract at compile time while remaining fully interoperable with Combine via `Publisher` conformance.

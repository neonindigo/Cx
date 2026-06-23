# Cx

A Swift package that provides Combine-native equivalents of the capabilities found in the RxSwift ecosystem. Intended for engineers migrating from RxSwift to Apple's Combine framework.

## Language

**Cx**:
The core Swift package target providing missing operators, Relay types, and ReplaySubject on top of Combine.
_Avoid_: RxSwift wrapper, Combine extensions

**CxCocoa**:
The Swift package target providing UIKit and AppKit reactive extensions, plus Driver and Signal, built on Combine.
_Avoid_: UI bindings module

**CxTest**:
The Swift package target providing publisher assertion helpers and a virtual-time scheduler for testing Combine pipelines.
_Avoid_: test utilities module

**Relay**:
A wrapper type around a Combine Subject with `Failure = Never` that intentionally hides `send(completion:)`, preventing accidental termination. Conforms to `Publisher`. Uses `send(_:)` for emitting values.
_Avoid_: relay subject, non-terminating subject

**PublishRelay**:
A Relay backed by `PassthroughSubject<Output, Never>`. Emits values to current subscribers only.
_Avoid_: publish subject relay

**BehaviorRelay**:
A Relay backed by `CurrentValueSubject<Output, Never>`. Replays the current value to new subscribers and exposes a `value` property.
_Avoid_: behavior subject relay

**ReplayRelay**:
A Relay that buffers the last N values and replays them to new subscribers. No Combine primitive equivalent.
_Avoid_: replay subject relay

**ReplaySubject**:
A Combine Subject that buffers the last N values and replays them to new subscribers. Can terminate with completion or failure. No Combine primitive equivalent.
_Avoid_: buffered subject

**Driver**:
A `CxCocoa` publisher that is guaranteed to deliver events on the main thread, never errors (`Failure = Never`), and replays the last value to new subscribers. Used for binding data to UI elements.
_Avoid_: UI publisher, main thread observable

**Signal**:
A `CxCocoa` publisher that is guaranteed to deliver events on the main thread, never errors (`Failure = Never`), and does not replay. Used for representing UI events.
_Avoid_: UI event publisher

## Relationships

- A **Relay** wraps a Combine `Subject` and restricts its interface to value-sending only
- A **Driver** is a specialised publisher used in **CxCocoa** for data binding; a **Signal** is a specialised publisher used in **CxCocoa** for events
- **CxTest** depends on `CombineSchedulers` for virtual-time scheduling and adds assertion helpers on top

## Mapping from RxSwift concepts

| RxSwift | Cx / Combine |
|---|---|
| `Observable<T>` | `AnyPublisher<T, Error>` |
| `Infallible<T>` | `AnyPublisher<T, Never>` |
| `Single<T>` | `Deferred { Future<T, Error> }` |
| `Completable` | `AnyPublisher<Never, Error>` |
| `Maybe<T>` | `AnyPublisher<T?, Error>` (no direct equivalent) |
| `PublishSubject` | `PassthroughSubject` |
| `BehaviorSubject` | `CurrentValueSubject` |
| `ReplaySubject` | `ReplaySubject` (Cx) |
| `PublishRelay` | `PublishRelay` (Cx) |
| `BehaviorRelay` | `BehaviorRelay` (Cx) |
| `ReplayRelay` | `ReplayRelay` (Cx) |
| `Disposable` / `DisposeBag` | `AnyCancellable` / `Set<AnyCancellable>` |
| `Driver` | `Driver` (CxCocoa) |
| `Signal` | `Signal` (CxCocoa) |

## Example dialogue

> **Dev:** "Should I return a `Driver` or a plain publisher from my view model?"
> **Domain expert:** "If you're binding it directly to a UI element, use a **Driver** — it guarantees main thread delivery and replays the last value so the view is never in an unknown state. If you're representing a discrete UI event like a button tap, use a **Signal**."

## Ecosystem

Cx is the foundation of a wider ecosystem of companion packages, each in its own repo:

- **Cx** (this repo) — core operators, Relays, ReplaySubject, CxCocoa, CxTest
- **CxDataSources** — Combine-native diffable data sources for UITableView / UICollectionView (based on RxDataSources)
- **CxCarPlay** — Combine reactive extensions for CarPlay

Naming convention for companion packages: `Cx{Domain}` (e.g. `CxCarPlay`, `CxDataSources`). Each companion declares a dependency on `Cx` and lives in its own repository with its own release cadence.

## Flagged ambiguities

- "relay" was initially discussed as a typealias — resolved: Relays are wrapper structs that enforce the no-termination contract, not typealiases over Combine Subjects.
- "Single" was mapped to `Future` — resolved: `Future` is eager; the correct Combine equivalent is `Deferred { Future { ... } }`. Cx does not introduce a `Single` type.

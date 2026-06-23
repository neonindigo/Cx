# Cx Implementation Plan

Combine-native equivalents of the RxSwift ecosystem for engineers migrating to Apple's Combine framework.

## Package structure

Single repo, three targets, iOS 15+ / macOS 12+:

```
Cx/
├── Sources/
│   ├── Cx/           — core operators, Relays, ReplaySubject
│   ├── CxCocoa/      — UIKit/AppKit extensions, Driver, Signal
│   └── CxTest/       — publisher assertion helpers (depends on CombineSchedulers)
├── Tests/
│   ├── CxTests/
│   ├── CxCocoaTests/
│   └── CxTestTests/
├── docs/
│   └── adr/
├── CONTEXT.md
└── Package.swift
```

---

## Cx (core)

### Relay types

Wrapper structs conforming to `Publisher` with `Failure = Never`. Expose `send(_:)`. Hide `send(completion:)` entirely.

- `PublishRelay<Output>` — backed by `PassthroughSubject<Output, Never>`
- `BehaviorRelay<Output>` — backed by `CurrentValueSubject<Output, Never>`, exposes `value` property
- `ReplayRelay<Output>` — backed by `ReplaySubject<Output, Never>`, buffers last N values

### ReplaySubject

`ReplaySubject<Output, Failure: Error>` — a `Subject` that buffers the last N values and replays them to new subscribers. Can terminate with completion or failure. No Combine primitive equivalent.

### Missing operators

Extensions on `Publisher` for operators absent from Combine:

| Operator | Behaviour |
|---|---|
| `withLatestFrom(_:)` | When self emits, combine with latest value from another publisher |
| `withUnretained(_:)` | Pair each value with a weak reference to an object; complete if object is deallocated |
| `materialize()` | Wrap each event (value, completion, failure) into a `Event<Output, Failure>` value |
| `dematerialize()` | Unwrap `Event` values back into a publisher stream |
| `retryWhen(_:)` | Retry when a notifier publisher emits, with custom back-off logic |
| `sample(_:)` | Emit the latest value from self only when a trigger publisher emits |
| `amb(_:)` / `race(_:)` | Forward whichever of two publishers emits first; ignore the other |
| `window(ofCount:)` | Split a stream into fixed-size publisher windows |
| `enumerated()` | Pair each value with its zero-based index |

---

## CxCocoa

### Driver and Signal

Real wrapper types. Both have `Failure = Never` and deliver on the main thread.

- `Driver<Output>` — replays last value to new subscribers (share replay 1)
- `Signal<Output>` — no replay (share replay 0)

### UIKit / AppKit reactive extensions

Extensions on common controls exposing Combine publishers:

| Control | Publisher |
|---|---|
| `UIButton` / `NSButton` | `.tapPublisher` |
| `UITextField` / `NSTextField` | `.textPublisher` |
| `UITextView` / `NSTextView` | `.textPublisher` |
| `UISwitch` / `NSSwitch` | `.isOnPublisher` |
| `UISlider` / `NSSlider` | `.valuePublisher` |
| `UISegmentedControl` | `.selectedSegmentIndexPublisher` |
| `UIBarButtonItem` | `.tapPublisher` |
| `UIGestureRecognizer` | `.publisher` |
| `UIControl` | `.publisher(for:)` for arbitrary control events |

---

## CxTest

Depends on [`CombineSchedulers`](https://github.com/pointfreeco/combine-schedulers) for virtual-time scheduling.

Adds:
- `XCTestCase` extensions for recording and asserting publisher emissions
- `TestSubscriber` — records received values, completions, and failures for assertion

---

## Ecosystem

Companion packages live in separate repos, each depending on `Cx`:

- **CxDataSources** — diffable data sources for UITableView / UICollectionView (based on RxDataSources)
- **CxCarPlay** — reactive extensions for CarPlay

Naming convention: `Cx{Domain}`.

---

## RxSwift → Cx mapping reference

| RxSwift | Cx / Combine |
|---|---|
| `Observable<T>` | `AnyPublisher<T, Error>` |
| `Infallible<T>` | `AnyPublisher<T, Never>` |
| `Single<T>` | `Deferred { Future<T, Error> }` |
| `Completable` | `AnyPublisher<Never, Error>` |
| `Maybe<T>` | `AnyPublisher<T?, Error>` |
| `PublishSubject` | `PassthroughSubject` |
| `BehaviorSubject` | `CurrentValueSubject` |
| `ReplaySubject` | `ReplaySubject` (Cx) |
| `PublishRelay` | `PublishRelay` (Cx) |
| `BehaviorRelay` | `BehaviorRelay` (Cx) |
| `ReplayRelay` | `ReplayRelay` (Cx) |
| `Disposable` / `DisposeBag` | `AnyCancellable` / `Set<AnyCancellable>` |
| `Driver` | `Driver` (CxCocoa) |
| `Signal` | `Signal` (CxCocoa) |
| `TestScheduler` | `CombineSchedulers.TestScheduler` (CxTest) |

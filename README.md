# Cx

**Cx** is a Combine-native Swift package that provides equivalents of the RxSwift ecosystem for engineers migrating to Apple's Combine framework. It delivers missing operators, Relay types, a ReplaySubject, UIKit/AppKit reactive extensions, and publisher test utilities — all built directly on Combine with no RxSwift dependency.

## Modules

| Module | Purpose |
|---|---|
| **Cx** | Core operators (`withLatestFrom`, `materialize`, `retryWhen`, `sample`, `amb`, …), Relay types (`PublishRelay`, `BehaviorRelay`, `ReplayRelay`), and `ReplaySubject` |
| **CxCocoa** | UIKit and AppKit reactive extensions, plus the `Driver` and `Signal` publisher traits |
| **CxTest** | `TestSubscriber` and `XCTestCase` assertion helpers for testing Combine pipelines; depends on [`CombineSchedulers`](https://github.com/pointfreeco/combine-schedulers) |

## Requirements

- iOS 15+ / macOS 12+
- Swift 5.9+
- Xcode 15+

## Installation

Add Cx as a Swift Package Manager dependency:

```swift
// In Package.swift
dependencies: [
    .package(url: "https://github.com/neonindigo/Cx.git", from: "0.1.0"),
],
targets: [
    .target(name: "MyTarget", dependencies: [
        .product(name: "Cx", package: "Cx"),
        // Optional:
        .product(name: "CxCocoa", package: "Cx"),
        .product(name: "CxTest", package: "Cx"),
    ]),
]
```

Or in Xcode: **File › Add Package Dependencies…** and enter `https://github.com/neonindigo/Cx.git`.

## RxSwift → Cx Mapping

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

## Docs

See [docs/plan.md](docs/plan.md) for the full implementation plan and architecture notes.

## Ecosystem

Companion packages live in separate repos and declare a dependency on `Cx`:

- **CxDataSources** — diffable data sources for `UITableView` / `UICollectionView`
- **CxCarPlay** — reactive extensions for CarPlay

## License

MIT

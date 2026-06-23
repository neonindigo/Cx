# Cx is Combine-native, not an RxSwift API shim

Cx targets engineers migrating from RxSwift to Combine. The deliberate choice is to use Combine vocabulary throughout (`Publisher`, `send(_:)`, `AnyCancellable`, `sink`) rather than mirroring RxSwift's API surface (`Observable`, `onNext`, `DisposeBag`, `subscribe`). This means Cx provides equivalent *capabilities* without providing compatible *call sites* — engineers learn Combine idioms, aided by familiar patterns.

The alternative — a drop-in shim with RxSwift-compatible types backed by Combine internals — was rejected because it fights Combine's type system, creates a false sense of migration progress, and produces code that is neither idiomatic RxSwift nor idiomatic Combine.

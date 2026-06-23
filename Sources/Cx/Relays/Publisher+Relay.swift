import Combine

extension Publisher where Failure == Never {
    /// Subscribes to this publisher and forwards each value into `relay`.
    /// Store the returned `AnyCancellable` — dropping it cancels the binding.
    public func bind(to relay: PublishRelay<Output>) -> AnyCancellable {
        sink { relay.send($0) }
    }

    /// Subscribes to this publisher and forwards each value into `relay`.
    /// Store the returned `AnyCancellable` — dropping it cancels the binding.
    public func bind(to relay: BehaviorRelay<Output>) -> AnyCancellable {
        sink { relay.send($0) }
    }
}

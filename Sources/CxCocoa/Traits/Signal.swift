import Combine
import Foundation

/// A publisher that delivers on the main thread, never errors, and does not replay.
/// Equivalent to RxSwift's `Signal`.
public struct Signal<Output>: Publisher {
    public typealias Failure = Never

    private let upstream: AnyPublisher<Output, Never>

    public init<P: Publisher>(_ publisher: P) where P.Output == Output, P.Failure == Never {
        upstream = publisher
            .receive(on: DispatchQueue.main)
            .share()
            .eraseToAnyPublisher()
    }

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Never {
        upstream.receive(subscriber: subscriber)
    }
}

extension Publisher where Failure == Never {
    /// Converts the publisher into a `Signal`.
    public func asSignal() -> Signal<Output> {
        Signal(self)
    }
}

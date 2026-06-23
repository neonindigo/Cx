import Combine
import Foundation

/// A publisher that delivers on the main thread, never errors, and replays the last value.
/// Equivalent to RxSwift's `Driver`.
/// - Note: Replay-1 semantics are implemented by Wave 1. This stub scaffolds the seam only.
public struct Driver<Output>: Publisher {
    public typealias Failure = Never

    private let upstream: AnyPublisher<Output, Never>

    public init<P: Publisher>(_ publisher: P) where P.Output == Output, P.Failure == Never {
        upstream = publisher
            .receive(on: DispatchQueue.main)
            .eraseToAnyPublisher()
    }

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Never {
        upstream.receive(subscriber: subscriber)
    }
}

extension Publisher where Failure == Never {
    /// Converts the publisher into a `Driver`.
    public func asDriver() -> Driver<Output> {
        Driver(self)
    }
}

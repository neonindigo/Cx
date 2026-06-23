import Combine
import Foundation

/// A publisher that delivers on the main thread, never errors, and replays the last value.
/// Equivalent to RxSwift's `Driver`.
private final class DriverState<Output> {
    let publisher: AnyPublisher<Output, Never>
    private var cancellable: AnyCancellable?

    init<P: Publisher>(
        _ upstream: P,
        initialValue: Output? = nil
    ) where P.Output == Output, P.Failure == Never {
        let subject = CurrentValueSubject<Output?, Never>(initialValue)

        publisher = subject
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .eraseToAnyPublisher()

        cancellable = upstream
            .receive(on: DispatchQueue.main)
            .sink { subject.send($0) }
    }
}

public struct Driver<Output>: Publisher {
    public typealias Failure = Never

    private let state: DriverState<Output>

    public init<P: Publisher>(_ publisher: P) where P.Output == Output, P.Failure == Never {
        state = DriverState(publisher)
    }

    /// Internal init used by `Signal.asDriver(initialValue:)` to seed the replay value.
    init<P: Publisher>(_ publisher: P, initialValue: Output) where P.Output == Output, P.Failure == Never {
        state = DriverState(publisher, initialValue: initialValue)
    }

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Never {
        state.publisher.receive(subscriber: subscriber)
    }
}

extension Publisher where Failure == Never {
    /// Converts the publisher into a `Driver`.
    public func asDriver() -> Driver<Output> {
        Driver(self)
    }
}

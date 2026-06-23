import Combine
import Foundation

/// A publisher that delivers on the main thread, never errors, and replays the last value.
/// Equivalent to RxSwift's `Driver`.
private final class DriverState<Output> {
    /// Replay-1 publisher: emits the current value to every new subscriber.
    let publisher: AnyPublisher<Output, Never>
    /// Non-replaying publisher: emits only future values. Used by `asSignal()`.
    let eventPublisher: AnyPublisher<Output, Never>
    private var cancellable: AnyCancellable?

    init<P: Publisher>(
        _ upstream: P,
        initialValue: Output? = nil
    ) where P.Output == Output, P.Failure == Never {
        let replaySubject = CurrentValueSubject<Output?, Never>(initialValue)
        let eventSubject = PassthroughSubject<Output, Never>()

        publisher = replaySubject
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .eraseToAnyPublisher()

        eventPublisher = eventSubject
            .receive(on: DispatchQueue.main)
            .eraseToAnyPublisher()

        cancellable = upstream
            .receive(on: DispatchQueue.main)
            .sink { value in
                replaySubject.send(value)
                eventSubject.send(value)
            }
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

    /// Converts to a Signal — exposes the non-replaying event stream of this Driver.
    /// Unlike `Signal(self)`, this correctly bypasses the replay-1 subject.
    func asSignalPublisher() -> AnyPublisher<Output, Never> {
        state.eventPublisher
    }
}

extension Publisher where Failure == Never {
    /// Converts the publisher into a `Driver`.
    public func asDriver() -> Driver<Output> {
        Driver(self)
    }
}

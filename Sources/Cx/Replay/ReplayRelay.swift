import Combine

/// A relay that buffers the last N values and replays them. Cannot terminate.
public final class ReplayRelay<Output>: Publisher {
    public typealias Failure = Never

    private let subject: ReplaySubject<Output, Never>

    public init(bufferSize: Int) {
        subject = ReplaySubject(bufferSize: bufferSize)
    }

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Never {
        subject.receive(subscriber: subscriber)
    }

    public func send(_ value: Output) {
        subject.send(value)
    }

    public func asPublisher() -> AnyPublisher<Output, Never> {
        subject.eraseToAnyPublisher()
    }
}

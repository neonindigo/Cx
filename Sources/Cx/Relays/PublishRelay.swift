import Combine

/// A relay backed by PassthroughSubject that cannot terminate.
public final class PublishRelay<Output>: Publisher {
    public typealias Failure = Never

    private let subject = PassthroughSubject<Output, Never>()

    public init() {}

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Never {
        subject.receive(subscriber: subscriber)
    }

    public func send(_ value: Output) {
        subject.send(value)
    }
}

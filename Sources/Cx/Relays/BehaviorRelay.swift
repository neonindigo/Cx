import Combine

/// A relay backed by CurrentValueSubject that cannot terminate. Exposes current value.
public final class BehaviorRelay<Output>: Publisher {
    public typealias Failure = Never

    private let subject: CurrentValueSubject<Output, Never>

    public var value: Output { subject.value }

    public init(_ initialValue: Output) {
        subject = CurrentValueSubject(initialValue)
    }

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Never {
        subject.receive(subscriber: subscriber)
    }

    public func send(_ value: Output) {
        subject.send(value)
    }
}

extension BehaviorRelay {
    public func asPublisher() -> AnyPublisher<Output, Never> {
        eraseToAnyPublisher()
    }
}

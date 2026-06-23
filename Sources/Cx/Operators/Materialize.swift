import Combine

/// Represents a materialised publisher event.
public enum Event<Output, Failure: Error> {
    case value(Output)
    case failure(Failure)
    case finished
}

// Existential helper so `dematerialize` can constrain on `Event`.
public protocol _EventProtocol {
    associatedtype Output
    associatedtype Failure: Error
    /// Dispatch self to one of three handlers.
    func _apply(value: (Output) -> Void, failure: (Failure) -> Void, finished: () -> Void)
}

extension Event: _EventProtocol {
    public func _apply(value: (Output) -> Void, failure: (Failure) -> Void, finished: () -> Void) {
        switch self {
        case .value(let v):   value(v)
        case .failure(let e): failure(e)
        case .finished:       finished()
        }
    }
}

extension Publisher {
    /// Wraps each event (value, completion, failure) into an `Event` value.
    /// The resulting publisher never fails.
    public func materialize() -> AnyPublisher<Event<Output, Failure>, Never> {
        let upstream = self
        return Deferred {
            let subject = PassthroughSubject<Event<Output, Failure>, Never>()
            let cancellable = upstream.sink(
                receiveCompletion: { completion in
                    switch completion {
                    case .finished:          subject.send(.finished)
                    case .failure(let err):  subject.send(.failure(err))
                    }
                    subject.send(completion: .finished)
                },
                receiveValue: { subject.send(.value($0)) }
            )
            return subject
                .handleEvents(receiveCancel: { cancellable.cancel() })
                .eraseToAnyPublisher()
        }.eraseToAnyPublisher()
    }
}

extension Publisher where Output: _EventProtocol, Failure == Never {
    /// Unwraps `Event` values back into a publisher stream.
    public func dematerialize() -> AnyPublisher<Output.Output, Output.Failure> {
        let upstream = self
        return Deferred {
            let subject = PassthroughSubject<Output.Output, Output.Failure>()
            let cancellable = upstream.sink(
                receiveCompletion: { _ in subject.send(completion: .finished) },
                receiveValue: { event in
                    event._apply(
                        value:    { subject.send($0) },
                        failure:  { subject.send(completion: .failure($0)) },
                        finished: { subject.send(completion: .finished) }
                    )
                }
            )
            return subject
                .handleEvents(receiveCancel: { cancellable.cancel() })
                .eraseToAnyPublisher()
        }.eraseToAnyPublisher()
    }
}

import Combine

/// Represents a materialised publisher event.
public enum Event<Output, Failure: Error> {
    case value(Output)
    case failure(Failure)
    case finished
}

extension Publisher {
    /// Wraps each event (value, completion, failure) into an `Event` value.
    /// The resulting publisher never fails.
    public func materialize() -> AnyPublisher<Event<Output, Failure>, Never> {
        // TODO: implement
        fatalError("stub")
    }
}

extension Publisher where Output: _EventProtocol, Failure == Never {
    /// Unwraps `Event` values back into a publisher stream.
    public func dematerialize() -> AnyPublisher<Output.Output, Output.Failure> {
        // TODO: implement
        fatalError("stub")
    }
}

// Existential helper so `dematerialize` can constrain on `Event`.
public protocol _EventProtocol {
    associatedtype Output
    associatedtype Failure: Error
}

extension Event: _EventProtocol {}

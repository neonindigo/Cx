import Combine

/// Records all emissions from a publisher for later assertion.
///
/// Unlike `TestSubscriber`, `PublisherRecorder` has no XCTest dependency and can be
/// used in any test helper that needs to capture publisher output.
public final class PublisherRecorder<Output, Failure: Error> {
    public private(set) var values: [Output] = []
    public private(set) var completion: Subscribers.Completion<Failure>?
    private var cancellable: AnyCancellable?

    public init<P: Publisher>(_ publisher: P) where P.Output == Output, P.Failure == Failure {
        cancellable = publisher.sink(
            receiveCompletion: { [weak self] in self?.completion = $0 },
            receiveValue: { [weak self] in self?.values.append($0) }
        )
    }

    /// Returns `true` once any completion event has been recorded.
    public var isCompleted: Bool { completion != nil }

    /// Cancels the subscription and stops recording.
    public func cancel() { cancellable = nil }
}

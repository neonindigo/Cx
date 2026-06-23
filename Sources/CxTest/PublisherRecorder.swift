import Combine
import Foundation

/// Records all emissions from a publisher for later assertion.
///
/// Unlike `TestSubscriber`, `PublisherRecorder` has no XCTest dependency and can be
/// used in any test helper that needs to capture publisher output.
public final class PublisherRecorder<Output, Failure: Error> {
    private let lock = NSLock()
    private var _values: [Output] = []
    private var _completion: Subscribers.Completion<Failure>?
    private var cancellable: AnyCancellable?

    public var values: [Output] {
        lock.lock(); defer { lock.unlock() }
        return _values
    }

    public var completion: Subscribers.Completion<Failure>? {
        lock.lock(); defer { lock.unlock() }
        return _completion
    }

    public init<P: Publisher>(_ publisher: P) where P.Output == Output, P.Failure == Failure {
        cancellable = publisher.sink(
            receiveCompletion: { [weak self] c in
                guard let self else { return }
                self.lock.lock(); self._completion = c; self.lock.unlock()
            },
            receiveValue: { [weak self] v in
                guard let self else { return }
                self.lock.lock(); self._values.append(v); self.lock.unlock()
            }
        )
    }

    /// Returns `true` once any completion event has been recorded.
    public var isCompleted: Bool { completion != nil }

    /// Cancels the subscription and stops recording.
    public func cancel() { cancellable = nil }
}

import Combine
import Foundation

/// Records received values, completions, and failures from a publisher for test assertions.
///
/// Thread-safe: all state mutations and reads are protected by an `NSLock`.
public final class TestSubscriber<Output, Failure: Error>: Subscriber {
    private let stateLock = NSLock()
    private var _receivedValues: [Output] = []
    private var _receivedCompletion: Subscribers.Completion<Failure>?
    private var _subscription: Subscription?
    private var _pendingSemaphores: [DispatchSemaphore] = []

    public init() {}

    // MARK: - Subscriber

    public func receive(subscription: Subscription) {
        stateLock.lock()
        _subscription = subscription
        stateLock.unlock()
        subscription.request(.unlimited)
    }

    public func receive(_ input: Output) -> Subscribers.Demand {
        stateLock.lock()
        _receivedValues.append(input)
        stateLock.unlock()
        return .none
    }

    public func receive(completion: Subscribers.Completion<Failure>) {
        stateLock.lock()
        _receivedCompletion = completion
        _subscription = nil
        let semaphores = _pendingSemaphores
        _pendingSemaphores = []
        stateLock.unlock()
        semaphores.forEach { $0.signal() }
    }

    // MARK: - Thread-safe accessors

    /// All values received so far.
    public var receivedValues: [Output] {
        stateLock.lock(); defer { stateLock.unlock() }
        return _receivedValues
    }

    /// The completion event, if one has been received.
    public var receivedCompletion: Subscribers.Completion<Failure>? {
        stateLock.lock(); defer { stateLock.unlock() }
        return _receivedCompletion
    }

    /// Returns `true` if the subscriber received a `.finished` completion.
    public var isFinished: Bool {
        stateLock.lock(); defer { stateLock.unlock() }
        guard case .finished = _receivedCompletion else { return false }
        return true
    }

    /// Returns the failure if the subscriber received a `.failure` completion.
    public var receivedError: Failure? {
        stateLock.lock(); defer { stateLock.unlock() }
        guard case let .failure(error) = _receivedCompletion else { return nil }
        return error
    }

    // MARK: - Operations

    /// Cancels the active subscription.
    public func cancel() {
        stateLock.lock()
        let sub = _subscription
        _subscription = nil
        stateLock.unlock()
        sub?.cancel()
    }

    /// Blocks the calling thread until a completion event is received or `timeout` elapses.
    /// - Returns: `true` if completion arrived within the timeout; `false` otherwise.
    @discardableResult
    public func waitForCompletion(timeout: TimeInterval) -> Bool {
        stateLock.lock()
        if _receivedCompletion != nil {
            stateLock.unlock()
            return true
        }
        let semaphore = DispatchSemaphore(value: 0)
        _pendingSemaphores.append(semaphore)
        stateLock.unlock()
        return semaphore.wait(timeout: .now() + timeout) == .success
    }

    /// Clears all recorded values. Useful for multi-phase test assertions.
    public func resetValues() {
        stateLock.lock()
        _receivedValues = []
        stateLock.unlock()
    }

    /// Returns the first `count` recorded values.
    public func prefix(_ count: Int) -> [Output] {
        stateLock.lock(); defer { stateLock.unlock() }
        return Array(_receivedValues.prefix(count))
    }
}

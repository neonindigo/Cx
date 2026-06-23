import Combine

/// Records received values, completions, and failures from a publisher for test assertions.
public final class TestSubscriber<Output, Failure: Error>: Subscriber {
    public private(set) var receivedValues: [Output] = []
    public private(set) var receivedCompletion: Subscribers.Completion<Failure>?
    private var subscription: Subscription?

    public init() {}

    public func receive(subscription: Subscription) {
        self.subscription = subscription
        subscription.request(.unlimited)
    }

    public func receive(_ input: Output) -> Subscribers.Demand {
        receivedValues.append(input)
        return .unlimited
    }

    public func receive(completion: Subscribers.Completion<Failure>) {
        receivedCompletion = completion
        subscription = nil
    }

    /// Cancels the subscription.
    public func cancel() {
        subscription?.cancel()
        subscription = nil
    }

    /// Returns `true` if the subscriber received a `.finished` completion.
    public var isFinished: Bool {
        guard case .finished = receivedCompletion else { return false }
        return true
    }

    /// Returns the failure if the subscriber received a `.failure` completion.
    public var receivedError: Failure? {
        guard case let .failure(error) = receivedCompletion else { return nil }
        return error
    }
}

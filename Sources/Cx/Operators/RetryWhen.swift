import Combine

extension Publisher {
    /// Retries the upstream publisher when a notifier publisher emits.
    public func retryWhen<Notifier: Publisher>(
        _ notifierFactory: @escaping (AnyPublisher<Failure, Never>) -> Notifier
    ) -> AnyPublisher<Output, Failure> {
        // TODO: implement
        fatalError("stub")
    }
}

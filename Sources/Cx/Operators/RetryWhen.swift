import Combine

extension Publisher {
    /// Retries the upstream publisher when a notifier publisher emits.
    /// The notifier receives errors and signals retry timing; it must never fail.
    public func retryWhen<Notifier: Publisher>(
        _ notifierFactory: @escaping (AnyPublisher<Failure, Never>) -> Notifier
    ) -> AnyPublisher<Output, Failure> where Notifier.Failure == Never {
        // TODO: implement
        fatalError("stub")
    }
}

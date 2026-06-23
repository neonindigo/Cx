import Combine

extension Publisher {
    /// Emits the latest value from `self` only when `trigger` emits.
    public func sample<Trigger: Publisher>(
        _ trigger: Trigger
    ) -> AnyPublisher<Output, Failure> where Trigger.Failure == Never {
        // TODO: implement
        fatalError("stub")
    }
}

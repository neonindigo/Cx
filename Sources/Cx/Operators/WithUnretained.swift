import Combine

extension Publisher {
    /// Pairs each emitted value with a weak reference to `object`.
    /// Completes if `object` is deallocated.
    public func withUnretained<Object: AnyObject>(
        _ object: Object
    ) -> AnyPublisher<(Object, Output), Failure> {
        // TODO: implement
        fatalError("stub")
    }
}

import Combine

extension Publisher {
    /// Pairs each emitted value with a weak reference to `object`.
    /// Completes if `object` is deallocated.
    public func withUnretained<Object: AnyObject>(
        _ object: Object
    ) -> AnyPublisher<(Object, Output), Failure> {
        map { [weak object] value -> (Object, Output)? in
            guard let object = object else { return nil }
            return (object, value)
        }
        .prefix(while: { $0 != nil })
        .compactMap { $0 }
        .eraseToAnyPublisher()
    }
}

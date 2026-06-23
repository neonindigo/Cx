import Combine

extension Publisher {
    /// Pairs each emitted value with its zero-based index.
    public func enumerated() -> AnyPublisher<(index: Int, element: Output), Failure> {
        // TODO: implement
        fatalError("stub")
    }
}

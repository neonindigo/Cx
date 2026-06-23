import Combine

extension Publisher {
    /// Pairs each emitted value with its zero-based index.
    public func enumerated() -> AnyPublisher<(index: Int, element: Output), Failure> {
        var index = 0
        return map { element -> (index: Int, element: Output) in
            let i = index
            index += 1
            return (index: i, element: element)
        }.eraseToAnyPublisher()
    }
}

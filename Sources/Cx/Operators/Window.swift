import Combine

extension Publisher {
    /// Splits the stream into fixed-size publisher windows of `count` elements.
    public func window(
        ofCount count: Int
    ) -> AnyPublisher<AnyPublisher<Output, Failure>, Failure> {
        // TODO: implement
        fatalError("stub")
    }
}

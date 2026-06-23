import Combine

extension Publisher {
    /// Forwards whichever of `self` or `other` emits first; ignores the other.
    public func amb<Other: Publisher>(
        _ other: Other
    ) -> AnyPublisher<Output, Failure>
    where Other.Output == Output, Other.Failure == Failure {
        // TODO: implement
        fatalError("stub")
    }

    /// Alias for `amb(_:)`.
    public func race<Other: Publisher>(
        _ other: Other
    ) -> AnyPublisher<Output, Failure>
    where Other.Output == Output, Other.Failure == Failure {
        amb(other)
    }
}

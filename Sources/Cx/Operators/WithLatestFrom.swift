import Combine

extension Publisher {
    /// Emits the latest value from `other` whenever `self` emits.
    public func withLatestFrom<Other: Publisher, Result>(
        _ other: Other,
        resultSelector: @escaping (Output, Other.Output) -> Result
    ) -> AnyPublisher<Result, Failure> where Other.Failure == Failure {
        // TODO: implement
        fatalError("stub")
    }

    /// Emits the latest value from `other` whenever `self` emits, combining as a tuple.
    public func withLatestFrom<Other: Publisher>(
        _ other: Other
    ) -> AnyPublisher<(Output, Other.Output), Failure> where Other.Failure == Failure {
        withLatestFrom(other) { ($0, $1) }
    }
}

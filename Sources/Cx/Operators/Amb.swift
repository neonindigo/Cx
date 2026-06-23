import Combine
import Foundation

extension Publisher {
    /// Forwards whichever of `self` or `other` emits first; ignores the other.
    public func amb<Other: Publisher>(
        _ other: Other
    ) -> AnyPublisher<Output, Failure>
    where Other.Output == Output, Other.Failure == Failure {
        AmbPublisher(first: self, second: other).eraseToAnyPublisher()
    }

    /// Alias for `amb(_:)`.
    public func race<Other: Publisher>(
        _ other: Other
    ) -> AnyPublisher<Output, Failure>
    where Other.Output == Output, Other.Failure == Failure {
        amb(other)
    }
}

// MARK: - Publisher wrapper

private struct AmbPublisher<First: Publisher, Second: Publisher>: Publisher
where First.Output == Second.Output, First.Failure == Second.Failure {
    typealias Output  = First.Output
    typealias Failure = First.Failure

    let first:  First
    let second: Second

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        let sub = AmbSubscription(downstream: subscriber, first: first, second: second)
        subscriber.receive(subscription: sub)
    }
}

// MARK: - Subscription

private final class AmbSubscription<
    Downstream: Subscriber,
    First: Publisher,
    Second: Publisher
>: Subscription
where First.Output == Second.Output,
      First.Failure == Second.Failure,
      Downstream.Input  == First.Output,
      Downstream.Failure == First.Failure {

    private enum Side { case first, second }

    private let lock      = NSLock()
    private var downstream: Downstream?
    private var firstSub:   Subscription?
    private var secondSub:  Subscription?
    private var winner:     Side?          // nil = undecided

    // Attempt to claim victory for `side`.
    // Returns (shouldForward, loserSubscriptionToCancel).
    private func claim(_ side: Side) -> (Bool, Subscription?) {
        lock.lock()
        defer { lock.unlock() }
        if winner == nil {
            winner = side
            let loser: Subscription?
            if side == .first  { loser = secondSub; secondSub = nil }
            else               { loser = firstSub;  firstSub  = nil }
            return (true, loser)
        }
        return (winner == side, nil)
    }

    init(downstream: Downstream, first: First, second: Second) {
        self.downstream = downstream

        first.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.firstSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] value -> Subscribers.Demand in
                guard let self else { return .none }
                let (fwd, loser) = self.claim(.first)
                loser?.cancel()
                if fwd {
                    self.lock.lock()
                    let ds = self.downstream
                    self.lock.unlock()
                    _ = ds?.receive(value)
                }
                return .none
            },
            receiveCompletion: { [weak self] completion in
                guard let self else { return }
                let (fwd, loser) = self.claim(.first)
                loser?.cancel()
                if fwd { self.complete(with: completion) }
            }
        ))

        second.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.secondSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] value -> Subscribers.Demand in
                guard let self else { return .none }
                let (fwd, loser) = self.claim(.second)
                loser?.cancel()
                if fwd {
                    self.lock.lock()
                    let ds = self.downstream
                    self.lock.unlock()
                    _ = ds?.receive(value)
                }
                return .none
            },
            receiveCompletion: { [weak self] completion in
                guard let self else { return }
                let (fwd, loser) = self.claim(.second)
                loser?.cancel()
                if fwd { self.complete(with: completion) }
            }
        ))
    }

    private func complete(with completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        let ds = downstream; downstream = nil
        lock.unlock()
        ds?.receive(completion: completion)
    }

    func request(_ demand: Subscribers.Demand) { /* using .unlimited */ }

    func cancel() {
        lock.lock()
        downstream = nil
        let s1 = firstSub;  firstSub  = nil
        let s2 = secondSub; secondSub = nil
        lock.unlock()
        s1?.cancel(); s2?.cancel()
    }
}

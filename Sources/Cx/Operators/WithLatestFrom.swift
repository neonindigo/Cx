import Combine
import Foundation

extension Publisher {
    /// Emits the latest value from `other` whenever `self` emits.
    public func withLatestFrom<Other: Publisher, Result>(
        _ other: Other,
        resultSelector: @escaping (Output, Other.Output) -> Result
    ) -> AnyPublisher<Result, Failure> where Other.Failure == Failure {
        WithLatestFromPublisher(upstream: self, other: other, resultSelector: resultSelector)
            .eraseToAnyPublisher()
    }

    /// Emits the latest value from `other` whenever `self` emits, combining as a tuple.
    public func withLatestFrom<Other: Publisher>(
        _ other: Other
    ) -> AnyPublisher<(Output, Other.Output), Failure> where Other.Failure == Failure {
        withLatestFrom(other) { ($0, $1) }
    }
}

// MARK: - Publisher wrapper

private struct WithLatestFromPublisher<Upstream: Publisher, Other: Publisher, Result>: Publisher
where Upstream.Failure == Other.Failure {
    typealias Output  = Result
    typealias Failure = Upstream.Failure

    let upstream:       Upstream
    let other:          Other
    let resultSelector: (Upstream.Output, Other.Output) -> Result

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        let sub = WithLatestFromSubscription(downstream:      subscriber,
                                             upstream:        upstream,
                                             other:           other,
                                             resultSelector:  resultSelector)
        subscriber.receive(subscription: sub)
    }
}

// MARK: - Subscription

private final class WithLatestFromSubscription<
    Downstream: Subscriber,
    Upstream: Publisher,
    Other: Publisher,
    Result
>: Subscription
where Upstream.Failure == Other.Failure,
      Downstream.Input == Result,
      Downstream.Failure == Upstream.Failure {

    private let lock        = NSLock()
    private var downstream: Downstream?
    private var upstreamSub: Subscription?
    private var otherSub:    Subscription?
    private var latestOther: Other.Output?
    private let resultSelector: (Upstream.Output, Other.Output) -> Result

    init(downstream: Downstream,
         upstream: Upstream,
         other: Other,
         resultSelector: @escaping (Upstream.Output, Other.Output) -> Result) {
        self.downstream     = downstream
        self.resultSelector = resultSelector

        // Subscribe to `other` immediately with unlimited demand so we always
        // have its latest value ready.
        other.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.otherSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] value in
                self?.lock.lock(); self?.latestOther = value; self?.lock.unlock()
                return .none
            },
            receiveCompletion: { [weak self] completion in
                if case .failure(let err) = completion {
                    self?.forwardCompletion(.failure(err))
                }
                // Normal `other` completion: keep last value, stay alive.
            }
        ))

        // Subscribe to upstream; demand is forwarded by request(_:).
        upstream.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.upstreamSub = sub; self?.lock.unlock()
            },
            receiveValue: { [weak self] value -> Subscribers.Demand in
                guard let self else { return .none }
                self.lock.lock()
                let latest = self.latestOther
                let ds     = self.downstream
                self.lock.unlock()

                guard let latest, let ds else {
                    return .max(1) // Drop and replenish demand.
                }
                let result = self.resultSelector(value, latest)
                return ds.receive(result)
            },
            receiveCompletion: { [weak self] completion in
                self?.forwardCompletion(completion)
            }
        ))
    }

    private func forwardCompletion(_ completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        let ds = downstream; downstream = nil
        lock.unlock()
        ds?.receive(completion: completion)
    }

    func request(_ demand: Subscribers.Demand) {
        lock.lock()
        let sub = upstreamSub
        lock.unlock()
        sub?.request(demand)
    }

    func cancel() {
        lock.lock()
        downstream = nil
        let up = upstreamSub; upstreamSub = nil
        let ot = otherSub;    otherSub    = nil
        lock.unlock()
        up?.cancel(); ot?.cancel()
    }
}

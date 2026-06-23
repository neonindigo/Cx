import Combine
import Foundation

extension Publisher {
    /// Emits the latest value from `self` only when `trigger` emits.
    /// The stored value is cleared after each emission so a second trigger
    /// without a new upstream value produces nothing.
    public func sample<Trigger: Publisher>(
        _ trigger: Trigger
    ) -> AnyPublisher<Output, Failure> where Trigger.Failure == Never {
        SamplePublisher(upstream: self, trigger: trigger).eraseToAnyPublisher()
    }
}

// MARK: - Publisher wrapper

private struct SamplePublisher<Upstream: Publisher, Trigger: Publisher>: Publisher
where Trigger.Failure == Never {
    typealias Output  = Upstream.Output
    typealias Failure = Upstream.Failure

    let upstream: Upstream
    let trigger:  Trigger

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        let sub = SampleSubscription(downstream: subscriber,
                                     upstream:   upstream,
                                     trigger:    trigger)
        subscriber.receive(subscription: sub)
    }
}

// MARK: - Subscription

private final class SampleSubscription<
    Downstream: Subscriber,
    Upstream: Publisher,
    Trigger: Publisher
>: Subscription
where Downstream.Input == Upstream.Output,
      Downstream.Failure == Upstream.Failure,
      Trigger.Failure == Never {

    private let lock        = NSLock()
    private var downstream: Downstream?
    private var upstreamSub: Subscription?
    private var triggerSub:  Subscription?
    private var latestValue: Upstream.Output?
    private var isCompleted = false

    init(downstream: Downstream, upstream: Upstream, trigger: Trigger) {
        self.downstream = downstream

        // Track the latest value from upstream.
        upstream.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.upstreamSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] value in
                self?.lock.lock(); self?.latestValue = value; self?.lock.unlock()
                return .none
            },
            receiveCompletion: { [weak self] completion in
                self?.complete(with: completion)
            }
        ))

        // On each trigger pulse, emit the stored value (if any) and clear it.
        trigger.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.triggerSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] _ -> Subscribers.Demand in
                guard let self else { return .none }
                self.lock.lock()
                let value = self.latestValue
                self.latestValue = nil
                let ds = self.downstream
                self.lock.unlock()
                if let value, let ds { _ = ds.receive(value) }
                return .none
            },
            receiveCompletion: { [weak self] _ in
                self?.complete(with: .finished)
            }
        ))
    }

    private func complete(with completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        guard !isCompleted else { lock.unlock(); return }
        isCompleted = true
        let ds = downstream
        downstream = nil
        lock.unlock()
        ds?.receive(completion: completion)
    }

    func request(_ demand: Subscribers.Demand) { /* upstream already uses .unlimited */ }

    func cancel() {
        lock.lock()
        guard !isCompleted else { lock.unlock(); return }
        isCompleted = true
        downstream  = nil
        let up = upstreamSub; upstreamSub = nil
        let tr = triggerSub;  triggerSub  = nil
        lock.unlock()
        up?.cancel(); tr?.cancel()
    }
}

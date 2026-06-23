import Combine
import Foundation

extension Publisher {
    /// Retries the upstream publisher when a notifier publisher emits.
    /// The notifier receives errors and signals retry timing; it must never fail.
    public func retryWhen<Notifier: Publisher>(
        _ notifierFactory: @escaping (AnyPublisher<Failure, Never>) -> Notifier
    ) -> AnyPublisher<Output, Failure> where Notifier.Failure == Never {
        RetryWhenPublisher(upstream: self, notifierFactory: notifierFactory)
            .eraseToAnyPublisher()
    }
}

// MARK: - Publisher wrapper

private struct RetryWhenPublisher<Upstream: Publisher, Notifier: Publisher>: Publisher
where Notifier.Failure == Never {
    typealias Output  = Upstream.Output
    typealias Failure = Upstream.Failure

    let upstream:        Upstream
    let notifierFactory: (AnyPublisher<Failure, Never>) -> Notifier

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        let errorSubject = PassthroughSubject<Failure, Never>()
        let notifier     = notifierFactory(errorSubject.eraseToAnyPublisher())
        let sub = RetryWhenSubscription(downstream:   subscriber,
                                        upstream:     upstream,
                                        notifier:     notifier,
                                        errorSubject: errorSubject)
        subscriber.receive(subscription: sub)
    }
}

// MARK: - Subscription

private final class RetryWhenSubscription<
    Downstream: Subscriber,
    Upstream: Publisher,
    Notifier: Publisher
>: Subscription
where Upstream.Output  == Downstream.Input,
      Upstream.Failure == Downstream.Failure,
      Notifier.Failure == Never {

    private let lock        = NSLock()
    private var downstream: Downstream?
    private var upstreamSub: Subscription?
    private var notifierSub: Subscription?
    private let upstream:    Upstream
    private let errorSubject: PassthroughSubject<Upstream.Failure, Never>
    private var lastError:   Upstream.Failure?
    private var isCompleted  = false
    private var hasStarted   = false

    init(downstream: Downstream,
         upstream: Upstream,
         notifier: Notifier,
         errorSubject: PassthroughSubject<Upstream.Failure, Never>) {
        self.downstream   = downstream
        self.upstream     = upstream
        self.errorSubject = errorSubject

        // Subscribe to the notifier up front; it only receives values after
        // errorSubject emits (which happens only after upstream fails).
        notifier.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.notifierSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] _ in
                self?.resubscribe()
                return .none
            },
            receiveCompletion: { [weak self] _ in
                // Notifier completed — propagate the last upstream error.
                self?.propagateLastError()
            }
        ))
    }

    private func subscribeToUpstream() {
        upstream.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.upstreamSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] value -> Subscribers.Demand in
                guard let self else { return .none }
                let ds = self.lock.withLock { self.downstream }
                _ = ds?.receive(value)
                return .none
            },
            receiveCompletion: { [weak self] completion in
                guard let self else { return }
                switch completion {
                case .finished:
                    self.complete(with: .finished)
                case .failure(let err):
                    self.lock.lock(); self.lastError = err; self.lock.unlock()
                    self.errorSubject.send(err)
                    // Do not complete — wait for the notifier to decide.
                }
            }
        ))
    }

    private func resubscribe() {
        lock.lock()
        guard !isCompleted, hasStarted else { lock.unlock(); return }
        let old = upstreamSub; upstreamSub = nil
        lock.unlock()
        old?.cancel()
        subscribeToUpstream()
    }

    private func propagateLastError() {
        lock.lock()
        let err = lastError
        lock.unlock()
        complete(with: err.map(Subscribers.Completion.failure) ?? .finished)
    }

    private func complete(with completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        guard !isCompleted else { lock.unlock(); return }
        isCompleted = true
        let ds = downstream; downstream = nil
        lock.unlock()
        ds?.receive(completion: completion)
    }

    func request(_ demand: Subscribers.Demand) {
        lock.lock()
        guard !isCompleted else { lock.unlock(); return }
        let first = !hasStarted
        hasStarted = true
        lock.unlock()
        if first { subscribeToUpstream() }
    }

    func cancel() {
        lock.lock()
        guard !isCompleted else { lock.unlock(); return }
        isCompleted = true
        downstream  = nil
        let up  = upstreamSub;  upstreamSub  = nil
        let not = notifierSub;  notifierSub  = nil
        lock.unlock()
        up?.cancel(); not?.cancel()
    }
}

// MARK: - NSLock convenience (macOS 12 safe)

private extension NSLock {
    @discardableResult
    func withLock<T>(_ body: () -> T) -> T {
        lock(); defer { unlock() }
        return body()
    }
}

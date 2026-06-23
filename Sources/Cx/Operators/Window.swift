import Combine
import Foundation

extension Publisher {
    /// Splits the stream into fixed-size publisher windows of `count` elements.
    public func window(
        ofCount count: Int
    ) -> AnyPublisher<AnyPublisher<Output, Failure>, Failure> {
        WindowPublisher(upstream: self, windowSize: count).eraseToAnyPublisher()
    }
}

// MARK: - Publisher wrapper

private struct WindowPublisher<Upstream: Publisher>: Publisher {
    typealias Output  = AnyPublisher<Upstream.Output, Upstream.Failure>
    typealias Failure = Upstream.Failure

    let upstream:   Upstream
    let windowSize: Int

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        let sub = WindowSubscription(downstream: subscriber,
                                     upstream:   upstream,
                                     windowSize: windowSize)
        subscriber.receive(subscription: sub)
    }
}

// MARK: - Subscription

private final class WindowSubscription<Downstream: Subscriber, Upstream: Publisher>: Subscription
where Downstream.Input  == AnyPublisher<Upstream.Output, Upstream.Failure>,
      Downstream.Failure == Upstream.Failure {

    private let lock        = NSLock()
    private var downstream: Downstream?
    private var upstreamSub: Subscription?
    private var currentWindow: PassthroughSubject<Upstream.Output, Upstream.Failure>?
    private var windowCount  = 0
    private let windowSize:  Int
    private var isCompleted  = false

    init(downstream: Downstream, upstream: Upstream, windowSize: Int) {
        self.downstream = downstream
        self.windowSize = windowSize

        upstream.receive(subscriber: AnySubscriber(
            receiveSubscription: { [weak self] sub in
                self?.lock.lock(); self?.upstreamSub = sub; self?.lock.unlock()
                sub.request(.unlimited)
            },
            receiveValue: { [weak self] value -> Subscribers.Demand in
                guard let self else { return .none }

                let window: PassthroughSubject<Upstream.Output, Upstream.Failure>
                let isNew:  Bool
                let count:  Int

                self.lock.lock()
                if let existing = self.currentWindow {
                    window = existing
                    isNew  = false
                } else {
                    let fresh = PassthroughSubject<Upstream.Output, Upstream.Failure>()
                    self.currentWindow = fresh
                    self.windowCount   = 0
                    window = fresh
                    isNew  = true
                }
                self.windowCount += 1
                count = self.windowCount
                let ds = self.downstream
                if count >= self.windowSize { self.currentWindow = nil }
                self.lock.unlock()

                // Emit the new window publisher before the first value so
                // downstream can subscribe before values arrive.
                if isNew { _ = ds?.receive(window.eraseToAnyPublisher()) }
                window.send(value)
                if count >= windowSize { window.send(completion: .finished) }

                return .none
            },
            receiveCompletion: { [weak self] completion in
                guard let self else { return }
                self.lock.lock()
                let partial = self.currentWindow
                self.currentWindow = nil
                self.lock.unlock()

                switch completion {
                case .finished:         partial?.send(completion: .finished)
                case .failure(let err): partial?.send(completion: .failure(err))
                }
                self.complete(with: completion)
            }
        ))
    }

    private func complete(with completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        guard !isCompleted else { lock.unlock(); return }
        isCompleted = true
        let ds = downstream; downstream = nil
        lock.unlock()
        ds?.receive(completion: completion)
    }

    func request(_ demand: Subscribers.Demand) { /* upstream uses .unlimited */ }

    func cancel() {
        lock.lock()
        isCompleted = true
        downstream  = nil
        let sub = upstreamSub; upstreamSub = nil
        lock.unlock()
        sub?.cancel()
    }
}

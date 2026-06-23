import Combine
import ObjectiveC
import Foundation

// A helper that fires a closure when it is deallocated.
// Stored as an associated object on the observed instance so its
// lifetime equals the observed object's lifetime.
private final class DeallocObserver: NSObject {
    let onDealloc: () -> Void
    init(_ onDealloc: @escaping () -> Void) { self.onDealloc = onDealloc }
    deinit { onDealloc() }
}

private var deallocObserverKey: UInt8 = 0

extension Publisher {
    /// Pairs each emitted value with the (still-alive) `object`.
    /// Sends `.finished` as soon as `object` is deallocated, regardless of
    /// whether the upstream is still emitting.
    public func withUnretained<Object: AnyObject>(
        _ object: Object
    ) -> AnyPublisher<(Object, Output), Failure> {
        WithUnretainedPublisher(upstream: self, object: object).eraseToAnyPublisher()
    }
}

private struct WithUnretainedPublisher<Upstream: Publisher, Object: AnyObject>: Publisher {
    typealias Output = (Object, Upstream.Output)
    typealias Failure = Upstream.Failure

    let upstream: Upstream
    weak var object: Object?
    let objectRef: ObjectIdentifier

    init(upstream: Upstream, object: Object) {
        self.upstream = upstream
        self.object = object
        self.objectRef = ObjectIdentifier(object)
    }

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        guard let object = object else {
            subscriber.receive(subscription: Subscriptions.empty)
            subscriber.receive(completion: .finished)
            return
        }
        let subscription = WithUnretainedSubscription(
            upstream: upstream, object: object, downstream: subscriber
        )
        subscriber.receive(subscription: subscription)
    }
}

private final class WithUnretainedSubscription<
    Upstream: Publisher,
    Object: AnyObject,
    Downstream: Subscriber
>: Subscription
where Downstream.Input == (Object, Upstream.Output), Downstream.Failure == Upstream.Failure {

    private weak var object: Object?
    private var downstream: Downstream?
    private var upstreamCancellable: AnyCancellable?
    private let lock = NSLock()

    init(upstream: Upstream, object: Object, downstream: Downstream) {
        self.object = object
        self.downstream = downstream

        // Attach a dealloc observer so we complete even if upstream goes silent.
        let observer = DeallocObserver { [weak self] in self?.complete() }
        objc_setAssociatedObject(object, &deallocObserverKey, observer, .OBJC_ASSOCIATION_RETAIN_NONATOMIC)

        upstreamCancellable = upstream.sink(
            receiveCompletion: { [weak self] completion in
                self?.lock.lock()
                let downstream = self?.downstream
                self?.downstream = nil
                self?.lock.unlock()
                downstream?.receive(completion: completion)
            },
            receiveValue: { [weak self] value in
                guard let self else { return }
                self.lock.lock()
                guard let object = self.object, let downstream = self.downstream else {
                    self.lock.unlock()
                    return
                }
                self.lock.unlock()
                _ = downstream.receive((object, value))
            }
        )
    }

    func request(_ demand: Subscribers.Demand) {}

    func cancel() {
        lock.lock()
        downstream = nil
        let c = upstreamCancellable
        upstreamCancellable = nil
        lock.unlock()
        c?.cancel()
    }

    private func complete() {
        lock.lock()
        let downstream = self.downstream
        self.downstream = nil
        lock.unlock()
        downstream?.receive(completion: .finished)
    }
}

import Combine
import Foundation

/// A Subject that buffers the last N values and replays them to new subscribers.
public final class ReplaySubject<Output, Failure: Error>: Subject {
    private let bufferSize: Int
    private var buffer: [Output] = []
    private var completion: Subscribers.Completion<Failure>?
    private var subscriptions: [ReplaySubjectSubscription<Output, Failure>] = []
    private let lock = NSRecursiveLock()

    public init(bufferSize: Int) {
        self.bufferSize = bufferSize
    }

    public func send(_ value: Output) {
        lock.lock(); defer { lock.unlock() }
        guard completion == nil else { return }
        buffer.append(value)
        if buffer.count > bufferSize { buffer.removeFirst() }
        subscriptions.forEach { $0.receive(value) }
    }

    public func send(completion: Subscribers.Completion<Failure>) {
        lock.lock(); defer { lock.unlock() }
        guard self.completion == nil else { return }
        self.completion = completion
        subscriptions.forEach { $0.receive(completion: completion) }
    }

    public func send(subscription: Subscription) {
        subscription.request(.unlimited)
    }

    public func receive<S: Subscriber>(
        subscriber: S
    ) where S.Input == Output, S.Failure == Failure {
        lock.lock(); defer { lock.unlock() }
        let subscription = ReplaySubjectSubscription(subscriber: subscriber) { [weak self] sub in
            self?.remove(subscription: sub)
        }
        subscriber.receive(subscription: subscription)
        subscriptions.append(subscription)
        // Replay through the subscription so cancel() is respected and the nil-guard is honoured.
        buffer.forEach { subscription.receive($0) }
        if let completion { subscription.receive(completion: completion) }
    }

    public static func replay(_ bufferSize: Int) -> ReplaySubject<Output, Failure> {
        ReplaySubject(bufferSize: bufferSize)
    }

    private func remove(subscription: ReplaySubjectSubscription<Output, Failure>) {
        lock.lock(); defer { lock.unlock() }
        subscriptions.removeAll { $0 === subscription }
    }
}

private final class ReplaySubjectSubscription<Output, Failure: Error>: Subscription {
    private var subscriber: AnySubscriber<Output, Failure>?
    private let onCancel: (ReplaySubjectSubscription<Output, Failure>) -> Void
    private let lock = NSLock()

    init<S: Subscriber>(
        subscriber: S,
        onCancel: @escaping (ReplaySubjectSubscription<Output, Failure>) -> Void
    ) where S.Input == Output, S.Failure == Failure {
        self.subscriber = AnySubscriber(subscriber)
        self.onCancel = onCancel
    }

    func request(_ demand: Subscribers.Demand) {}

    func cancel() {
        lock.lock()
        subscriber = nil
        lock.unlock()
        onCancel(self)
    }

    func receive(_ value: Output) {
        lock.lock()
        let sub = subscriber
        lock.unlock()
        _ = sub?.receive(value)
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        lock.lock()
        let sub = subscriber
        subscriber = nil
        lock.unlock()
        sub?.receive(completion: completion)
    }
}

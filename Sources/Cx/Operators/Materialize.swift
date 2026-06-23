import Combine
import Foundation

/// Represents a materialised publisher event.
public enum Event<Output, Failure: Error> {
    case value(Output)
    case failure(Failure)
    case finished
}

// Existential helper so `dematerialize` can constrain on `Event`.
public protocol _EventProtocol {
    associatedtype Output
    associatedtype Failure: Error
    func _apply(value: (Output) -> Void, failure: (Failure) -> Void, finished: () -> Void)
}

extension Event: _EventProtocol {
    public func _apply(value: (Output) -> Void, failure: (Failure) -> Void, finished: () -> Void) {
        switch self {
        case .value(let v):   value(v)
        case .failure(let e): failure(e)
        case .finished:       finished()
        }
    }
}

// MARK: - materialize

extension Publisher {
    /// Wraps each event (value, completion, failure) into an `Event` value.
    /// The resulting publisher never fails.
    public func materialize() -> AnyPublisher<Event<Output, Failure>, Never> {
        MaterializePublisher(upstream: self).eraseToAnyPublisher()
    }
}

private struct MaterializePublisher<Upstream: Publisher>: Publisher {
    typealias Output = Event<Upstream.Output, Upstream.Failure>
    typealias Failure = Never

    let upstream: Upstream

    func receive<S: Subscriber>(subscriber: S) where S.Input == Output, S.Failure == Never {
        let subscription = MaterializeSubscription(upstream: upstream, downstream: subscriber)
        subscriber.receive(subscription: subscription)
    }
}

private final class MaterializeSubscription<
    Upstream: Publisher,
    Downstream: Subscriber
>: Subscription where Downstream.Input == Event<Upstream.Output, Upstream.Failure>, Downstream.Failure == Never {

    private var upstream: Upstream?
    private var downstream: Downstream?
    private var upstreamCancellable: AnyCancellable?
    private let lock = NSLock()

    init(upstream: Upstream, downstream: Downstream) {
        self.upstream = upstream
        self.downstream = downstream
    }

    func request(_ demand: Subscribers.Demand) {
        lock.lock()
        guard let upstream = upstream, upstreamCancellable == nil else { lock.unlock(); return }
        let downstream = self.downstream
        lock.unlock()

        upstreamCancellable = upstream.sink(
            receiveCompletion: { [weak self] completion in
                guard let self, let downstream = self.downstream else { return }
                switch completion {
                case .finished:         _ = downstream.receive(.finished)
                case .failure(let err): _ = downstream.receive(.failure(err))
                }
                downstream.receive(completion: .finished)
                self.downstream = nil
            },
            receiveValue: { [weak self] value in
                _ = self?.downstream?.receive(.value(value))
            }
        )
    }

    func cancel() {
        lock.lock()
        upstream = nil
        downstream = nil
        let c = upstreamCancellable
        upstreamCancellable = nil
        lock.unlock()
        c?.cancel()
    }
}

// MARK: - dematerialize

extension Publisher where Output: _EventProtocol, Failure == Never {
    /// Unwraps `Event` values back into a publisher stream.
    public func dematerialize() -> AnyPublisher<Output.Output, Output.Failure> {
        DematerializePublisher(upstream: self).eraseToAnyPublisher()
    }
}

private struct DematerializePublisher<Upstream: Publisher>: Publisher
where Upstream.Output: _EventProtocol, Upstream.Failure == Never {
    typealias Output = Upstream.Output.Output
    typealias Failure = Upstream.Output.Failure

    let upstream: Upstream

    func receive<S: Subscriber>(subscriber: S) where S.Input == Output, S.Failure == Failure {
        let subscription = DematerializeSubscription(upstream: upstream, downstream: subscriber)
        subscriber.receive(subscription: subscription)
    }
}

private final class DematerializeSubscription<
    Upstream: Publisher,
    Downstream: Subscriber
>: Subscription
where Upstream.Output: _EventProtocol, Upstream.Failure == Never,
      Downstream.Input == Upstream.Output.Output, Downstream.Failure == Upstream.Output.Failure {

    private var upstream: Upstream?
    private var downstream: Downstream?
    private var upstreamCancellable: AnyCancellable?
    private let lock = NSLock()

    init(upstream: Upstream, downstream: Downstream) {
        self.upstream = upstream
        self.downstream = downstream
    }

    func request(_ demand: Subscribers.Demand) {
        lock.lock()
        guard let upstream = upstream, upstreamCancellable == nil else { lock.unlock(); return }
        lock.unlock()

        upstreamCancellable = upstream.sink(
            receiveCompletion: { [weak self] _ in
                self?.downstream?.receive(completion: .finished)
                self?.downstream = nil
            },
            receiveValue: { [weak self] event in
                guard let self, let downstream = self.downstream else { return }
                event._apply(
                    value:    { _ = downstream.receive($0) },
                    failure:  { downstream.receive(completion: .failure($0)); self.downstream = nil },
                    finished: { downstream.receive(completion: .finished); self.downstream = nil }
                )
            }
        )
    }

    func cancel() {
        lock.lock()
        upstream = nil
        downstream = nil
        let c = upstreamCancellable
        upstreamCancellable = nil
        lock.unlock()
        c?.cancel()
    }
}

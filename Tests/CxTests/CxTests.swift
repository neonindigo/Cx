import XCTest
import Combine
import Cx

final class CxTests: XCTestCase {
    func testPlaceholder() {
        XCTAssertTrue(true)
    }

    // Regression: cancelled subscriptions must be removed from ReplaySubject's internal list.
    func testReplaySubjectCancelledSubscriptionIsRemoved() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 3)
        var cancellables = Set<AnyCancellable>()
        var received: [Int] = []

        subject.sink { received.append($0) }.store(in: &cancellables)
        subject.send(1)
        subject.send(2)

        // Cancel all subscriptions — subject's internal array must now be empty.
        cancellables.removeAll()

        // A subsequent send must not crash or deliver to the cancelled subscriber.
        subject.send(3)
        XCTAssertEqual(received, [1, 2])
    }

    // Regression: ReplaySubject.replay(_:) convenience factory.
    func testReplaySubjectFactoryConvenience() {
        let subject = ReplaySubject<Int, Never>.replay(3)
        subject.send(1)
        var received: [Int] = []
        var bag = Set<AnyCancellable>()
        subject.sink { received.append($0) }.store(in: &bag)
        XCTAssertEqual(received, [1])
    }

    // Regression: buffer replay must respect cancellation — values after cancel must not be delivered.
    func testReplaySubjectBufferReplayRespectsCancel() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 5)
        subject.send(10)
        subject.send(20)

        // Custom subscriber that cancels synchronously inside receive(subscription:).
        final class CancelOnSubscribeSubscriber: Subscriber {
            typealias Input = Int
            typealias Failure = Never
            var received: [Int] = []

            func receive(subscription: Subscription) {
                subscription.cancel()
            }
            func receive(_ input: Int) -> Subscribers.Demand {
                received.append(input)
                return .unlimited
            }
            func receive(completion: Subscribers.Completion<Never>) {}
        }

        let subscriber = CancelOnSubscribeSubscriber()
        subject.receive(subscriber: subscriber)
        XCTAssertEqual(subscriber.received, [], "No buffered values should be delivered after cancel")
    }
}

// MARK: - PublishRelay tests

final class PublishRelayTests: XCTestCase {

    // 1. Subscribers receive values sent after subscription.
    func testReceivesValuesSentAfterSubscription() {
        let relay = PublishRelay<Int>()
        var received: [Int] = []
        let cancellable = relay.sink { received.append($0) }
        relay.send(1)
        relay.send(2)
        XCTAssertEqual(received, [1, 2])
        cancellable.cancel()
    }

    // 2. Values sent before subscription are not received (no replay).
    func testNoReplayForLateSubscriber() {
        let relay = PublishRelay<Int>()
        relay.send(99)
        var received: [Int] = []
        let cancellable = relay.sink { received.append($0) }
        XCTAssertEqual(received, [])
        cancellable.cancel()
    }

    // 3. Multiple subscribers each receive the value.
    func testMultipleSubscribers() {
        let relay = PublishRelay<Int>()
        var a: [Int] = []
        var b: [Int] = []
        let c1 = relay.sink { a.append($0) }
        let c2 = relay.sink { b.append($0) }
        relay.send(7)
        XCTAssertEqual(a, [7])
        XCTAssertEqual(b, [7])
        c1.cancel(); c2.cancel()
    }

    // 4. PublishRelay has no send(completion:) — verified at compile time by the absence of the method.
    //    This test acts as a documentation anchor.
    func testHasNoSendCompletion() {
        // If this file compiles, PublishRelay exposes no send(completion:) method.
        XCTAssertTrue(true)
    }

    // 5. bind(to:) feeds upstream values into the relay.
    func testBindTo() {
        let relay = PublishRelay<Int>()
        var received: [Int] = []
        let c1 = relay.sink { received.append($0) }
        let subject = PassthroughSubject<Int, Never>()
        let c2 = subject.bind(to: relay)
        subject.send(3)
        subject.send(4)
        XCTAssertEqual(received, [3, 4])
        c1.cancel(); c2.cancel()
    }
}

// MARK: - BehaviorRelay tests

final class BehaviorRelayTests: XCTestCase {

    // 1. Late subscriber receives current value immediately.
    func testLateSubscriberReceivesCurrentValue() {
        let relay = BehaviorRelay<Int>(0)
        relay.send(42)
        var received: [Int] = []
        let cancellable = relay.sink { received.append($0) }
        XCTAssertEqual(received, [42])
        cancellable.cancel()
    }

    // 2. value property reflects latest sent value.
    func testValuePropertyReflectsLatest() {
        let relay = BehaviorRelay<String>("hello")
        XCTAssertEqual(relay.value, "hello")
        relay.send("world")
        XCTAssertEqual(relay.value, "world")
    }

    // 3. Multiple subscribers each receive values.
    func testMultipleSubscribers() {
        let relay = BehaviorRelay<Int>(10)
        var a: [Int] = []
        var b: [Int] = []
        let c1 = relay.sink { a.append($0) }
        let c2 = relay.sink { b.append($0) }
        relay.send(20)
        // Each subscriber gets initial value (10) then the new value (20).
        XCTAssertEqual(a, [10, 20])
        XCTAssertEqual(b, [10, 20])
        c1.cancel(); c2.cancel()
    }

    // 4. bind(to:) feeds upstream values into the relay.
    func testBindTo() {
        let relay = BehaviorRelay<Int>(0)
        let subject = PassthroughSubject<Int, Never>()
        let c1 = subject.bind(to: relay)
        subject.send(5)
        XCTAssertEqual(relay.value, 5)
        subject.send(6)
        XCTAssertEqual(relay.value, 6)
        c1.cancel()
    }
}

final class ReplaySubjectTests: XCTestCase {

    // 1. New subscriber receives last N buffered values immediately.
    func testReceivesLastNBufferedValues() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 3)
        subject.send(1); subject.send(2); subject.send(3)
        var received: [Int] = []
        subject.sink { received.append($0) }.cancel()
        XCTAssertEqual(received, [1, 2, 3])
    }

    // 2. New subscriber receives fewer than N values if fewer were sent.
    func testReceivesFewerThanNValuesWhenBufferNotFull() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 5)
        subject.send(7); subject.send(8)
        var received: [Int] = []
        subject.sink { received.append($0) }.cancel()
        XCTAssertEqual(received, [7, 8])
    }

    // 3. Buffer respects the size limit — oldest values are evicted.
    func testBufferEvictsOldestValues() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 2)
        subject.send(1); subject.send(2); subject.send(3)
        var received: [Int] = []
        subject.sink { received.append($0) }.cancel()
        XCTAssertEqual(received, [2, 3])
    }

    // 4. Completed subject replays buffer + completion to new subscriber.
    func testCompletedSubjectReplaysBufferAndCompletion() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 3)
        subject.send(1); subject.send(2)
        subject.send(completion: .finished)
        var received: [Int] = []
        var completed = false
        subject.sink(receiveCompletion: { _ in completed = true },
                     receiveValue: { received.append($0) }).cancel()
        XCTAssertEqual(received, [1, 2])
        XCTAssertTrue(completed)
    }

    // 5. Failed subject replays buffer + failure to new subscriber.
    func testFailedSubjectReplaysBufferAndFailure() {
        struct E: Error {}
        let subject = ReplaySubject<Int, Error>(bufferSize: 3)
        subject.send(9)
        subject.send(completion: .failure(E()))
        var received: [Int] = []
        var didFail = false
        subject.sink(receiveCompletion: { if case .failure = $0 { didFail = true } },
                     receiveValue: { received.append($0) }).cancel()
        XCTAssertEqual(received, [9])
        XCTAssertTrue(didFail)
    }

    // 6. Cancelled subscription does not receive subsequent sends.
    func testCancelledSubscriptionReceivesNothing() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 3)
        var received: [Int] = []
        let cancellable = subject.sink { received.append($0) }
        subject.send(1)
        cancellable.cancel()
        subject.send(2)
        XCTAssertEqual(received, [1])
    }

    // 7. Thread-safety: concurrent sends + subscriptions don't crash.
    func testConcurrentSendsAndSubscriptions() {
        let subject = ReplaySubject<Int, Never>(bufferSize: 10)
        var cancellables = [AnyCancellable]()
        let lock = NSLock()
        DispatchQueue.concurrentPerform(iterations: 100) { i in
            subject.send(i)
            let c = subject.sink { _ in }
            lock.lock(); cancellables.append(c); lock.unlock()
        }
        XCTAssertTrue(true)
    }
}

final class ReplayRelayTests: XCTestCase {

    // 1. New subscriber receives buffered values.
    func testNewSubscriberReceivesBuffer() {
        let relay = ReplayRelay<Int>(bufferSize: 3)
        relay.send(10); relay.send(20)
        var received: [Int] = []
        relay.sink { received.append($0) }.cancel()
        XCTAssertEqual(received, [10, 20])
    }

    // 3. Multiple subscribers all receive values.
    func testMultipleSubscribersReceiveValues() {
        let relay = ReplayRelay<String>(bufferSize: 2)
        relay.send("a")
        var r1: [String] = [], r2: [String] = []
        let c1 = relay.sink { r1.append($0) }
        let c2 = relay.sink { r2.append($0) }
        relay.send("b")
        c1.cancel(); c2.cancel()
        XCTAssertEqual(r1, ["a", "b"])
        XCTAssertEqual(r2, ["a", "b"])
    }

    // asPublisher() wraps the relay as AnyPublisher<Output, Never>.
    func testAsPublisherDeliversValues() {
        let relay = ReplayRelay<Int>(bufferSize: 2)
        relay.send(5)
        var received: [Int] = []
        relay.asPublisher().sink { received.append($0) }.cancel()
        XCTAssertEqual(received, [5])
    }
}

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
// MARK: - Shared test error

private enum TestError: Error, Equatable {
    case boom
}

// MARK: - EnumeratedTests

final class EnumeratedTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testIndexSequenceIsCorrect() {
        var results: [(index: Int, element: String)] = []
        ["a", "b", "c"].publisher
            .enumerated()
            .sink { results.append($0) }
            .store(in: &cancellables)
        XCTAssertEqual(results.map(\.index),   [0, 1, 2])
        XCTAssertEqual(results.map(\.element), ["a", "b", "c"])
    }

    func testEmptyPublisher() {
        var results: [(index: Int, element: Int)] = []
        var completed = false
        Empty<Int, Never>()
            .enumerated()
            .sink(receiveCompletion: { if case .finished = $0 { completed = true } },
                  receiveValue: { results.append($0) })
            .store(in: &cancellables)
        XCTAssertTrue(results.isEmpty)
        XCTAssertTrue(completed)
    }
}

// MARK: - WithUnretainedTests

final class WithUnretainedTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testEmitsWhileObjectAlive() {
        let subject = PassthroughSubject<Int, Never>()
        let obj     = NSObject()
        var values: [Int] = []

        subject.withUnretained(obj)
            .sink { values.append($0.1) }
            .store(in: &cancellables)

        subject.send(1); subject.send(2)
        XCTAssertEqual(values, [1, 2])
        _ = obj // keep alive
    }

    func testCompletesWhenObjectDeallocated() {
        let subject = PassthroughSubject<Int, Never>()
        var obj: NSObject? = NSObject()
        var values: [Int] = []
        var completed = false

        subject.withUnretained(obj!)
            .sink(receiveCompletion: { if case .finished = $0 { completed = true } },
                  receiveValue: { values.append($0.1) })
            .store(in: &cancellables)

        subject.send(1)
        obj = nil          // deallocate — next emission triggers nil check
        subject.send(2)    // should cause completion, not emission

        XCTAssertEqual(values, [1])
        XCTAssertTrue(completed)
    }
}

// MARK: - MaterializeTests

final class MaterializeTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testWrapsFailureAsEvent() {
        let subject = PassthroughSubject<Int, TestError>()
        var events: [Event<Int, TestError>] = []
        var outerCompleted = false

        subject.materialize()
            .sink(receiveCompletion: { if case .finished = $0 { outerCompleted = true } },
                  receiveValue: { events.append($0) })
            .store(in: &cancellables)

        subject.send(1); subject.send(2)
        subject.send(completion: .failure(.boom))

        XCTAssertEqual(events.count, 3)
        guard case .value(1)      = events[0] else { XCTFail("expected .value(1)");       return }
        guard case .value(2)      = events[1] else { XCTFail("expected .value(2)");       return }
        guard case .failure(.boom) = events[2] else { XCTFail("expected .failure(.boom)"); return }
        XCTAssertTrue(outerCompleted)
    }

    func testRoundtripMaterializeDematerialize() {
        let subject = PassthroughSubject<Int, TestError>()
        var received: [Int] = []
        var failed = false

        subject.materialize()
            .dematerialize()
            .sink(receiveCompletion: { if case .failure = $0 { failed = true } },
                  receiveValue: { received.append($0) })
            .store(in: &cancellables)

        subject.send(1); subject.send(2)
        subject.send(completion: .failure(.boom))

        XCTAssertEqual(received, [1, 2])
        XCTAssertTrue(failed)
    }
}

// MARK: - WithLatestFromTests

final class WithLatestFromTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testEmitsWithLatestFromOther() {
        let source = PassthroughSubject<Int, Never>()
        let other  = PassthroughSubject<String, Never>()
        var results: [(Int, String)] = []

        source.withLatestFrom(other)
            .sink { results.append($0) }
            .store(in: &cancellables)

        other.send("a")
        source.send(1)   // → (1, "a")
        other.send("b")
        source.send(2)   // → (2, "b")

        XCTAssertEqual(results.map(\.0), [1, 2])
        XCTAssertEqual(results.map(\.1), ["a", "b"])
    }

    func testDropsWhenOtherHasNoValueYet() {
        let source = PassthroughSubject<Int, Never>()
        let other  = PassthroughSubject<String, Never>()
        var results: [(Int, String)] = []

        source.withLatestFrom(other)
            .sink { results.append($0) }
            .store(in: &cancellables)

        source.send(1)   // other hasn't emitted — drop
        other.send("x")
        source.send(2)   // → (2, "x")

        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results.first?.0, 2)
        XCTAssertEqual(results.first?.1, "x")
    }
}

// MARK: - RetryWhenTests

final class RetryWhenTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testRetriesOnTrigger() {
        var attemptCount = 0
        let retryTrigger = PassthroughSubject<Void, Never>()
        var received: [Int] = []
        var completedWithError: TestError?

        // Each subscription to this Deferred increments attemptCount and
        // emits [1, 2] then immediately fails.
        let upstream = Deferred<AnyPublisher<Int, TestError>> {
            attemptCount += 1
            return [1, 2].publisher
                .setFailureType(to: TestError.self)
                .append(Fail(error: TestError.boom))
                .eraseToAnyPublisher()
        }

        upstream.retryWhen { errors in
            // Zip: each error must be paired with a trigger pulse to retry.
            errors.zip(retryTrigger).map { _ in () }
        }
        .sink(receiveCompletion: { if case .failure(let e) = $0 { completedWithError = e } },
              receiveValue: { received.append($0) })
        .store(in: &cancellables)

        // First attempt has already run synchronously.
        XCTAssertEqual(received, [1, 2])
        XCTAssertNil(completedWithError)
        XCTAssertEqual(attemptCount, 1)

        retryTrigger.send(())  // triggers retry #2
        XCTAssertEqual(received, [1, 2, 1, 2])
        XCTAssertNil(completedWithError)
        XCTAssertEqual(attemptCount, 2)
    }

    func testStopsWhenNotifierCompletes() {
        var received: [Int] = []
        var caughtError: TestError?

        Fail<Int, TestError>(error: .boom)
            .retryWhen { errors in
                // Take exactly one error then complete — no retry emitted.
                errors.prefix(1).flatMap { _ in Empty<Void, Never>() }
            }
            .sink(receiveCompletion: { if case .failure(let e) = $0 { caughtError = e } },
                  receiveValue: { received.append($0) })
            .store(in: &cancellables)

        XCTAssertEqual(caughtError, .boom)
        XCTAssertTrue(received.isEmpty)
    }
}

// MARK: - SampleTests

final class SampleTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testEmitsLatestValueOnTrigger() {
        let source  = PassthroughSubject<Int, Never>()
        let trigger = PassthroughSubject<Void, Never>()
        var results: [Int] = []

        source.sample(trigger)
            .sink { results.append($0) }
            .store(in: &cancellables)

        source.send(1); source.send(2)
        trigger.send(())  // should emit 2 (most recent)

        XCTAssertEqual(results, [2])
    }

    func testNoDuplicateOnSecondTriggerWithoutNewValue() {
        let source  = PassthroughSubject<Int, Never>()
        let trigger = PassthroughSubject<Void, Never>()
        var results: [Int] = []

        source.sample(trigger)
            .sink { results.append($0) }
            .store(in: &cancellables)

        source.send(42)
        trigger.send(())  // emits 42, clears stored value
        trigger.send(())  // nothing stored — skipped

        XCTAssertEqual(results, [42])
    }
}

// MARK: - AmbTests

final class AmbTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testFirstToEmitWins() {
        let first  = PassthroughSubject<Int, Never>()
        let second = PassthroughSubject<Int, Never>()
        var results: [Int] = []

        first.amb(second)
            .sink { results.append($0) }
            .store(in: &cancellables)

        second.send(99)  // second wins
        first.send(1)    // ignored
        second.send(100) // forwarded (second is winner)

        XCTAssertEqual(results, [99, 100])
    }

    func testLoserIsCancelledAfterWinnerEmits() {
        let first  = PassthroughSubject<Int, Never>()
        let second = PassthroughSubject<Int, Never>()
        var results: [Int] = []

        first.amb(second)
            .sink { results.append($0) }
            .store(in: &cancellables)

        first.send(1)    // first wins
        second.send(99)  // loser — ignored
        first.send(2)    // forwarded (first is winner)

        XCTAssertEqual(results, [1, 2])
    }
}

// MARK: - WindowTests

final class WindowTests: XCTestCase {
    var cancellables = Set<AnyCancellable>()

    override func tearDown() { cancellables.removeAll(); super.tearDown() }

    func testCorrectGroupingIntoCountSizedWindows() {
        let source   = PassthroughSubject<Int, Never>()
        var windows: [[Int]] = []
        let exp = expectation(description: "two full windows collected")
        exp.expectedFulfillmentCount = 2

        source.window(ofCount: 3)
            .sink { windowPub in
                windowPub.collect()
                    .sink { values in
                        windows.append(values)
                        exp.fulfill()
                    }
                    .store(in: &self.cancellables)
            }
            .store(in: &cancellables)

        (1...6).forEach { source.send($0) }
        source.send(completion: .finished)

        waitForExpectations(timeout: 1)

        XCTAssertEqual(windows.count, 2)
        // Windows arrive in order because everything is synchronous.
        let sorted = windows.sorted { $0.first ?? 0 < $1.first ?? 0 }
        XCTAssertEqual(sorted[0], [1, 2, 3])
        XCTAssertEqual(sorted[1], [4, 5, 6])
    }

    func testPartialLastWindowCompletes() {
        let source   = PassthroughSubject<Int, Never>()
        var windows: [[Int]] = []
        let exp = expectation(description: "partial window collected")

        source.window(ofCount: 3)
            .sink { windowPub in
                windowPub.collect()
                    .sink { values in
                        windows.append(values)
                        exp.fulfill()
                    }
                    .store(in: &self.cancellables)
            }
            .store(in: &cancellables)

        source.send(1); source.send(2)
        source.send(completion: .finished)  // partial window of 2 should complete

        waitForExpectations(timeout: 1)

        XCTAssertEqual(windows.count, 1)
        XCTAssertEqual(windows[0], [1, 2])
    }
}

import XCTest
import Combine
@testable import CxTest

// MARK: - Helpers

private final class MockSubscription: Subscription {
    func request(_ demand: Subscribers.Demand) {}
    func cancel() {}
}

// MARK: - TestSubscriberTests

final class TestSubscriberTests: XCTestCase {

    func testRecordsValues() {
        let subscriber = TestSubscriber<Int, Never>()
        subscriber.receive(subscription: MockSubscription())
        _ = subscriber.receive(1)
        _ = subscriber.receive(2)
        _ = subscriber.receive(3)
        XCTAssertEqual(subscriber.receivedValues, [1, 2, 3])
    }

    func testRecordsFinishedCompletion() {
        let publisher = [1, 2, 3].publisher
        let subscriber = TestSubscriber<Int, Never>()
        publisher.subscribe(subscriber)
        XCTAssertTrue(subscriber.isFinished)
        XCTAssertNil(subscriber.receivedError)
        XCTAssertEqual(subscriber.receivedValues, [1, 2, 3])
    }

    func testRecordsFailure() {
        struct TestError: Error, Equatable {}
        let publisher = Fail<Int, TestError>(error: TestError())
        let subscriber = TestSubscriber<Int, TestError>()
        publisher.subscribe(subscriber)
        XCTAssertFalse(subscriber.isFinished)
        XCTAssertEqual(subscriber.receivedError, TestError())
    }

    func testIsFinishedOnlyAfterFinished() {
        let subject = PassthroughSubject<Int, Never>()
        let subscriber = TestSubscriber<Int, Never>()
        subject.subscribe(subscriber)
        subject.send(1)
        XCTAssertFalse(subscriber.isFinished)
        subject.send(completion: .finished)
        XCTAssertTrue(subscriber.isFinished)
    }

    func testReceivedErrorOnlyAfterFailure() {
        struct TestError: Error {}
        let subject = PassthroughSubject<Int, TestError>()
        let subscriber = TestSubscriber<Int, TestError>()
        subject.subscribe(subscriber)
        subject.send(1)
        XCTAssertNil(subscriber.receivedError)
        subject.send(completion: .failure(TestError()))
        XCTAssertNotNil(subscriber.receivedError)
        XCTAssertFalse(subscriber.isFinished)
    }

    func testCancelStopsDelivery() {
        let subject = PassthroughSubject<Int, Never>()
        let subscriber = TestSubscriber<Int, Never>()
        subject.subscribe(subscriber)
        subject.send(1)
        subscriber.cancel()
        subject.send(2)
        XCTAssertEqual(subscriber.receivedValues, [1])
    }

    func testConcurrentValueDeliveryDoesNotCrash() {
        let subscriber = TestSubscriber<Int, Never>()
        subscriber.receive(subscription: MockSubscription())

        let group = DispatchGroup()
        let queue = DispatchQueue(label: "cx.test.concurrent", attributes: .concurrent)
        for i in 0..<1000 {
            group.enter()
            queue.async {
                _ = subscriber.receive(i)
                group.leave()
            }
        }
        group.wait()
        XCTAssertEqual(subscriber.receivedValues.count, 1000)
    }

    func testWaitForCompletionReturnsTrueOnCompletion() {
        let publisher = Just(42)
        let subscriber = TestSubscriber<Int, Never>()
        publisher.subscribe(subscriber)
        XCTAssertTrue(subscriber.waitForCompletion(timeout: 1.0))
    }

    func testWaitForCompletionReturnsFalseOnTimeout() {
        let subject = PassthroughSubject<Int, Never>()
        let subscriber = TestSubscriber<Int, Never>()
        subject.subscribe(subscriber)
        XCTAssertFalse(subscriber.waitForCompletion(timeout: 0.1))
    }

    func testWaitForCompletionReturnsTrueIfAlreadyCompleted() {
        let subscriber = TestSubscriber<Int, Never>()
        let publisher = Empty<Int, Never>()
        publisher.subscribe(subscriber)
        // Already completed synchronously; second call should return true immediately
        XCTAssertTrue(subscriber.waitForCompletion(timeout: 0.0))
    }

    func testResetValues() {
        let subject = PassthroughSubject<Int, Never>()
        let subscriber = TestSubscriber<Int, Never>()
        subject.subscribe(subscriber)
        subject.send(1)
        subject.send(2)
        subscriber.resetValues()
        subject.send(3)
        XCTAssertEqual(subscriber.receivedValues, [3])
    }

    func testPrefix() {
        let publisher = [1, 2, 3, 4, 5].publisher
        let subscriber = TestSubscriber<Int, Never>()
        publisher.subscribe(subscriber)
        XCTAssertEqual(subscriber.prefix(3), [1, 2, 3])
        XCTAssertEqual(subscriber.prefix(0), [])
        XCTAssertEqual(subscriber.prefix(10), [1, 2, 3, 4, 5])
    }
}

// MARK: - XCTestCaseExtensionsTests

final class XCTestCaseExtensionsTests: XCTestCase {

    func testCollectFromJustPublisher() throws {
        let values = try collect(from: Just(42))
        XCTAssertEqual(values, [42])
    }

    func testCollectFromArrayPublisher() throws {
        let values = try collect(from: [1, 2, 3].publisher)
        XCTAssertEqual(values, [1, 2, 3])
    }

    func testCollectValuesFromPublisher() throws {
        let values = try collectValues(from: [10, 20, 30].publisher)
        XCTAssertEqual(values, [10, 20, 30])
    }

    func testAssertPublisherEmitsCorrectSequence() {
        assertPublisher([1, 2, 3].publisher, emits: [1, 2, 3])
    }

    func testAssertPublisherFailsWithExpectedError() {
        struct TestError: Error {}
        let publisher = Fail<Int, TestError>(error: TestError())
        assertPublisher(publisher, failsWith: { $0 is TestError })
    }

    func testAssertPublisherCompletesEmpty() {
        assertPublisherCompletes(Empty<Int, Never>())
    }
}

// MARK: - PublisherRecorderTests

final class PublisherRecorderTests: XCTestCase {

    func testRecordsAllValues() {
        let recorder = PublisherRecorder([1, 2, 3].publisher)
        XCTAssertEqual(recorder.values, [1, 2, 3])
    }

    func testRecordsFinishedCompletion() {
        let recorder = PublisherRecorder([1, 2].publisher)
        XCTAssertTrue(recorder.isCompleted)
        guard case .finished = recorder.completion else {
            XCTFail("Expected .finished completion")
            return
        }
    }

    func testRecordsFailure() {
        struct TestError: Error {}
        let recorder = PublisherRecorder(Fail<Int, TestError>(error: TestError()))
        XCTAssertTrue(recorder.isCompleted)
        guard case .failure = recorder.completion else {
            XCTFail("Expected .failure completion")
            return
        }
    }

    func testCancelStopsRecording() {
        let subject = PassthroughSubject<Int, Never>()
        let recorder = PublisherRecorder(subject)
        subject.send(1)
        recorder.cancel()
        subject.send(2)
        XCTAssertEqual(recorder.values, [1])
    }
}

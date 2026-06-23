import XCTest
import Combine

extension XCTestCase {
    /// Subscribes to `publisher`, collects all values until completion or `timeout`, then returns them.
    /// Throws the publisher's error if it completes with a failure.
    public func collectValues<P: Publisher>(
        from publisher: P,
        count: Int = Int.max,
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) throws -> [P.Output] {
        let subscriber = TestSubscriber<P.Output, P.Failure>()
        publisher.subscribe(subscriber)
        defer { subscriber.cancel() }

        if !subscriber.waitForCompletion(timeout: timeout) {
            XCTFail("Publisher did not complete within \(timeout)s", file: file, line: line)
        }

        if let error = subscriber.receivedError {
            throw error
        }
        let values = subscriber.receivedValues
        return count == Int.max ? values : Array(values.prefix(count))
    }

    /// Subscribes to `publisher` and collects values until completion or `timeout`.
    /// Returns the collected values.
    public func collect<P: Publisher>(
        from publisher: P,
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) throws -> [P.Output] {
        try collectValues(from: publisher, timeout: timeout, file: file, line: line)
    }

    /// Asserts that `publisher` emits exactly `expectedValues` and then finishes.
    public func assertPublisher<P: Publisher>(
        _ publisher: P,
        emits expectedValues: [P.Output],
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) where P.Output: Equatable {
        let subscriber = TestSubscriber<P.Output, P.Failure>()
        publisher.subscribe(subscriber)
        defer { subscriber.cancel() }

        if !subscriber.waitForCompletion(timeout: timeout) {
            XCTFail("Publisher did not complete within \(timeout)s", file: file, line: line)
            return
        }

        XCTAssertEqual(subscriber.receivedValues, expectedValues, file: file, line: line)
        XCTAssertTrue(subscriber.isFinished, "Expected .finished completion", file: file, line: line)
    }

    /// Asserts that `publisher` fails with an error satisfying `predicate`.
    public func assertPublisher<P: Publisher>(
        _ publisher: P,
        failsWith predicate: (P.Failure) -> Bool,
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) {
        let subscriber = TestSubscriber<P.Output, P.Failure>()
        publisher.subscribe(subscriber)
        defer { subscriber.cancel() }

        if !subscriber.waitForCompletion(timeout: timeout) {
            XCTFail("Publisher did not complete within \(timeout)s", file: file, line: line)
            return
        }

        guard let error = subscriber.receivedError else {
            XCTFail("Expected publisher to fail, but it completed with .finished", file: file, line: line)
            return
        }
        XCTAssertTrue(predicate(error), "Error did not satisfy predicate: \(error)", file: file, line: line)
    }

    /// Asserts that `publisher` completes with `.finished` without emitting any values.
    public func assertPublisherCompletes<P: Publisher>(
        _ publisher: P,
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) {
        let subscriber = TestSubscriber<P.Output, P.Failure>()
        publisher.subscribe(subscriber)
        defer { subscriber.cancel() }

        if !subscriber.waitForCompletion(timeout: timeout) {
            XCTFail("Publisher did not complete within \(timeout)s", file: file, line: line)
            return
        }

        XCTAssertTrue(subscriber.isFinished,
                      "Expected .finished completion but got \(String(describing: subscriber.receivedCompletion))",
                      file: file, line: line)
        XCTAssertTrue(subscriber.receivedValues.isEmpty,
                      "Expected no values but received \(subscriber.receivedValues.count)",
                      file: file, line: line)
    }
}

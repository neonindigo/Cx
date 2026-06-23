import XCTest
import Combine

extension XCTestCase {
    /// Subscribes to `publisher`, collects up to `count` values within `timeout`, then returns them.
    public func collectValues<P: Publisher>(
        from publisher: P,
        count: Int = Int.max,
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) throws -> [P.Output] {
        // TODO: implement
        fatalError("stub")
    }

    /// Asserts that `publisher` emits exactly `expectedValues` and then finishes.
    public func assertPublisher<P: Publisher>(
        _ publisher: P,
        emits expectedValues: [P.Output],
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) where P.Output: Equatable {
        // TODO: implement
        fatalError("stub")
    }

    /// Asserts that `publisher` fails with an error satisfying `predicate`.
    public func assertPublisher<P: Publisher>(
        _ publisher: P,
        failsWith predicate: (P.Failure) -> Bool,
        timeout: TimeInterval = 1.0,
        file: StaticString = #file,
        line: UInt = #line
    ) {
        // TODO: implement
        fatalError("stub")
    }
}

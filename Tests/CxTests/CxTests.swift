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

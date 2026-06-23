#if canImport(UIKit)
import UIKit
import Combine

/// A Combine publisher that emits the sending control whenever a UIControl fires specified events.
struct UIControlPublisher<Control: UIControl>: Publisher {
    typealias Output = Control
    typealias Failure = Never

    let control: Control
    let events: UIControl.Event

    func receive<S: Subscriber>(subscriber: S) where S.Input == Control, S.Failure == Never {
        let subscription = UIControlSubscription(subscriber: subscriber, control: control, events: events)
        subscriber.receive(subscription: subscription)
    }
}

private final class UIControlSubscription<S: Subscriber, Control: UIControl>: NSObject, Subscription
where S.Input == Control, S.Failure == Never {
    private var subscriber: S?
    private weak var control: Control?
    private let events: UIControl.Event

    init(subscriber: S, control: Control, events: UIControl.Event) {
        self.subscriber = subscriber
        self.control = control
        self.events = events
        super.init()
        control.addTarget(self, action: #selector(handleEvent(_:)), for: events)
    }

    func request(_ demand: Subscribers.Demand) {}

    func cancel() {
        control?.removeTarget(self, action: #selector(handleEvent(_:)), for: events)
        subscriber = nil
    }

    @objc private func handleEvent(_ sender: UIControl) {
        guard let subscriber, let control else { return }
        _ = subscriber.receive(control)
    }
}
#endif

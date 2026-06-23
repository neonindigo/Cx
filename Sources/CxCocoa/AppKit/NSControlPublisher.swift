#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

/// A Combine publisher that emits the control itself each time it fires its target/action.
///
/// - Note: This publisher replaces the control's existing `target` and `action` for the lifetime
///   of the subscription. Any prior target/action pair is not preserved or restored.
struct NSControlPublisher<Control: NSControl>: Publisher {
    typealias Output = Control
    typealias Failure = Never

    let control: Control

    func receive<S: Subscriber>(subscriber: S) where S.Input == Control, S.Failure == Never {
        let subscription = NSControlSubscription(control: control, subscriber: subscriber)
        subscriber.receive(subscription: subscription)
    }
}

// MARK: - Target shim

private final class NSControlTarget: NSObject {
    var handler: (() -> Void)?
    @objc func action(_ sender: Any?) { handler?() }
}

// MARK: - Subscription

private final class NSControlSubscription<Control: NSControl, S: Subscriber>: Subscription
where S.Input == Control, S.Failure == Never {

    private var control: Control?
    private var subscriber: S?
    private let actionTarget = NSControlTarget()

    init(control: Control, subscriber: S) {
        self.control = control
        self.subscriber = subscriber

        actionTarget.handler = { [weak self] in
            guard let self, let control = self.control, let subscriber = self.subscriber else { return }
            _ = subscriber.receive(control)
        }
        control.target = actionTarget
        control.action = #selector(NSControlTarget.action(_:))
    }

    func request(_ demand: Subscribers.Demand) {}

    func cancel() {
        // Only clear the target/action if this subscription is still the installed target.
        // A later subscription may have overwritten our slot; clearing blindly would kill it.
        if control?.target === actionTarget {
            control?.target = nil
            control?.action = nil
        }
        control = nil
        subscriber = nil
    }
}
#endif

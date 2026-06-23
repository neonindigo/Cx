#if canImport(UIKit)
import UIKit
import Combine

private struct UIGestureRecognizerPublisher: Publisher {
    typealias Output = UIGestureRecognizer
    typealias Failure = Never

    let recognizer: UIGestureRecognizer

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == UIGestureRecognizer, S.Failure == Never {
        let subscription = UIGestureRecognizerSubscription(subscriber: subscriber, recognizer: recognizer)
        subscriber.receive(subscription: subscription)
    }
}

private final class UIGestureRecognizerSubscription<S: Subscriber>: NSObject, Subscription
where S.Input == UIGestureRecognizer, S.Failure == Never {
    private var subscriber: S?
    private weak var recognizer: UIGestureRecognizer?

    init(subscriber: S, recognizer: UIGestureRecognizer) {
        self.subscriber = subscriber
        self.recognizer = recognizer
        super.init()
        recognizer.addTarget(self, action: #selector(handleGesture(_:)))
    }

    func request(_ demand: Subscribers.Demand) {}

    func cancel() {
        recognizer?.removeTarget(self, action: #selector(handleGesture(_:)))
        subscriber = nil
    }

    @objc private func handleGesture(_ recognizer: UIGestureRecognizer) {
        guard let subscriber else { return }
        _ = subscriber.receive(recognizer)
    }
}

extension UIGestureRecognizer {
    /// Emits the gesture recognizer each time it fires.
    public var publisher: AnyPublisher<UIGestureRecognizer, Never> {
        UIGestureRecognizerPublisher(recognizer: self).eraseToAnyPublisher()
    }
}
#endif

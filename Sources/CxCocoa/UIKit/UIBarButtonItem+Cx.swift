#if canImport(UIKit)
import UIKit
import Combine

private final class BarButtonItemHandler: NSObject {
    let subject = PassthroughSubject<Void, Never>()

    @objc func handleTap() {
        subject.send()
    }
}

private var barButtonItemHandlerKey: UInt8 = 0

extension UIBarButtonItem {
    /// Emits `Void` each time the bar button item is tapped.
    ///
    /// Uses an associated `BarButtonItemHandler` so multiple subscribers share one target/action pair.
    ///
    /// - Important: Do not mix `tapPublisher` with a manually set `target`/`action` on the same
    ///   bar button item. Accessing this property installs its own target; any prior target/action
    ///   pair will be replaced. If the item was configured with a target/action (e.g. via
    ///   `init(barButtonSystemItem:target:action:)`), remove it before using `tapPublisher`.
    public var tapPublisher: AnyPublisher<Void, Never> {
        let handler: BarButtonItemHandler
        if let existing = objc_getAssociatedObject(self, &barButtonItemHandlerKey) as? BarButtonItemHandler {
            handler = existing
        } else {
            precondition(
                target == nil,
                "UIBarButtonItem already has a target/action set. Remove it before using tapPublisher."
            )
            handler = BarButtonItemHandler()
            objc_setAssociatedObject(self, &barButtonItemHandlerKey, handler, .OBJC_ASSOCIATION_RETAIN_NONATOMIC)
            target = handler
            action = #selector(BarButtonItemHandler.handleTap)
        }
        return handler.subject.eraseToAnyPublisher()
    }
}
#endif

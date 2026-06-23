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
    public var tapPublisher: AnyPublisher<Void, Never> {
        let handler: BarButtonItemHandler
        if let existing = objc_getAssociatedObject(self, &barButtonItemHandlerKey) as? BarButtonItemHandler {
            handler = existing
        } else {
            handler = BarButtonItemHandler()
            objc_setAssociatedObject(self, &barButtonItemHandlerKey, handler, .OBJC_ASSOCIATION_RETAIN_NONATOMIC)
            target = handler
            action = #selector(BarButtonItemHandler.handleTap)
        }
        return handler.subject.eraseToAnyPublisher()
    }
}
#endif

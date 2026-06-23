#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSTextField {
    /// Emits the current string value immediately on subscription, then each time it changes.
    public var textPublisher: AnyPublisher<String, Never> {
        let initial = Deferred { [weak self] in Just(self?.stringValue ?? "") }
        return NotificationCenter.default
            .publisher(for: NSControl.textDidChangeNotification, object: self)
            .compactMap { ($0.object as? NSTextField)?.stringValue }
            .prepend(initial)
            .eraseToAnyPublisher()
    }
}
#endif

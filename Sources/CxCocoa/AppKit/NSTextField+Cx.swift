#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSTextField {
    /// Emits the current string value immediately on subscription, then each time it changes.
    public var textPublisher: AnyPublisher<String, Never> {
        NotificationCenter.default
            .publisher(for: NSControl.textDidChangeNotification, object: self)
            .compactMap { ($0.object as? NSTextField)?.stringValue }
            .prepend(stringValue)
            .eraseToAnyPublisher()
    }
}
#endif

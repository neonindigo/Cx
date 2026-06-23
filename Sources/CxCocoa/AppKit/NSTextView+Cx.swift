#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSTextView {
    /// Emits the current string value immediately on subscription, then each time it changes.
    public var textPublisher: AnyPublisher<String, Never> {
        NotificationCenter.default
            .publisher(for: NSText.didChangeNotification, object: self)
            .compactMap { ($0.object as? NSTextView)?.string }
            .prepend(string)
            .eraseToAnyPublisher()
    }
}
#endif

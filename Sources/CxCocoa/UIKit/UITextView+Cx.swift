#if canImport(UIKit)
import UIKit
import Combine

extension UITextView {
    /// Emits the current text immediately on subscription, then on each change.
    public var textPublisher: AnyPublisher<String, Never> {
        NotificationCenter.default
            .publisher(for: UITextView.textDidChangeNotification, object: self)
            .compactMap { ($0.object as? UITextView)?.text }
            .prepend(text ?? "")
            .eraseToAnyPublisher()
    }
}
#endif

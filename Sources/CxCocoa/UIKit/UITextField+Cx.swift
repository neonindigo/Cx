#if canImport(UIKit)
import UIKit
import Combine

extension UITextField {
    /// Emits the current text immediately on subscription, then on each edit.
    public var textPublisher: AnyPublisher<String, Never> {
        publisher(for: .editingChanged)
            .compactMap { $0.text }
            .prepend(text ?? "")
            .eraseToAnyPublisher()
    }
}
#endif

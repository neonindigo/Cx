#if canImport(UIKit)
import UIKit
import Combine

extension UITextView {
    /// Emits the current text each time it changes.
    public var textPublisher: AnyPublisher<String?, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

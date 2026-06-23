#if canImport(UIKit)
import UIKit
import Combine

extension UIGestureRecognizer {
    /// Emits the gesture recognizer each time it fires.
    public var publisher: AnyPublisher<UIGestureRecognizer, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

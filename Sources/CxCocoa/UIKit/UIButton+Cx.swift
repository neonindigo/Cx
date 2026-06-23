#if canImport(UIKit)
import UIKit
import Combine

extension UIButton {
    /// Emits `Void` each time the button is tapped.
    public var tapPublisher: AnyPublisher<Void, Never> {
        publisher(for: .touchUpInside).map { _ in () }.eraseToAnyPublisher()
    }
}
#endif

#if canImport(UIKit)
import UIKit
import Combine

extension UISlider {
    /// Emits the current value immediately on subscription, then on each change.
    public var valuePublisher: AnyPublisher<Float, Never> {
        publisher(for: .valueChanged).map { $0.value }.prepend(value).eraseToAnyPublisher()
    }
}
#endif

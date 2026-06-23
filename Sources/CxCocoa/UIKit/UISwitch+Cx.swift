#if canImport(UIKit)
import UIKit
import Combine

extension UISwitch {
    /// Emits the current `isOn` state immediately on subscription, then on each change.
    public var isOnPublisher: AnyPublisher<Bool, Never> {
        publisher(for: .valueChanged).map { $0.isOn }.prepend(isOn).eraseToAnyPublisher()
    }
}
#endif

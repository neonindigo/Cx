#if canImport(UIKit)
import UIKit
import Combine

extension UISwitch {
    /// Emits the current `isOn` state each time it changes.
    public var isOnPublisher: AnyPublisher<Bool, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

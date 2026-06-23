#if canImport(UIKit)
import UIKit
import Combine

extension UIControl {
    /// Emits the control itself each time the specified control event fires.
    public func publisher(for events: UIControl.Event) -> AnyPublisher<Self, Never> {
        UIControlPublisher(control: self, events: events).eraseToAnyPublisher()
    }
}
#endif

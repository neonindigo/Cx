#if canImport(UIKit)
import UIKit
import Combine

extension UIControl {
    /// Emits `Void` each time the specified control event fires.
    public func publisher(for events: UIControl.Event) -> AnyPublisher<Void, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

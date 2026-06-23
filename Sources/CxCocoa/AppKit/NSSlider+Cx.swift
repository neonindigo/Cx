#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSlider {
    /// Emits the current double value each time it changes.
    public var valuePublisher: AnyPublisher<Double, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

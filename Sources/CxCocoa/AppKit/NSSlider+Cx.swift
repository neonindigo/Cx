#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSlider {
    /// Emits the current double value immediately on subscription, then each time it changes.
    public var valuePublisher: AnyPublisher<Double, Never> {
        NSControlPublisher(control: self)
            .map { $0.doubleValue }
            .prepend(doubleValue)
            .eraseToAnyPublisher()
    }
}
#endif

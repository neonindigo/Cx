#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSlider {
    /// Emits the current double value immediately on subscription, then each time it changes.
    public var valuePublisher: AnyPublisher<Double, Never> {
        let initial = Deferred { [weak self] in Just(self?.doubleValue ?? 0) }
        return NSControlPublisher(control: self)
            .map { $0.doubleValue }
            .prepend(initial)
            .eraseToAnyPublisher()
    }
}
#endif

#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

@available(macOS 10.15, *)
extension NSSwitch {
    /// Emits the current `NSControl.StateValue` immediately on subscription, then each time it changes.
    /// - Note: Returns the full `StateValue` (`.on`, `.off`, `.mixed`) rather than a `Bool`
    ///   to avoid silent loss of the `.mixed` state.
    public var statePublisher: AnyPublisher<NSControl.StateValue, Never> {
        let initial = Deferred { [weak self] in Just(self?.state ?? .off) }
        return NSControlPublisher(control: self)
            .map { $0.state }
            .prepend(initial)
            .eraseToAnyPublisher()
    }
}
#endif

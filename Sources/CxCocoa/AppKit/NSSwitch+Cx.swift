#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSwitch {
    /// Emits the current `NSControl.StateValue` each time it changes.
    /// - Note: Returns the full `StateValue` (`.on`, `.off`, `.mixed`) rather than a `Bool`
    ///   to avoid silent loss of the `.mixed` state.
    public var statePublisher: AnyPublisher<NSControl.StateValue, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

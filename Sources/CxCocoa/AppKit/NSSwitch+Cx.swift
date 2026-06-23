#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSwitch {
    /// Emits the current state each time it changes.
    public var isOnPublisher: AnyPublisher<Bool, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

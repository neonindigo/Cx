#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSTextField {
    /// Emits the current string value each time it changes.
    public var textPublisher: AnyPublisher<String, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

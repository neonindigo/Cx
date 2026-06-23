#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSButton {
    /// Emits `Void` each time the button is clicked.
    public var tapPublisher: AnyPublisher<Void, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

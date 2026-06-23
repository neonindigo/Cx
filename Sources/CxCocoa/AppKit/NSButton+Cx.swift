#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSButton {
    /// Emits `Void` each time the button is clicked.
    public var tapPublisher: AnyPublisher<Void, Never> {
        NSControlPublisher(control: self)
            .map { _ in () }
            .eraseToAnyPublisher()
    }
}
#endif

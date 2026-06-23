#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSegmentedControl {
    /// Emits the selected segment index each time it changes.
    public var selectedSegmentIndexPublisher: AnyPublisher<Int, Never> {
        // TODO: implement
        fatalError("stub")
    }
}
#endif

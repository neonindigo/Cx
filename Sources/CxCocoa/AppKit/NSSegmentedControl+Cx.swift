#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

extension NSSegmentedControl {
    /// Emits the selected segment index immediately on subscription, then each time it changes.
    public var selectedSegmentIndexPublisher: AnyPublisher<Int, Never> {
        let initial = Deferred { [weak self] in Just(self?.selectedSegment ?? -1) }
        return NSControlPublisher(control: self)
            .map { $0.selectedSegment }
            .prepend(initial)
            .eraseToAnyPublisher()
    }
}
#endif

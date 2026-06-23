#if canImport(UIKit)
import UIKit
import Combine

extension UISegmentedControl {
    /// Emits the selected segment index immediately on subscription, then on each change.
    public var selectedSegmentIndexPublisher: AnyPublisher<Int, Never> {
        publisher(for: .valueChanged)
            .map { $0.selectedSegmentIndex }
            .prepend(selectedSegmentIndex)
            .eraseToAnyPublisher()
    }
}
#endif

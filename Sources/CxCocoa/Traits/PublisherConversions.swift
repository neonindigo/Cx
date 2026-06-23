import Combine
import Foundation

extension Driver {
    /// Converts to Signal (same stream, no replay).
    public func asSignal() -> Signal<Output> {
        Signal(self)
    }
}

extension Signal {
    /// Converts to Driver by providing an initial/default value for replay.
    public func asDriver(initialValue: Output) -> Driver<Output> {
        Driver(self, initialValue: initialValue)
    }
}

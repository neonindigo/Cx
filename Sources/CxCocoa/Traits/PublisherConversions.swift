import Combine
import Foundation

extension Driver {
    /// Converts to Signal (same stream, no replay).
    /// Routes through the Driver's internal non-replaying event subject,
    /// bypassing the CurrentValueSubject to prevent leaking the current value.
    public func asSignal() -> Signal<Output> {
        Signal(asSignalPublisher())
    }
}

extension Signal {
    /// Converts to Driver by providing an initial/default value for replay.
    public func asDriver(initialValue: Output) -> Driver<Output> {
        Driver(self, initialValue: initialValue)
    }
}

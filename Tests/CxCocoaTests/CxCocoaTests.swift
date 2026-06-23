import XCTest
import Combine
import CxCocoa

final class CxCocoaTests: XCTestCase {
    func testPlaceholder() {
        // Placeholder — real tests follow in subsequent waves.
        XCTAssertTrue(true)
    }
}

#if canImport(UIKit)
import UIKit
import Combine

final class UIControlPublisherTests: XCTestCase {
    // 1. publisher(for:) emits the control when sendActions is called
    func testPublisherEmitsOnSendActions() {
        let button = UIButton()
        var received: [UIButton] = []
        let cancellable = button.publisher(for: .touchUpInside).sink { received.append($0) }
        button.sendActions(for: .touchUpInside)
        XCTAssertEqual(received.count, 1)
        XCTAssertTrue(received.first === button)
        cancellable.cancel()
    }

    // 2. Cancel removes the target — no emission after cancel
    func testCancelStopsEmission() {
        let button = UIButton()
        var count = 0
        let cancellable = button.publisher(for: .touchUpInside).sink { _ in count += 1 }
        cancellable.cancel()
        button.sendActions(for: .touchUpInside)
        XCTAssertEqual(count, 0)
    }

    // 3. UIButton.tapPublisher emits Void on touchUpInside
    func testButtonTapPublisher() {
        let button = UIButton()
        var count = 0
        let cancellable = button.tapPublisher.sink { count += 1 }
        button.sendActions(for: .touchUpInside)
        XCTAssertEqual(count, 1)
        cancellable.cancel()
    }

    // 4. UISwitch.isOnPublisher emits initial value synchronously on subscribe
    func testSwitchIsOnPublisherEmitsInitialValue() {
        let sw = UISwitch()
        sw.isOn = true
        var values: [Bool] = []
        let cancellable = sw.isOnPublisher.sink { values.append($0) }
        XCTAssertEqual(values, [true])
        sw.sendActions(for: .valueChanged)
        XCTAssertEqual(values.count, 2)
        cancellable.cancel()
    }

    // 5. UISlider.valuePublisher emits initial value synchronously on subscribe
    func testSliderValuePublisherEmitsInitialValue() {
        let slider = UISlider()
        slider.value = 0.5
        var values: [Float] = []
        let cancellable = slider.valuePublisher.sink { values.append($0) }
        XCTAssertEqual(values, [0.5])
        slider.sendActions(for: .valueChanged)
        XCTAssertEqual(values.count, 2)
        cancellable.cancel()
    }
#if canImport(AppKit) && !canImport(UIKit)
import AppKit
import Combine

final class NSControlPublisherTests: XCTestCase {

    // 1. NSButton.tapPublisher emits when the action fires
    func testButtonTapPublisher() {
        let button = NSButton()
        var taps = 0
        let cancellable = button.tapPublisher.sink { taps += 1 }
        // performClick requires a window hierarchy; use sendAction for headless testing
        _ = NSApplication.shared.sendAction(button.action!, to: button.target, from: button)
        XCTAssertEqual(taps, 1)
        _ = NSApplication.shared.sendAction(button.action!, to: button.target, from: button)
        XCTAssertEqual(taps, 2)
        cancellable.cancel()
    }

    // 2. NSTextField.textPublisher emits initial value on subscription
    func testTextFieldInitialValue() {
        let field = NSTextField()
        field.stringValue = "hello"
        var received: [String] = []
        let cancellable = field.textPublisher.sink { received.append($0) }
        XCTAssertEqual(received.first, "hello")
        cancellable.cancel()
    }

    // 3. NSSlider.valuePublisher emits initial value on subscription
    func testSliderInitialValue() {
        let slider = NSSlider()
        slider.doubleValue = 0.75
        var received: [Double] = []
        let cancellable = slider.valuePublisher.sink { received.append($0) }
        XCTAssertEqual(received.first, 0.75)
        cancellable.cancel()
    }

    // 4. NSSwitch.statePublisher returns NSControl.StateValue (not Bool)
    @available(macOS 10.15, *)
    func testSwitchStatePublisher() {
        let sw = NSSwitch()
        sw.state = .on
        var received: [NSControl.StateValue] = []
        let cancellable = sw.statePublisher.sink { received.append($0) }
        XCTAssertFalse(received.isEmpty, "Should emit initial value")
        XCTAssertEqual(received.first, .on)
        cancellable.cancel()
    }

    // 5. Cancel prevents further emissions
    func testCancelPreventsEmission() {
        let button = NSButton()
        var taps = 0
        let cancellable = button.tapPublisher.sink { taps += 1 }
        // Capture action/target before cancelling
        let action = button.action!
        let target = button.target
        cancellable.cancel()
        // After cancel, action is cleared and subscriber is nil — no emission expected
        _ = NSApplication.shared.sendAction(action, to: target, from: button)
        XCTAssertEqual(taps, 0)
    }
}
#endif
final class DriverTests: XCTestCase {

    // 1. Late subscriber receives the last emitted value
    func testDriverReplaysLastValue() {
        let exp = XCTestExpectation(description: "Late subscriber receives last emitted value")
        let subject = PassthroughSubject<Int, Never>()
        let driver = subject.asDriver()
        var cancellable: AnyCancellable?

        subject.send(42)

        // Wait for 42 to propagate into the Driver's CurrentValueSubject on the main queue,
        // then subscribe — driver must replay 42 to the late subscriber.
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
            cancellable = driver.sink { value in
                XCTAssertEqual(value, 42)
                exp.fulfill()
            }
        }

        wait(for: [exp], timeout: 2.0)
        _ = cancellable
    }

    // 2. All values are delivered on the main thread
    func testDriverDeliversOnMainThread() {
        let exp = XCTestExpectation(description: "Driver delivers on main thread")
        let subject = PassthroughSubject<Int, Never>()
        let driver = subject.asDriver()

        let cancellable = driver.sink { _ in
            XCTAssertTrue(Thread.isMainThread, "Driver must deliver on the main thread")
            exp.fulfill()
        }

        DispatchQueue.global().async { subject.send(1) }

        wait(for: [exp], timeout: 2.0)
        _ = cancellable
    }

    // 3. Multiple subscribers all receive values
    func testDriverMultipleSubscribers() {
        let exp1 = XCTestExpectation(description: "Subscriber 1 receives value")
        let exp2 = XCTestExpectation(description: "Subscriber 2 receives value")
        let subject = PassthroughSubject<Int, Never>()
        let driver = subject.asDriver()

        let c1 = driver.sink { v in if v == 99 { exp1.fulfill() } }
        let c2 = driver.sink { v in if v == 99 { exp2.fulfill() } }

        subject.send(99)

        wait(for: [exp1, exp2], timeout: 2.0)
        _ = (c1, c2)
    }

    // 4. asDriver() converts a plain Publisher
    func testAsDriverExtension() {
        let exp = XCTestExpectation(description: "asDriver delivers value")
        let subject = PassthroughSubject<String, Never>()
        let driver = subject.asDriver()

        let cancellable = driver.sink { value in
            XCTAssertEqual(value, "hello")
            exp.fulfill()
        }

        subject.send("hello")

        wait(for: [exp], timeout: 2.0)
        _ = cancellable
    }
}

final class SignalTests: XCTestCase {

    // 1. Late subscriber does NOT receive previous values
    func testSignalNoReplay() {
        let exp1 = XCTestExpectation(description: "Subscriber 1 gets value 1")
        let exp2 = XCTestExpectation(description: "Subscriber 2 gets value 2")
        let subject = PassthroughSubject<Int, Never>()
        let signal = subject.asSignal()

        var values2: [Int] = []
        var c1: AnyCancellable?
        var c2: AnyCancellable?

        c1 = signal.sink { v in if v == 1 { exp1.fulfill() } }
        subject.send(1)

        // Wait for sub1 to receive value 1, then subscribe sub2
        wait(for: [exp1], timeout: 2.0)

        c2 = signal.sink { v in
            values2.append(v)
            if v == 2 { exp2.fulfill() }
        }
        subject.send(2)

        wait(for: [exp2], timeout: 2.0)

        XCTAssertFalse(values2.contains(1), "Late subscriber must not receive previously emitted values")
        XCTAssertTrue(values2.contains(2), "Late subscriber must receive future values")
        _ = (c1, c2)
    }

    // 2. Values are delivered on the main thread
    func testSignalDeliversOnMainThread() {
        let exp = XCTestExpectation(description: "Signal delivers on main thread")
        let subject = PassthroughSubject<Int, Never>()
        let signal = subject.asSignal()

        let cancellable = signal.sink { _ in
            XCTAssertTrue(Thread.isMainThread, "Signal must deliver on the main thread")
            exp.fulfill()
        }

        DispatchQueue.global().async { subject.send(1) }

        wait(for: [exp], timeout: 2.0)
        _ = cancellable
    }

    // 3. asSignal() converts a plain Publisher
    func testAsSignalExtension() {
        let exp = XCTestExpectation(description: "asSignal delivers value")
        let subject = PassthroughSubject<String, Never>()
        let signal = subject.asSignal()

        let cancellable = signal.sink { value in
            XCTAssertEqual(value, "world")
            exp.fulfill()
        }

        subject.send("world")

        wait(for: [exp], timeout: 2.0)
        _ = cancellable
    }
}


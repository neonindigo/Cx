import XCTest
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


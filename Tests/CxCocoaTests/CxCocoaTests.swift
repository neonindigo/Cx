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
}
#endif


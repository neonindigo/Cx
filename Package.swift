// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "Cx",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [
        .library(name: "Cx", targets: ["Cx"]),
        .library(name: "CxCocoa", targets: ["CxCocoa"]),
        .library(name: "CxTest", targets: ["CxTest"]),
    ],
    dependencies: [
        .package(url: "https://github.com/pointfreeco/combine-schedulers", from: "1.0.0"),
    ],
    targets: [
        .target(name: "Cx", dependencies: []),
        .target(name: "CxCocoa", dependencies: ["Cx"]),
        .target(name: "CxTest", dependencies: [
            "Cx",
            .product(name: "CombineSchedulers", package: "combine-schedulers"),
        ]),
        .testTarget(name: "CxTests", dependencies: ["Cx"]),
        .testTarget(name: "CxCocoaTests", dependencies: ["CxCocoa"]),
        .testTarget(name: "CxTestTests", dependencies: ["CxTest"]),
    ]
)

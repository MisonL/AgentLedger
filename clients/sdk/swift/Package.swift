// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "AgentLedgerSDK",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "AgentLedgerSDK", targets: ["AgentLedgerSDK"])
    ],
    targets: [
        .target(name: "AgentLedgerSDK", path: "Sources/AgentLedgerSDK")
    ]
)

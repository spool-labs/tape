//! Basic end-to-end flow tests.
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! ```bash
//! cargo test -p tape-e2e --test basic_flow -- --ignored --nocapture
//! ```

use serial_test::serial;
use tape_e2e::TestContext;

/// Test system initialization.
///
/// Verifies that the system can be initialized and basic
/// account queries work.
#[tokio::test]
#[ignore]
#[serial]
async fn test_system_init() {

    // Setup without nodes - just validator and system init
    let ctx = TestContext::builder()
        .nodes(0)
        .build()
        .await
        .expect("Failed to setup test context");

    // Query system state
    let system = ctx.system().await.expect("Failed to get system account");
    assert_eq!(system.total_nodes, 0);

    // Query epoch state
    let epoch = ctx.epoch().await.expect("Failed to get epoch account");
    // Epoch ID is always set now - just check it's valid
    assert!(epoch.id.as_u64() >= 1, "Expected epoch ID to be set");
    assert!(epoch.state.is_active(), "Expected epoch to be in Active phase");

    println!("System initialized successfully");
}

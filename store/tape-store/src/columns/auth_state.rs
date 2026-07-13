//! Write-authorization control-state column family (singleton).

use store::Column;

use crate::types::{AuthState, UnitKey};

/// Durable write-authorization control state: the global kill switch, the policy
/// version, and an optional default-budget override.
pub struct AuthStateCol;

impl Column for AuthStateCol {
    const CF_NAME: &'static str = "auth_state";
    type Key = UnitKey;
    type Value = AuthState;
}

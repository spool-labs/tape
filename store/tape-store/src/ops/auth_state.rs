//! Write-authorization control-state operations.

use store::Store;

use crate::columns::AuthStateCol;
use crate::error::Result;
use crate::types::{AuthState, BudgetLimits, UnitKey};
use crate::TapeStore;

/// Operations for the durable write-authorization control state
pub trait AuthStateOps {
    /// Read the control state, defaulting to the empty control state when unset
    fn get_auth_state(&self) -> Result<AuthState>;

    /// Engage or release the global write kill switch
    fn set_kill_switch(&self, is_engaged: bool) -> Result<()>;

    /// Whether the global write kill switch is currently engaged
    fn is_write_killed(&self) -> Result<bool>;

    /// Bump the policy version, called on every policy mutation
    fn bump_policy_version(&self) -> Result<u64>;

    /// Set the operator default-budget override
    fn set_default_budget(&self, budget: BudgetLimits) -> Result<()>;
}

impl<Backend: Store> AuthStateOps for TapeStore<Backend> {
    fn get_auth_state(&self) -> Result<AuthState> {
        Ok(self.get::<AuthStateCol>(&UnitKey)?.unwrap_or_default())
    }

    fn set_kill_switch(&self, is_engaged: bool) -> Result<()> {
        let mut state = self.get_auth_state()?;
        state.is_kill_switch_engaged = is_engaged;
        self.put::<AuthStateCol>(&UnitKey, &state)?;
        Ok(())
    }

    fn is_write_killed(&self) -> Result<bool> {
        Ok(self.get_auth_state()?.is_kill_switch_engaged)
    }

    fn bump_policy_version(&self) -> Result<u64> {
        let mut state = self.get_auth_state()?;
        state.policy_version = state.policy_version.saturating_add(1);
        let version = state.policy_version;
        self.put::<AuthStateCol>(&UnitKey, &state)?;
        Ok(version)
    }

    fn set_default_budget(&self, budget: BudgetLimits) -> Result<()> {
        let mut state = self.get_auth_state()?;
        state.default_budget = Some(budget);
        self.put::<AuthStateCol>(&UnitKey, &state)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;

    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    // an unset state reads as the default
    #[test]
    fn defaults() {
        let s = store();
        assert_eq!(s.get_auth_state().expect("get state"), AuthState::default());
        assert!(!s.is_write_killed().expect("read kill switch"));
    }

    // the kill switch persists across writes
    #[test]
    fn kill_switch() {
        let s = store();
        s.set_kill_switch(true).expect("set kill switch");
        assert!(s.is_write_killed().expect("read kill switch"));
        s.set_kill_switch(false).expect("set kill switch");
        assert!(!s.is_write_killed().expect("read kill switch"));
    }

    // policy version is monotonic and independent of the kill switch
    #[test]
    fn policy_version() {
        let s = store();
        s.set_kill_switch(true).expect("set kill switch");
        assert_eq!(s.bump_policy_version().expect("bump version"), 1);
        assert_eq!(s.bump_policy_version().expect("bump version"), 2);
        // Bumping the version must not disturb the kill switch.
        assert!(s.is_write_killed().expect("read kill switch"));
        assert_eq!(s.get_auth_state().expect("get state").policy_version, 2);
    }

    // the default-budget override persists
    #[test]
    fn default_budget() {
        let s = store();
        let budget = BudgetLimits {
            sol_per_day: 1,
            bytes_per_day: 2,
            puts_per_hour: 3,
            max_concurrent_multipart: 4,
        };
        s.set_default_budget(budget).expect("set budget");
        assert_eq!(
            s.get_auth_state().expect("get state").default_budget,
            Some(budget)
        );
    }
}

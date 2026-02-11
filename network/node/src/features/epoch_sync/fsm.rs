//! Node-specific FSM extension.
//!
//! This module extends the base FSM from `tape_api::fsm` with node-specific
//! states that the on-chain state machine doesn't model, such as:
//! - Catch-up detection (processing historical blocks)
//! - Local spool sync tracking
//! - Recovery operations

use tape_api::fsm::NodeAction as BaseNodeAction;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;

/// Extended node action that includes node-specific states.
///
/// This wraps the base FSM action with additional states that are
/// specific to the node's runtime behavior, not modeled on-chain.
#[derive(Debug, Clone)]
pub enum LocalNodeAction {
    /// Action from the base FSM (on-chain state machine).
    Base(BaseNodeAction),

    /// Node is catching up on historical blocks.
    /// Should skip transaction submissions until caught up.
    CatchingUp {
        current_epoch: EpochNumber,
        chain_epoch: EpochNumber,
    },

    /// Node needs to sync spools from previous owners.
    SyncSpools {
        gained: Vec<SpoolIndex>,
        lost: Vec<SpoolIndex>,
    },

    /// Waiting for local spool sync to complete before submitting SyncEpoch.
    WaitForLocalSync { epoch: EpochNumber },

    /// Spool sync failed, queued for erasure recovery.
    RecoveryQueued { spool_idx: SpoolIndex },
}

impl LocalNodeAction {
    /// Create an extended action from a base action.
    pub fn from_base(action: BaseNodeAction) -> Self {
        LocalNodeAction::Base(action)
    }

    /// Returns true if this action requires submitting a transaction.
    pub fn requires_transaction(&self) -> bool {
        match self {
            LocalNodeAction::Base(base) => base.requires_transaction(),
            _ => false,
        }
    }

    /// Returns true if the node is in catch-up mode.
    pub fn is_catching_up(&self) -> bool {
        matches!(self, LocalNodeAction::CatchingUp { .. })
    }

    /// Returns true if the node is waiting for something.
    pub fn is_waiting(&self) -> bool {
        match self {
            LocalNodeAction::Base(base) => base.is_waiting(),
            LocalNodeAction::WaitForLocalSync { .. } => true,
            LocalNodeAction::CatchingUp { .. } => true,
            _ => false,
        }
    }

    /// Returns true if the node is blocked and cannot proceed.
    pub fn is_blocked(&self) -> bool {
        match self {
            LocalNodeAction::Base(base) => base.is_blocked(),
            _ => false,
        }
    }

    /// Get the underlying base action if this is a Base variant.
    pub fn as_base(&self) -> Option<&BaseNodeAction> {
        match self {
            LocalNodeAction::Base(base) => Some(base),
            _ => None,
        }
    }
}

impl From<BaseNodeAction> for LocalNodeAction {
    fn from(action: BaseNodeAction) -> Self {
        LocalNodeAction::Base(action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::fsm::NodeAction;

    #[test]
    fn test_from_base() {
        let base = NodeAction::SyncEpoch;
        let extended = LocalNodeAction::from_base(base.clone());

        assert!(extended.requires_transaction());
        assert!(!extended.is_catching_up());
        assert!(!extended.is_waiting());
        assert!(!extended.is_blocked());
    }

    #[test]
    fn test_catching_up() {
        let action = LocalNodeAction::CatchingUp {
            current_epoch: EpochNumber(5),
            chain_epoch: EpochNumber(10),
        };

        assert!(!action.requires_transaction());
        assert!(action.is_catching_up());
        assert!(action.is_waiting());
        assert!(!action.is_blocked());
    }

    #[test]
    fn test_wait_for_local_sync() {
        let action = LocalNodeAction::WaitForLocalSync {
            epoch: EpochNumber(5),
        };

        assert!(!action.requires_transaction());
        assert!(!action.is_catching_up());
        assert!(action.is_waiting());
        assert!(!action.is_blocked());
    }

    #[test]
    fn test_as_base() {
        let base = NodeAction::AdvanceEpoch;
        let extended = LocalNodeAction::from_base(base.clone());

        assert!(extended.as_base().is_some());
        assert_eq!(*extended.as_base().unwrap(), NodeAction::AdvanceEpoch);

        let catching_up = LocalNodeAction::CatchingUp {
            current_epoch: EpochNumber(5),
            chain_epoch: EpochNumber(10),
        };
        assert!(catching_up.as_base().is_none());
    }
}

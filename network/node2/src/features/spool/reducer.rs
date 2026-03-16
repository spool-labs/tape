use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::system::SpoolState;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;

use crate::core::config::SpoolManagerConfig;
use crate::core::error::NodeError;
use crate::features::spool::fsm::{SpoolTransition, apply};
use crate::features::spool::types::SpoolEvent;
use crate::features::state::cleanup::purge_spool_local;

pub fn apply_event<Db: Store>(
    store: &TapeStore<Db>,
    config: &SpoolManagerConfig,
    event: &SpoolEvent,
) -> Result<(), NodeError> {
    let spool_id = spool_id(event);
    let current = store.get_spool_state(spool_id).map_err(store_error)?;

    if summary_is_stale(current, event) {
        return Ok(());
    }

    if let SpoolEvent::MissingCertifiedSlice { track, .. } = event {
        if matches!(
            current,
            Some(state) if state.is_active() || state.is_recovering()
        ) {
            store
                .add_pending_recovery(spool_id, *track)
                .map_err(store_error)?;
        }
    }

    let transition = apply(current, event, config.locked_spool_retention_epochs);
    persist_transition(store, spool_id, current, transition)
}

fn persist_transition<Db: Store>(
    store: &TapeStore<Db>,
    spool_id: SpoolIndex,
    current: Option<SpoolState>,
    transition: SpoolTransition,
) -> Result<(), NodeError> {
    if transition.clear_pending_recoveries {
        store
            .clear_all_pending_recoveries(spool_id)
            .map_err(store_error)?;
    }

    if transition.clear_sync_cursor {
        store
            .remove_spool_sync_cursor(spool_id)
            .map_err(store_error)?;
    }

    if transition.purge_local {
        purge_spool_local(store, spool_id)?;
        return Ok(());
    }

    match (current, transition.next_state) {
        (Some(previous), Some(next)) if previous == next => Ok(()),
        (_, Some(next)) => store.set_spool_state(spool_id, next).map_err(store_error),
        (Some(_), None) => store.remove_spool_state(spool_id).map_err(store_error),
        (None, None) => Ok(()),
    }
}

fn summary_is_stale(
    current: Option<SpoolState>,
    event: &SpoolEvent,
) -> bool {
    let SpoolEvent::TaskSummary { work, .. } = event else {
        return false;
    };

    match current {
        Some(state) => state.epoch != work.epoch,
        None => true,
    }
}

fn spool_id(event: &SpoolEvent) -> SpoolIndex {
    match event {
        SpoolEvent::EpochReconcile { spool_id, .. } => *spool_id,
        SpoolEvent::TaskSummary { work, .. } => work.spool_id,
        SpoolEvent::MissingCertifiedSlice { spool_id, .. } => *spool_id,
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::types::EpochNumber;
    use tape_store::ops::{SliceOps, SpoolOps};
    use tape_store::types::Pubkey;
    use tape_store::TapeStore;

    use super::apply_event;
    use crate::core::config::AppConfig;
    use crate::core::config::NodeConfig;
    use crate::features::spool::types::{SpoolEvent, SpoolTaskKind, SpoolTaskSummary, SpoolWorkItem};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn test_config() -> crate::core::config::SpoolManagerConfig {
        AppConfig::production(NodeConfig {
            node_keypair: String::new(),
            bls_keypair: std::path::PathBuf::new(),
            rpc_url: "http://localhost".to_string(),
            storage_path: "/tmp".to_string(),
            start_slot: tape_core::types::SlotNumber(1),
        })
        .unwrap()
        .spool
    }

    #[test]
    fn stale_task_summary_is_ignored() {
        let store = test_store();
        let config = test_config();

        store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Recover, EpochNumber(9)))
            .unwrap();

        apply_event(
            &store,
            &config,
            &SpoolEvent::TaskSummary {
                work: SpoolWorkItem {
                    spool_id: 5,
                    epoch: EpochNumber(8),
                    kind: SpoolTaskKind::Recover,
                },
                summary: SpoolTaskSummary::RecoverDone { remaining: 0 },
            },
        )
        .unwrap();

        assert_eq!(
            store.get_spool_state(5).unwrap(),
            Some(SpoolState::new(SpoolStatus::Recover, EpochNumber(9)))
        );
    }

    #[test]
    fn locked_retention_purges_local_spool_data() {
        let store = test_store();
        let config = test_config();
        let track = Pubkey::new_unique();

        store
            .set_spool_state(5, SpoolState::new(SpoolStatus::LockedToMove, EpochNumber(1)))
            .unwrap();
        store.put_slice(5, track, vec![1, 2, 3]).unwrap();

        apply_event(
            &store,
            &config,
            &SpoolEvent::EpochReconcile {
                spool_id: 5,
                epoch: EpochNumber(5),
                owned: false,
                prev_owner: None,
                prev_helpers: [None; tape_core::erasure::SPOOL_GROUP_SIZE],
            },
        )
        .unwrap();

        assert!(store.get_spool_state(5).unwrap().is_none());
        assert!(store.get_slice(5, track).unwrap().is_none());
    }
}

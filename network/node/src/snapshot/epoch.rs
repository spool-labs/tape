use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_store::ops::MetaOps;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

pub enum SnapshotNeed {
    AllowMissing,
    RequireBuild,
    RequireCertify,
    RequireRegister,
}

pub fn snapshot_ready(epoch: EpochNumber) -> bool {
    epoch.0 >= 2
}

pub fn snapshot_target(epoch: EpochNumber) -> Option<EpochNumber> {
    if snapshot_ready(epoch) {
        Some(EpochNumber(epoch.0 - 1))
    } else {
        None
    }
}

pub fn snapshot_epochs<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    need: SnapshotNeed,
) -> Result<(EpochNumber, EpochNumber), TaskOutcome> {
    let current = match context.store.get_chain_epoch() {
        Ok(Some(epoch)) => epoch,
        Ok(None) => return Err(TaskOutcome::Retryable("missing chain epoch".into())),
        Err(e) => return Err(TaskOutcome::Retryable(format!("read chain epoch: {e}"))),
    };

    let target = match snapshot_target(current) {
        Some(target) => target,
        None => {
            return Err(match need {
                SnapshotNeed::AllowMissing => TaskOutcome::Success,
                SnapshotNeed::RequireBuild => {
                    TaskOutcome::Retryable("build target not ready".into())
                }
                SnapshotNeed::RequireCertify => {
                    TaskOutcome::Retryable("certify target not ready".into())
                }
                SnapshotNeed::RequireRegister => {
                    TaskOutcome::Retryable("register target not ready".into())
                }
            });
        }
    };

    Ok((current, target))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_false() {
        assert!(!snapshot_ready(EpochNumber(0)));
        assert!(!snapshot_ready(EpochNumber(1)));
    }

    #[test]
    fn ready_true() {
        assert!(snapshot_ready(EpochNumber(2)));
    }

    #[test]
    fn target_none() {
        assert_eq!(snapshot_target(EpochNumber(1)), None);
    }

    #[test]
    fn target_some() {
        assert_eq!(snapshot_target(EpochNumber(3)), Some(EpochNumber(2)));
    }
}

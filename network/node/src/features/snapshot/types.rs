use std::sync::Arc;

use tape_blocks::ParsedBlock;
use tape_protocol::ProtocolState;

#[derive(Debug, Clone)]
pub enum SnapshotManagerInput {
    State(Arc<ProtocolState>),
    Block(Arc<ParsedBlock>),
}

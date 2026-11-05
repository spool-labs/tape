use tape_core::types::EpochNumber;
use tape_crypto::address::Address;

/// Who owned one virtual group's positions over the kept epochs
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerHistory {
    /// The epoch the history was read at
    pub now: EpochNumber,
    /// The recorded epochs, newest first; an epoch the node missed is left out
    pub epochs: Vec<EpochOwners>,
}

/// One recorded epoch's owners, by position
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochOwners {
    /// The epoch they owned the spools in
    pub epoch: EpochNumber,
    /// Each position's owner, zero where nobody holds it
    pub owners: Vec<Address>,
}

/// A past owner of a spool, told apart by the epoch it owned the spool in
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Holder {
    /// The holder's address
    pub node: Address,
    /// The newest epoch it owned the spool in
    pub epoch: EpochNumber,
}

/// A peer at one position of the spool_group, a repair's helper or a
/// recovery's source
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositionPeer {
    /// The position in the spool_group
    pub position: usize,
    /// The peer's address
    pub peer: Address,
}

/// The holders of a position: its owners of the epochs before now, newest
/// first, each node once at its newest epoch, our node left out
pub fn get_holders(
    history: &OwnerHistory,
    position: usize,
    me: Address,
) -> Vec<Holder> {
    let mut holders: Vec<Holder> = Vec::new();
    for owners in &history.epochs {
        if owners.epoch >= history.now {
            continue;
        }
        let Some(node) = get_owner_at(owners, position, me) else {
            continue;
        };
        if !has_holder(&holders, node) {
            holders.push(Holder { node, epoch: owners.epoch });
        }
    }
    holders
}

/// The peers of a position: its owner now, then its holders
pub fn get_position_peers(
    history: &OwnerHistory,
    position: usize,
    me: Address,
) -> Vec<Address> {
    let mut peers = Vec::new();
    if let Some(owner) = get_owner_now(history, position, me) {
        peers.push(owner);
    }
    for holder in get_holders(history, position, me) {
        push_once(&mut peers, holder.node);
    }
    peers
}

/// The peers of every other position of the spool_group, by position
pub fn get_group_peers(
    history: &OwnerHistory,
    ours: usize,
    me: Address,
) -> Vec<PositionPeer> {
    let mut group_peers = Vec::new();
    for position in 0..get_width(history) {
        if position == ours {
            continue;
        }
        for peer in get_position_peers(history, position, me) {
            group_peers.push(PositionPeer { position, peer });
        }
    }
    group_peers
}

/// The sync_peers of a spool: every node in the spool_group now, then the
/// spool's holders
pub fn get_sync_peers(
    history: &OwnerHistory,
    position: usize,
    me: Address,
) -> Vec<Address> {
    let mut peers = Vec::new();
    for other in 0..get_width(history) {
        if let Some(owner) = get_owner_now(history, other, me) {
            push_once(&mut peers, owner);
        }
    }
    for holder in get_holders(history, position, me) {
        push_once(&mut peers, holder.node);
    }
    peers
}

fn get_owner_at(
    owners: &EpochOwners,
    position: usize,
    me: Address,
) -> Option<Address> {
    let node = *owners.owners.get(position)?;
    (node != Address::default() && node != me).then_some(node)
}

fn has_holder(holders: &[Holder], node: Address) -> bool {
    for holder in holders {
        if holder.node == node {
            return true;
        }
    }
    false
}

fn get_owner_now(
    history: &OwnerHistory,
    position: usize,
    me: Address,
) -> Option<Address> {
    let owners = history.epochs.first()?;
    if owners.epoch != history.now {
        return None;
    }
    get_owner_at(owners, position, me)
}

fn push_once(peers: &mut Vec<Address>, peer: Address) {
    if !peers.contains(&peer) {
        peers.push(peer);
    }
}

fn get_width(history: &OwnerHistory) -> usize {
    match history.epochs.first() {
        Some(owners) => owners.owners.len(),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn history(now: u64, rows: &[(u64, [u8; 3])]) -> OwnerHistory {
        let mut epochs = Vec::new();
        for (epoch, bytes) in rows {
            let mut owners = Vec::new();
            for byte in bytes {
                owners.push(address(*byte));
            }
            epochs.push(EpochOwners { epoch: EpochNumber(*epoch), owners });
        }
        OwnerHistory { now: EpochNumber(now), epochs }
    }

    // holders come newest first, once each, without our node or the owner now
    #[test]
    fn holder_order() {
        let me = address(9);
        let rows = [
            (10, [9, 1, 1]),
            (9, [2, 1, 1]),
            (8, [9, 1, 1]),
            (7, [3, 1, 1]),
            (6, [2, 1, 1]),
        ];
        let history = history(10, &rows);

        let holders = get_holders(&history, 0, me);

        let expected = vec![
            Holder { node: address(2), epoch: EpochNumber(9) },
            Holder { node: address(3), epoch: EpochNumber(7) },
        ];
        assert_eq!(holders, expected);
    }

    // sync_peers holds the group now, then the spool's holders, and the group
    // peers are every other position's owner now and holders
    #[test]
    fn sync_list() {
        let me = address(9);
        let history = history(10, &[(10, [9, 4, 5]), (9, [2, 4, 6])]);

        let peers = get_sync_peers(&history, 0, me);
        let others = get_position_peers(&history, 1, me);
        let group = get_group_peers(&history, 0, me);

        assert_eq!(peers, vec![address(4), address(5), address(2)]);
        assert_eq!(others, vec![address(4)]);
        let expected = vec![
            PositionPeer { position: 1, peer: address(4) },
            PositionPeer { position: 2, peer: address(5) },
            PositionPeer { position: 2, peer: address(6) },
        ];
        assert_eq!(group, expected);
    }
}

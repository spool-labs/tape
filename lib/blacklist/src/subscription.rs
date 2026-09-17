//! What a consumer follows: lists by rack tape, and the actions honored from each

use std::collections::HashSet;

use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer};
use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;

use crate::error::BlacklistError;

/// An action a consumer can honor from a list
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HonoredAction {
    /// Deny tracks the list removes
    Remove,
    /// Deny tapes the list unlists
    Unlist,
}

/// One followed list
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Subscription {
    pub name: String,
    #[serde(deserialize_with = "deserialize_address")]
    pub tape: Address,
    pub actions: Vec<HonoredAction>,
}

impl Subscription {
    /// Whether this subscription denies a target its list denies
    pub fn honors(&self, target: &BlacklistEntry) -> bool {
        let needed = match (target.is_track(), target.is_tape()) {
            (true, _) => HonoredAction::Remove,
            (false, true) => HonoredAction::Unlist,
            (false, false) => return false,
        };
        self.actions.contains(&needed)
    }
}

/// Check a consumer's subscriptions: unique names and tapes, each honoring an action
pub fn validate(subscriptions: &[Subscription]) -> Result<(), BlacklistError> {
    let mut names = HashSet::new();
    let mut tapes = HashSet::new();
    for subscription in subscriptions {
        if !names.insert(subscription.name.as_str()) {
            return Err(BlacklistError::DuplicateName(subscription.name.clone()));
        }
        if !tapes.insert(subscription.tape) {
            return Err(BlacklistError::DuplicateTape(subscription.tape));
        }
        if subscription.actions.is_empty() {
            return Err(BlacklistError::NoActions(subscription.name.clone()));
        }
    }
    Ok(())
}

fn deserialize_address<'de, D>(deserializer: D) -> Result<Address, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value
        .parse()
        .map_err(|_| D::Error::custom(format!("`{value}` is not an address")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Config {
        blacklists: Vec<Subscription>,
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn subscription(name: &str, tape: Address, actions: Vec<HonoredAction>) -> Subscription {
        Subscription {
            name: name.to_string(),
            tape,
            actions,
        }
    }

    // the documented yaml shape loads with base58 tapes
    #[test]
    fn yaml_shape() {
        let yaml = format!(
            "blacklists:\n  - name: tapedrive-moderation\n    tape: {}\n    actions: [remove, unlist]\n",
            address(7)
        );

        let config: Config = serde_yaml::from_str(&yaml).expect("parse");

        assert_eq!(
            config.blacklists,
            vec![subscription(
                "tapedrive-moderation",
                address(7),
                vec![HonoredAction::Remove, HonoredAction::Unlist]
            )]
        );
    }

    // a tape that is not an address fails to load
    #[test]
    fn bad_tape() {
        let yaml = "blacklists:\n  - name: a\n    tape: nope\n    actions: [remove]\n";

        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }

    // a subscription honors only the kinds its actions cover
    #[test]
    fn honored_kinds() {
        let removals_only = subscription("a", address(1), vec![HonoredAction::Remove]);

        assert!(removals_only.honors(&BlacklistEntry::track(address(2))));
        assert!(!removals_only.honors(&BlacklistEntry::tape(address(3))));
    }

    // repeated names or tapes and empty actions are refused
    #[test]
    fn invalid_sets() {
        let first = subscription("a", address(1), vec![HonoredAction::Remove]);

        assert_eq!(
            validate(&[first.clone(), subscription("a", address(2), vec![HonoredAction::Remove])]),
            Err(BlacklistError::DuplicateName("a".to_string()))
        );
        assert_eq!(
            validate(&[first.clone(), subscription("b", address(1), vec![HonoredAction::Remove])]),
            Err(BlacklistError::DuplicateTape(address(1)))
        );
        assert_eq!(
            validate(&[subscription("c", address(3), Vec::new())]),
            Err(BlacklistError::NoActions("c".to_string()))
        );
        assert_eq!(validate(&[first]), Ok(()));
    }
}

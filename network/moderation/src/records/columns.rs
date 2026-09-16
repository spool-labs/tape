//! Parsing text columns back into typed values

use std::str::FromStr;

use tape_blacklist::entry::{Action, Entry};
use tape_crypto::address::Address;

use crate::records::error::RecordsError;
use crate::types::target::{Target, TargetKind};

pub fn address(column: &'static str, value: &str) -> Result<Address, RecordsError> {
    Address::from_str(value).map_err(|_| corrupt(column, value))
}

pub fn optional_address(
    column: &'static str,
    value: Option<String>,
) -> Result<Option<Address>, RecordsError> {
    match value {
        Some(value) => address(column, &value).map(Some),
        None => Ok(None),
    }
}

pub fn target(kind: &str, value: &str) -> Result<Target, RecordsError> {
    let kind = TargetKind::parse(kind).ok_or_else(|| corrupt("target_kind", kind))?;
    Ok(Target {
        kind,
        address: address("target", value)?,
    })
}

pub fn entry(action: &str, kind: &str, value: &str) -> Result<Entry, RecordsError> {
    let action = Action::parse(action).ok_or_else(|| corrupt("action", action))?;
    let entry = Entry {
        action,
        target: target(kind, value)?.to_blacklist_entry(),
    };
    match entry.is_valid() {
        true => Ok(entry),
        false => Err(RecordsError::InvalidEntry),
    }
}

pub fn parsed<Value>(
    column: &'static str,
    value: &str,
    parse: fn(&str) -> Option<Value>,
) -> Result<Value, RecordsError> {
    parse(value).ok_or_else(|| corrupt(column, value))
}

fn corrupt(column: &'static str, value: &str) -> RecordsError {
    RecordsError::Corrupt {
        column,
        value: value.to_string(),
    }
}

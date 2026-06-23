use tape_core::types::{ContentType, SlotNumber, StorageUnits, TrackNumber};
use tape_crypto::Hash;
use tape_crypto::prelude::Address;

const OBJECT_LIST_PAGE_LIMIT: u32 = 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: Hash,
    pub content_type: ContentType,
    pub block_time: Option<i64>,
    pub slot: SlotNumber,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedObject {
    pub name: Vec<u8>,
    pub size: StorageUnits,
    pub etag: Hash,
    pub block_time: Option<i64>,
    pub slot: SlotNumber,
    pub data_tape: Address,
    pub track_number: TrackNumber,
    pub kind: u64,
    pub content_type: ContentType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectListPage {
    pub objects: Vec<ListedObject>,
    pub common_prefixes: Vec<Vec<u8>>,
    pub next_cursor: Option<Vec<u8>>,
    pub is_truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListObjectsQuery {
    pub prefix: Vec<u8>,
    pub delimiter: Option<Vec<u8>>,
    pub cursor: Option<Vec<u8>>,
    pub limit: u32,
}

impl ListObjectsQuery {
    pub fn new(prefix: impl AsRef<[u8]>) -> Self {
        Self {
            prefix: prefix.as_ref().to_vec(),
            ..Self::default()
        }
    }

    pub fn with_delimiter(mut self, delimiter: impl AsRef<[u8]>) -> Self {
        self.delimiter = Some(delimiter.as_ref().to_vec());
        self
    }

    pub fn with_cursor(mut self, cursor: impl Into<Vec<u8>>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }
}

impl Default for ListObjectsQuery {
    fn default() -> Self {
        Self {
            prefix: Vec::new(),
            delimiter: None,
            cursor: None,
            limit: OBJECT_LIST_PAGE_LIMIT,
        }
    }
}

impl From<tape_protocol::api::ObjectListItem> for ListedObject {
    fn from(value: tape_protocol::api::ObjectListItem) -> Self {
        Self {
            name: value.name,
            size: value.size,
            etag: value.etag,
            block_time: value.block_time,
            slot: value.slot,
            data_tape: value.data_tape,
            track_number: value.track_number,
            kind: value.kind,
            content_type: value.content_type,
        }
    }
}

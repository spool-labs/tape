use rpc::Rpc;
use tape_api::compute::TRACK_WRITE_CU;
use tape_api::instruction::build_delete_track_ix;
use tape_api::program::tapedrive::track_pda;
use tape_core::prelude::CompressedTrack;
use tape_core::types::{ContentType, SlotNumber, StorageUnits, TrackNumber};
use tape_crypto::hash::hash;
use tape_crypto::prelude::Address;
use tape_crypto::Hash;
use tape_protocol::api::{ApiError, FindTrackVersion, ListObjectsReq, ListObjectsRes};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::stream::manifest::{ChunkManifest, CHUNK_SIZE};
use crate::tapedrive::Tapedrive;
use crate::track::queryable_peers;

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

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {

    /// Write a named object into a bucket.
    pub async fn put_object(
        &self,
        bucket: &TapeKey,
        name: &str,
        data: &[u8],
        content_type: Option<&str>,
    ) -> Result<Address, TapedriveError> {
        let content_type = content_type
            .map(ContentType::from_str)
            .unwrap_or(ContentType::Unknown);

        if data.len() > CHUNK_SIZE {
            let receipt = self.write_named_bytes(
                bucket,
                name,
                content_type,
                data
            ).await?;

            Ok(receipt.manifest)
        } else {
            let track = self.write_named_track(
                bucket, 
                name, 
                content_type, 
                data
            ).await?;

            Ok(track_pda(track.tape, track.track_number).0.into())
        }
    }

    /// Read a named object from a bucket.
    pub async fn get_object(&self, bucket: &Address, name: &str) -> Result<Vec<u8>, TapedriveError> {
        let address = self.resolve_object(bucket, name).await?;
        let bytes = self.read(&address).await?;

        // A stream's representing track is its manifest; detect and follow it.
        // (Hardening: a magic-prefixed manifest would make this unambiguous.)
        if ChunkManifest::from_bytes(&bytes).is_ok() {
            return self.read_bytes(&address).await;
        }

        Ok(bytes)
    }

    /// Fetch metadata for a named object without downloading its data.
    pub async fn head_object(
        &self,
        bucket: &Address,
        name: &str,
    ) -> Result<ObjectMeta, TapedriveError> {
        let entry = self.lookup_object_entry(bucket, name.as_bytes()).await?;
        Ok(ObjectMeta {
            size: entry.size.to_bytes(),
            etag: entry.etag,
            content_type: entry.content_type,
            block_time: entry.block_time,
            slot: entry.slot,
        })
    }

    /// Delete a named object from a bucket.
    pub async fn delete_object(&self, bucket: &TapeKey, name: &str) -> Result<(), TapedriveError> {
        let address = self.resolve_object(&bucket.address(), name).await?;
        let proof = self.get_track_proof(&address).await?;

        let payer = self.payer()?;
        let tape_signer = bucket.keypair();
        let ix = build_delete_track_ix(payer.pubkey().into(), bucket.pubkey().into(), proof);

        self.rpc()
            .send_instructions_with_signers_and_compute_unit_limit(
                payer,
                TRACK_WRITE_CU,
                vec![ix],
                &[tape_signer],
            )
            .await?;

        Ok(())
    }

    /// List one page of objects in a bucket.
    pub async fn list_objects(
        &self,
        bucket: &Address,
        query: ListObjectsQuery,
    ) -> Result<ObjectListPage, TapedriveError> {
        let page = self
            .list_object_page(
                bucket,
                &query.prefix,
                query.delimiter.as_deref(),
                query.cursor,
                query.limit,
            )
            .await?;

        Ok(ObjectListPage {
            objects: page.objects.into_iter().map(ListedObject::from).collect(),
            common_prefixes: page.common_prefixes,
            next_cursor: page.next_cursor,
            is_truncated: page.is_truncated,
        })
    }

    /// Resolve a name to its representing track's address (latest version).
    async fn resolve_object(&self, bucket: &Address, name: &str) -> Result<Address, TapedriveError> {
        let track = self.lookup_object(bucket, name).await?;
        Ok(track_pda(track.tape, track.track_number).0.into())
    }

    /// Resolve a name to its representing track (latest version) via `hash(name)`.
    async fn lookup_object(&self, bucket: &Address, name: &str) -> Result<CompressedTrack, TapedriveError> {
        let key = hash(name.as_bytes());
        self.find_track(bucket, key, FindTrackVersion::Latest).await
    }

    async fn lookup_object_entry(
        &self,
        bucket: &Address,
        name: &[u8],
    ) -> Result<ListedObject, TapedriveError> {
        let page = self
            .list_objects(
                bucket,
                ListObjectsQuery::new(name).with_limit(1),
            )
            .await?;

        page.objects
            .into_iter()
            .find(|object| object.name == name)
            .ok_or(TapedriveError::NotFound)
    }

    async fn list_object_page(
        &self,
        bucket: &Address,
        prefix: &[u8],
        delimiter: Option<&[u8]>,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> Result<ListObjectsRes, TapedriveError> {
        let peers = queryable_peers(self).await?;
        let mut last_error = None;
        let mut saw_not_found = false;

        for node in peers {
            let req = ListObjectsReq {
                bucket: *bucket,
                prefix: prefix.to_vec(),
                delimiter: delimiter.map(|value| value.to_vec()),
                cursor: cursor.clone(),
                limit,
            };
            match self.api.list_objects(node, &req).await {
                Ok(res) => return Ok(res),
                Err(ApiError::NotFound) => saw_not_found = true,
                Err(error) => last_error = Some(error),
            }
        }

        Err(finish_object_peer_query(last_error, saw_not_found))
    }
}

fn finish_object_peer_query(last_error: Option<ApiError>, saw_not_found: bool) -> TapedriveError {
    if let Some(error) = last_error {
        TapedriveError::Peer(error)
    } else if saw_not_found {
        TapedriveError::NotFound
    } else {
        TapedriveError::Peer(ApiError::Other("no responsive peers available".into()))
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

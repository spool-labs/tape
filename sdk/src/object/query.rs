use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_core::prelude::CompressedTrack;
use tape_crypto::hash::hash;
use tape_crypto::prelude::Address;
use tape_protocol::Api;
use tape_protocol::api::{ApiError, FindTrackVersion, ListObjectsReq, ListObjectsRes};

use super::types::{ListObjectsQuery, ListedObject, ObjectListPage, ObjectMeta};
use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;
use crate::track::queryable_peers;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
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
    pub(super) async fn resolve_object(
        &self,
        bucket: &Address,
        name: &str,
    ) -> Result<Address, TapedriveError> {
        let track = self.lookup_object(bucket, name).await?;
        Ok(track_pda(track.tape, track.track_number).0)
    }

    /// Resolve a name to its representing track (latest version) via `hash(name)`.
    async fn lookup_object(
        &self,
        bucket: &Address,
        name: &str,
    ) -> Result<CompressedTrack, TapedriveError> {
        let key = hash(name.as_bytes());
        self.find_track(bucket, key, FindTrackVersion::Latest).await
    }

    async fn lookup_object_entry(
        &self,
        bucket: &Address,
        name: &[u8],
    ) -> Result<ListedObject, TapedriveError> {
        let page = self
            .list_objects(bucket, ListObjectsQuery::new(name).with_limit(1))
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

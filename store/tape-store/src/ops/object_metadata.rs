//! Object metadata operations for named object reverse lookup.

use tape_store_api::Store;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;

use crate::columns::ObjectMetadataCol;
use crate::error::Result;
use crate::types::{ObjectMetadata, TrackKey};
use crate::TapeStore;

/// Operations for durable named-object metadata.
pub trait ObjectMetadataOps {
    /// Get object metadata by track address and track number.
    fn get_object_metadata(
        &self,
        address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<ObjectMetadata>>;

    /// Store object metadata by track address and track number.
    fn put_object_metadata(
        &self,
        address: Address,
        track_number: TrackNumber,
        metadata: ObjectMetadata,
    ) -> Result<()>;

    /// Delete object metadata by track address and track number.
    fn delete_object_metadata(&self, address: Address, track_number: TrackNumber) -> Result<()>;
}

impl<S: Store> ObjectMetadataOps for TapeStore<S> {
    fn get_object_metadata(
        &self,
        address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<ObjectMetadata>> {
        Ok(self.get::<ObjectMetadataCol>(&TrackKey::new(address, track_number))?)
    }

    fn put_object_metadata(
        &self,
        address: Address,
        track_number: TrackNumber,
        metadata: ObjectMetadata,
    ) -> Result<()> {
        self.put::<ObjectMetadataCol>(&TrackKey::new(address, track_number), &metadata)?;
        Ok(())
    }

    fn delete_object_metadata(&self, address: Address, track_number: TrackNumber) -> Result<()> {
        self.delete::<ObjectMetadataCol>(&TrackKey::new(address, track_number))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tape_store_memory::MemoryStore;
    use tape_core::types::ContentType;

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    // metadata round-trips by track address and track number
    #[test]
    fn metadata_roundtrip() {
        let store = test_store();
        let track = Address::new_unique();
        let metadata = ObjectMetadata {
            name: b"photos/cat.jpg".to_vec(),
            content_type: ContentType::ImageJpeg,
        };

        store
            .put_object_metadata(track, TrackNumber(2), metadata.clone())
            .expect("put metadata");

        assert_eq!(
            store.get_object_metadata(track, TrackNumber(2)).expect("get metadata"),
            Some(metadata)
        );
        assert_eq!(store.get_object_metadata(track, TrackNumber(3)).expect("get metadata"), None);
    }
}

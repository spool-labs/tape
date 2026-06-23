//! Object metadata operations for named object reverse lookup.

use store::Store;
use tape_crypto::address::Address;

use crate::columns::ObjectMetadataCol;
use crate::error::Result;
use crate::types::ObjectMetadata;
use crate::TapeStore;

/// Operations for durable named-object metadata.
pub trait ObjectMetadataOps {
    /// Get object metadata by track address.
    fn get_object_metadata(&self, address: Address) -> Result<Option<ObjectMetadata>>;

    /// Store object metadata by track address.
    fn put_object_metadata(&self, address: Address, metadata: ObjectMetadata) -> Result<()>;

    /// Delete object metadata by track address.
    fn delete_object_metadata(&self, address: Address) -> Result<()>;
}

impl<S: Store> ObjectMetadataOps for TapeStore<S> {
    fn get_object_metadata(&self, address: Address) -> Result<Option<ObjectMetadata>> {
        Ok(self.get::<ObjectMetadataCol>(&address)?)
    }

    fn put_object_metadata(&self, address: Address, metadata: ObjectMetadata) -> Result<()> {
        self.put::<ObjectMetadataCol>(&address, &metadata)?;
        Ok(())
    }

    fn delete_object_metadata(&self, address: Address) -> Result<()> {
        self.delete::<ObjectMetadataCol>(&address)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::types::ContentType;

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    // metadata round-trips by track address
    #[test]
    fn metadata_roundtrip() {
        let store = test_store();
        let track = Address::new_unique();
        let metadata = ObjectMetadata {
            name: b"photos/cat.jpg".to_vec(),
            content_type: ContentType::ImageJpeg,
        };

        store
            .put_object_metadata(track, metadata.clone())
            .expect("put metadata");

        assert_eq!(
            store.get_object_metadata(track).expect("get metadata"),
            Some(metadata)
        );
    }
}

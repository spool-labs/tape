//! Tape management operations

use crate::columns::*;
use crate::error::{Result, TapeStoreError};
use crate::types::*;
use crate::TapeStore;
use store::{Column, Store, WriteBatch};

/// High-level operations for tape management
pub trait TapeOps {
    /// Put a tape and update all indices atomically
    ///
    /// This operation atomically updates:
    /// - TapesById: main tape data
    /// - TapesByAddress: reverse lookup by authority
    /// - TapesActiveIndex: presence index for active tapes
    ///
    /// # Arguments
    /// * `tape` - The tape data to store
    ///
    /// # Example
    /// ```
    /// use tape_store::{TapeStore, types::*, ops::TapeOps};
    /// use store::MemoryStore;
    ///
    /// let store = TapeStore::new(MemoryStore::new());
    /// let tape = TapeData {
    ///     id: TapeNumber(1),
    ///     authority: StoredPubkey::new([0u8; 32]),
    ///     capacity: 1_000_000,
    ///     used: 0,
    ///     active_epoch: EpochNumber(100),
    ///     expiry_epoch: EpochNumber(200),
    ///     track_count: 0,
    /// };
    /// store.put_tape(&tape).unwrap();
    /// ```
    fn put_tape(&self, tape: &TapeData) -> Result<()>;

    /// Get tape by address
    ///
    /// Performs a reverse lookup from authority pubkey to tape number,
    /// then retrieves the full tape data.
    ///
    /// # Arguments
    /// * `address` - The authority pubkey of the tape (as StoredPubkey)
    ///
    /// # Returns
    /// * `Ok(Some(tape))` if found
    /// * `Ok(None)` if not found
    /// * `Err` on database or consistency errors
    fn get_tape_by_address(&self, address: &StoredPubkey) -> Result<Option<TapeData>>;

    /// Delete a tape and all its indices atomically
    ///
    /// This operation atomically removes:
    /// - TapesById: main tape data
    /// - TapesByAddress: reverse lookup entry
    /// - TapesActiveIndex: presence index entry
    ///
    /// # Arguments
    /// * `tape_id` - The tape number to delete
    ///
    /// # Note
    /// This does NOT delete associated tracks. Tracks should be deleted separately
    /// or handled through cascade logic in the application layer.
    fn delete_tape(&self, tape_id: TapeNumber) -> Result<()>;
}

impl<S: Store> TapeOps for TapeStore<S> {
    fn put_tape(&self, tape: &TapeData) -> Result<()> {
        let mut batch = WriteBatch::new();

        // Serialize all keys and values
        let tape_key = TapeKey(tape.id);
        let tape_key_bytes = wincode::serialize(&tape_key)
            .map_err(|e| TapeStoreError::Serialization(format!("tape key: {}", e)))?;
        let tape_value_bytes = wincode::serialize(tape)
            .map_err(|e| TapeStoreError::Serialization(format!("tape value: {}", e)))?;
        let address_key_bytes = wincode::serialize(&tape.authority)
            .map_err(|e| TapeStoreError::Serialization(format!("address: {}", e)))?;
        let tape_number_bytes = wincode::serialize(&tape.id)
            .map_err(|e| TapeStoreError::Serialization(format!("tape number: {}", e)))?;
        let unit_bytes = wincode::serialize(&())
            .map_err(|e| TapeStoreError::Serialization(format!("unit: {}", e)))?;

        // Add all operations to batch (atomic)
        batch.put(TapesById::CF_NAME, &tape_key_bytes, &tape_value_bytes);
        batch.put(TapesByAddress::CF_NAME, &address_key_bytes, &tape_number_bytes);
        batch.put(TapesActiveIndex::CF_NAME, &tape_key_bytes, &unit_bytes);

        // Execute atomically
        self.inner().inner().write_batch(batch)?;

        Ok(())
    }

    fn get_tape_by_address(&self, address: &StoredPubkey) -> Result<Option<TapeData>> {
        // Look up tape number by address
        let tape_number = match self.get::<TapesByAddress>(address)? {
            Some(num) => num,
            None => return Ok(None),
        };

        // Look up tape by number
        let tape = self.get::<TapesById>(&TapeKey(tape_number))?;

        // Check consistency
        if tape.is_none() {
            return Err(TapeStoreError::InconsistentTapeIndex(tape_number));
        }

        Ok(tape)
    }

    fn delete_tape(&self, tape_id: TapeNumber) -> Result<()> {
        // First get the tape to know its address
        let tape = match self.get::<TapesById>(&TapeKey(tape_id))? {
            Some(t) => t,
            None => return Ok(()), // Already deleted
        };

        let mut batch = WriteBatch::new();

        // Serialize keys
        let tape_key_bytes = wincode::serialize(&TapeKey(tape_id))
            .map_err(|e| TapeStoreError::Serialization(format!("tape key: {}", e)))?;
        let address_key_bytes = wincode::serialize(&tape.authority)
            .map_err(|e| TapeStoreError::Serialization(format!("address: {}", e)))?;

        // Add all delete operations to batch (atomic)
        batch.delete(TapesById::CF_NAME, &tape_key_bytes);
        batch.delete(TapesByAddress::CF_NAME, &address_key_bytes);
        batch.delete(TapesActiveIndex::CF_NAME, &tape_key_bytes);

        // Execute atomically
        self.inner().inner().write_batch(batch)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::MemoryStore;

    #[test]
    fn put_tape_atomic() {
        let store = TapeStore::new(MemoryStore::new());
        let authority = StoredPubkey::new_unique();

        let tape = TapeData {
            id: TapeNumber(1),
            authority,
            capacity: 1_000_000,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };

        store.put_tape(&tape).unwrap();

        // Verify all indices are updated
        let retrieved = store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
        assert_eq!(retrieved, Some(tape.clone()));

        let by_address = store.get::<TapesByAddress>(&authority).unwrap();
        assert_eq!(by_address, Some(TapeNumber(1)));

        let in_index = store.get::<TapesActiveIndex>(&TapeKey(TapeNumber(1))).unwrap();
        assert_eq!(in_index, Some(()));
    }

    #[test]
    fn get_tape_by_address() {
        let store = TapeStore::new(MemoryStore::new());
        let authority = StoredPubkey::new_unique();

        let tape = TapeData {
            id: TapeNumber(42),
            authority,
            capacity: 1_000_000,
            used: 500_000,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 10,
        };

        store.put_tape(&tape).unwrap();

        let found = store.get_tape_by_address(&authority).unwrap();
        assert_eq!(found, Some(tape));

        let not_found = store.get_tape_by_address(&StoredPubkey::new_unique()).unwrap();
        assert_eq!(not_found, None);
    }

    #[test]
    fn delete_tape_atomic() {
        let store = TapeStore::new(MemoryStore::new());
        let authority = StoredPubkey::new_unique();

        let tape = TapeData {
            id: TapeNumber(1),
            authority,
            capacity: 1_000_000,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };

        store.put_tape(&tape).unwrap();
        store.delete_tape(TapeNumber(1)).unwrap();

        // Verify all indices are removed
        let by_id = store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
        assert_eq!(by_id, None);

        let by_address = store.get::<TapesByAddress>(&authority).unwrap();
        assert_eq!(by_address, None);

        let in_index = store.get::<TapesActiveIndex>(&TapeKey(TapeNumber(1))).unwrap();
        assert_eq!(in_index, None);
    }
}

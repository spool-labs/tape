//! Per-bucket object listing index for S3-style `ListObjects`.

use store::{Column, Direction, Store};
use tape_crypto::address::Address;

use crate::columns::ObjectListCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{ObjectListEntry, ObjectListKey};
use crate::TapeStore;

/// One page of a listing scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectListPage {
    /// Matching objects in lexicographic name order: `(name, entry)`.
    pub objects: Vec<(Vec<u8>, ObjectListEntry)>,
    /// Rolled-up folder prefixes (only when a delimiter is given), in order.
    pub common_prefixes: Vec<Vec<u8>>,
    /// Resume token (a name to seek to, inclusive) when `is_truncated`.
    pub next: Option<Vec<u8>>,
    /// True when more results remain beyond this page.
    pub is_truncated: bool,
}

/// Operations for the per-bucket object listing index.
pub trait ObjectListOps {
    /// Insert or overwrite the listing entry for `(bucket, name)`.
    ///
    /// Last-write-wins: there is exactly one entry per name, so re-writing a
    /// name simply repoints it. The previously pointed-at object track is left
    /// untouched (an unnamed previous version).
    fn put_object_entry(&self, bucket: Address, name: &[u8], entry: ObjectListEntry) -> Result<()>;

    /// Remove the listing entry for `(bucket, name)`, if present.
    fn delete_object_entry(&self, bucket: Address, name: &[u8]) -> Result<()>;

    /// Fetch a single listing entry by exact name.
    fn get_object_entry(&self, bucket: Address, name: &[u8]) -> Result<Option<ObjectListEntry>>;

    /// List objects under `bucket`, S3-style.
    ///
    /// - `prefix` filters to names starting with it (empty = whole bucket).
    /// - `delimiter` (e.g. `b"/"`) rolls names that contain it after the prefix
    ///   into `common_prefixes` (folders) instead of returning them as objects.
    /// - `start` is an inclusive resume token (the `next` from a prior page);
    ///   `None` begins at the prefix.
    /// - `max_keys` caps `objects.len() + common_prefixes.len()`.
    fn list_objects(
        &self,
        bucket: Address,
        prefix: &[u8],
        delimiter: Option<&[u8]>,
        start: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<ObjectListPage>;
}

impl<S: Store> ObjectListOps for TapeStore<S> {
    fn put_object_entry(&self, bucket: Address, name: &[u8], entry: ObjectListEntry) -> Result<()> {
        let key = ObjectListKey::new(bucket, name.to_vec());
        self.put::<ObjectListCol>(&key, &entry)?;
        Ok(())
    }

    fn delete_object_entry(&self, bucket: Address, name: &[u8]) -> Result<()> {
        let key = ObjectListKey::new(bucket, name.to_vec());
        self.delete::<ObjectListCol>(&key)?;
        Ok(())
    }

    fn get_object_entry(&self, bucket: Address, name: &[u8]) -> Result<Option<ObjectListEntry>> {
        let key = ObjectListKey::new(bucket, name.to_vec());
        Ok(self.get::<ObjectListCol>(&key)?)
    }

    fn list_objects(
        &self,
        bucket: Address,
        prefix: &[u8],
        delimiter: Option<&[u8]>,
        start: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<ObjectListPage> {
        let bucket_prefix = ObjectListKey::bucket_prefix(bucket);

        // Inclusive name to begin scanning at: the resume token when it lies
        // past the prefix, otherwise the prefix itself.
        let mut start_name: Vec<u8> = match start {
            Some(s) if s > prefix => s.to_vec(),
            _ => prefix.to_vec(),
        };

        let mut page = ObjectListPage::default();

        'scan: loop {
            let mut seek = Vec::with_capacity(32 + start_name.len());
            seek.extend_from_slice(&bucket_prefix);
            seek.extend_from_slice(&start_name);

            let iter =
                self.inner()
                    .inner()
                    .iter_from(ObjectListCol::CF_NAME, &seek, Direction::Asc)?;

            // Set when we roll up a folder and must re-seek past it.
            let mut reseek: Option<Vec<u8>> = None;

            for (key_bytes, value_bytes) in iter {
                // Left this bucket entirely.
                if key_bytes.len() < 32 || key_bytes[..32] != bucket_prefix {
                    break 'scan;
                }
                let name = &key_bytes[32..];
                // Past the prefix range; keys are sorted, so we are done.
                if !name.starts_with(prefix) {
                    break 'scan;
                }

                // Does the part after the prefix contain the delimiter?
                let folder: Option<&[u8]> = delimiter.and_then(|delim| {
                    let rest = &name[prefix.len()..];
                    find_subslice(rest, delim).map(|pos| &name[..prefix.len() + pos + delim.len()])
                });

                if let Some(cp) = folder {
                    // New folder? (sorted order groups them; we skip folders so
                    // this is also belt-and-suspenders against duplicates.)
                    if page.common_prefixes.last().map(|v| v.as_slice()) != Some(cp) {
                        if page.objects.len() + page.common_prefixes.len() >= max_keys {
                            page.is_truncated = true;
                            page.next = Some(name.to_vec());
                            break 'scan;
                        }
                        page.common_prefixes.push(cp.to_vec());
                    }
                    // Skip the whole folder by re-seeking past it.
                    match prefix_successor(cp) {
                        Some(succ) => {
                            reseek = Some(succ);
                            break;
                        }
                        None => break 'scan,
                    }
                } else {
                    if page.objects.len() + page.common_prefixes.len() >= max_keys {
                        page.is_truncated = true;
                        page.next = Some(name.to_vec());
                        break 'scan;
                    }
                    page.objects.push((name.to_vec(), decode_entry(&value_bytes)?));
                }
            }

            match reseek {
                Some(succ) => {
                    start_name = succ;
                    continue 'scan;
                }
                None => break 'scan,
            }
        }

        Ok(page)
    }
}

fn decode_entry(bytes: &[u8]) -> Result<ObjectListEntry> {
    wincode::deserialize(bytes)
        .map_err(|e| TapeStoreError::Serialization(format!("object list entry: {}", e)))
}

/// First byte string strictly greater than every string having `prefix` as a
/// prefix, or `None` when `prefix` is all `0xFF` (no successor exists).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut v = prefix.to_vec();
    while let Some(&last) = v.last() {
        if last != u8::MAX {
            *v.last_mut().unwrap() = last + 1;
            return Some(v);
        }
        v.pop();
    }
    None
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::types::{ContentType, SlotNumber, StorageUnits, TrackNumber};
    use tape_crypto::Hash;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn entry(n: u64) -> ObjectListEntry {
        ObjectListEntry {
            size: StorageUnits(n),
            etag: Hash::new_unique(),
            block_time: Some(1_700_000_000 + n as i64),
            slot: SlotNumber(n),
            data_tape: Address::new_unique(),
            track_number: TrackNumber(n),
            kind: 1,
            content_type: ContentType::Unknown,
        }
    }

    fn names(page: &ObjectListPage) -> Vec<Vec<u8>> {
        page.objects.iter().map(|(n, _)| n.clone()).collect()
    }

    #[test]
    fn put_get_roundtrip() {
        let s = store();
        let b = Address::new_unique();
        assert!(s.get_object_entry(b, b"a").unwrap().is_none());
        let e = entry(7);
        s.put_object_entry(b, b"a", e.clone()).unwrap();
        assert_eq!(s.get_object_entry(b, b"a").unwrap(), Some(e));
    }

    #[test]
    fn overwrite_last_write_wins() {
        let s = store();
        let b = Address::new_unique();
        s.put_object_entry(b, b"k", entry(1)).unwrap();
        let e2 = entry(2);
        s.put_object_entry(b, b"k", e2.clone()).unwrap();
        assert_eq!(s.get_object_entry(b, b"k").unwrap(), Some(e2));
        let page = s.list_objects(b, b"", None, None, 100).unwrap();
        assert_eq!(page.objects.len(), 1);
    }

    #[test]
    fn delete_entry() {
        let s = store();
        let b = Address::new_unique();
        s.put_object_entry(b, b"k", entry(1)).unwrap();
        s.delete_object_entry(b, b"k").unwrap();
        assert!(s.get_object_entry(b, b"k").unwrap().is_none());
    }

    #[test]
    fn lists_in_sorted_order() {
        let s = store();
        let b = Address::new_unique();
        for n in [b"banana".as_slice(), b"apple", b"cherry"] {
            s.put_object_entry(b, n, entry(1)).unwrap();
        }
        let page = s.list_objects(b, b"", None, None, 100).unwrap();
        assert_eq!(
            names(&page),
            vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
        );
        assert!(!page.is_truncated);
    }

    #[test]
    fn prefix_filters() {
        let s = store();
        let b = Address::new_unique();
        for n in [b"photos/a".as_slice(), b"photos/b", b"docs/c", b"zoo"] {
            s.put_object_entry(b, n, entry(1)).unwrap();
        }
        let page = s.list_objects(b, b"photos/", None, None, 100).unwrap();
        assert_eq!(names(&page), vec![b"photos/a".to_vec(), b"photos/b".to_vec()]);
    }

    #[test]
    fn buckets_isolated() {
        let s = store();
        let a = Address::new_unique();
        let b = Address::new_unique();
        s.put_object_entry(a, b"x", entry(1)).unwrap();
        s.put_object_entry(b, b"y", entry(1)).unwrap();
        assert_eq!(names(&s.list_objects(a, b"", None, None, 100).unwrap()), vec![b"x".to_vec()]);
        assert_eq!(names(&s.list_objects(b, b"", None, None, 100).unwrap()), vec![b"y".to_vec()]);
    }

    #[test]
    fn delimiter_rolls_up_folders() {
        let s = store();
        let b = Address::new_unique();
        for n in [b"a".as_slice(), b"p/1", b"p/2", b"q/r/s", b"z"] {
            s.put_object_entry(b, n, entry(1)).unwrap();
        }
        let page = s.list_objects(b, b"", Some(b"/"), None, 100).unwrap();
        assert_eq!(names(&page), vec![b"a".to_vec(), b"z".to_vec()]);
        assert_eq!(page.common_prefixes, vec![b"p/".to_vec(), b"q/".to_vec()]);
        assert!(!page.is_truncated);
    }

    #[test]
    fn delimiter_within_prefix() {
        let s = store();
        let b = Address::new_unique();
        for n in [b"p/file".as_slice(), b"p/sub/x", b"p/sub/y", b"p/z"] {
            s.put_object_entry(b, n, entry(1)).unwrap();
        }
        let page = s.list_objects(b, b"p/", Some(b"/"), None, 100).unwrap();
        assert_eq!(names(&page), vec![b"p/file".to_vec(), b"p/z".to_vec()]);
        assert_eq!(page.common_prefixes, vec![b"p/sub/".to_vec()]);
    }

    #[test]
    fn pagination_objects() {
        let s = store();
        let b = Address::new_unique();
        for n in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            s.put_object_entry(b, n, entry(1)).unwrap();
        }
        let mut seen = Vec::new();
        let mut start: Option<Vec<u8>> = None;
        loop {
            let page = s.list_objects(b, b"", None, start.as_deref(), 2).unwrap();
            for (n, _) in &page.objects {
                seen.push(n.clone());
            }
            if page.is_truncated {
                start = page.next.clone();
                assert!(start.is_some());
            } else {
                break;
            }
        }
        assert_eq!(
            seen,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec()
            ]
        );
    }

    #[test]
    fn pagination_with_delimiter_mixed() {
        let s = store();
        let b = Address::new_unique();
        for n in [b"a".as_slice(), b"p/1", b"p/2", b"q/1", b"z"] {
            s.put_object_entry(b, n, entry(1)).unwrap();
        }
        let p1 = s.list_objects(b, b"", Some(b"/"), None, 2).unwrap();
        assert_eq!(names(&p1), vec![b"a".to_vec()]);
        assert_eq!(p1.common_prefixes, vec![b"p/".to_vec()]);
        assert!(p1.is_truncated);

        let p2 = s.list_objects(b, b"", Some(b"/"), p1.next.as_deref(), 2).unwrap();
        assert_eq!(names(&p2), vec![b"z".to_vec()]);
        assert_eq!(p2.common_prefixes, vec![b"q/".to_vec()]);
        assert!(!p2.is_truncated);
    }

    #[test]
    fn empty_bucket() {
        let s = store();
        let b = Address::new_unique();
        let page = s.list_objects(b, b"", None, None, 100).unwrap();
        assert!(page.objects.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert!(!page.is_truncated);
    }

    #[test]
    fn key_equal_to_prefix_is_returned() {
        let s = store();
        let b = Address::new_unique();
        s.put_object_entry(b, b"photos/", entry(1)).unwrap();
        s.put_object_entry(b, b"photos/a", entry(1)).unwrap();
        let page = s.list_objects(b, b"photos/", None, None, 100).unwrap();
        assert_eq!(names(&page), vec![b"photos/".to_vec(), b"photos/a".to_vec()]);
    }
}

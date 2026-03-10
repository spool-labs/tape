use super::SPOOL_GROUP_SIZE;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::MaybeUninit;
use std::ops::Deref;
use wincode::{SchemaRead, SchemaWrite};

/// Index of a slice within a blob's erasure-coded output.
/// Valid range: 0 to SPOOL_GROUP_SIZE-1.
///
/// Each blob is encoded into SPOOL_GROUP_SIZE slices. The slice at index N
/// for any blob is stored in spool N on the network.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, SchemaWrite)]
pub struct SliceIndex(usize);

impl SliceIndex {
    pub fn new(index: usize) -> Self {
        debug_assert!(index < SPOOL_GROUP_SIZE, "SliceIndex out of bounds: {index}");
        Self(index)
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..SPOOL_GROUP_SIZE).map(Self)
    }
}

impl Deref for SliceIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for SliceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for SliceIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VisitorImpl;
        impl<'de> serde::de::Visitor<'de> for VisitorImpl {
            type Value = SliceIndex;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a usize in [0, SPOOL_GROUP_SIZE)")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let index = v as usize;
                if index < SPOOL_GROUP_SIZE {
                    Ok(SliceIndex(index))
                } else {
                    Err(E::custom(format!("index {} out of bounds", v)))
                }
            }
        }
        deserializer.deserialize_u64(VisitorImpl)
    }
}

impl<'de> SchemaRead<'de> for SliceIndex {
    type Dst = Self;

    fn read(
        reader: &mut impl wincode::io::Reader<'de>,
        dst: &mut MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        unsafe {
            reader.copy_into_t(dst)?;
            if dst.assume_init_ref().0 >= SPOOL_GROUP_SIZE {
                Err(wincode::ReadError::Custom("slice index out of bounds"))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wincode;

    #[test]
    fn infallible_new() {
        let si = SliceIndex::new(0);
        assert_eq!(*si, 0);
        let si = SliceIndex::new(SPOOL_GROUP_SIZE - 1);
        assert_eq!(*si, SPOOL_GROUP_SIZE - 1);
    }

    #[test]
    fn serde_roundtrip_ok() {
        let vals = [0, 1, SPOOL_GROUP_SIZE - 1];
        for v in vals {
            let s = serde_json::to_string(&SliceIndex(v)).unwrap();
            let _idx: SliceIndex = serde_json::from_str(&s).unwrap();
        }
    }

    #[test]
    fn serde_fail() {
        let vals = [SPOOL_GROUP_SIZE, SPOOL_GROUP_SIZE + 1];
        for v in vals {
            let s = serde_json::to_string(&v).unwrap();
            let res: Result<SliceIndex, _> = serde_json::from_str(&s);
            assert!(res.is_err());
        }
    }

    #[test]
    fn wincode_roundtrip_ok() {
        let vals = [0, SPOOL_GROUP_SIZE - 1];
        for v in vals {
            let b = wincode::serialize(&SliceIndex(v)).unwrap();
            let _idx: SliceIndex = wincode::deserialize(&b).unwrap();
        }
    }

    #[test]
    fn wincode_fail() {
        let vals = [SPOOL_GROUP_SIZE, SPOOL_GROUP_SIZE + 1, usize::MAX];
        for v in vals {
            let b = wincode::serialize(&SliceIndex(v)).unwrap();
            let res: Result<SliceIndex, _> = wincode::deserialize(&b);
            assert!(res.is_err());
        }
    }
}

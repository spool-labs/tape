use tape_core::erasure::GROUP_SIZE;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::Deref;

/// Index of a slice within a blob's erasure-coded output.
/// Valid range: 0 to GROUP_SIZE-1.
///
/// Each blob is encoded into GROUP_SIZE slices. The slice at index N
/// for any blob is stored in spool N on the network.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct SliceIndex(usize);

impl SliceIndex {
    pub fn new(index: usize) -> Self {
        debug_assert!(index < GROUP_SIZE, "SliceIndex out of bounds: {index}");
        Self(index)
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..GROUP_SIZE).map(Self)
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
                write!(f, "a usize in [0, GROUP_SIZE)")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let index = v as usize;
                if index < GROUP_SIZE {
                    Ok(SliceIndex(index))
                } else {
                    Err(E::custom(format!("index {} out of bounds", v)))
                }
            }
        }
        deserializer.deserialize_u64(VisitorImpl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infallible_new() {
        let si = SliceIndex::new(0);
        assert_eq!(*si, 0);
        let si = SliceIndex::new(GROUP_SIZE - 1);
        assert_eq!(*si, GROUP_SIZE - 1);
    }

    #[test]
    fn serde_roundtrip_ok() {
        let vals = [0, 1, GROUP_SIZE - 1];
        for v in vals {
            let s = serde_json::to_string(&SliceIndex(v)).unwrap();
            let _idx: SliceIndex = serde_json::from_str(&s).unwrap();
        }
    }

    #[test]
    fn serde_fail() {
        let vals = [GROUP_SIZE, GROUP_SIZE + 1];
        for v in vals {
            let s = serde_json::to_string(&v).unwrap();
            let res: Result<SliceIndex, _> = serde_json::from_str(&s);
            assert!(res.is_err());
        }
    }

}

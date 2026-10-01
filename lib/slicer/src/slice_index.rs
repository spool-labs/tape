use tape_core::erasure::MAX_GROUP_SIZE;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::Deref;

/// Index of a slice within a blob's erasure-coded output.
/// Valid range: 0 to n-1, where n is the network's group size and never
/// exceeds MAX_GROUP_SIZE.
///
/// Each blob is encoded into n slices. The slice at index N for any blob is
/// stored at position N of the track's spool group.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct SliceIndex(usize);

impl SliceIndex {
    pub fn new(index: usize) -> Self {
        debug_assert!(index < MAX_GROUP_SIZE, "SliceIndex out of bounds: {index}");
        Self(index)
    }

    /// Every slice index of a group of `width` slices.
    pub fn all(width: usize) -> impl Iterator<Item = Self> {
        (0..width).map(Self)
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
                write!(f, "a usize in [0, MAX_GROUP_SIZE)")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let index = v as usize;
                if index < MAX_GROUP_SIZE {
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
        let si = SliceIndex::new(MAX_GROUP_SIZE - 1);
        assert_eq!(*si, MAX_GROUP_SIZE - 1);
    }

    #[test]
    fn serde_roundtrip_ok() {
        let vals = [0, 1, MAX_GROUP_SIZE - 1];
        for v in vals {
            let s = serde_json::to_string(&SliceIndex(v)).unwrap();
            let _idx: SliceIndex = serde_json::from_str(&s).unwrap();
        }
    }

    #[test]
    fn serde_fail() {
        let vals = [MAX_GROUP_SIZE, MAX_GROUP_SIZE + 1];
        for v in vals {
            let s = serde_json::to_string(&v).unwrap();
            let res: Result<SliceIndex, _> = serde_json::from_str(&s);
            assert!(res.is_err());
        }
    }

}

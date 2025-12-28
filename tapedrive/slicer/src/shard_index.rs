use super::SLICE_COUNT;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::MaybeUninit;
use std::ops::Deref;
use wincode::{SchemaRead, SchemaWrite};

/// Shard index type. Ensures value is < SLICE_COUNT.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, SchemaWrite)]
pub struct ShardIndex(usize);

impl ShardIndex {
    pub fn new(index: usize) -> Option<Self> {
        if index < SLICE_COUNT {
            Some(Self(index))
        } else {
            None
        }
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..SLICE_COUNT).map(Self)
    }
}

impl Deref for ShardIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for ShardIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for ShardIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VisitorImpl;
        impl<'de> serde::de::Visitor<'de> for VisitorImpl {
            type Value = ShardIndex;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a usize in [0, SLICE_COUNT)")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ShardIndex::new(v as usize)
                    .ok_or_else(|| E::custom(format!("index {} out of bounds", v)))
            }
        }
        deserializer.deserialize_u64(VisitorImpl)
    }
}

impl<'de> SchemaRead<'de> for ShardIndex {
    type Dst = Self;

    fn read(
        reader: &mut impl wincode::io::Reader<'de>,
        dst: &mut MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        unsafe {
            reader.copy_into_t(dst)?;
            if dst.assume_init_ref().0 >= SLICE_COUNT {
                Err(wincode::ReadError::Custom("shard index out of bounds"))
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
    fn serde_roundtrip_ok() {
        let vals = [0, 1, SLICE_COUNT - 1];
        for v in vals {
            let s = serde_json::to_string(&ShardIndex(v)).unwrap();
            let _idx: ShardIndex = serde_json::from_str(&s).unwrap();
        }
    }

    #[test]
    fn serde_fail() {
        let vals = [SLICE_COUNT, SLICE_COUNT + 1];
        for v in vals {
            let s = serde_json::to_string(&v).unwrap();
            let res: Result<ShardIndex, _> = serde_json::from_str(&s);
            assert!(res.is_err());
        }
    }

    #[test]
    fn wincode_roundtrip_ok() {
        let vals = [0, SLICE_COUNT - 1];
        for v in vals {
            let b = wincode::serialize(&ShardIndex(v)).unwrap();
            let _idx: ShardIndex = wincode::deserialize(&b).unwrap();
        }
    }

    #[test]
    fn wincode_fail() {
        let vals = [SLICE_COUNT, SLICE_COUNT + 1, usize::MAX];
        for v in vals {
            let b = wincode::serialize(&ShardIndex(v)).unwrap();
            let res: Result<ShardIndex, _> = wincode::deserialize(&b);
            assert!(res.is_err());
        }
    }
}

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::MaybeUninit;
use wincode::{SchemaRead, SchemaWrite};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, SchemaWrite)]
pub struct SliceIndex(u64);

impl SliceIndex {
    pub fn new(index: u64) -> Self {
        Self(index)
    }

    pub fn inner(self) -> u64 {
        self.0
    }

    pub fn first() -> Self {
        Self(0)
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
                write!(f, "a non-negative u64 representing a stripe index")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SliceIndex::new(v))
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
        // SAFETY: reading 8 bytes initializes a u64 which we wrap.
        unsafe {
            reader.copy_into_t(dst)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(SliceIndex::first().inner(), 0);
        assert_eq!(SliceIndex::new(42).inner(), 42);
    }
}

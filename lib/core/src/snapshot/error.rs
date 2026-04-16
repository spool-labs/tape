use core::fmt;

#[derive(Debug)]
pub enum SnapshotError {
    #[cfg(feature = "wincode")]
    Wincode(wincode::Error),
    UnsupportedVersion(u8),
    ChunkPayloadTooShort(usize),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "wincode")]
            Self::Wincode(error) => write!(formatter, "wincode: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported snapshot version: {version}")
            }
            Self::ChunkPayloadTooShort(len) => {
                write!(formatter, "snapshot chunk payload too short: {len} bytes")
            }
        }
    }
}

#[cfg(feature = "wincode")]
impl From<wincode::ReadError> for SnapshotError {
    fn from(error: wincode::ReadError) -> Self {
        Self::Wincode(error.into())
    }
}

#[cfg(feature = "wincode")]
impl From<wincode::WriteError> for SnapshotError {
    fn from(error: wincode::WriteError) -> Self {
        Self::Wincode(error.into())
    }
}

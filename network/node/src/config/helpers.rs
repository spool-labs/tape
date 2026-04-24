use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer};

pub fn expand_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let raw = path.to_string_lossy();

    if raw == "~" {
        return dirs::home_dir().unwrap_or_else(|| path.to_path_buf());
    }

    if let Some(suffix) = raw.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(suffix);
        }
    }

    path.to_path_buf()
}

pub fn deserialize_pathbuf<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path = String::deserialize(deserializer)?;
    Ok(expand_path(path))
}

pub fn deserialize_optional_pathbuf<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path: Option<String> = Option::deserialize(deserializer)?;
    Ok(path.map(expand_path))
}


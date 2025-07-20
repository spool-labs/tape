use std::fs::{self, File};
use std::path::Path;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use tar::Builder;
use tar::Archive;
use tempdir::TempDir;
use rocksdb::{DB, checkpoint::Checkpoint};

use crate::store::{TapeStore, StoreError};

pub fn create_snapshot(db: &DB, archive_path: impl AsRef<Path>) -> Result<(), StoreError> {
    let archive_path = archive_path.as_ref();
    let checkpoint = Checkpoint::new(db)?;
    let temp_dir = TempDir::new("tapestore")?;
    let checkpoint_dir = temp_dir.path().join("checkpoint");
    checkpoint.create_checkpoint(checkpoint_dir.to_str().unwrap())?;

    let file = File::create(archive_path)?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = Builder::new(enc);
    tar.append_dir_all(".", &checkpoint_dir)?;
    Ok(())
}

pub fn load_from_snapshot(archive_path: impl AsRef<Path>, extract_to: impl AsRef<Path>) -> Result<TapeStore, StoreError> {
    let archive_path = archive_path.as_ref();
    let extract_to = extract_to.as_ref();

    if extract_to.exists() {
        fs::remove_dir_all(extract_to)?;
    }
    fs::create_dir_all(extract_to)?;

    let file = File::open(archive_path)?;
    let dec = GzDecoder::new(file);
    let mut archive = Archive::new(dec);
    archive.unpack(extract_to)?;

    TapeStore::new(extract_to)
}

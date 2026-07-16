use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use clap::Args;
use rand::{CryptoRng, RngCore, SeedableRng, rngs::OsRng};
use rand_chacha::ChaCha20Rng;
use tape_crypto::bls12254::min_sig::PrivKey as BlsPrivKey;
use tape_crypto::ed25519::Keypair;

#[derive(Debug, Args)]
pub struct KeygenArgs {
    /// Directory to write key files and node.yaml into.
    #[arg(long)]
    pub out: PathBuf,

    /// Node name written into node.yaml (max 32 bytes).
    #[arg(long, default_value = "tape-node")]
    pub name: String,

    /// Hex-encoded 32-byte seed for deterministic generation.
    #[arg(long)]
    pub seed: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum KeygenError {
    #[error("invalid seed: {0}")]
    InvalidSeed(String),

    #[error("node name must not be empty")]
    EmptyName,

    #[error("node name must be at most 32 bytes (got {0})")]
    NameTooLong(usize),

    #[error("failed to write {path}: {source}")]
    Write { path: String, source: io::Error },

    #[error("failed to create directory {path}: {source}")]
    CreateDir { path: String, source: io::Error },

    #[error("failed to serialize JSON: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn run(args: KeygenArgs) -> Result<(), KeygenError> {
    validate_name(&args.name)?;

    fs::create_dir_all(&args.out).map_err(|source| KeygenError::CreateDir {
        path: args.out.display().to_string(),
        source,
    })?;

    match args.seed.as_deref() {
        Some(hex_seed) => {
            let mut rng = seeded_rng(hex_seed)?;
            generate(&args.out, &args.name, &mut rng)
        }
        None => {
            let mut rng = OsRng;
            generate(&args.out, &args.name, &mut rng)
        }
    }
}

fn validate_name(name: &str) -> Result<(), KeygenError> {
    if name.trim().is_empty() {
        return Err(KeygenError::EmptyName);
    }
    let len = name.as_bytes().len();
    if len > 32 {
        return Err(KeygenError::NameTooLong(len));
    }
    Ok(())
}

fn seeded_rng(hex_seed: &str) -> Result<ChaCha20Rng, KeygenError> {
    let bytes = hex::decode(hex_seed.trim_start_matches("0x"))
        .map_err(|error| KeygenError::InvalidSeed(error.to_string()))?;
    if bytes.len() != 32 {
        return Err(KeygenError::InvalidSeed(format!(
            "expected 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    Ok(ChaCha20Rng::from_seed(seed))
}

fn generate<R>(out: &Path, name: &str, rng: &mut R) -> Result<(), KeygenError>
where
    R: RngCore + CryptoRng,
{
    let identity = Keypair::new(rng);
    let tls = Keypair::new(rng);
    let bls = BlsPrivKey::from_rng(rng);

    let identity_path = out.join("identity.json");
    let bls_path = out.join("bls.json");
    let tls_path = out.join("tls.json");
    let yaml_path = out.join("node.yaml");

    write_ed25519(&identity_path, &identity)?;
    write_ed25519(&tls_path, &tls)?;
    write_bls(&bls_path, &bls.0)?;
    write_node_yaml(&yaml_path, name, &identity_path, &bls_path, &tls_path)?;

    println!("wrote keys and node.yaml to {}", out.display());
    Ok(())
}

fn write_ed25519(path: &Path, keypair: &Keypair) -> Result<(), KeygenError> {
    let bytes: [u8; 64] = keypair.to_keypair_bytes();
    let json = serde_json::to_vec(&bytes.to_vec())?;
    write_secret(path, &json)
}

fn write_bls(path: &Path, bytes: &[u8; 32]) -> Result<(), KeygenError> {
    let json = serde_json::to_vec(&bytes.to_vec())?;
    write_secret(path, &json)
}

fn write_secret(path: &Path, contents: &[u8]) -> Result<(), KeygenError> {
    fs::write(path, contents).map_err(|source| KeygenError::Write {
        path: path.display().to_string(),
        source,
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, perms).map_err(|source| KeygenError::Write {
            path: path.display().to_string(),
            source,
        })?;
    }

    Ok(())
}

fn write_node_yaml(
    path: &Path,
    name: &str,
    identity: &Path,
    bls: &Path,
    tls: &Path,
) -> Result<(), KeygenError> {
    let contents = format!(
        "node:\n  name: \"{name}\"\n  node_keypair: \"{identity}\"\n  bls_keypair: \"{bls}\"\n  commission: 0\nsolana:\n  rpc: \"http://127.0.0.1:8899\"\ntls:\n  identity_keypair: \"{tls}\"\n",
        name = name,
        identity = identity.display(),
        bls = bls.display(),
        tls = tls.display(),
    );
    fs::write(path, contents.as_bytes()).map_err(|source| KeygenError::Write {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_node::config::node::NodeConfig;
    use tempfile::TempDir;

    #[test]
    fn generates_files() {
        let dir = TempDir::new().unwrap();
        run(KeygenArgs {
            out: dir.path().to_path_buf(),
            name: "test-node".into(),
            seed: None,
        })
        .unwrap();

        assert!(dir.path().join("identity.json").exists());
        assert!(dir.path().join("bls.json").exists());
        assert!(dir.path().join("tls.json").exists());
        assert!(dir.path().join("node.yaml").exists());
    }

    #[test]
    fn generated_yaml_parses_as_node_config() {
        let dir = TempDir::new().unwrap();
        run(KeygenArgs {
            out: dir.path().to_path_buf(),
            name: "test-node".into(),
            seed: None,
        })
        .unwrap();

        let config = NodeConfig::from_yaml_file(dir.path().join("node.yaml")).unwrap();
        assert_eq!(config.node.name, "test-node");
    }

    #[test]
    fn deterministic_with_seed() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        let seed = "0x".to_string() + &"ab".repeat(32);

        for dir in [a.path(), b.path()] {
            run(KeygenArgs {
                out: dir.to_path_buf(),
                name: "node".into(),
                seed: Some(seed.clone()),
            })
            .unwrap();
        }

        let read = |p: &Path| std::fs::read(p).unwrap();
        assert_eq!(
            read(&a.path().join("identity.json")),
            read(&b.path().join("identity.json"))
        );
        assert_eq!(
            read(&a.path().join("bls.json")),
            read(&b.path().join("bls.json"))
        );
        assert_eq!(
            read(&a.path().join("tls.json")),
            read(&b.path().join("tls.json"))
        );
    }

    #[test]
    fn rejects_short_seed() {
        let dir = TempDir::new().unwrap();
        let err = run(KeygenArgs {
            out: dir.path().to_path_buf(),
            name: "node".into(),
            seed: Some("abcd".into()),
        })
        .unwrap_err();
        assert!(matches!(err, KeygenError::InvalidSeed(_)));
    }

    #[test]
    fn rejects_overlong_name() {
        let dir = TempDir::new().unwrap();
        let err = run(KeygenArgs {
            out: dir.path().to_path_buf(),
            name: "a".repeat(33),
            seed: None,
        })
        .unwrap_err();
        assert!(matches!(err, KeygenError::NameTooLong(33)));
    }
}

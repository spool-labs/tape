//! Cluster-name resolution — `l`/`d`/`t`/`m` magic letters map to Solana's
//! standard URLs, anything else is treated as a literal URL. Mirrors the
//! solana CLI's `-ul/-ud/-ut/-um` shorthand.

pub const LOCALHOST: &str = "http://127.0.0.1:8899";
pub const DEVNET: &str = "https://api.devnet.solana.com";
pub const TESTNET: &str = "https://api.testnet.solana.com";
pub const MAINNET_BETA: &str = "https://api.mainnet-beta.solana.com";

/// Resolve `-u <value>` into a concrete URL. Accepts the same single-letter
/// shortcuts as `solana config set --url`: `l`, `d`, `t`, `m`. Anything else
/// passes through unchanged.
pub fn resolve(raw: &str) -> String {
    match raw {
        "l" | "localhost" => LOCALHOST.into(),
        "d" | "devnet" => DEVNET.into(),
        "t" | "testnet" => TESTNET.into(),
        "m" | "mainnet" | "mainnet-beta" => MAINNET_BETA.into(),
        other => other.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shortcuts_expand() {
        assert_eq!(resolve("l"), LOCALHOST);
        assert_eq!(resolve("d"), DEVNET);
        assert_eq!(resolve("t"), TESTNET);
        assert_eq!(resolve("m"), MAINNET_BETA);
    }

    #[test]
    fn long_shortcuts_expand() {
        assert_eq!(resolve("devnet"), DEVNET);
        assert_eq!(resolve("mainnet-beta"), MAINNET_BETA);
    }

    #[test]
    fn url_passes_through() {
        assert_eq!(resolve("https://custom.rpc"), "https://custom.rpc");
    }
}

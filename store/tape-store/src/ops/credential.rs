//! S3 write-authorization credential operations.
//!
//! Credentials are keyed by access key id. Secrets are never stored.

use store::Store;

use crate::columns::CredentialCol;
use crate::error::Result;
use crate::types::{Credential, CredentialStatus};
use crate::TapeStore;

/// Operations for the durable S3 write-credential store
pub trait CredentialOps {
    /// Insert or overwrite the credential for `access_key_id`
    fn put_credential(&self, access_key_id: &str, credential: &Credential) -> Result<()>;

    /// Fetch the credential for `access_key_id`, if present
    fn get_credential(&self, access_key_id: &str) -> Result<Option<Credential>>;

    /// Durably revoke the credential for `access_key_id`
    fn revoke_credential(&self, access_key_id: &str) -> Result<bool>;

    /// List every stored credential as `(access_key_id, credential)`
    fn list_credentials(&self) -> Result<Vec<(String, Credential)>>;
}

impl<Backend: Store> CredentialOps for TapeStore<Backend> {
    fn put_credential(&self, access_key_id: &str, credential: &Credential) -> Result<()> {
        self.put::<CredentialCol>(&access_key_id.to_string(), credential)?;
        Ok(())
    }

    fn get_credential(&self, access_key_id: &str) -> Result<Option<Credential>> {
        Ok(self.get::<CredentialCol>(&access_key_id.to_string())?)
    }

    fn revoke_credential(&self, access_key_id: &str) -> Result<bool> {
        let key = access_key_id.to_string();
        match self.get::<CredentialCol>(&key)? {
            Some(mut credential) => {
                if !matches!(credential.status, CredentialStatus::Revoked) {
                    credential.status = CredentialStatus::Revoked;
                    self.put::<CredentialCol>(&key, &credential)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn list_credentials(&self) -> Result<Vec<(String, Credential)>> {
        Ok(self.iter::<CredentialCol>()?)
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_crypto::address::Address;

    use crate::types::{CredentialCaps, CredentialScope};
    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn credential() -> Credential {
        Credential {
            secret_hmac: [0x11; 32],
            principal: Address::new_unique(),
            scope: CredentialScope::AnyOwned,
            caps: CredentialCaps::all(),
            status: CredentialStatus::Active,
            not_after: None,
            grade: None,
        }
    }

    // a stored credential reads back unchanged
    #[test]
    fn put_get() {
        let store = store();
        assert!(store.get_credential("AKID").expect("get credential").is_none());
        let credential = credential();
        store.put_credential("AKID", &credential).expect("put credential");
        assert_eq!(store.get_credential("AKID").expect("get credential"), Some(credential));
    }

    // a second put overwrites the first
    #[test]
    fn overwrite() {
        let store = store();
        store.put_credential("AKID", &credential()).expect("put credential");
        let mut updated = credential();
        updated.caps = CredentialCaps::none();
        store.put_credential("AKID", &updated).expect("put credential");
        assert_eq!(
            store.get_credential("AKID").expect("get credential"),
            Some(updated)
        );
    }

    // revoke flips status to Revoked and is fail-closed
    #[test]
    fn revoke() {
        let store = store();
        store.put_credential("AKID", &credential()).expect("put credential");

        assert!(store.revoke_credential("AKID").expect("revoke credential"));
        let got = store
            .get_credential("AKID")
            .expect("get credential")
            .expect("credential present");
        assert_eq!(got.status, CredentialStatus::Revoked);
        assert!(!got.is_usable(0), "revoked is never usable");

        // Re-revoking an existing credential is idempotent.
        assert!(store.revoke_credential("AKID").expect("revoke credential"));
        // Revoking an unknown credential reports "nothing to revoke".
        assert!(!store
            .revoke_credential("does-not-exist")
            .expect("revoke credential"));
    }

    // listing returns every stored credential
    #[test]
    fn list_all() {
        let store = store();
        store.put_credential("a", &credential()).expect("put credential");
        store.put_credential("b", &credential()).expect("put credential");
        let ids: Vec<String> = store
            .list_credentials()
            .expect("list credentials")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"a".to_string()));
        assert!(ids.contains(&"b".to_string()));
    }
}

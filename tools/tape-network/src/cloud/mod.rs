//! Cloud-provider abstraction. One implementation today (DigitalOcean); the
//! trait is in place so Hetzner/AWS can be added without rewriting callers.

pub mod digitalocean;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::settings::Settings;

/// A provisioned cloud instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub provider_id: String,
    pub name: String,
    pub public_ip: Option<String>,
    pub region: String,
    pub status: String,
}

#[async_trait]
pub trait CloudProvider: Send + Sync {
    /// Create `count` instances named `{testbed_id}-node-{i}` tagged with the
    /// testbed id. Returns the created instances (not necessarily ready yet).
    async fn create_instances(&self, count: u32) -> anyhow::Result<Vec<Instance>>;

    /// Create a single instance with a caller-chosen name, tag, and optional
    /// size override (falls back to the settings-configured size). Used by
    /// the builder-droplet flow which needs a separate tag + beefier size.
    async fn create_one(
        &self,
        name: &str,
        tag: &str,
        size_override: Option<&str>,
    ) -> anyhow::Result<Instance>;

    /// Get current state of a specific instance by provider id.
    async fn get_instance(&self, provider_id: &str) -> anyhow::Result<Instance>;

    /// List all instances tagged with this testbed id.
    async fn list_instances(&self) -> anyhow::Result<Vec<Instance>>;

    /// List all instances with a specific tag (not the testbed default).
    async fn list_instances_by_tag(&self, tag: &str) -> anyhow::Result<Vec<Instance>>;

    /// Delete an instance by provider id.
    async fn delete_instance(&self, provider_id: &str) -> anyhow::Result<()>;

    /// Register the configured SSH public key with the provider if it is not
    /// already known. Returns a stable identifier the provider accepts in
    /// `create_instances` — usually a fingerprint or numeric id.
    async fn ensure_ssh_key(&self) -> anyhow::Result<String>;
}

/// Build the concrete `CloudProvider` implementation for the configured
/// provider in `settings`.
pub fn from_settings(settings: &Settings) -> anyhow::Result<Box<dyn CloudProvider>> {
    match settings.cloud.provider {
        crate::settings::Provider::Digitalocean => {
            Ok(Box::new(digitalocean::DigitalOcean::new(settings.clone())?))
        }
    }
}

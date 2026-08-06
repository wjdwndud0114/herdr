use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_FILE: &str = "remotes.toml";
const REGISTRY_LOCK_FILE: &str = ".remotes.lock";
static NEXT_INSTANCE_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct InstanceId(String);

impl InstanceId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RemoteInstance {
    pub(crate) id: InstanceId,
    pub(crate) name: String,
    pub(crate) target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session: Option<String>,
    #[serde(default = "enabled_by_default")]
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct FleetRegistry {
    version: u32,
    #[serde(default)]
    pub(crate) instances: Vec<RemoteInstance>,
    #[serde(default, skip_serializing_if = "FleetPresentation::is_default")]
    presentation: FleetPresentation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub(crate) struct FleetPresentation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sidebar_width: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sidebar_section_split: Option<f32>,
    #[serde(default, skip_serializing_if = "std::collections::HashSet::is_empty")]
    pub(crate) collapsed_space_keys: std::collections::HashSet<String>,
}

impl FleetPresentation {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

impl Default for FleetRegistry {
    fn default() -> Self {
        Self {
            version: REGISTRY_VERSION,
            instances: Vec::new(),
            presentation: FleetPresentation::default(),
        }
    }
}

impl FleetRegistry {
    pub(crate) fn presentation(&self) -> &FleetPresentation {
        &self.presentation
    }

    pub(crate) fn set_presentation(&mut self, presentation: FleetPresentation) {
        self.presentation = presentation;
    }

    pub(crate) fn enabled_instances(&self) -> impl Iterator<Item = &RemoteInstance> {
        self.instances.iter().filter(|instance| instance.enabled)
    }

    pub(crate) fn add(
        &mut self,
        target: String,
        name: Option<String>,
        session: Option<String>,
    ) -> std::io::Result<RemoteInstance> {
        let target = normalize_target(&target)?;
        let session = normalize_optional_value(session, "session")?;
        if self
            .instances
            .iter()
            .any(|instance| instance.target == target && instance.session == session)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "that remote target and session are already configured",
            ));
        }

        let name = match name {
            Some(name) => normalize_name(&name)?,
            None => default_name(&target),
        };
        if self.instances.iter().any(|instance| instance.name == name) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("a remote named {name:?} already exists"),
            ));
        }

        let instance = RemoteInstance {
            id: self.next_id(&target, session.as_deref()),
            name,
            target,
            session,
            enabled: true,
        };
        self.instances.push(instance.clone());
        Ok(instance)
    }

    pub(crate) fn remove(&mut self, id: &str) -> std::io::Result<RemoteInstance> {
        let index = self
            .instances
            .iter()
            .position(|instance| instance.id.as_str() == id)
            .ok_or_else(|| remote_not_found(id))?;
        Ok(self.instances.remove(index))
    }

    pub(crate) fn set_enabled(
        &mut self,
        id: &str,
        enabled: bool,
    ) -> std::io::Result<RemoteInstance> {
        let instance = self
            .instances
            .iter_mut()
            .find(|instance| instance.id.as_str() == id)
            .ok_or_else(|| remote_not_found(id))?;
        instance.enabled = enabled;
        Ok(instance.clone())
    }

    fn next_id(&self, target: &str, session: Option<&str>) -> InstanceId {
        loop {
            let nonce = NEXT_INSTANCE_NONCE.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default();
            let mut digest = Sha256::new();
            digest.update(target.as_bytes());
            digest.update([0]);
            digest.update(session.unwrap_or_default().as_bytes());
            digest.update([0]);
            digest.update(std::process::id().to_le_bytes());
            digest.update(nanos.to_le_bytes());
            digest.update(nonce.to_le_bytes());
            let encoded = format!("{:x}", digest.finalize());
            let id = InstanceId(format!("remote-{}", &encoded[..16]));
            if self.instances.iter().all(|instance| instance.id != id) {
                return id;
            }
        }
    }
}

fn enabled_by_default() -> bool {
    true
}

fn normalize_target(value: &str) -> std::io::Result<String> {
    let target = value.trim();
    if target.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "remote target must not be empty",
        ));
    }
    if target.starts_with('-') || target.contains(['\n', '\r', '\0']) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "remote target must be a single SSH destination and must not start with '-'",
        ));
    }
    Ok(target.to_string())
}

fn normalize_name(value: &str) -> std::io::Result<String> {
    let name = value.trim();
    if name.is_empty() || name.contains(['\n', '\r', '\0']) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "remote name must not be empty or contain control characters",
        ));
    }
    Ok(name.to_string())
}

fn normalize_optional_value(value: Option<String>, field: &str) -> std::io::Result<Option<String>> {
    value
        .map(|value| {
            let value = value.trim();
            if value.is_empty() || value.contains(['\n', '\r', '\0']) {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{field} must not be empty or contain control characters"),
                ))
            } else {
                Ok(value.to_string())
            }
        })
        .transpose()
}

fn default_name(target: &str) -> String {
    target
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(target)
        .split([':', '/'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(target)
        .to_string()
}

fn remote_not_found(id: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("remote instance {id:?} was not found"),
    )
}

pub(crate) fn registry_path() -> PathBuf {
    crate::config::config_dir().join(REGISTRY_FILE)
}

fn registry_lock_path() -> PathBuf {
    crate::config::config_dir().join(REGISTRY_LOCK_FILE)
}

pub(crate) fn try_load() -> std::io::Result<FleetRegistry> {
    load_from_path(&registry_path())
}

pub(crate) fn load() -> FleetRegistry {
    match try_load() {
        Ok(registry) => registry,
        Err(err) => {
            warn!(path = %registry_path().display(), err = %err, "failed to load remote registry");
            FleetRegistry::default()
        }
    }
}

pub(crate) fn update<T>(
    mutation: impl FnOnce(&mut FleetRegistry) -> std::io::Result<T>,
) -> std::io::Result<(T, FleetRegistry)> {
    let lock_path = registry_lock_path();
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.lock()?;

    let mut registry = try_load()?;
    let result = mutation(&mut registry)?;
    save_to_path(&registry_path(), &registry)?;
    Ok((result, registry))
}

fn load_from_path(path: &Path) -> std::io::Result<FleetRegistry> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(FleetRegistry::default());
        }
        Err(err) => return Err(err),
    };
    let registry: FleetRegistry = toml::from_str(&content)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    if registry.version != REGISTRY_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported remote registry version {}; expected {REGISTRY_VERSION}",
                registry.version
            ),
        ));
    }
    Ok(registry)
}

fn save_to_path(path: &Path, registry: &FleetRegistry) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = toml::to_string_pretty(registry)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let tmp_path = path.with_extension("toml.tmp");
    std::fs::write(&tmp_path, content)?;
    #[cfg(windows)]
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    if let Err(err) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(err);
    }
    Ok(())
}

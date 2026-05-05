//! Forward-only config schema migration.
//!
//! Old config layouts are typed structs. Migration deserializes into the legacy
//! struct, moves field values into the new layout, and returns a clean [`Config`].
//!
//! The on-disk file is never rewritten by migration.
//!
//! ## When to bump the schema version
//!
//! Only when props are **renamed, moved, or removed**. New props with `#[serde(default)]`
//! don't need a bump.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use toml_edit::DocumentMut;

use super::schema::ModelProviderConfig;

pub const CURRENT_SCHEMA_VERSION: u32 = 2;

/// Top-level keys from V1 that are consumed by V1Compat during migration.
/// Used by the unknown-key detector to suppress false "unknown key" warnings.
pub const V1_LEGACY_KEYS: &[&str] = &[
    "api_key",
    "api_url",
    "api_path",
    "default_provider",
    "model_provider",
    "default_model",
    "model",
    "default_temperature",
    "provider_timeout_secs",
    "provider_max_tokens",
    "extra_headers",
    "model_providers",
    "model_routes",
    "embedding_routes",
    "channels_config",
];

/// Wraps the current Config with extra fields from V1 that no longer exist on Config.
/// `#[serde(flatten)]` lets Config consume its known fields; the old fields are
/// captured here.
#[derive(Deserialize)]
pub struct V1Compat {
    #[serde(flatten)]
    pub config: super::schema::Config,

    // ── Old top-level provider fields (removed in V2) ──
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    api_url: Option<String>,
    #[serde(default)]
    api_path: Option<String>,
    #[serde(default, alias = "model_provider")]
    default_provider: Option<String>,
    #[serde(default, alias = "model")]
    default_model: Option<String>,
    #[serde(default)]
    model_providers: HashMap<String, ModelProviderConfig>,
    #[serde(default)]
    default_temperature: Option<f64>,
    #[serde(default)]
    provider_timeout_secs: Option<u64>,
    #[serde(default)]
    provider_max_tokens: Option<u32>,
    #[serde(default)]
    extra_headers: Option<HashMap<String, String>>,
    #[serde(default)]
    model_routes: Vec<super::schema::ModelRouteConfig>,
    #[serde(default)]
    embedding_routes: Vec<super::schema::EmbeddingRouteConfig>,
}

impl V1Compat {
    /// Consume self, migrating old fields into the current Config layout.
    pub fn into_config(mut self) -> super::schema::Config {
        let from = self.config.schema_version;
        let needs_migration = from < CURRENT_SCHEMA_VERSION || self.has_legacy_fields();

        if !needs_migration {
            return self.config;
        }

        self.migrate_providers();
        self.config.schema_version = CURRENT_SCHEMA_VERSION;

        tracing::info!(
            from = from,
            to = CURRENT_SCHEMA_VERSION,
            "Config schema migrated in-memory from version {from} to {CURRENT_SCHEMA_VERSION}. \
             Run `zeroclaw config migrate` to update the file on disk.",
        );

        self.config
    }

    fn has_legacy_fields(&self) -> bool {
        self.api_key.is_some()
            || self.api_url.is_some()
            || self.api_path.is_some()
            || self.default_provider.is_some()
            || self.default_model.is_some()
            || !self.model_providers.is_empty()
            || self.default_temperature.is_some()
            || self.provider_timeout_secs.is_some()
            || self.provider_max_tokens.is_some()
            || self.extra_headers.as_ref().is_some_and(|h| !h.is_empty())
            || !self.model_routes.is_empty()
            || !self.embedding_routes.is_empty()
    }

    fn migrate_providers(&mut self) {
        // Only migrate legacy provider fields when they actually exist.
        // V2 configs that just need a schema_version bump should not get
        // a spurious "default" entry injected into providers.models.
        let has_legacy_provider_fields = self.default_provider.is_some()
            || !self.model_providers.is_empty()
            || self.api_key.is_some()
            || self.api_url.is_some()
            || self.api_path.is_some()
            || self.default_model.is_some()
            || self.default_temperature.is_some()
            || self.provider_timeout_secs.is_some()
            || self.provider_max_tokens.is_some()
            || self.extra_headers.as_ref().is_some_and(|h| !h.is_empty());

        if has_legacy_provider_fields {
            let fallback = self
                .default_provider
                .take()
                .unwrap_or_else(|| "default".into());

            // First, move old model_providers entries into providers.models.
            // These take precedence over top-level fields (more specific).
            for (key, profile) in std::mem::take(&mut self.model_providers) {
                self.config.providers.models.entry(key).or_insert(profile);
            }

            // Then fill gaps in the fallback entry from top-level fields.
            let entry = self
                .config
                .providers
                .models
                .entry(fallback.clone())
                .or_default();

            if entry.api_key.is_none() {
                entry.api_key = self.api_key.take();
            }
            if entry.base_url.is_none() {
                entry.base_url = self.api_url.take();
            }
            if entry.api_path.is_none() {
                entry.api_path = self.api_path.take();
            }
            if entry.model.is_none() {
                entry.model = self.default_model.take();
            }
            if entry.temperature.is_none() {
                entry.temperature = self.default_temperature.take();
            }
            if entry.timeout_secs.is_none() {
                entry.timeout_secs = self.provider_timeout_secs.take();
            }
            if entry.max_tokens.is_none() {
                entry.max_tokens = self.provider_max_tokens.take();
            }
            if entry.extra_headers.is_empty()
                && let Some(headers) = self.extra_headers.take()
            {
                entry.extra_headers = headers;
            }

            if self.config.providers.fallback.is_none() {
                self.config.providers.fallback = Some(fallback);
            }
        }

        // Move routing rules into providers (applies to both legacy and V2 configs).
        if self.config.providers.model_routes.is_empty() && !self.model_routes.is_empty() {
            self.config.providers.model_routes = std::mem::take(&mut self.model_routes);
        }
        if self.config.providers.embedding_routes.is_empty() && !self.embedding_routes.is_empty() {
            self.config.providers.embedding_routes = std::mem::take(&mut self.embedding_routes);
        }
    }
}

/// Pre-deserialization table migration for nested field changes that
/// `#[serde(flatten)]` cannot capture (e.g. removing a field from a nested
/// struct and moving its value elsewhere).
///
/// Called on the raw `toml::Table` before it is deserialized into `V1Compat`.
pub fn prepare_table(table: &mut toml::Table) {
    // Migrate channels_config.matrix.room_id → channels_config.matrix.allowed_rooms
    for key in &["channels_config", "channels"] {
        if let Some(toml::Value::Table(channels)) = table.get_mut(*key)
            && let Some(toml::Value::Table(matrix)) = channels.get_mut("matrix")
            && let Some(toml::Value::String(room_id)) = matrix.remove("room_id")
            && !room_id.is_empty()
        {
            let rooms = matrix
                .entry("allowed_rooms")
                .or_insert_with(|| toml::Value::Array(Vec::new()));
            if let toml::Value::Array(arr) = rooms {
                let already_present = arr.iter().any(|v| v.as_str() == Some(room_id.as_str()));
                if !already_present {
                    arr.push(toml::Value::String(room_id));
                }
            }
        }
    }

    // Migrate channels.slack.channel_id → channels.slack.channel_ids
    for key in &["channels_config", "channels"] {
        if let Some(toml::Value::Table(channels)) = table.get_mut(*key)
            && let Some(toml::Value::Table(slack)) = channels.get_mut("slack")
            && let Some(toml::Value::String(channel_id)) = slack.remove("channel_id")
            && !channel_id.is_empty()
            && channel_id != "*"
        {
            let ids = slack
                .entry("channel_ids")
                .or_insert_with(|| toml::Value::Array(Vec::new()));
            if let toml::Value::Array(arr) = ids {
                let already_present = arr.iter().any(|v| v.as_str() == Some(channel_id.as_str()));
                if !already_present {
                    arr.push(toml::Value::String(channel_id));
                }
            }
        }
    }

    // Rename legacy `channels_config` key to `channels`
    if table.contains_key("channels_config")
        && !table.contains_key("channels")
        && let Some(val) = table.remove("channels_config")
    {
        table.insert("channels".to_string(), val);
    }
}

// ── File-level migration (comment-preserving) ───────────────────────────────
//
// Uses V1Compat (the single source of migration logic) to compute the migrated
// Config, then syncs the original toml_edit document to match. The sync function
// is generic — it doesn't know field names, it just diffs two table structures.

/// Migrate a raw TOML config file, preserving comments and formatting.
/// Returns `None` if already at current version.
pub fn migrate_file(raw: &str) -> Result<Option<String>> {
    let mut table: toml::Table = toml::from_str(raw).context("Failed to parse config table")?;
    prepare_table(&mut table);
    let prepared = toml::to_string(&table).context("Failed to re-serialize prepared table")?;
    let compat: V1Compat = toml::from_str(&prepared).context("Failed to deserialize config")?;
    if compat.config.schema_version >= CURRENT_SCHEMA_VERSION && !compat.has_legacy_fields() {
        return Ok(None);
    }
    let config = compat.into_config();

    // Serialize the migrated config to get the target table structure.
    let target: toml::Table = toml::from_str(&toml::to_string(&config)?)
        .context("Failed to round-trip migrated config")?;

    // Sync the original document (with comments) to match the target.
    let mut doc: DocumentMut = raw.parse().context("Failed to parse config.toml")?;

    // Rename channels_config → channels in the document to preserve comments.
    if doc.contains_key("channels_config")
        && !doc.contains_key("channels")
        && let Some(val) = doc.remove("channels_config")
    {
        doc.insert("channels", val);
    }

    sync_table(doc.as_table_mut(), &target);

    Ok(Some(doc.to_string()))
}

/// Recursively sync a `toml_edit` table to match a target `toml::Table`.
/// - Keys absent from target are removed.
/// - Keys present in target but not in doc are inserted.
/// - Sub-tables are recursed. Leaf values are updated only if changed.
/// - Unchanged entries retain their original formatting and comments.
pub fn sync_table(doc: &mut toml_edit::Table, target: &toml::Table) {
    // Remove keys not in target.
    let to_remove: Vec<String> = doc
        .iter()
        .map(|(k, _)| k.to_string())
        .filter(|k| !target.contains_key(k))
        .collect();
    for key in &to_remove {
        doc.remove(key);
    }

    // Add or update keys from target.
    for (key, target_value) in target {
        match target_value {
            toml::Value::Table(sub_target) => {
                let entry = doc
                    .entry(key)
                    .or_insert(toml_edit::Item::Table(toml_edit::Table::new()));
                if let Some(sub_doc) = entry.as_table_mut() {
                    sync_table(sub_doc, sub_target);
                }
            }
            _ => {
                if let Some(existing) = doc.get(key).and_then(|i| i.as_value()) {
                    // Compare raw values, ignoring formatting/comments.
                    if values_equal(existing, target_value) {
                        continue;
                    }
                }
                doc.insert(key, toml_edit::value(toml_to_edit_value(target_value)));
            }
        }
    }
}

/// Compare a `toml_edit::Value` and a `toml::Value` for semantic equality,
/// ignoring formatting, whitespace, and comments.
fn values_equal(edit: &toml_edit::Value, toml: &toml::Value) -> bool {
    match (edit, toml) {
        (toml_edit::Value::String(a), toml::Value::String(b)) => a.value() == b,
        (toml_edit::Value::Integer(a), toml::Value::Integer(b)) => a.value() == b,
        (toml_edit::Value::Float(a), toml::Value::Float(b)) => (a.value() - b).abs() < f64::EPSILON,
        (toml_edit::Value::Boolean(a), toml::Value::Boolean(b)) => a.value() == b,
        (toml_edit::Value::Array(a), toml::Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(ae, be)| values_equal(ae, be))
        }
        _ => false,
    }
}

/// Convert a `toml::Value` to a `toml_edit::Value`.
fn toml_to_edit_value(v: &toml::Value) -> toml_edit::Value {
    match v {
        toml::Value::String(s) => toml_edit::Value::from(s.as_str()),
        toml::Value::Integer(i) => toml_edit::Value::from(*i),
        toml::Value::Float(f) => toml_edit::Value::from(*f),
        toml::Value::Boolean(b) => toml_edit::Value::from(*b),
        toml::Value::Array(arr) => {
            let mut a = toml_edit::Array::new();
            for item in arr {
                a.push(toml_to_edit_value(item));
            }
            toml_edit::Value::Array(a)
        }
        toml::Value::Datetime(dt) => dt
            .to_string()
            .parse()
            .unwrap_or_else(|_| toml_edit::Value::from(dt.to_string())),
        toml::Value::Table(tbl) => {
            // Tables inside arrays (e.g. `[[providers.model_routes]]`) need to be
            // converted to inline tables so they can be pushed into a toml_edit Array.
            let mut inline = toml_edit::InlineTable::new();
            for (k, v) in tbl {
                inline.insert(k, toml_to_edit_value(v));
            }
            toml_edit::Value::InlineTable(inline)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// V2-style config with explicit `providers.fallback` and `providers.models.X`
    /// should NOT get a spurious "default" entry injected during migration.
    #[test]
    fn v2_config_no_spurious_default_entry() {
        let toml_str = r#"
[providers]
fallback = "anthropic"

[providers.models.anthropic]
name = "anthropic"
model = "claude-sonnet-4-6"
temperature = 0.7
extended_thinking_budget = 32000
"#;
        let mut table: toml::Table = toml::from_str(toml_str).unwrap();
        prepare_table(&mut table);
        let prepared = toml::to_string(&table).unwrap();
        let compat: V1Compat = toml::from_str(&prepared).unwrap();
        let config = compat.into_config();

        assert!(
            !config.providers.models.contains_key("default"),
            "Migration should not create a spurious 'default' model entry for V2 configs"
        );
        assert_eq!(config.providers.fallback.as_deref(), Some("anthropic"));
        assert_eq!(
            config.providers.models["anthropic"].extended_thinking_budget,
            Some(32000)
        );
    }

    /// V2 config with `fallback = "default"` and `[providers.models.default]`
    /// should preserve the entry without corruption.
    #[test]
    fn v2_config_with_default_key_preserved() {
        let toml_str = r#"
[providers]
fallback = "default"

[providers.models.default]
name = "anthropic"
model = "claude-sonnet-4-6"
temperature = 0.7
extended_thinking_budget = 16000
"#;
        let mut table: toml::Table = toml::from_str(toml_str).unwrap();
        prepare_table(&mut table);
        let prepared = toml::to_string(&table).unwrap();
        let compat: V1Compat = toml::from_str(&prepared).unwrap();
        let config = compat.into_config();

        assert_eq!(config.providers.fallback.as_deref(), Some("default"));
        let entry = &config.providers.models["default"];
        assert_eq!(entry.name.as_deref(), Some("anthropic"));
        assert_eq!(entry.model.as_deref(), Some("claude-sonnet-4-6"));
        assert_eq!(entry.extended_thinking_budget, Some(16000));
    }

    /// Legacy V0/V1 config with top-level `default_provider` and `model`
    /// should migrate into `providers.models.<provider>`.
    #[test]
    fn legacy_v0_config_migrates_correctly() {
        let toml_str = r#"
default_provider = "anthropic"
model = "claude-sonnet-4-6"
api_key = "sk-test-123"
"#;
        let mut table: toml::Table = toml::from_str(toml_str).unwrap();
        prepare_table(&mut table);
        let prepared = toml::to_string(&table).unwrap();
        let compat: V1Compat = toml::from_str(&prepared).unwrap();
        let config = compat.into_config();

        assert_eq!(config.providers.fallback.as_deref(), Some("anthropic"));
        let entry = &config.providers.models["anthropic"];
        assert_eq!(entry.model.as_deref(), Some("claude-sonnet-4-6"));
        assert_eq!(entry.api_key.as_deref(), Some("sk-test-123"));
    }

    /// Legacy V0 config without `default_provider` should create a "default"
    /// fallback entry (the only case where "default" key is appropriate).
    #[test]
    fn legacy_v0_without_provider_creates_default_entry() {
        let toml_str = r#"
model = "gpt-4"
api_key = "sk-test-456"
"#;
        let mut table: toml::Table = toml::from_str(toml_str).unwrap();
        prepare_table(&mut table);
        let prepared = toml::to_string(&table).unwrap();
        let compat: V1Compat = toml::from_str(&prepared).unwrap();
        let config = compat.into_config();

        assert_eq!(config.providers.fallback.as_deref(), Some("default"));
        let entry = &config.providers.models["default"];
        assert_eq!(entry.model.as_deref(), Some("gpt-4"));
        assert_eq!(entry.api_key.as_deref(), Some("sk-test-456"));
    }

    /// Routing rules should still migrate even for V2 configs that only need
    /// schema_version bump (no legacy provider fields).
    #[test]
    fn routing_rules_migrate_without_legacy_provider_fields() {
        let toml_str = r#"
[providers]
fallback = "anthropic"

[providers.models.anthropic]
name = "anthropic"
model = "claude-sonnet-4-6"

[[model_routes]]
hint = "fast"
provider = "anthropic"
model = "claude-haiku-4-5"
"#;
        let mut table: toml::Table = toml::from_str(toml_str).unwrap();
        prepare_table(&mut table);
        let prepared = toml::to_string(&table).unwrap();
        let compat: V1Compat = toml::from_str(&prepared).unwrap();
        let config = compat.into_config();

        assert_eq!(config.providers.model_routes.len(), 1);
        assert_eq!(config.providers.model_routes[0].hint, "fast");
        assert!(!config.providers.models.contains_key("default"));
    }
}

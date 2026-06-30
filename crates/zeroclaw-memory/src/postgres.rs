//! PostgreSQL-backed memory implementation.
//!
//! Compiled in only when the crate is built with `--features memory-postgres`.
//! Selected at runtime by setting `[memory].backend = "postgres"` and supplying
//! `db_url` under `[storage.provider.config]`.
//!
//! Designed for multi-instance / serverless deployments where agents need to
//! share a single durable memory store with concurrent writes, decoupling
//! memory from a POSIX filesystem (EFS) — the SQLite backend cannot serve that
//! use case.
//!
//! This is a keyword (FTS) backend: recall ranks rows with PostgreSQL
//! `ts_rank_cd` over `key` and `content`. Per-agent scoping and pgvector
//! semantic recall are intentionally out of scope for this backend revision.

use super::traits::{ExportFilter, Memory, MemoryCategory, MemoryEntry};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls, Row};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

/// Maximum allowed connect timeout (seconds) to avoid unreasonable waits.
const POSTGRES_CONNECT_TIMEOUT_CAP_SECS: u64 = 300;

/// Drops its inner value on a background OS thread.
///
/// `postgres::Client::drop` calls `Runtime::block_on` internally to send a
/// clean-shutdown message. That panics if called from inside an existing Tokio
/// runtime. Wrapping the `Arc<Mutex<Client>>` in this type ensures the final
/// drop always happens on a plain OS thread.
struct DropOnThread<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> DropOnThread<T> {
    fn new(value: T) -> Self {
        Self(Some(value))
    }
    fn get(&self) -> &T {
        self.0.as_ref().expect("DropOnThread value already taken")
    }
}

impl<T: Send + 'static> Drop for DropOnThread<T> {
    fn drop(&mut self) {
        let Some(value) = self.0.take() else { return };
        // Wrap in ManuallyDrop so the value is NOT dropped on the current
        // thread if spawn fails — ManuallyDrop's own Drop is a no-op.
        let slot = std::mem::ManuallyDrop::new(value);
        if std::thread::Builder::new()
            .name("postgres-client-drop".to_string())
            .spawn(move || drop(std::mem::ManuallyDrop::into_inner(slot)))
            .is_err()
        {
            // The OS refused to spawn a thread. Intentionally leak the value
            // rather than drop it here: postgres::Client::drop calls
            // Runtime::block_on, which panics on a Tokio runtime thread. A
            // controlled leak is preferable to an unrecoverable panic.
            tracing::warn!(
                "postgres-client-drop thread spawn failed; leaking client to avoid nested-runtime panic"
            );
        }
    }
}

/// PostgreSQL-backed persistent memory (keyword/FTS recall).
pub struct PostgresMemory {
    client: DropOnThread<Arc<Mutex<Client>>>,
    qualified_table: String,
}

impl PostgresMemory {
    pub fn new(
        db_url: &str,
        schema: &str,
        table: &str,
        connect_timeout_secs: Option<u64>,
    ) -> Result<Self> {
        validate_identifier(schema, "storage schema")?;
        validate_identifier(table, "storage table")?;

        let schema_ident = quote_identifier(schema);
        let table_ident = quote_identifier(table);
        let qualified_table = format!("{schema_ident}.{table_ident}");

        let client = Self::initialize_client(
            db_url.to_string(),
            connect_timeout_secs,
            schema_ident,
            qualified_table.clone(),
        )?;

        Ok(Self {
            client: DropOnThread::new(Arc::new(Mutex::new(client))),
            qualified_table,
        })
    }

    fn initialize_client(
        db_url: String,
        connect_timeout_secs: Option<u64>,
        schema_ident: String,
        qualified_table: String,
    ) -> Result<Client> {
        let init_handle = std::thread::Builder::new()
            .name("postgres-memory-init".to_string())
            .spawn(move || -> Result<Client> {
                let mut config: postgres::Config = db_url
                    .parse()
                    .context("invalid PostgreSQL connection URL")?;

                if let Some(timeout_secs) = connect_timeout_secs {
                    let bounded = timeout_secs.min(POSTGRES_CONNECT_TIMEOUT_CAP_SECS);
                    config.connect_timeout(Duration::from_secs(bounded));
                }

                let mut client = config
                    .connect(NoTls)
                    .context("failed to connect to PostgreSQL memory backend")?;

                Self::init_schema(&mut client, &schema_ident, &qualified_table)?;
                Ok(client)
            })
            .context("failed to spawn PostgreSQL initializer thread")?;

        init_handle.join().map_err(|_| {
            tracing::error!("PostgreSQL initializer thread panicked");
            anyhow::Error::msg("PostgreSQL initializer thread panicked")
        })?
    }

    fn init_schema(client: &mut Client, schema_ident: &str, qualified_table: &str) -> Result<()> {
        client.batch_execute(&format!(
            "
            CREATE SCHEMA IF NOT EXISTS {schema_ident};

            CREATE TABLE IF NOT EXISTS {qualified_table} (
                id TEXT PRIMARY KEY,
                key TEXT NOT NULL,
                content TEXT NOT NULL,
                category TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                session_id TEXT,
                namespace TEXT NOT NULL DEFAULT 'default',
                importance DOUBLE PRECISION
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_namespace_key
                ON {qualified_table}(namespace, key);
            CREATE INDEX IF NOT EXISTS idx_memories_category ON {qualified_table}(category);
            CREATE INDEX IF NOT EXISTS idx_memories_session_id ON {qualified_table}(session_id);
            CREATE INDEX IF NOT EXISTS idx_memories_namespace ON {qualified_table}(namespace);
            CREATE INDEX IF NOT EXISTS idx_memories_updated_at ON {qualified_table}(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_memories_content_fts
                ON {qualified_table} USING gin(to_tsvector('simple', content));
            CREATE INDEX IF NOT EXISTS idx_memories_key_fts
                ON {qualified_table} USING gin(to_tsvector('simple', key));
            "
        ))?;
        Ok(())
    }

    fn category_to_str(category: &MemoryCategory) -> String {
        category.to_string()
    }

    fn parse_category(value: &str) -> MemoryCategory {
        match value {
            "core" => MemoryCategory::Core,
            "daily" => MemoryCategory::Daily,
            "conversation" => MemoryCategory::Conversation,
            other => MemoryCategory::Custom(other.to_string()),
        }
    }

    fn row_to_entry(row: &Row) -> Result<MemoryEntry> {
        // Named access so row_to_entry is immune to SELECT column reordering.
        let timestamp: DateTime<Utc> = row.get("created_at");

        Ok(MemoryEntry {
            id: row.get("id"),
            key: row.get("key"),
            content: row.get("content"),
            category: Self::parse_category(&row.get::<_, String>("category")),
            timestamp: timestamp.to_rfc3339(),
            session_id: row.get("session_id"),
            score: row.try_get("score").ok(),
            namespace: row
                .try_get::<_, String>("namespace")
                .unwrap_or_else(|_| "default".into()),
            importance: row.try_get("importance").ok(),
            superseded_by: None,
        })
    }
}

/// Run a blocking closure on a plain OS thread to avoid nested Tokio runtime
/// panics. The sync `postgres` crate internally calls `Runtime::block_on()`,
/// which conflicts with `tokio::task::spawn_blocking` threads that are still
/// associated with the Tokio runtime's blocking pool. Plain OS threads have no
/// runtime context, so the nested `block_on` succeeds.
async fn run_on_os_thread<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    std::thread::Builder::new()
        .name("postgres-memory-op".to_string())
        .spawn(move || {
            let _ = tx.send(f());
        })
        .context("failed to spawn PostgreSQL operation thread")?;

    rx.await.map_err(|_| {
        tracing::error!("PostgreSQL operation thread terminated unexpectedly");
        anyhow::Error::msg("PostgreSQL operation thread terminated unexpectedly")
    })?
}

fn validate_identifier(value: &str, field_name: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("{field_name} must not be empty");
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        anyhow::bail!("{field_name} must start with an ASCII letter or underscore; got '{value}'");
    }

    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        anyhow::bail!(
            "{field_name} can only contain ASCII letters, numbers, and underscores; got '{value}'"
        );
    }

    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{value}\"")
}

impl PostgresMemory {
    fn upsert(
        &self,
        key: &str,
        content: &str,
        category: MemoryCategory,
        session_id: Option<&str>,
        namespace: Option<&str>,
        importance: Option<f64>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let key = key.to_string();
        let content = content.to_string();
        let category = Self::category_to_str(&category);
        let sid = session_id.map(str::to_string);
        let namespace = namespace.unwrap_or("default").to_string();

        async move {
            run_on_os_thread(move || -> Result<()> {
                let now = Utc::now();
                let mut client = client.lock().expect("postgres client mutex poisoned");
                let stmt = format!(
                    "
                    INSERT INTO {qualified_table}
                        (id, key, content, category, created_at, updated_at, session_id, namespace, importance)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (namespace, key) DO UPDATE SET
                        content = EXCLUDED.content,
                        category = EXCLUDED.category,
                        updated_at = EXCLUDED.updated_at,
                        session_id = EXCLUDED.session_id,
                        importance = EXCLUDED.importance
                    "
                );
                let id = Uuid::new_v4().to_string();
                client.execute(
                    &stmt,
                    &[
                        &id, &key, &content, &category, &now, &now, &sid, &namespace, &importance,
                    ],
                )?;
                Ok(())
            })
            .await
        }
    }
}

#[async_trait]
impl Memory for PostgresMemory {
    fn name(&self) -> &str {
        "postgres"
    }

    async fn store(
        &self,
        key: &str,
        content: &str,
        category: MemoryCategory,
        session_id: Option<&str>,
    ) -> Result<()> {
        self.upsert(key, content, category, session_id, None, None)
            .await
    }

    async fn store_with_metadata(
        &self,
        key: &str,
        content: &str,
        category: MemoryCategory,
        session_id: Option<&str>,
        namespace: Option<&str>,
        importance: Option<f64>,
    ) -> Result<()> {
        self.upsert(key, content, category, session_id, namespace, importance)
            .await
    }

    async fn recall(
        &self,
        query: &str,
        limit: usize,
        session_id: Option<&str>,
        since: Option<&str>,
        until: Option<&str>,
    ) -> Result<Vec<MemoryEntry>> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let query = query.trim().to_string();
        let sid = session_id.map(str::to_string);
        let since_owned = since.map(str::to_string);
        let until_owned = until.map(str::to_string);

        run_on_os_thread(move || -> Result<Vec<MemoryEntry>> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let since_ref = since_owned.as_deref();
            let until_ref = until_owned.as_deref();

            let time_filter: &str = match (since_ref, until_ref) {
                (Some(_), Some(_)) => {
                    " AND created_at >= $4::TIMESTAMPTZ AND created_at <= $5::TIMESTAMPTZ"
                }
                (Some(_), None) => " AND created_at >= $4::TIMESTAMPTZ",
                (None, Some(_)) => " AND created_at <= $4::TIMESTAMPTZ",
                (None, None) => "",
            };

            let stmt = format!(
                "
                SELECT id, key, content, category, created_at, session_id, namespace, importance,
                       (
                         CASE WHEN to_tsvector('simple', key) @@ plainto_tsquery('simple', $1)
                           THEN ts_rank_cd(to_tsvector('simple', key), plainto_tsquery('simple', $1)) * 2.0
                           ELSE 0.0 END +
                         CASE WHEN to_tsvector('simple', content) @@ plainto_tsquery('simple', $1)
                           THEN ts_rank_cd(to_tsvector('simple', content), plainto_tsquery('simple', $1))
                           ELSE 0.0 END
                       )::double precision AS score
                FROM {qualified_table}
                WHERE ($2::TEXT IS NULL OR session_id = $2)
                  AND ($1 = '' OR to_tsvector('simple', key || ' ' || content) @@ plainto_tsquery('simple', $1))
                  {time_filter}
                ORDER BY score DESC, updated_at DESC
                LIMIT $3
                ",
            );

            #[allow(clippy::cast_possible_wrap)]
            let limit_i64 = limit as i64;

            let rows = match (since_ref, until_ref) {
                (Some(s), Some(u)) => client.query(&stmt, &[&query, &sid, &limit_i64, &s, &u])?,
                (Some(s), None) => client.query(&stmt, &[&query, &sid, &limit_i64, &s])?,
                (None, Some(u)) => client.query(&stmt, &[&query, &sid, &limit_i64, &u])?,
                (None, None) => client.query(&stmt, &[&query, &sid, &limit_i64])?,
            };
            rows.iter().map(Self::row_to_entry).collect()
        })
        .await
    }

    async fn get(&self, key: &str) -> Result<Option<MemoryEntry>> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let key = key.to_string();

        run_on_os_thread(move || -> Result<Option<MemoryEntry>> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!(
                "
                SELECT id, key, content, category, created_at, session_id, namespace, importance
                FROM {qualified_table}
                WHERE key = $1
                ORDER BY updated_at DESC
                LIMIT 1
                "
            );
            let row = client.query_opt(&stmt, &[&key])?;
            row.as_ref().map(Self::row_to_entry).transpose()
        })
        .await
    }

    async fn list(
        &self,
        category: Option<&MemoryCategory>,
        session_id: Option<&str>,
    ) -> Result<Vec<MemoryEntry>> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let category = category.map(Self::category_to_str);
        let sid = session_id.map(str::to_string);

        run_on_os_thread(move || -> Result<Vec<MemoryEntry>> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!(
                "
                SELECT id, key, content, category, created_at, session_id, namespace, importance
                FROM {qualified_table}
                WHERE ($1::TEXT IS NULL OR category = $1)
                  AND ($2::TEXT IS NULL OR session_id = $2)
                ORDER BY updated_at DESC
                "
            );
            let category_ref = category.as_deref();
            let session_ref = sid.as_deref();
            let rows = client.query(&stmt, &[&category_ref, &session_ref])?;
            rows.iter().map(Self::row_to_entry).collect()
        })
        .await
    }

    async fn forget(&self, key: &str) -> Result<bool> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let key = key.to_string();

        run_on_os_thread(move || -> Result<bool> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!("DELETE FROM {qualified_table} WHERE key = $1");
            Ok(client.execute(&stmt, &[&key])? > 0)
        })
        .await
    }

    async fn purge_namespace(&self, namespace: &str) -> Result<usize> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let namespace = namespace.to_string();

        run_on_os_thread(move || -> Result<usize> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!("DELETE FROM {qualified_table} WHERE namespace = $1");
            let deleted = client.execute(&stmt, &[&namespace])?;
            usize::try_from(deleted).context("PostgreSQL returned an oversized delete count")
        })
        .await
    }

    async fn purge_session(&self, session_id: &str) -> Result<usize> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let session_id = session_id.to_string();

        run_on_os_thread(move || -> Result<usize> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!("DELETE FROM {qualified_table} WHERE session_id = $1");
            let deleted = client.execute(&stmt, &[&session_id])?;
            usize::try_from(deleted).context("PostgreSQL returned an oversized delete count")
        })
        .await
    }

    async fn recall_namespaced(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
        session_id: Option<&str>,
        since: Option<&str>,
        until: Option<&str>,
    ) -> Result<Vec<MemoryEntry>> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let namespace = namespace.to_string();
        let query = query.trim().to_string();
        let sid = session_id.map(str::to_string);
        let since_owned = since.map(str::to_string);
        let until_owned = until.map(str::to_string);

        run_on_os_thread(move || -> Result<Vec<MemoryEntry>> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let since_ref = since_owned.as_deref();
            let until_ref = until_owned.as_deref();

            let time_filter: &str = match (since_ref, until_ref) {
                (Some(_), Some(_)) => {
                    " AND created_at >= $5::TIMESTAMPTZ AND created_at <= $6::TIMESTAMPTZ"
                }
                (Some(_), None) => " AND created_at >= $5::TIMESTAMPTZ",
                (None, Some(_)) => " AND created_at <= $5::TIMESTAMPTZ",
                (None, None) => "",
            };

            let stmt = format!(
                "
                SELECT id, key, content, category, created_at, session_id, namespace, importance,
                       (
                         CASE WHEN to_tsvector('simple', key) @@ plainto_tsquery('simple', $1)
                           THEN ts_rank_cd(to_tsvector('simple', key), plainto_tsquery('simple', $1)) * 2.0
                           ELSE 0.0 END +
                         CASE WHEN to_tsvector('simple', content) @@ plainto_tsquery('simple', $1)
                           THEN ts_rank_cd(to_tsvector('simple', content), plainto_tsquery('simple', $1))
                           ELSE 0.0 END
                       )::double precision AS score
                FROM {qualified_table}
                WHERE namespace = $4
                  AND ($2::TEXT IS NULL OR session_id = $2)
                  AND ($1 = '' OR to_tsvector('simple', key || ' ' || content) @@ plainto_tsquery('simple', $1))
                  {time_filter}
                ORDER BY score DESC, updated_at DESC
                LIMIT $3
                ",
            );

            #[allow(clippy::cast_possible_wrap)]
            let limit_i64 = limit as i64;

            let rows = match (since_ref, until_ref) {
                (Some(s), Some(u)) => {
                    client.query(&stmt, &[&query, &sid, &limit_i64, &namespace, &s, &u])?
                }
                (Some(s), None) => {
                    client.query(&stmt, &[&query, &sid, &limit_i64, &namespace, &s])?
                }
                (None, Some(u)) => {
                    client.query(&stmt, &[&query, &sid, &limit_i64, &namespace, &u])?
                }
                (None, None) => client.query(&stmt, &[&query, &sid, &limit_i64, &namespace])?,
            };
            rows.iter().map(Self::row_to_entry).collect()
        })
        .await
    }

    async fn export(&self, filter: &ExportFilter) -> Result<Vec<MemoryEntry>> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();
        let ns = filter.namespace.clone();
        let sid = filter.session_id.clone();
        let category = filter.category.as_ref().map(Self::category_to_str);
        let since = filter.since.clone();
        let until = filter.until.clone();

        run_on_os_thread(move || -> Result<Vec<MemoryEntry>> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!(
                "
                SELECT id, key, content, category, created_at, session_id, namespace, importance
                FROM {qualified_table}
                WHERE ($1::TEXT IS NULL OR namespace = $1)
                  AND ($2::TEXT IS NULL OR session_id = $2)
                  AND ($3::TEXT IS NULL OR category = $3)
                  AND ($4::TIMESTAMPTZ IS NULL OR created_at >= $4::TIMESTAMPTZ)
                  AND ($5::TIMESTAMPTZ IS NULL OR created_at <= $5::TIMESTAMPTZ)
                ORDER BY created_at ASC
                "
            );
            let rows = client.query(&stmt, &[&ns, &sid, &category, &since, &until])?;
            rows.iter().map(Self::row_to_entry).collect()
        })
        .await
    }

    async fn count(&self) -> Result<usize> {
        let client = self.client.get().clone();
        let qualified_table = self.qualified_table.clone();

        run_on_os_thread(move || -> Result<usize> {
            let mut client = client.lock().expect("postgres client mutex poisoned");
            let stmt = format!("SELECT COUNT(*) FROM {qualified_table}");
            let count: i64 = client.query_one(&stmt, &[])?.get(0);
            usize::try_from(count).context("PostgreSQL returned a negative memory count")
        })
        .await
    }

    async fn health_check(&self) -> bool {
        let client = self.client.get().clone();
        run_on_os_thread(move || {
            Ok(client
                .lock()
                .expect("postgres client mutex poisoned")
                .simple_query("SELECT 1")
                .is_ok())
        })
        .await
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers_pass_validation() {
        assert!(validate_identifier("public", "schema").is_ok());
        assert!(validate_identifier("_memories_01", "table").is_ok());
    }

    #[test]
    fn invalid_identifiers_are_rejected() {
        assert!(validate_identifier("", "schema").is_err());
        assert!(validate_identifier("1bad", "schema").is_err());
        assert!(validate_identifier("bad-name", "table").is_err());
    }

    #[test]
    fn parse_category_maps_known_and_custom_values() {
        assert_eq!(PostgresMemory::parse_category("core"), MemoryCategory::Core);
        assert_eq!(
            PostgresMemory::parse_category("daily"),
            MemoryCategory::Daily
        );
        assert_eq!(
            PostgresMemory::parse_category("conversation"),
            MemoryCategory::Conversation
        );
        assert_eq!(
            PostgresMemory::parse_category("custom_notes"),
            MemoryCategory::Custom("custom_notes".into())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn new_does_not_panic_inside_tokio_runtime() {
        // Regression for the nested-runtime path: PostgresMemory::new must run
        // connect + drop off the Tokio runtime thread, so an unreachable
        // endpoint returns an error rather than panicking.
        let outcome = std::panic::catch_unwind(|| {
            PostgresMemory::new(
                "postgres://zeroclaw:password@127.0.0.1:1/zeroclaw",
                "public",
                "memories",
                Some(1),
            )
        });

        assert!(outcome.is_ok(), "PostgresMemory::new should not panic");
        assert!(
            outcome.unwrap().is_err(),
            "PostgresMemory::new should return a connect error for an unreachable endpoint"
        );
    }
}

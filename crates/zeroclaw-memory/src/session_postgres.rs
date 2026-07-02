//! PostgreSQL-backed session persistence, schema-scoped per tenant.
//!
//! Compiled only with `--features memory-postgres`. Stores gateway WebSocket
//! sessions in a per-enterprise Postgres schema (`cx_<namespace>`) so sessions
//! survive pod restarts and one pod can densify many tenants — schema-isolated,
//! mirroring the memory backend. Two tables live inside the schema:
//! `sessions` (one row per message) and `session_metadata` (name, counts,
//! agent attribution, run state).
//!
//! ## Why an actor thread
//!
//! [`SessionBackend`] is a synchronous trait, but the gateway calls it from a
//! Tokio worker. The `postgres` crate's blocking client drives its own runtime
//! inside every `query()`/`execute()`, and starting a runtime from within the
//! Tokio runtime panics ("Cannot start a runtime from within a runtime").
//! `PostgresMemory` sidesteps this because its trait is async (it offloads to
//! `spawn_blocking`); a sync trait cannot `await`.
//!
//! So the client lives on one dedicated OS thread (never a Tokio worker). Each
//! trait method ships an owned closure over an mpsc channel and blocks on the
//! reply — blocking a `recv`, not starting a runtime, so no panic. The client
//! is also dropped on that thread when the channel closes, which is where
//! `Client::drop`'s own `block_on` is safe.

use crate::postgres::{connect_postgres, quote_identifier, validate_identifier};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::Client;
use std::sync::mpsc::{Sender, channel};
use std::time::Duration;
use zeroclaw_api::model_provider::ChatMessage;
use zeroclaw_infra::session_backend::{
    SessionBackend, SessionContext, SessionMetadata, SessionState, TimestampedMessage,
};

const CONNECT_TIMEOUT_CAP_SECS: u64 = 300;

type Job = Box<dyn FnOnce(&mut Client) + Send + 'static>;

/// PostgreSQL-backed session store bound to one schema.
pub struct PostgresSessionBackend {
    job_tx: Sender<Job>,
    qualified_sessions: String,
    qualified_meta: String,
}

impl PostgresSessionBackend {
    /// Open (and initialise) the session tables in `schema`.
    ///
    /// `schema` is validated as a bare identifier; the caller substitutes the
    /// tenant namespace into it (e.g. `cx_<namespace>`) before calling. Spawns
    /// the dedicated client thread and blocks until it has connected and created
    /// the tables (or returns the connect/init error).
    pub fn new(db_url: &str, schema: &str, connect_timeout_secs: Option<u64>) -> Result<Self> {
        validate_identifier(schema, "session schema")?;
        let schema_ident = quote_identifier(schema);
        let qualified_sessions = format!("{schema_ident}.sessions");
        let qualified_meta = format!("{schema_ident}.session_metadata");

        let (job_tx, job_rx) = channel::<Job>();
        let (ready_tx, ready_rx) = channel::<Result<()>>();

        let db_url = db_url.to_string();
        let (qs, qm) = (qualified_sessions.clone(), qualified_meta.clone());
        std::thread::Builder::new()
            .name("postgres-session".to_string())
            .spawn(move || {
                let connect = || -> Result<Client> {
                    let mut config: postgres::Config = db_url
                        .parse()
                        .context("invalid PostgreSQL connection URL")?;
                    if let Some(secs) = connect_timeout_secs {
                        config.connect_timeout(Duration::from_secs(
                            secs.min(CONNECT_TIMEOUT_CAP_SECS),
                        ));
                    }
                    let mut client = connect_postgres(&config)
                        .context("failed to connect to PostgreSQL session backend")?;
                    Self::init_schema(&mut client, &schema_ident, &qs, &qm)?;
                    Ok(client)
                };
                match connect() {
                    Ok(mut client) => {
                        let _ = ready_tx.send(Ok(()));
                        // Run jobs until the backend (and its Sender) is dropped;
                        // `client` is then dropped here, off the Tokio runtime.
                        while let Ok(job) = job_rx.recv() {
                            job(&mut client);
                        }
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                }
            })
            .context("failed to spawn PostgreSQL session thread")?;

        ready_rx
            .recv()
            .context("PostgreSQL session thread died during init")??;

        Ok(Self {
            job_tx,
            qualified_sessions,
            qualified_meta,
        })
    }

    fn init_schema(
        client: &mut Client,
        schema_ident: &str,
        qualified_sessions: &str,
        qualified_meta: &str,
    ) -> Result<()> {
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema_ident};

                 CREATE TABLE IF NOT EXISTS {qualified_sessions} (
                     id          BIGSERIAL PRIMARY KEY,
                     session_key TEXT NOT NULL,
                     role        TEXT NOT NULL,
                     content     TEXT NOT NULL,
                     created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                 );
                 CREATE INDEX IF NOT EXISTS idx_sessions_key_id
                     ON {qualified_sessions}(session_key, id);

                 CREATE TABLE IF NOT EXISTS {qualified_meta} (
                     session_key     TEXT PRIMARY KEY,
                     created_at      TIMESTAMPTZ NOT NULL,
                     last_activity   TIMESTAMPTZ NOT NULL,
                     message_count   INTEGER NOT NULL DEFAULT 0,
                     name            TEXT,
                     agent_alias     TEXT,
                     state           TEXT NOT NULL DEFAULT 'idle',
                     turn_id         TEXT,
                     turn_started_at TIMESTAMPTZ
                 );
                 CREATE INDEX IF NOT EXISTS idx_session_metadata_agent_alias
                     ON {qualified_meta}(agent_alias);"
            ))
            .context("failed to initialise session schema")
    }

    /// Run `f` against the client on its dedicated thread and block for the
    /// result. Panicking the caller (rather than defaulting) if the worker died
    /// would lose data silently; instead callers pass closures that fold errors
    /// into the returned value.
    fn with_client<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Client) -> T + Send + 'static,
        on_worker_dead: T,
    ) -> T {
        let (tx, rx) = channel::<T>();
        let job: Job = Box::new(move |c| {
            let _ = tx.send(f(c));
        });
        if self.job_tx.send(job).is_err() {
            return on_worker_dead;
        }
        rx.recv().unwrap_or(on_worker_dead)
    }

    fn meta_row_to_metadata(session_key: &str, row: &postgres::Row) -> SessionMetadata {
        let count: i32 = row.get("message_count");
        SessionMetadata {
            key: session_key.to_string(),
            name: row.get("name"),
            created_at: row.get("created_at"),
            last_activity: row.get("last_activity"),
            message_count: count.max(0) as usize,
            agent_alias: row.get("agent_alias"),
            channel_id: None,
            room_id: None,
            sender_id: None,
        }
    }
}

impl SessionBackend for PostgresSessionBackend {
    fn load(&self, session_key: &str) -> Vec<ChatMessage> {
        self.load_with_timestamps(session_key)
            .into_iter()
            .map(|t| t.message)
            .collect()
    }

    fn load_with_timestamps(&self, session_key: &str) -> Vec<TimestampedMessage> {
        let sql = format!(
            "SELECT role, content, created_at FROM {} WHERE session_key = $1 ORDER BY id",
            self.qualified_sessions
        );
        let key = session_key.to_string();
        self.with_client(
            move |c| match c.query(&sql, &[&key]) {
                Ok(rows) => rows
                    .iter()
                    .map(|row| TimestampedMessage {
                        message: ChatMessage {
                            role: row.get("role"),
                            content: row.get("content"),
                        },
                        created_at: Some(row.get::<_, DateTime<Utc>>("created_at")),
                    })
                    .collect(),
                Err(_) => Vec::new(),
            },
            Vec::new(),
        )
    }

    fn append(&self, session_key: &str, message: &ChatMessage) -> std::io::Result<()> {
        let now = Utc::now();
        let key = session_key.to_string();
        let role = message.role.clone();
        let content = message.content.clone();
        let insert = format!(
            "INSERT INTO {} (session_key, role, content, created_at) VALUES ($1, $2, $3, $4)",
            self.qualified_sessions
        );
        let upsert = format!(
            "INSERT INTO {meta} (session_key, created_at, last_activity, message_count)
             VALUES ($1, $2, $2, 1)
             ON CONFLICT (session_key) DO UPDATE SET
                 last_activity = EXCLUDED.last_activity,
                 message_count = {meta}.message_count + 1",
            meta = self.qualified_meta
        );
        self.with_client(
            move |c| {
                c.execute(&insert, &[&key, &role, &content, &now])
                    .map_err(std::io::Error::other)?;
                c.execute(&upsert, &[&key, &now])
                    .map_err(std::io::Error::other)?;
                Ok(())
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn remove_last(&self, session_key: &str) -> std::io::Result<bool> {
        let key = session_key.to_string();
        let del = format!(
            "DELETE FROM {sessions} WHERE id = (
                 SELECT id FROM {sessions} WHERE session_key = $1 ORDER BY id DESC LIMIT 1
             )",
            sessions = self.qualified_sessions
        );
        let dec = format!(
            "UPDATE {} SET message_count = GREATEST(message_count - 1, 0) WHERE session_key = $1",
            self.qualified_meta
        );
        self.with_client(
            move |c| {
                let n = c.execute(&del, &[&key]).map_err(std::io::Error::other)?;
                if n > 0 {
                    c.execute(&dec, &[&key]).map_err(std::io::Error::other)?;
                }
                Ok(n > 0)
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn list_sessions(&self) -> Vec<String> {
        let sql = format!(
            "SELECT session_key FROM {} ORDER BY last_activity DESC",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query(&sql, &[]) {
                Ok(rows) => rows.iter().map(|r| r.get("session_key")).collect(),
                Err(_) => Vec::new(),
            },
            Vec::new(),
        )
    }

    fn list_sessions_with_metadata(&self) -> Vec<SessionMetadata> {
        let sql = format!(
            "SELECT session_key, created_at, last_activity, message_count, name, agent_alias
             FROM {} ORDER BY last_activity DESC",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query(&sql, &[]) {
                Ok(rows) => rows
                    .iter()
                    .map(|row| {
                        let key: String = row.get("session_key");
                        Self::meta_row_to_metadata(&key, row)
                    })
                    .collect(),
                Err(_) => Vec::new(),
            },
            Vec::new(),
        )
    }

    fn clear_messages(&self, session_key: &str) -> std::io::Result<usize> {
        let key = session_key.to_string();
        let del = format!(
            "DELETE FROM {} WHERE session_key = $1",
            self.qualified_sessions
        );
        let reset = format!(
            "UPDATE {} SET message_count = 0 WHERE session_key = $1",
            self.qualified_meta
        );
        self.with_client(
            move |c| {
                let n = c.execute(&del, &[&key]).map_err(std::io::Error::other)?;
                c.execute(&reset, &[&key]).map_err(std::io::Error::other)?;
                Ok(n as usize)
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn delete_session(&self, session_key: &str) -> std::io::Result<bool> {
        let key = session_key.to_string();
        let del_msgs = format!(
            "DELETE FROM {} WHERE session_key = $1",
            self.qualified_sessions
        );
        let del_meta = format!("DELETE FROM {} WHERE session_key = $1", self.qualified_meta);
        self.with_client(
            move |c| {
                c.execute(&del_msgs, &[&key])
                    .map_err(std::io::Error::other)?;
                let n = c
                    .execute(&del_meta, &[&key])
                    .map_err(std::io::Error::other)?;
                Ok(n > 0)
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn session_exists(&self, session_key: &str) -> bool {
        let key = session_key.to_string();
        let sql = format!(
            "SELECT 1 FROM {} WHERE session_key = $1 LIMIT 1",
            self.qualified_meta
        );
        self.with_client(
            move |c| matches!(c.query_opt(&sql, &[&key]), Ok(Some(_))),
            false,
        )
    }

    fn set_session_name(&self, session_key: &str, name: &str) -> std::io::Result<()> {
        let now = Utc::now();
        let key = session_key.to_string();
        let name = name.to_string();
        let sql = format!(
            "INSERT INTO {} (session_key, created_at, last_activity, message_count, name)
             VALUES ($1, $2, $2, 0, $3)
             ON CONFLICT (session_key) DO UPDATE SET name = EXCLUDED.name",
            self.qualified_meta
        );
        self.with_client(
            move |c| {
                c.execute(&sql, &[&key, &now, &name])
                    .map_err(std::io::Error::other)
                    .map(|_| ())
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn get_session_name(&self, session_key: &str) -> std::io::Result<Option<String>> {
        let key = session_key.to_string();
        let sql = format!(
            "SELECT name FROM {} WHERE session_key = $1",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query_opt(&sql, &[&key]) {
                Ok(Some(row)) => Ok(row.get("name")),
                Ok(None) => Ok(None),
                Err(e) => Err(std::io::Error::other(e)),
            },
            Ok(None),
        )
    }

    fn set_session_agent_alias(&self, session_key: &str, agent_alias: &str) -> std::io::Result<()> {
        let now = Utc::now();
        let key = session_key.to_string();
        let alias = agent_alias.to_string();
        let sql = format!(
            "INSERT INTO {} (session_key, created_at, last_activity, message_count, agent_alias)
             VALUES ($1, $2, $2, 0, $3)
             ON CONFLICT (session_key) DO UPDATE SET agent_alias = EXCLUDED.agent_alias",
            self.qualified_meta
        );
        self.with_client(
            move |c| {
                c.execute(&sql, &[&key, &now, &alias])
                    .map_err(std::io::Error::other)
                    .map(|_| ())
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn get_session_agent_alias(&self, session_key: &str) -> std::io::Result<Option<String>> {
        let key = session_key.to_string();
        let sql = format!(
            "SELECT agent_alias FROM {} WHERE session_key = $1",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query_opt(&sql, &[&key]) {
                Ok(Some(row)) => Ok(row.get("agent_alias")),
                Ok(None) => Ok(None),
                Err(e) => Err(std::io::Error::other(e)),
            },
            Ok(None),
        )
    }

    fn set_session_context(
        &self,
        _session_key: &str,
        _context: SessionContext<'_>,
    ) -> std::io::Result<()> {
        // Channel routing columns are not tracked by the gateway-scoped Postgres
        // session store (WS sessions have no channel/room/sender). No-op.
        Ok(())
    }

    fn get_session_metadata(&self, session_key: &str) -> Option<SessionMetadata> {
        let key = session_key.to_string();
        let sql = format!(
            "SELECT session_key, created_at, last_activity, message_count, name, agent_alias
             FROM {} WHERE session_key = $1",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query_opt(&sql, &[&key]) {
                Ok(Some(row)) => Some(Self::meta_row_to_metadata(&key, &row)),
                _ => None,
            },
            None,
        )
    }

    fn set_session_state(
        &self,
        session_key: &str,
        state: &str,
        turn_id: Option<&str>,
    ) -> std::io::Result<()> {
        let now = Utc::now();
        let key = session_key.to_string();
        let state = state.to_string();
        let turn_id = turn_id.map(str::to_string);
        let started_at: Option<DateTime<Utc>> = (state == "running").then_some(now);
        let sql = format!(
            "UPDATE {} SET state = $1, turn_id = $2, turn_started_at = $3 WHERE session_key = $4",
            self.qualified_meta
        );
        self.with_client(
            move |c| {
                c.execute(&sql, &[&state, &turn_id, &started_at, &key])
                    .map_err(std::io::Error::other)
                    .map(|_| ())
            },
            Err(std::io::Error::other("session worker unavailable")),
        )
    }

    fn get_session_state(&self, session_key: &str) -> std::io::Result<Option<SessionState>> {
        let key = session_key.to_string();
        let sql = format!(
            "SELECT state, turn_id, turn_started_at FROM {} WHERE session_key = $1",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query_opt(&sql, &[&key]) {
                Ok(Some(row)) => Ok(Some(SessionState {
                    state: row.get("state"),
                    turn_id: row.get("turn_id"),
                    turn_started_at: row.get("turn_started_at"),
                })),
                Ok(None) => Ok(None),
                Err(e) => Err(std::io::Error::other(e)),
            },
            Ok(None),
        )
    }

    fn list_running_sessions(&self) -> Vec<SessionMetadata> {
        let sql = format!(
            "SELECT session_key, created_at, last_activity, message_count, name, agent_alias
             FROM {} WHERE state = 'running' ORDER BY turn_started_at DESC NULLS LAST",
            self.qualified_meta
        );
        self.with_client(
            move |c| match c.query(&sql, &[]) {
                Ok(rows) => rows
                    .iter()
                    .map(|row| {
                        let key: String = row.get("session_key");
                        Self::meta_row_to_metadata(&key, row)
                    })
                    .collect(),
                Err(_) => Vec::new(),
            },
            Vec::new(),
        )
    }
}

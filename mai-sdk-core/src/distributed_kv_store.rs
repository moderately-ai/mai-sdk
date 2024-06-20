use crate::{network::PeerId, service::Startable, task_queue::TaskId};
use async_channel::Sender;
use libp2p::futures::TryStreamExt;
use sqlx::Row;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::RwLock;

pub type TaskAssignments = Arc<RwLock<HashMap<TaskId, PeerId>>>;

pub type RemoteTasks<Task> = Arc<RwLock<HashMap<TaskId, Task>>>;

pub type OwnedTasks<Task, TaskOutput> = Arc<RwLock<HashMap<TaskId, (Task, Sender<TaskOutput>)>>>;

use anyhow::{bail, Result};
use slog::{error, info, Logger};

use crate::event_bridge::EventBridge;

#[derive(Clone)]
pub struct DistributedKVStore {
    logger: Logger,
    bridge: EventBridge,
    connection_pool: sqlx::SqlitePool,
}

impl Debug for DistributedKVStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedKVStore").finish()
    }
}

type Value = Vec<u8>;

#[derive(Debug, Clone)]
pub struct SetEvent {
    pub key: String,
    pub value: Value,
    pub result: async_channel::Sender<Result<()>>,
}

#[derive(Debug, Clone)]
pub struct GetEvent {
    pub key: String,
    pub result: async_channel::Sender<Result<Option<Value>>>,
}

impl DistributedKVStore {
    pub async fn new(logger: &Logger, bridge: &EventBridge, sqlite_path: &str) -> Self {
        info!(logger, "connecting to sqlite database"; "path" => sqlite_path);
        let connection_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(sqlite_path)
            .await
            .unwrap();

        // initialize the kv table
        // TODO: convert to use sqlx::migrate! macro
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value BLOB
            )
            "#,
        )
        .execute(&connection_pool)
        .await
        .unwrap();

        DistributedKVStore {
            logger: logger.clone(),
            bridge: bridge.clone(),
            connection_pool,
        }
    }

    pub async fn get(&self, key: String) -> Result<Option<Value>> {
        // First check the local store, then remote store
        if let Some(value) = {
            let mut rows = sqlx::query("SELECT value FROM kv WHERE key = ?")
                .bind(key.clone())
                .fetch(&self.connection_pool);
            let row = rows.try_next().await?;
            if let Some(row) = row {
                let value: Vec<u8> = row.get(0);
                Some(value)
            } else {
                None
            }
        } {
            return Ok(Some(value));
        };

        // Send a get event to the bridge then await for a response
        let (tx, rx) = async_channel::bounded(1);
        if let Err(e) = self
            .bridge
            .publish(crate::event_bridge::PublishEvents::GetEvent(GetEvent {
                key,
                result: tx.clone(),
            }))
            .await
        {
            error!(self.logger, "Failed to send get event"; "error" => ?e);
            bail!(e)
        };
        rx.recv().await?
    }

    pub async fn set(&self, key: String, value: Value) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)")
            .bind(key.clone())
            .bind(value.clone())
            .execute(&self.connection_pool)
            .await?;
        Ok(())
    }
}

impl Startable for DistributedKVStore {
    /// The kv store loops continuously to ensure the local store is in sync with the remote store
    async fn start(&self) -> Result<()> {
        let mut keys_synced: Vec<String> = vec![];
        loop {
            let keys: Vec<String> = sqlx::query(
                r#"
                SELECT key FROM kv
                "#,
            )
            .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
            .fetch_all(&self.connection_pool)
            .await?;

            let keys_to_publish = keys
                .iter()
                .filter(|key| !keys_synced.contains(key))
                .cloned()
                .collect::<Vec<String>>();

            for key in keys_to_publish {
                let value = self.get(key.clone()).await?.unwrap();
                if let Ok(_) = self
                    .bridge
                    .publish(crate::event_bridge::PublishEvents::SetEvent(SetEvent {
                        key: key.clone(),
                        value,
                        result: async_channel::bounded(1).0,
                    }))
                    .await
                {
                    keys_synced.push(key.clone());
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bridge::EventBridge;

    #[tokio::test]
    async fn test_set_get() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        let bridge = EventBridge::new(&logger);
        let store = DistributedKVStore::new(&logger, &bridge, ":memory:").await;

        let key = "key".to_string();
        let value = vec![1, 2, 3];

        store.set(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key.clone()).await.unwrap();
        assert_eq!(result, Some(value));
    }
}

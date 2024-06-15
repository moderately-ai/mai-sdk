use crate::{network::PeerId, task_queue::TaskId};
use async_channel::Sender;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::{Mutex, RwLock};

pub type TaskAssignments = Arc<RwLock<HashMap<TaskId, PeerId>>>;

pub type RemoteTasks<Task> = Arc<RwLock<HashMap<TaskId, Task>>>;

pub type OwnedTasks<Task, TaskOutput> = Arc<RwLock<HashMap<TaskId, (Task, Sender<TaskOutput>)>>>;

use anyhow::{bail, Result};
use slog::{error, info, warn, Logger};

use crate::event_bridge::EventBridge;

#[derive(Clone)]
pub struct DistributedKVStore {
    logger: Logger,
    bridge: EventBridge,
    connection: Arc<Mutex<sqlite::Connection>>,
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
    pub fn new(logger: &Logger, bridge: &EventBridge, persist: bool) -> Self {
        let connection = if persist {
            info!(logger, "Using persistent database");
            sqlite::open("mai_sdk.sqlite").unwrap()
        } else {
            warn!(logger, "Using in-memory database");
            sqlite::open(":memory:").unwrap()
        };
        let query = "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB)";
        connection.execute(query).unwrap();
        DistributedKVStore {
            logger: logger.clone(),
            bridge: bridge.clone(),
            connection: Arc::new(Mutex::new(connection)),
        }
    }

    pub async fn get(&self, key: String) -> Result<Option<Value>> {
        // First check the local store, then remote store
        if let Some(value) = {
            let query = "SELECT value FROM kv WHERE key = ?";
            let connection = self.connection.lock().await;
            let mut statement = connection.prepare(query)?;
            statement.bind((1, key.as_str()))?;
            if let sqlite::State::Row = statement.next()? {
                Some(statement.read(0)?)
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
        // Store locally then send a set event to the bridge
        let connection = self.connection.lock().await;
        let query = "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)";
        let mut statement = connection.prepare(query)?;
        statement.bind((1, key.as_str()))?;
        statement.bind((2, value.as_slice()))?;
        statement.next()?;

        Ok(())
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
        let store = DistributedKVStore::new(&logger, &bridge, false);

        let key = "key".to_string();
        let value = vec![1, 2, 3];

        store.set(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key.clone()).await.unwrap();
        assert_eq!(result, Some(value));
    }
}

use crate::{handler::Startable, network::PeerId, task_queue::TaskId};
use async_channel::Sender;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub type TaskAssignments = Arc<RwLock<HashMap<TaskId, PeerId>>>;

pub type RemoteTasks<Task> = Arc<RwLock<HashMap<TaskId, Task>>>;

pub type OwnedTasks<Task, TaskOutput> = Arc<RwLock<HashMap<TaskId, (Task, Sender<TaskOutput>)>>>;

use anyhow::{bail, Result};
use slog::{error, Logger};

use crate::bridge::EventBridge;

#[derive(Clone, Debug)]
pub struct DistributedKVStore {
    logger: Logger,
    bridge: EventBridge,
    local_store: Arc<RwLock<HashMap<String, Value>>>,
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
    pub fn new(logger: &Logger, bridge: &EventBridge) -> Self {
        let local_store = Arc::new(RwLock::new(HashMap::new()));
        DistributedKVStore {
            logger: logger.clone(),
            bridge: bridge.clone(),
            local_store,
        }
    }

    pub async fn get(&self, key: String) -> Result<Option<Value>> {
        // First check the local store, then remote store
        if let Some(value) = self.local_store.read().await.get(&key) {
            return Ok(Some(value.clone()));
        }

        // Send a get event to the bridge then await for a response
        let (tx, rx) = async_channel::bounded(1);
        if let Err(e) = self
            .bridge
            .publish(crate::bridge::PublishEvents::GetEvent(GetEvent {
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
        self.local_store
            .write()
            .await
            .insert(key.clone(), value.clone());

        Ok(())
    }
}

impl Startable for DistributedKVStore {
    async fn start(&self) -> Result<()> {
        loop {
            // Publish them to the network
            for (key, value) in self.local_store.read().await.iter() {
                let (tx, rx) = async_channel::bounded(1);
                if let Err(e) = self
                    .bridge
                    .publish(crate::bridge::PublishEvents::SetEvent(SetEvent {
                        key: key.clone(),
                        value: value.clone(),
                        result: tx.clone(),
                    }))
                    .await
                {
                    error!(self.logger, "Failed to send set event"; "error" => ?e);
                    bail!(e)
                };
                if let Err(e) = rx.recv().await {
                    error!(self.logger, "Failed to sync key"; "error" => ?e);
                    bail!(e)
                }
            }

            // Sleep for a bit
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::EventBridge;

    #[tokio::test]
    async fn test_set_get() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        let bridge = EventBridge::new(&logger);
        let store = DistributedKVStore::new(&logger, &bridge);

        let key = "key".to_string();
        let value = vec![1, 2, 3];

        store.set(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key.clone()).await.unwrap();
        assert_eq!(result, Some(value));
    }
}

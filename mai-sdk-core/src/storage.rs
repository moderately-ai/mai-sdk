use crate::{network::PeerId, tasks::TaskId};
use async_channel::Sender;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub type TaskAssignments = Arc<RwLock<HashMap<TaskId, PeerId>>>;

pub type RemoteTasks<Task> = Arc<RwLock<HashMap<TaskId, Task>>>;

pub type OwnedTasks<Task, TaskOutput> = Arc<RwLock<HashMap<TaskId, (Task, Sender<TaskOutput>)>>>;

use anyhow::{bail, Result};
use slog::{error, info, Logger};

use crate::bridge::EventBridge;

#[derive(Clone, Debug)]
pub struct DistributedKVStore {
    logger: Logger,
    bridge: EventBridge,
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
    pub fn new(logger: Logger, bridge: EventBridge) -> Self {
        DistributedKVStore { logger, bridge }
    }

    pub async fn get(&self, key: String) -> Result<Option<Value>> {
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
        let (tx, rx) = async_channel::bounded(1);
        if let Err(e) = self
            .bridge
            .publish(crate::bridge::PublishEvents::SetEvent(SetEvent {
                key: key.clone(),
                value,
                result: tx.clone(),
            }))
            .await
        {
            error!(self.logger, "Failed to send set event"; "error" => ?e);
            bail!(e)
        };
        if let Err(e) = rx.recv().await {
            error!(self.logger, "Failed to set key"; "error" => ?e);
            bail!(e)
        } else {
            info!(self.logger, "Set key"; "key" => key);
        };
        Ok(())
    }
}

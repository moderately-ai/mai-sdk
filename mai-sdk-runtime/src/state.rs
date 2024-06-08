use anyhow::Result;
use mai_sdk_core::{
    network::P2PNetwork,
    storage::DistributedKVStore,
    task_queue::{DistributedTaskQueue, Runnable, TaskId},
};
use mai_sdk_plugins::ollama::{OllamaPluginState, OllamaPluginTask, OllamaPluginTaskOutput};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};

use crate::system_monitor::SystemMonitor;

/// Collection of the variants of tasks that can be executed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Task {
    Ollama(OllamaPluginTask),
}

impl Runnable<TaskOutput, RunnableState> for Task {
    fn id(&self) -> TaskId {
        match self {
            Task::Ollama(task) => task.id(),
        }
    }

    async fn run(&self, state: RunnableState) -> Result<TaskOutput> {
        info!(state.logger, "running task"; "task_id" => format!("{:?}", self.id()));
        match self {
            Task::Ollama(ollama_task) => Ok(TaskOutput::Ollama(
                ollama_task.run(state.ollama_state).await?,
            )),
        }
    }
}

/// Collection of the variants of task outputs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskOutput {
    Ollama(OllamaPluginTaskOutput),
}

/// State that is injected into the task's execution environment
#[derive(Debug, Clone)]
pub struct RunnableState {
    logger: Logger,
    ollama_state: OllamaPluginState,
}

/// RuntimeState
/// Holds various components of the runtime
pub struct RuntimeState {
    /// System Monitor
    /// Periodically checks the system's status and publishes it to the network
    system_monitor: SystemMonitor,

    /// P2P Network
    /// An instance of the P2P network, enabling communication between nodes
    p2p_network: P2PNetwork,

    /// Distributed Task Queue
    /// A distributed task queue that allows for the execution of tasks across the network
    distributed_task_queue: DistributedTaskQueue<Task, TaskOutput, RunnableState>,

    /// Distributed KV Store
    /// A distributed key-value store that allows for the storage of data across the network
    distributed_kv_store: DistributedKVStore,
}

impl RuntimeState {
    /// Create a new instance of the runtime state
    pub fn new(
        system_monitor: &SystemMonitor,
        p2p_network: &P2PNetwork,
        distributed_task_queue: &DistributedTaskQueue<Task, TaskOutput, RunnableState>,
        distributed_kv_store: &DistributedKVStore,
    ) -> Self {
        RuntimeState {
            system_monitor: system_monitor.clone(),
            p2p_network: p2p_network.clone(),
            distributed_task_queue: distributed_task_queue.clone(),
            distributed_kv_store: distributed_kv_store.clone(),
        }
    }

    pub fn system_monitor(&self) -> &SystemMonitor {
        &self.system_monitor
    }

    pub fn p2p_network(&self) -> &P2PNetwork {
        &self.p2p_network
    }

    pub fn distributed_task_queue(&self) -> &DistributedTaskQueue<Task, TaskOutput, RunnableState> {
        &self.distributed_task_queue
    }

    pub fn distributed_kv_store(&self) -> &DistributedKVStore {
        &self.distributed_kv_store
    }
}

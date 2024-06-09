use anyhow::Result;
use mai_sdk_core::{
    bridge::EventBridge,
    handler::Startable,
    network::P2PNetwork,
    storage::DistributedKVStore,
    task_queue::{DistributedTaskQueue, Runnable, TaskId},
};
use mai_sdk_plugins::{
    ollama::{OllamaPluginState, OllamaPluginTask, OllamaPluginTaskOutput},
    transcription::{
        TranscriptionPluginState, TranscriptionPluginTaskTranscribe,
        TranscriptionPluginTaskTranscribeOutput,
    },
};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};

use crate::system_monitor::SystemMonitor;

/// Collection of the variants of tasks that can be executed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Task {
    Ollama(OllamaPluginTask),
    Transcription(TranscriptionPluginTaskTranscribe),
}

/// Collection of the variants of task outputs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskOutput {
    Ollama(OllamaPluginTaskOutput),
    Transcription(TranscriptionPluginTaskTranscribeOutput),
}

impl Runnable<TaskOutput, RunnableState> for Task {
    fn id(&self) -> TaskId {
        match self {
            Task::Ollama(task) => task.id(),
            Task::Transcription(task) => task.id(),
        }
    }

    async fn run(&self, state: RunnableState) -> Result<TaskOutput> {
        info!(state.logger, "running task"; "task_id" => format!("{:?}", self.id()));
        match self {
            Task::Ollama(ollama_task) => Ok(TaskOutput::Ollama(
                ollama_task.run(state.ollama_state).await?,
            )),
            Task::Transcription(transcription_task) => transcription_task
                .run(state.transcription_state)
                .await
                .map(TaskOutput::Transcription),
        }
    }
}

/// State that is injected into the task's execution environment
#[derive(Debug, Clone)]
pub struct RunnableState {
    logger: Logger,
    ollama_state: OllamaPluginState,
    transcription_state: TranscriptionPluginState,
}

impl RunnableState {
    /// Create a new instance of the runnable state
    pub fn new(logger: &Logger) -> Self {
        let ollama_state = OllamaPluginState::new(logger);
        let transcription_state = TranscriptionPluginState::new(logger);
        RunnableState {
            logger: logger.clone(),
            ollama_state: ollama_state.clone(),
            transcription_state: transcription_state.clone(),
        }
    }
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

    /// Event Bridge
    /// A bridge that allows for the communication of events across the network
    event_bridge: EventBridge,
}

impl RuntimeState {
    /// Create a new instance of the runtime state
    pub fn new(
        system_monitor: &SystemMonitor,
        p2p_network: &P2PNetwork,
        distributed_task_queue: &DistributedTaskQueue<Task, TaskOutput, RunnableState>,
        distributed_kv_store: &DistributedKVStore,
        event_bridge: &EventBridge,
    ) -> Self {
        RuntimeState {
            system_monitor: system_monitor.clone(),
            p2p_network: p2p_network.clone(),
            distributed_task_queue: distributed_task_queue.clone(),
            distributed_kv_store: distributed_kv_store.clone(),
            event_bridge: event_bridge.clone(),
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

impl Startable for RuntimeState {
    async fn start(&self) -> Result<()> {
        tokio::select! {
            _ = {
                let system_monitor = self.system_monitor.clone();
                tokio::spawn(async move {
                    system_monitor.start().await
                })
            } => {},
            _ = {
                let p2p_network = self.p2p_network.clone();
                tokio::spawn(async move {
                    p2p_network.start().await
                })
            } => {},
            _ = {
                let distributed_task_queue = self.distributed_task_queue.clone();
                tokio::spawn(async move {
                    distributed_task_queue.start().await
                })
            } => {},
            _ = {
                let event_bridge = self.event_bridge.clone();
                tokio::spawn(async move {
                    event_bridge.start().await
                })
            } => {},
        }
        Ok(())
    }
}

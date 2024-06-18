use anyhow::Result;
use mai_sdk_core::{
    distributed_kv_store::DistributedKVStore,
    event_bridge::EventBridge,
    handler::Startable,
    network::{Network, P2PNetwork, P2PNetworkConfig},
    task_queue::{DistributedTaskQueue, Lifecycle, Runnable, TaskId},
};
use mai_sdk_plugins::{
    text_generation::{
        TextGenerationPluginState, TextGenerationPluginTask, TextGenerationPluginTaskOutput,
    },
    transcription::{
        TranscriptionPluginState, TranscriptionPluginTaskTranscribe,
        TranscriptionPluginTaskTranscribeOutput,
    },
    web_scraping::{
        WebScrapingPluginState, WebScrapingPluginTaskScrape, WebScrapingPluginTaskScrapeOutput,
    },
};
use serde::{Deserialize, Serialize};
use slog::{debug, error, info, warn, Logger};

use crate::system_monitor::SystemMonitor;

/// Collection of the variants of tasks that can be executed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Task {
    TextGeneration(TextGenerationPluginTask),
    Transcribe(TranscriptionPluginTaskTranscribe),
    Scrape(WebScrapingPluginTaskScrape),
}

/// Collection of the variants of task outputs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskOutput {
    TextGeneration(TextGenerationPluginTaskOutput),
    Transcription(TranscriptionPluginTaskTranscribeOutput),
    Scrape(WebScrapingPluginTaskScrapeOutput),
}

impl Runnable<TaskOutput, RunnableState> for Task {
    fn id(&self) -> TaskId {
        match self {
            Task::TextGeneration(task) => task.id(),
            Task::Transcribe(task) => task.id(),
            Task::Scrape(task) => task.id(),
        }
    }

    async fn run(&self, state: RunnableState) -> Result<TaskOutput> {
        info!(state.logger, "running task"; "task_id" => format!("{:?}", self.id()));
        match self {
            Task::TextGeneration(ollama_task) => Ok(TaskOutput::TextGeneration(
                ollama_task.run(state.ollama_state).await?,
            )),
            Task::Transcribe(transcription_task) => transcription_task
                .run(state.transcription_state)
                .await
                .map(TaskOutput::Transcription),
            Task::Scrape(web_scraping_task) => Ok(TaskOutput::Scrape(
                web_scraping_task.run(state.web_scraping_state).await?,
            )),
        }
    }
}

impl Lifecycle<RunnableState> for Task {
    async fn pre_submit(&self, state: &RunnableState) -> Result<()> {
        match self {
            Task::TextGeneration(ollama_task) => ollama_task.pre_submit(&state.ollama_state).await,
            Task::Transcribe(transcription_task) => {
                transcription_task
                    .pre_submit(&state.transcription_state)
                    .await
            }
            Task::Scrape(web_scraping_task) => {
                web_scraping_task
                    .pre_submit(&state.web_scraping_state)
                    .await
            }
        }
    }
}

/// State that is injected into the task's execution environment
#[derive(Debug, Clone)]
pub struct RunnableState {
    logger: Logger,
    ollama_state: TextGenerationPluginState,
    transcription_state: TranscriptionPluginState,
    web_scraping_state: WebScrapingPluginState,
}

impl RunnableState {
    /// Create a new instance of the runnable state
    pub fn new(logger: &Logger, distributed_kv_store: &DistributedKVStore) -> Self {
        RunnableState {
            logger: logger.clone(),
            ollama_state: TextGenerationPluginState::new(logger),
            transcription_state: TranscriptionPluginState::new(logger, distributed_kv_store),
            web_scraping_state: WebScrapingPluginState::new(logger),
        }
    }
}

/// RuntimeState
/// Holds various components of the runtime
#[derive(Clone, Debug)]
pub struct RuntimeState {
    /// Logger
    /// A logger instance that is used to log messages
    logger: Logger,

    /// System Monitor
    /// Periodically checks the system's status and publishes it to the network
    system_monitor: SystemMonitor,

    /// P2P Network
    /// An instance of the P2P network, enabling communication between nodes
    p2p_network: P2PNetwork,

    /// Distributed Task Queue
    /// A distributed task queue that allows for the execution of tasks across the network
    distributed_task_queue: Option<DistributedTaskQueue<Task, TaskOutput, RunnableState>>,

    /// Event Bridge
    /// A bridge that allows for the communication of events across the network
    event_bridge: EventBridge,
}

pub struct RuntimeStateArgs {
    pub logger: Logger,
    pub listen_addrs: Vec<String>,
    pub bootstrap_addrs: Vec<String>,
    pub ping_interval: std::time::Duration,
    pub gossipsub_heartbeat_interval: std::time::Duration,
    pub psk: Option<String>,
}

impl RuntimeState {
    /// Create a new instance of the runtime state
    pub async fn new_worker(args: RuntimeStateArgs) -> Self {
        let event_bridge = EventBridge::new(&args.logger);
        let p2p_network = P2PNetwork::new(P2PNetworkConfig {
            logger: args.logger.clone(),
            bridge: event_bridge.clone(),
            listen_addrs: args
                .listen_addrs
                .iter()
                .map(|a| a.parse().unwrap())
                .collect(),
            bootstrap_addrs: args
                .bootstrap_addrs
                .iter()
                .map(|a| a.parse().unwrap())
                .collect(),
            ping_interval: args.ping_interval,
            gossipsub_heartbeat_interval: args.gossipsub_heartbeat_interval,
            psk: args.psk,
        });
        let distributed_kv_store = DistributedKVStore::new(&args.logger, &event_bridge, true).await;
        let distributed_task_queue = DistributedTaskQueue::new(
            &args.logger,
            &p2p_network.peer_id(),
            &RunnableState::new(&args.logger, &distributed_kv_store),
            &event_bridge,
            &distributed_kv_store,
        );
        let system_monitor =
            SystemMonitor::new(&args.logger, &p2p_network.peer_id(), &distributed_kv_store);
        RuntimeState {
            logger: args.logger,
            system_monitor: system_monitor.clone(),
            p2p_network: p2p_network.clone(),
            distributed_task_queue: Some(distributed_task_queue.clone()),
            event_bridge: event_bridge.clone(),
        }
    }

    /// Get the system monitor
    pub fn system_monitor(&self) -> &SystemMonitor {
        &self.system_monitor
    }

    /// Get the P2P network
    pub fn p2p_network(&self) -> &P2PNetwork {
        &self.p2p_network
    }

    /// Get the distributed task queue
    pub fn distributed_task_queue(
        &self,
    ) -> &Option<DistributedTaskQueue<Task, TaskOutput, RunnableState>> {
        &self.distributed_task_queue
    }

    /// Get the event bridge
    pub fn event_bridge(&self) -> &EventBridge {
        &self.event_bridge
    }
}

impl Startable for RuntimeState {
    async fn start(&self) -> Result<()> {
        // if anything exits or restarts, we restart the full loop
        loop {
            tokio::select! {
                _ = {
                    let system_monitor = self.system_monitor.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        if let Err(e) = system_monitor.start().await {
                            error!(logger, "system monitor crashed: {:?}", e);
                            debug!(logger, "trace: {:?}", e.backtrace());
                        } else {
                            warn!(logger, "System monitor exited");
                        }
                    })
                } => {},
                _ = {
                    let p2p_network = self.p2p_network.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        if let Err(e) = p2p_network.start().await {
                            error!(logger, "p2p network crashed: {:?}", e);
                            debug!(logger, "trace: {:?}", e.backtrace());
                        } else {
                            warn!(logger, "P2P network exited");
                        }
                    })
                } => {},
                _ = {
                    let distributed_task_queue = self.distributed_task_queue.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        if let Some(distributed_task_queue) = distributed_task_queue {
                            if let Err(e) = distributed_task_queue.start().await {
                                error!(logger, "distributed task queue crashed: {:?}", e);
                                debug!(logger, "trace: {:?}", e.backtrace());
                            } else {
                                warn!(logger, "Distributed task queue exited");
                            }
                        } else {
                            std::future::pending().await
                        }
                    })
                } => {},
                _ = {
                    let event_bridge = self.event_bridge.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        if let Err(e) = event_bridge.start().await {
                            error!(logger, "event bridge crashed: {:?}", e);
                            debug!(logger, "trace: {:?}", e.backtrace());
                        } else {
                            warn!(logger, "Event bridge exited");
                        }
                    })
                } => {},
            }
        }
    }
}

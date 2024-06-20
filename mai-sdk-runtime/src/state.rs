use crate::{Task, TaskOutput};
use anyhow::Result;
use mai_sdk_core::{
    distributed_kv_store::DistributedKVStore,
    event_bridge::EventBridge,
    network::{Network, P2PNetwork, P2PNetworkConfig},
    service::Startable,
    system_monitor::SystemMonitor,
    task_queue::DistributedTaskQueue,
};
use mai_sdk_plugins::{
    text_generation::TextGenerationPluginState, transcription::TranscriptionPluginState,
    web_scraping::WebScrapingPluginState,
};
use slog::{debug, error, warn, Logger};

/// Application state required to inject into tasks
#[derive(Debug, Clone)]
pub struct RunnableState {
    pub(crate) logger: Logger,
    pub(crate) distributed_kv_store: DistributedKVStore,
}

impl From<RunnableState> for TextGenerationPluginState {
    fn from(state: RunnableState) -> Self {
        TextGenerationPluginState::new(&state.logger)
    }
}

impl From<RunnableState> for TranscriptionPluginState {
    fn from(state: RunnableState) -> Self {
        TranscriptionPluginState::new(&state.logger, &state.distributed_kv_store)
    }
}

impl From<RunnableState> for WebScrapingPluginState {
    fn from(state: RunnableState) -> Self {
        WebScrapingPluginState::new(&state.logger)
    }
}

impl RunnableState {
    /// Create a new instance of the runnable state
    pub fn new(logger: &Logger, distributed_kv_store: &DistributedKVStore) -> Self {
        RunnableState {
            logger: logger.clone(),
            distributed_kv_store: distributed_kv_store.clone(),
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

    /// Distributed KV Store
    /// A distributed key-value store that allows for the storage of data across the network
    distributed_kv_store: DistributedKVStore,

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
    pub sqlite_path: String,
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
        let distributed_kv_store =
            DistributedKVStore::new(&args.logger, &event_bridge, &args.sqlite_path).await;
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
            distributed_kv_store: distributed_kv_store.clone(),
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
                    let logger = self.logger.clone();
                    let distributed_kv_store = self.distributed_kv_store.clone();
                    tokio::spawn(async move {
                        if let Err(e) = distributed_kv_store.start().await {
                            error!(logger, "distributed kv store crashed: {:?}", e);
                            debug!(logger, "trace: {:?}", e.backtrace());
                        } else {
                            warn!(logger, "Distributed KV store exited");
                        }
                    })
                } => {}
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

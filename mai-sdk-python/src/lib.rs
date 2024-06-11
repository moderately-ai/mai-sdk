use std::time::Duration;

use anyhow::Result;
use mai_sdk_core::{
    bridge::EventBridge,
    handler::Startable,
    network::{Network, P2PNetwork, P2PNetworkConfig},
    storage::DistributedKVStore,
    task_queue::DistributedTaskQueue,
};
use mai_sdk_runtime::{
    state::{RunnableState, RuntimeState},
    system_monitor::SystemMonitor,
};
use pyo3::prelude::*;
use slog::o;

#[pyclass]
#[derive(Debug, Clone)]
pub struct PythonRuntimeArgs {
    pub gossip_listen_addrs: Vec<String>,
    pub bootstrap_addrs: Vec<String>,
    pub gossipsub_heartbeat_interval: u64,
    pub ping_interval: u64,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PythonRuntime {
    state: RuntimeState,
}

#[pymethods]
impl PythonRuntime {
    /// Starts the runtime
    /// if blocking is true, the function will block until the runtime is stopped, otherwise the runtime will start in a new thread
    fn start(&self, blocking: bool) -> Result<(), PyErr> {
        if blocking {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(self.state.start()).unwrap();
        } else {
            let state = self.state.clone();
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(state.start())
                    .unwrap();
            });
        }
        Ok(())
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn start_worker(args: PythonRuntimeArgs) -> PyResult<PythonRuntime> {
    let logger = slog::Logger::root(slog::Discard, o!());
    let event_bridge = EventBridge::new(logger.clone());
    let p2p_network = P2PNetwork::new(P2PNetworkConfig {
        listen_addrs: args
            .gossip_listen_addrs
            .iter()
            .map(|a| a.parse().unwrap())
            .collect(),
        ping_interval: Duration::from_secs(args.ping_interval),
        gossipsub_heartbeat_interval: Duration::from_secs(args.gossipsub_heartbeat_interval),
        logger: logger.clone(),
        bridge: event_bridge.clone(),
        bootstrap_addrs: args
            .bootstrap_addrs
            .iter()
            .map(|a| a.parse().unwrap())
            .collect(),
        psk: None,
    });
    let runnable_state: RunnableState = RunnableState::new(&logger);
    let distributed_task_queue = DistributedTaskQueue::new(
        &logger,
        &p2p_network.peer_id(),
        &runnable_state,
        &event_bridge,
    );
    let distributed_kv_store = DistributedKVStore::new(&logger, &event_bridge);
    let system_monitor = SystemMonitor::new(&logger, &p2p_network.peer_id(), &distributed_kv_store);
    let state = RuntimeState::new_worker(
        &system_monitor,
        &p2p_network,
        &distributed_task_queue,
        &event_bridge,
    );
    Ok(PythonRuntime { state })
}

/// A Python module implemented in Rust.
#[pymodule]
fn mai_sdk_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_worker, m)?)?;
    Ok(())
}

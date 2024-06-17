use std::time::Duration;

use anyhow::Result;
use mai_sdk_core::handler::Startable;
use mai_sdk_runtime::state::{RuntimeState, RuntimeStateArgs};
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
pub struct PythonRuntime {
    rt: tokio::runtime::Runtime,
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let state = rt.block_on(RuntimeState::new_worker(RuntimeStateArgs {
        logger,
        listen_addrs: args.gossip_listen_addrs,
        bootstrap_addrs: args.bootstrap_addrs,
        gossipsub_heartbeat_interval: Duration::from_secs(args.gossipsub_heartbeat_interval),
        ping_interval: Duration::from_secs(args.ping_interval),
        psk: None,
    }));
    Ok(PythonRuntime { state, rt })
}

/// A Python module implemented in Rust.
#[pymodule]
fn mai_sdk_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_worker, m)?)?;
    Ok(())
}

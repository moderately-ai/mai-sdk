use anyhow::Result;
use gethostname::gethostname;
use mai_sdk_core::{handler::Startable, network::PeerId, storage::DistributedKVStore};
use serde::{Deserialize, Serialize};
use slog::{error, info, Logger};

/// SystemStatus
/// Holds information about the system's status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemStatus {
    /// OS Variant
    os_variant: String,

    /// Host Name
    host_name: String,

    /// CPU count, this is the number of logical cores
    cpu_capacity: usize,

    /// Total system memory (MB)
    system_memory: usize,

    /// A measure of the GPU's compute capability (GFLOPS)
    gpu_capacity: usize,

    /// A measure of the GPU's memory capacity (MB)
    gpu_memory: usize,
}

/// SystemMonitor
/// Starts a process that periodically checks the system's status and publishes it to the network
#[derive(Clone, Debug)]
pub struct SystemMonitor {
    logger: Logger,
    local_peer_id: PeerId,
    kv_store: DistributedKVStore,
    polling_interval: u64,
}

impl SystemMonitor {
    /// Create a new instance of the system monitor
    pub fn new(logger: &Logger, local_peer_id: &PeerId, kv_store: &DistributedKVStore) -> Self {
        SystemMonitor {
            logger: logger.clone(),
            local_peer_id: local_peer_id.clone(),
            kv_store: kv_store.clone(),
            polling_interval: 60,
        }
    }

    /// Set the polling interval
    pub fn with_polling_interval(mut self, polling_interval: u64) -> Self {
        self.polling_interval = polling_interval;
        self
    }
}

impl Startable for SystemMonitor {
    /// Start the system monitor
    async fn start(&self) -> Result<()> {
        loop {
            info!(self.logger, "checking system status");

            // Query cpu count
            let cpu_capacity = candle_core::utils::get_num_threads();

            // Query system memory
            let system_memory = {
                // get system memory in bytes
                let system_memory = sysinfo::System::new_all().total_memory() as usize;

                // convert to MB
                system_memory / 1024 / 1024
            };

            // Query GPU compute
            let (gpu_capacity, gpu_memory) = {
                if candle_core::utils::cuda_is_available() {
                    let nvml = nvml_wrapper::Nvml::init()?;
                    let device = nvml.device_by_index(0)?;
                    let memory_info = device.memory_info()?;
                    let compute_capability = device.num_cores()? as usize;
                    let gpu_memory = memory_info.total as usize / 1024 / 1024;
                    (compute_capability, gpu_memory)
                } else if candle_core::utils::metal_is_available() {
                    // NOTE: this could use a more thorough scaling since the M series laptops are very performant compared to regular CPUs
                    (cpu_capacity, system_memory)
                } else {
                    (0, 0)
                }
            };

            // Random host info
            let os_variant = std::env::consts::OS.to_string();
            let host_name = gethostname().to_string_lossy().to_string();

            // Construct the system status
            let system_status = SystemStatus {
                os_variant,
                host_name,
                cpu_capacity,
                system_memory,
                gpu_capacity,
                gpu_memory,
            };
            info!(self.logger, "system status: {:?}", system_status);

            // Store the system status in the kv store
            let key = format!("systemStatus/{}", self.local_peer_id);
            let value = bincode::serialize(&system_status)?;
            if let Err(e) = self.kv_store.set(key, value).await {
                error!(
                    self.logger,
                    "failed to set system status in kv store: {:?}", e
                );
            };

            // Sleep for polling period
            tokio::time::sleep(std::time::Duration::from_secs(self.polling_interval)).await;
        }
    }
}

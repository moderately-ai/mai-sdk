use mai_sdk_core::distributed_kv_store::DistributedKVStore;
use slog::Logger;

/// The runtime state of the transcription plugin.
#[derive(Debug, Clone)]
pub struct TranscriptionPluginState {
    pub logger: Logger,
    pub distributed_kv_store: DistributedKVStore,
}

impl TranscriptionPluginState {
    pub fn new(logger: &Logger, distributed_kv_store: &DistributedKVStore) -> Self {
        Self {
            logger: logger.clone(),
            distributed_kv_store: distributed_kv_store.clone(),
        }
    }
}

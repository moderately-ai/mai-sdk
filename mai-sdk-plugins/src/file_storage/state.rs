use mai_sdk_core::distributed_kv_store::DistributedKVStore;

#[derive(Debug, Clone)]
pub struct FileStorageState {
    pub(crate) distributed_kv_store: DistributedKVStore,
}

impl FileStorageState {
    pub fn new(distributed_kv_store: DistributedKVStore) -> Self {
        Self {
            distributed_kv_store,
        }
    }
}
